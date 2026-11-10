"""A polygonize of a noisy tile holds a few bytes for each region, not the regions themselves.

The earlier path read every region from GDAL's in-memory layer into a list (about 440 bytes a region, more than 500 MB for
a 1,600 px noise tile). The engine now has GDAL write to a file on local disk and hands the features out one at a time, so
what stays is GDAL's own bookkeeping (about 30 bytes a region measured). A fresh process runs a warm-up pass first, as
``test_engine_leaks`` does, then measures the peak RSS growth over a pass that makes over a million regions.
Needs the payload's osgeo (the container with GBX_NATIVE_ROOT).
"""

import json
import subprocess
import sys

import pytest

from databricks.labs.gbx.engine import run

# bytes of peak RSS growth a region may cost; the in-memory layer cost 440 and the stream measured about 30
BYTES_PER_REGION = 120
SMALL, BIG = 700, 1600

CODE = f"""
import json
import numpy as np
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import l3


def rss_mb():
    with open("/proc/self/status") as f:
        return int(next(l for l in f if l.startswith("VmHWM")).split()[1]) / 1024


def tile(n):
    z = np.random.RandomState(1).randint(0, 4, size=(n, n)).astype("uint8")
    return oracles._tile(oracles._gtiff(z[np.newaxis], "EPSG:3857", nodata=None, dtype="uint8"))


def consume(handler, t):
    return sum(1 for _ in handler.eval(t))


handler = l3.udtf("gbx_rst_polygonize").func()
consume(handler, tile(300))  # warm-up: imports, GDAL's caches
small, big = tile({SMALL}), tile({BIG})
before = rss_mb()
n_small = consume(handler, small)
mid = rss_mb()
n_big = consume(handler, big)
after = rss_mb()
import glob, tempfile
left = lambda: len(glob.glob(tempfile.gettempdir() + "/gbx-spec-*"))
full = left()
gen = handler.eval(small)
next(gen); next(gen)
gen.close()  # a read cut short, as a LIMIT or a failed task does
print(json.dumps(dict(before=before, mid=mid, after=after, n_small=n_small, n_big=n_big,
                      left_after_full=full, left_after_cut=left())))
"""


@pytest.fixture(scope="module")
def measured():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    out = subprocess.run(
        [sys.executable, "-c", CODE], capture_output=True, text=True, timeout=900
    )
    assert out.returncode == 0, out.stderr[-3000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_the_peak_growth_is_a_few_bytes_for_each_region(measured):
    m = measured
    assert m["n_big"] > 1_000_000, "the tile must make over a million regions"
    grown = (m["after"] - m["before"]) * 2**20
    assert grown / m["n_big"] < BYTES_PER_REGION, (
        f"{grown / m['n_big']:.0f} bytes a region over {m['n_big']} regions "
        f"(growth {m['after'] - m['before']:.0f} MB)"
    )


def test_the_file_is_removed_after_a_full_read_and_after_a_read_cut_short(measured):
    assert measured["left_after_full"] == 0
    assert measured["left_after_cut"] == 0
