"""The engine leaves nothing behind: 1,000-tile passes on local[8] leave /vsimem/gbx-spec empty in every worker,
and once the workers are warm more passes do not grow their memory. Needs the payload's osgeo (the container with
GBX_NATIVE_ROOT).
"""

import os

import pytest

from databricks.labs.gbx import register
from databricks.labs.gbx.engine import run
from databricks.labs.gbx.manifest import TILE_SQL

N, PARTS = 1000, 8


def test_reading_an_engine_input_does_not_import_the_light_function_module():
    """A worker's first engine row must not import pyrx.functions (about 10,000 lines: over 0.3 s on every new
    worker, paid even for a NULL row the batch wrapper routes to the row-wise UDF). `_inputs` on a materialized
    tile reads no pixels and needs no osgeo, so this runs on the host."""
    import subprocess
    import sys

    code = (
        "import sys\n"
        "from databricks.labs.gbx import manifest\n"
        "from databricks.labs.gbx.engine import l3\n"
        "fn = manifest.load().functions['gbx_rst_slope']\n"
        "tile = {'cellid': 7, 'raster': b'II*\\x00' + bytes(60), 'path': None, 'metadata': {}}\n"
        "rasters, passthrough, cellid = l3._inputs(fn, tile)\n"
        "assert rasters and cellid == 7\n"
        "assert l3._inputs(fn, None) == (None, None, None)\n"
        "print('databricks.labs.gbx.pyrx.functions' in sys.modules)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=300
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.strip() == "False", out.stdout + out.stderr[-2000:]


@pytest.fixture(scope="module")
def tiles(spark8):
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    from databricks.labs.gbx.bench import levels_forms

    tile = levels_forms._se_dem_tile(
        256, 1
    )  # 256 KiB decoded, noisy: a leaked input or dataset shows
    return (
        spark8.createDataFrame([(tile,)] * N, f"tile {TILE_SQL}")
        .repartition(PARTS)
        .cache()
    )


def _worker_state(spark):
    """{pid: (files under /vsimem/gbx-spec, RSS in kB)} of the Python workers, by a probe on many partitions."""
    import pandas as pd
    from pyspark.sql import functions as F

    def probe(it):
        from databricks.labs.gbx.engine import run as engine_run

        left = engine_run._gdal().ReadDirRecursive(engine_run.PREFIX) or []
        with open("/proc/self/status") as fh:
            rss = next(int(ln.split()[1]) for ln in fh if ln.startswith("VmRSS:"))
        for _ in it:
            pass
        yield pd.DataFrame({"pid": [os.getpid()], "left": [len(left)], "rss_kb": [rss]})

    rows = (
        spark.range(64 * PARTS)
        .repartition(64 * PARTS)
        .mapInPandas(probe, "pid long, left long, rss_kb long")
        .groupBy("pid")
        .agg(F.max("left").alias("left"), F.max("rss_kb").alias("rss_kb"))
        .collect()
    )
    return {r["pid"]: (r["left"], r["rss_kb"]) for r in rows}


def _count(spark, df, level, variant):
    register(spark, only=["gbx_rst_slope"], _levels={"gbx_rst_slope": (level, variant)})
    df.createOrReplaceTempView("_gbx_leak")
    (r,) = spark.sql(
        "SELECT count(*) AS n, count(s.raster) AS ok FROM "
        "(SELECT gbx_rst_slope(tile) AS s FROM _gbx_leak)"
    ).collect()
    return r["n"], r["ok"]


def test_a_thousand_tiles_through_c1_leave_no_vsimem_file_and_flat_workers(
    spark8, tiles
):
    # A worker's first real engine batch loads GDAL's drivers and the light tier (about 90 MiB, once), and a batch
    # holds its rows' buffers while it runs (up to about 30 MiB): three passes warm every worker up; five more must
    # not raise the workers' peak beyond that. A leak of an input (about 256 KiB a call here) would add some
    # 150 MiB over them.
    for _ in range(3):
        assert _count(spark8, tiles, "L3", "engine-c1") == (N, N)
    warm = _worker_state(spark8)
    for _ in range(5):
        assert _count(spark8, tiles, "L3", "engine-c1") == (N, N)
    later = _worker_state(spark8)
    states = list(warm.values()) + list(later.values())
    assert states and all(left == 0 for left, _ in states)
    grown = (
        max(r for _, r in later.values()) - max(r for _, r in warm.values())
    ) // 1024
    assert grown <= 40, f"the workers' peak RSS grew {grown} MiB over 5,000 more tiles"
