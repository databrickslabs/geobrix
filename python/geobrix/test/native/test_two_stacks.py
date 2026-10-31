"""Two GDAL stacks in one interpreter: the dev image's PyPI geospatial wheels beside the payload's.

The native dev image keeps the PyPI wheels of rasterio's neighbours (pyogrio, pyproj, shapely: each carries its own
libgdal, libproj or libgeos in a ``*.libs`` directory) on disk, and the payload's build of every one of them is first
on ``sys.path`` from interpreter start (a ``.pth`` file; ``GBX_NATIVE_PTH=0`` turns it off). Three legs, each a fresh
interpreter that opens the oracle raster with rasterio and layer 0 of the oracle GeoPackage with ``geopandas.read_file``
and ``pyogrio.raw.read``:

- ``gbx-first``: geobrix is imported first, then geopandas;
- ``pypi-first``: geopandas, pyogrio, shapely and pyproj are imported before rasterio and geobrix;
- ``pypi-first-no-pth``: the same with the startup activation off, the real two-stack case. geopandas loads its PyPI
  shapely, pyproj and pyogrio first, each on its own libraries, and rasterio then loads the payload's GDAL through the
  loader path (the image registers the payload's library directory with ldconfig).

What holds:

- every leg exits 0, reports no ``DATABASE.LAYOUT.VERSION`` (a PROJ that reads another PROJ's database), and reads the
  same values (pixel sum, feature counts);
- with the startup activation (the first two legs) no PyPI wheel's ``*.libs`` library is mapped and every binding is
  the payload's.

The third leg's libraries are recorded (``record_property`` and the test output), not asserted.

Needs Linux, a payload (``GBX_NATIVE_ROOT``: ``/opt/gbx-native`` in the native dev image, or
``build/env-wheel/env6/payload``), geopandas and pyogrio, and the oracle data under ``/Volumes`` (the dev container
mounts ``sample-data/Volumes`` there). In the native dev image::

    gbx:test:python --path python/geobrix/test/native/test_two_stacks.py
"""

import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

VOLUMES = Path("/Volumes/main/default/test-data/geobrix-examples")
TIF = VOLUMES / "london" / "sentinel2" / "london_sentinel2_red.tif"
GPKG = VOLUMES / "nyc" / "geopackage" / "nyc_complete.gpkg"
SIZE = (388, 385)  # the oracle raster: width x height
FOREIGN = ("rasterio.libs", "pyogrio.libs", "pyproj.libs", "shapely.libs")

LEG = r"""
import json, os, sys

order = os.environ["TWO_STACKS_ORDER"]
if order == "gbx-first":
    import databricks.labs.gbx  # the package, so its startup rules have run
    import geopandas, pyogrio.raw
    import rasterio
else:
    import geopandas, pyogrio.raw, shapely, pyproj  # the PyPI names first
    import rasterio
    import databricks.labs.gbx

out = {"order": order}
with rasterio.open(os.environ["TWO_STACKS_TIF"]) as ds:
    out["size"] = [ds.width, ds.height]
    out["pixel_sum"] = int(ds.read(1).astype("int64").sum())
layer = pyogrio.list_layers(os.environ["TWO_STACKS_GPKG"])[0][0]
out["geopandas_features"] = len(geopandas.read_file(os.environ["TWO_STACKS_GPKG"], layer=layer))
out["pyogrio_features"] = len(pyogrio.raw.read(os.environ["TWO_STACKS_GPKG"], layer=layer)[2])  # (meta, fids, geometry, fields)
foreign = set()
for line in open("/proc/self/maps"):
    foreign.update(p for p in line.split("/") if p in FOREIGN_LIBS)
out["pypi_libs"] = sorted(foreign)
try:
    from databricks.labs.gbx._native import _selfcheck

    out["stack"] = _selfcheck.stack()
except Exception as exc:  # recorded, not asserted, in the third leg
    out["stack_error"] = type(exc).__name__ + ": " + str(exc)[:300]
print("TWO_STACKS " + json.dumps(out, default=str))
"""
LEG = LEG.replace("FOREIGN_LIBS", repr(FOREIGN))

LEGS = {
    "gbx-first": ("gbx-first", {}),
    "pypi-first": ("pypi-first", {}),
    "pypi-first-no-pth": ("pypi-first", {"GBX_NATIVE_PTH": "0"}),
}


def _payload_ready():
    if not sys.platform.startswith("linux"):
        return "Linux only"
    root = os.environ.get("GBX_NATIVE_ROOT")
    if not root or not any(Path(root, "_native").glob("*/MANIFEST.json")):
        return "no payload (GBX_NATIVE_ROOT)"
    for name in ("geopandas", "pyogrio", "rasterio"):
        # found, not imported: this process must stay as it is
        if importlib.util.find_spec(name) is None:
            return f"{name} is not installed"
    if not (TIF.is_file() and GPKG.is_file()):
        return "the oracle data is not under /Volumes"
    return None


pytestmark = pytest.mark.skipif(
    _payload_ready() is not None, reason=str(_payload_ready())
)


@pytest.fixture(scope="module")
def legs():
    """{leg: {"returncode", "output", "result"}}: each leg is a fresh interpreter."""
    out = {}
    for name, (order, extra) in LEGS.items():
        env = dict(
            os.environ,
            TWO_STACKS_ORDER=order,
            TWO_STACKS_TIF=str(TIF),
            TWO_STACKS_GPKG=str(GPKG),
        )
        env.pop("GBX_NATIVE_PTH", None)
        env.pop("GBX_NATIVE_DISABLE", None)
        env.update(extra)
        res = subprocess.run(
            [sys.executable, "-c", LEG], capture_output=True, text=True, env=env
        )
        marks = [ln for ln in res.stdout.splitlines() if ln.startswith("TWO_STACKS ")]
        out[name] = {
            "returncode": res.returncode,
            "output": res.stdout + res.stderr,
            "result": json.loads(marks[-1].split(" ", 1)[1]) if marks else None,
        }
    return out


def test_each_leg_reads_the_same_values(legs, record_property):
    for name, leg in legs.items():
        assert leg["returncode"] == 0, (name, leg["output"][-2000:])
        assert "DATABASE.LAYOUT.VERSION" not in leg["output"], name
        assert leg["result"] is not None, (name, leg["output"][-2000:])
    results = {name: leg["result"] for name, leg in legs.items()}
    for name, r in results.items():
        assert tuple(r["size"]) == SIZE, name
        assert r["geopandas_features"] == r["pyogrio_features"] > 0, name
    assert len({r["pixel_sum"] for r in results.values()}) == 1, {
        n: r["pixel_sum"] for n, r in results.items()
    }
    assert len({r["geopandas_features"] for r in results.values()}) == 1
    # the two-stack leg's libraries and stack, for the decision record
    third = results["pypi-first-no-pth"]
    stack = json.dumps(third.get("stack", third.get("stack_error")), default=str)
    record_property("two_stack_pypi_libs", ",".join(third["pypi_libs"]))
    record_property("two_stack_stack", stack)
    print("TWO_STACKS pypi-first-no-pth pypi_libs=", third["pypi_libs"])
    print("TWO_STACKS pypi-first-no-pth stack=", stack)


@pytest.mark.parametrize("name", ["gbx-first", "pypi-first"])
def test_startup_activation_loads_one_stack(legs, name):
    r = legs[name]["result"]
    assert r is not None, legs[name]["output"][-2000:]
    assert r["pypi_libs"] == [], (name, r["pypi_libs"])
    assert "stack" in r, r.get("stack_error")
    from_payload = r["stack"]["from_payload"]
    assert from_payload and all(from_payload.values()), (name, from_payload)
    assert not re.search(r"DATABASE\.LAYOUT", legs[name]["output"])
