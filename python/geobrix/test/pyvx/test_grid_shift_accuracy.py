"""Functional accuracy test: a registered PROJ grid is actually consulted.

This proves the whole point of the custom-PROJ-grid-registration feature end to
end on the light tier: registering a directory of grid-shift files causes an
in-process ``pyvx._crs.st_transformcrs`` to *find and apply* a grid referenced by
filename from a CRS string, and NOT applying it (no registration) yields a
measurably different result.

## Why a synthetic grid, not OSTN15

The ``geobrix-dev`` container has no general network egress, so a real grid
(OSTN15 etc.) cannot be downloaded. Instead we commit a tiny, valid NTv2 ``.gsb``
(see ``sample-data/.../proj-grids/gen_synthetic_gsb.py``) that applies a KNOWN
constant shift of ``+30`` arc-seconds of latitude everywhere inside its box. A
constant shift makes the expected output exact and node-ordering-independent, so
the assertion below is an equality against a hand-computable coordinate — the
strongest possible evidence the grid was consulted (a ballpark / no-grid
transform gives the input back unchanged, or errors because the grid is missing).

## What exercises the feature

The source CRS is an authority-less PROJ.4 string that references the grid by
filename via ``+nadgrids=synthetic.gsb``. PROJ can only build the datum
transformation to WGS84 if it locates ``synthetic.gsb`` on its search path — and
the light vector transform engine is pyproj, which resolves and caches its PROJ
search path at import and therefore ignores a ``PROJ_DATA`` env var changed
afterwards. So the ONLY way an in-process transform finds the grid is if
``st_transformcrs`` pushes the registered directory into pyproj's search path
before building the transformer. That is exactly the driver-path fix this test
guards (RED before it, GREEN after).
"""

import os

import pytest

pyproj = pytest.importorskip("pyproj")
pytest.importorskip("shapely")
from shapely import from_wkt  # noqa: E402

from databricks.labs.gbx import crs_grids  # noqa: E402
from databricks.labs.gbx.core import crs as _corecrs  # noqa: E402
from databricks.labs.gbx.core import proj_grids  # noqa: E402
from databricks.labs.gbx.pyvx import _crs  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture location — resolved from the repo checkout (always mounted at
# /root/geobrix in the dev container), so the test does not depend on the
# optional sample-data -> /Volumes bind mount. The same bytes also appear at
# /Volumes/main/geobrix_samples/geobrix-examples/proj-grids/ when that mount is
# present, which is the documented on-cluster path for this fixture.
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
GRID_DIR = os.path.join(
    _REPO_ROOT,
    "sample-data",
    "Volumes",
    "main",
    "geobrix_samples",
    "geobrix-examples",
    "proj-grids",
)
GRID_FILE = os.path.join(GRID_DIR, "synthetic.gsb")

# The synthetic grid applies a constant +30 arc-second latitude shift, 0 in
# longitude, everywhere inside its box (see the generator).
SHIFT_SEC = 30.0
# Authority-less source CRS that references the grid BY FILENAME. Transforming
# from it to WGS84 requires the grid; that is what makes "the CRS string finds
# the grid" the thing under test.
SRC_CRS = "+proj=longlat +ellps=GRS80 +nadgrids=synthetic.gsb +no_defs"
TGT_CRS = "EPSG:4326"
PT_WKT = "POINT (0 51.5)"  # inside the grid box (50..53 N, 1 E .. 1 W)
EXPECT_LON = 0.0
EXPECT_LAT = 51.5 + SHIFT_SEC / 3600.0  # 51.508333...


@pytest.fixture(autouse=True)
def _isolate():
    """Reset every piece of process-global state the transform touches, so the
    tests are order-independent and a registered grid never leaks between them:

    * the driver grid registry,
    * geobrix's thread-local pyproj Transformer cache (same CRS pair would
      otherwise return a cached grid-less transformer),
    * pyproj's PROJ search path (restored to its import-time value), and
    * the GDAL_DATA / PROJ_DATA / PROJ_LIB env vars (``st_transformcrs`` calls
      ``configure_gdal_env``, which points PROJ_DATA at the registered grids).
    """
    orig_datadir = pyproj.datadir.get_data_dir()
    pyproj.network.set_network_enabled(False)
    env_keys = ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")
    orig_env = {k: os.environ.get(k) for k in env_keys}

    def _reset():
        proj_grids.set_registered_dirs([], replace=True)
        _corecrs._thread_local.__dict__.pop("transformers", None)
        pyproj.datadir.set_data_dir(orig_datadir)
        for k, v in orig_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    _reset()
    yield
    _reset()


def _lonlat(result):
    """Extract (lon, lat) from a medium-preserving ``st_transformcrs`` result.

    Text results are EWKT (``SRID=4326;POINT (...)``); strip the SRID prefix that
    shapely's WKT reader does not accept.
    """
    assert isinstance(result, str), f"expected EWKT str, got {type(result).__name__}"
    body = result.split(";", 1)[1] if result.startswith("SRID=") else result
    g = from_wkt(body)
    return (g.x, g.y)


def test_synthetic_grid_fixture_present():
    assert os.path.isfile(GRID_FILE), (
        f"synthetic NTv2 fixture missing at {GRID_FILE}; "
        f"regenerate via gen_synthetic_gsb.py"
    )


def test_transform_without_registration_does_not_apply_shift():
    """Control: with nothing registered, the grid is not on pyproj's search path.

    ``+nadgrids`` makes the grid REQUIRED, so PROJ either raises (grid not found)
    or, in a ballpark fallback, returns the point unshifted. Either is acceptable
    for the control — the one thing that must NOT happen is the +30 arc-second
    shift appearing without registration.
    """
    try:
        out = _crs.st_transformcrs(PT_WKT, TGT_CRS, SRC_CRS)
    except Exception:
        return  # grid-required raise: control satisfied (no shift possible)
    if out is None:
        return
    _lon, lat = _lonlat(out)
    assert lat != pytest.approx(EXPECT_LAT, abs=1e-7), (
        "grid shift was applied WITHOUT registration — the control broke; the "
        "grid must not be reachable when no directory is registered"
    )


def test_registration_makes_grid_consulted():
    """RED before the driver-path fix, GREEN after.

    After ``register_proj_grids``, an in-process ``st_transformcrs`` must locate
    ``synthetic.gsb`` by filename and apply its exact +30 arc-second latitude
    shift. Before the fix, ``st_transformcrs`` never puts the registered dir on
    pyproj's search path, so this raises / does not shift and the test fails.
    """
    registered = crs_grids.register_proj_grids(spark=None, dirs=GRID_DIR)
    assert GRID_DIR in registered

    out = _crs.st_transformcrs(PT_WKT, TGT_CRS, SRC_CRS)
    assert out is not None, "transform returned NULL after registration"
    lon, lat = _lonlat(out)
    assert lon == pytest.approx(EXPECT_LON, abs=1e-6)
    assert lat == pytest.approx(EXPECT_LAT, abs=1e-6), (
        f"expected +{SHIFT_SEC} arc-sec lat shift to {EXPECT_LAT}, got {lat}; "
        f"the registered grid was not consulted"
    )


def test_worker_grid_dirs_survive_the_transform_itself():
    """A worker holds no driver registry: its UDF closure puts the grid dirs on the search path, and the transform
    that follows must not read the empty registry as "no grids" and take them off again. The driver registry stays
    empty here, as on a worker, so this holds on any machine (a PROJ data dir that already holds the grid, as the
    dev container's does, cannot hide a regression)."""
    from databricks.labs.gbx.pyrx import _env

    assert proj_grids.get_registered_dirs() == []
    _env.configure_gdal_env(extra_proj_dirs=[GRID_DIR])

    lat = _lonlat(_crs.st_transformcrs(PT_WKT, TGT_CRS, SRC_CRS))[1]

    assert lat == pytest.approx(EXPECT_LAT, abs=1e-6)


_SERVERLESS_ORDER_PROBE = """
import sys
from pyspark.sql import SparkSession
from shapely import wkb
from databricks.labs.gbx import _api, crs_grids, manager

spark = SparkSession.builder.master("local[2]").getOrCreate()
name = "gbx_st_transformcrs"
manager.register_family(spark, ("gbx_st_",), only=[name], level="L3")
assert _api.chosen(spark)[name][0] == "L3"
crs_grids.register_proj_grids(spark, sys.argv[1])
assert _api.chosen(spark)[name][0] == "L3"
query = "SELECT %s('%s', '%s', '%s') AS g" % (name, sys.argv[2], sys.argv[3], sys.argv[4])
blob = spark.sql(query).collect()[0]["g"]
print("LAT", wkb.loads(bytes(blob)).y if blob is not None else None)
"""


def test_grids_registered_after_the_function_reach_the_live_binding():
    """The documented Serverless order: register the function, then the grids. The function stays at its level and
    its live SQL binding sees the new grid dirs (a re-registered closure), not the dirs of its first registration.
    Runs in its own process: a Python worker that already started PROJ keeps its search path.
    """
    import subprocess
    import sys

    out = subprocess.run(
        [
            sys.executable,
            "-c",
            _SERVERLESS_ORDER_PROBE,
            GRID_DIR,
            PT_WKT,
            TGT_CRS,
            SRC_CRS,
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert out.returncode == 0, out.stderr[-2000:]
    lat = [ln for ln in out.stdout.splitlines() if ln.startswith("LAT ")][-1][4:]
    assert (
        lat != "None"
    ), "the live binding returned NULL after the grids were registered"
    assert float(lat) == pytest.approx(EXPECT_LAT, abs=1e-6)


def test_grid_dirs_reregistration_prefers_the_recorded_level(monkeypatch):
    """Each name is registered again with its recorded level as a preference, never forced; a name a JAR companion
    holds at a level it has no row at is left alone. The session is a fresh object, so nothing an earlier test
    registered in the shared one can change what is read."""
    from types import SimpleNamespace

    from databricks.labs.gbx import _api, manager

    session = SimpleNamespace()
    kept, companion = "gbx_st_transformcrs", "gbx_rst_clip"
    _api.mark_registered(session, [kept, companion, "gbx_st_setcrs"])
    _api.chosen(session).update({kept: ("L1", "-"), companion: ("L1", "-")})
    seen = {}
    monkeypatch.setattr(manager, "register", lambda s, **kw: seen.update(kw))

    crs_grids._reregister_active_light_tiers(session)

    assert seen["only"] == [kept]
    assert seen["_prefer"] == {kept: "L1"}
    assert "_levels" not in seen
