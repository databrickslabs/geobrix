"""A PROJ grid directory registered after the process's first PROJ use reaches GDAL's transforms (I-C).

The first CRS operation builds the process's default PROJ context with its search path. Setting PROJ_DATA afterwards
changes nothing for that context; osr.SetPROJSearchPaths does. configure_gdal_env sets both, so a registration made after
the first use is seen by the raster transforms. Each row runs in a fresh process: "after the first use" is the point.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
GRID = (
    REPO
    / "sample-data/Volumes/main/geobrix_samples/geobrix-examples/proj-grids/synthetic.gsb"
)

CODE = r"""
import os, shutil, sys, tempfile, uuid
from databricks.labs.gbx.pyrx import _env
from osgeo import osr
grid = sys.argv[1]
def shift_of_lat(name):
    # the fixture grid is a constant +30 arc-second latitude shift over lon -1..1, lat 50..53: a point inside it moves
    # by 30/3600 degrees in latitude (traditional lon, lat order, so the point is (lon, lat))
    src = osr.SpatialReference(); src.SetFromUserInput("EPSG:4326")
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst = osr.SpatialReference()
    dst.SetFromUserInput(f"+proj=longlat +ellps=GRS80 +nadgrids={name} +no_defs +type=crs")
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    t = osr.CoordinateTransformation(src, dst)
    x, y, _ = t.TransformPoint(0.0, 52.0)
    return y - 52.0
# the first CRS operation: the default PROJ context exists from here on
a = osr.SpatialReference(); a.SetFromUserInput("EPSG:27700"); b = osr.SpatialReference(); b.SetFromUserInput("EPSG:4326")
osr.CoordinateTransformation(a, b)
d = tempfile.mkdtemp()
name = f"gbx_probe_{uuid.uuid4().hex}.gsb"
shutil.copy(grid, os.path.join(d, name))
# the registration the way the light tier makes it: the environment, then the PROJ search path of the context
_env.configure_gdal_env([d])
delta = shift_of_lat(name)
ok = isinstance(delta, float) and abs(abs(delta) - 30 / 3600) < 1e-6
print("REGISTERED " + ("OK" if ok else f"NONE delta={delta!r}"), flush=True)
"""


def _run(code: str = CODE, *args: str) -> str:
    env = {**os.environ}
    proc = subprocess.run(
        [sys.executable, "-c", code, *args],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, (proc.stdout + proc.stderr)[-3000:]
    return proc.stdout


def _payload_mode_in_a_fresh_process() -> str:
    """The driver policy's mode in a process that imports GeoBrix first, as product code does. This test's own process
    cannot answer it: pytest's pyrx conftest imports rasterio before GeoBrix, which binds rasterio to another GDAL.
    """
    probe = (
        "from databricks.labs.gbx import _gdal_policy\n"
        "print('MODE ' + _gdal_policy.state()['mode'])\n"
    )
    return _run(probe).split("MODE ", 1)[1].split()[0]


@pytest.mark.integration
@pytest.mark.parametrize("order", ["first", "second"])
def test_a_grid_dir_registered_after_the_first_proj_use_reaches_the_transform(order):
    """Run twice (order is the run number): the result must not depend on which run it is."""
    if _payload_mode_in_a_fresh_process() != "full":
        pytest.skip(
            "PRECONDITION MISSING: the payload's osgeo on rasterio's GDAL (GBX_NATIVE_ROOT)"
        )
    if not GRID.is_file():
        pytest.skip("PRECONDITION MISSING: the synthetic.gsb fixture under sample-data")
    out = _run(CODE, str(GRID))
    assert (
        "REGISTERED OK" in out
    ), f"the registered grid did not reach GDAL ({order} run): {out[-400:]}"
