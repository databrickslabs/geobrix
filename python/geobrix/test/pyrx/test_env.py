import os
import subprocess
import sys

import pytest

from databricks.labs.gbx.pyrx import _env


def test_configure_gdal_env_sets_gdal_data():
    # Clear to prove configure_gdal_env populates from the rasterio wheel.
    os.environ.pop("GDAL_DATA", None)
    _env.configure_gdal_env()
    assert os.environ.get(
        "GDAL_DATA"
    ), "GDAL_DATA should be set from rasterio's bundled data"
    assert os.path.isdir(os.environ["GDAL_DATA"])


def test_configure_gdal_env_is_idempotent_and_respects_existing():
    os.environ["GDAL_DATA"] = "/tmp/preset-gdal-data"
    _env.configure_gdal_env()
    assert os.environ["GDAL_DATA"] == "/tmp/preset-gdal-data"


def test_assert_rasterio_available_returns_versions():
    gdal_ver, rio_ver = _env.assert_rasterio_available()
    assert gdal_ver and rio_ver


@pytest.mark.parametrize("make_dir", [False, True], ids=["missing", "without-proj-db"])
def test_a_proj_data_this_process_cannot_read_still_resolves_epsg_4326(
    tmp_path, make_dir
):
    """DBR 19 exports PROJ_DATA=/databricks/native/proj-data, which the notebook user of a
    standard-access cluster cannot read. rasterio took it as its PROJ search path at
    import, so every light CRS lookup on the driver failed with "Cannot find proj.db".
    A fresh interpreter starts with that environment, as the driver does."""
    unreadable = tmp_path / "proj-data"
    if make_dir:
        unreadable.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in ("PROJ_DATA", "PROJ_LIB")}
    env["PROJ_DATA"] = str(unreadable)
    code = "\n".join(
        [
            "import os, rasterio",
            "from rasterio.crs import CRS",
            "from databricks.labs.gbx.core.crs import crs_to_canonical, resolve_crs",
            "print(crs_to_canonical(resolve_crs('EPSG:4326')))",
            "print(CRS.from_epsg(27700).to_epsg())",
            "with rasterio.Env():",
            "    print(CRS.from_user_input('EPSG:3857').to_epsg())",
            f"print(os.environ['PROJ_DATA'] != {str(unreadable)!r})",
        ]
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert out.returncode == 0, out.stderr[-3000:]
    assert out.stdout.split() == ["EPSG:4326", "27700", "3857", "True"]


def test_a_registered_grid_reaches_rasterio_after_an_unreadable_proj_data(tmp_path):
    """configure_gdal_env replaces an unreadable PROJ_DATA with the bundled data and then
    merges the registered grid dirs: rasterio's PROJ search path must follow to the merged
    dir, or a transform built without a new rasterio.Env misses the grid. The synthetic
    NTv2 grid shifts latitude by +30 arc-seconds; without it PROJ finds no operation."""
    grids = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "sample-data",
        "Volumes",
        "main",
        "geobrix_samples",
        "geobrix-examples",
        "proj-grids",
    )
    grids = os.path.abspath(grids)
    assert os.path.isfile(os.path.join(grids, "synthetic.gsb"))
    env = {k: v for k, v in os.environ.items() if k not in ("PROJ_DATA", "PROJ_LIB")}
    env["PROJ_DATA"] = str(tmp_path / "unreadable-proj-data")
    code = "\n".join(
        [
            "from databricks.labs.gbx.pyrx import _env",
            "from rasterio.crs import CRS",
            "from rasterio._warp import _transform",
            f"_env.configure_gdal_env(extra_proj_dirs=[{grids!r}])",
            "src = CRS.from_user_input('+proj=longlat +ellps=GRS80 "
            "+nadgrids=synthetic.gsb +no_defs')",
            "xs, ys = _transform(src, CRS.from_user_input('EPSG:4326'), [0.0], [51.5], None)",
            "print(repr(ys[0]))",
        ]
    )
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert float(out.stdout.strip().splitlines()[-1]) == pytest.approx(
        51.5 + 30.0 / 3600.0, abs=1e-9
    )
