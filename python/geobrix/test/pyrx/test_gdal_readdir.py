"""A light-tier open does not list the folder of the file it opens.

To find a raster's sidecar files (external overviews, masks, ``.aux.xml``, world files)
GDAL lists the folder of the file on open, unless ``GDAL_DISABLE_READDIR_ON_OPEN`` says
otherwise. On a Volume that listing took about 10 s on a Serverless worker for a folder of
745 hourly GeoTIFFs, and every light-tier open paid it. ``configure_gdal_env`` sets the
option to ``TRUE``: GDAL then checks each sidecar by name (``CPLCheckForFile`` and its
siblings stat each candidate when the sibling list is NULL), so sidecars are still found.
``EMPTY_DIR`` would skip them; it is used only for the direct-read probe, which reads one
pixel. A value the user set is kept.
"""

import os

import numpy as np
import pytest
import rasterio
import shapely
from rasterio.env import get_gdal_config
from rasterio.transform import Affine, from_origin

from databricks.labs.gbx.ds import file_gbx
from databricks.labs.gbx.pyrx import _env
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import open_tile as ot

KEY = "GDAL_DISABLE_READDIR_ON_OPEN"


@pytest.fixture
def readdir(monkeypatch):
    """Set the option process-wide as a light-tier worker has it: ``readdir(None)`` runs
    ``configure_gdal_env`` on an environment without it, ``readdir(value)`` sets ``value``
    (the control). The environment is restored afterwards."""
    monkeypatch.setenv(
        KEY, "placeholder"
    )  # recorded, so teardown restores the original

    def _set(value):
        monkeypatch.delenv(KEY)
        if value is None:
            _env.configure_gdal_env()
        else:
            monkeypatch.setenv(KEY, value)
        return os.environ.get(KEY)

    yield _set


def _tile(path):
    return {
        "cellid": 0,
        "raster": None,
        "path": str(path),
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


def test_configure_gdal_env_turns_the_listing_off_and_keeps_a_users_value(readdir):
    assert readdir(None) == "TRUE"
    for users in ("NO", "EMPTY_DIR", "YES"):
        readdir(users)
        _env.configure_gdal_env()
        assert os.environ[KEY] == users


# World file: 2 m pixels, upper-left pixel centre at (500001, 5000099).
_TFW = (2.0, 0.0, 0.0, -2.0, 500001.0, 5000099.0)


def _world_file_tiff(tmp_path):
    """A GeoTIFF without internal georeferencing, georeferenced by its ``.tfw``."""
    data = np.arange(50 * 60, dtype="float32").reshape(1, 50, 60)
    path = tmp_path / "plain.tif"
    with rasterio.Env(GDAL_PAM_ENABLED="NO"):  # no .aux.xml: the world file alone
        with rasterio.open(
            path, "w", driver="GTiff", width=60, height=50, count=1, dtype="float32"
        ) as dst:
            dst.write(data)
    (tmp_path / "plain.tfw").write_text("\n".join(str(v) for v in _TFW) + "\n")
    assert sorted(os.listdir(tmp_path)) == ["plain.tfw", "plain.tif"]
    return path, data


def _georef(tile):
    with ot._open(tile) as ds:
        return ds.transform


@pytest.mark.parametrize("setting", [None, "EMPTY_DIR"], ids=["TRUE", "EMPTY_DIR"])
def test_a_world_file_georeferences_a_virtual_tile(tmp_path, readdir, setting):
    """With the option ``configure_gdal_env`` sets, the ``.tfw`` of a GeoTIFF that has no
    georeferencing of its own is found by a virtual tile's open (``open_tile`` and the point
    read of ``rst_sample``). ``EMPTY_DIR``, the control, hides the world file: the tile then
    has no georeferencing, so the row measures the sidecar lookup."""
    path, data = _world_file_tiff(tmp_path)
    readdir(setting)
    tile = _tile(path)
    # pixel (col 7, row 4): centre x = 500001 + 7*2, y = 5000099 - 4*2
    pt = shapely.to_wkb(shapely.Point(500015.0, 5000091.0))
    want = Affine(2.0, 0.0, 500000.0, 0.0, -2.0, 5000100.0)
    if setting is None:
        assert _georef(tile) == want
        assert prx._sample_udf.func(tile, pt) == [float(data[0, 4, 7])]
    else:
        assert _georef(tile) == Affine.identity()
        assert prx._sample_udf.func(tile, pt) is None  # outside the pixel grid


@pytest.mark.parametrize("setting", [None, "EMPTY_DIR"], ids=["TRUE", "EMPTY_DIR"])
def test_an_external_overview_is_found(tmp_path, readdir, setting):
    """An external ``.ovr`` beside the file is found by a light-tier open of a virtual tile
    with the option ``configure_gdal_env`` sets; ``EMPTY_DIR`` (the control) hides it.
    """
    path = tmp_path / "base.tif"
    data = np.arange(256 * 256, dtype="uint16").reshape(1, 256, 256)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=256,
        height=256,
        count=1,
        dtype="uint16",
        crs="EPSG:32633",
        transform=from_origin(500000.0, 5000000.0, 10.0, 10.0),
    ) as dst:
        dst.write(data)
    with rasterio.Env(TIFF_USE_OVR=True):  # GDAL writes base.tif.ovr beside it
        with rasterio.open(path, "r+") as ds:
            ds.build_overviews([2, 4])
    assert (tmp_path / "base.tif.ovr").exists()
    readdir(setting)
    with ot.open_header(_tile(path)) as ds:
        overviews = ds.overviews(1)
    assert overviews == ([2, 4] if setting is None else [])


def test_the_direct_read_probe_opens_without_a_folder_listing(
    tmp_path, readdir, monkeypatch
):
    """``_stage_local_if_needed`` probes a Volume path with one open that reads one pixel:
    that open runs with ``EMPTY_DIR`` (no listing, no sidecar checks), and the option is
    back to the process value afterwards."""
    path = tmp_path / "v.tif"
    with rasterio.open(
        path, "w", driver="GTiff", width=8, height=8, count=1, dtype="uint8"
    ) as dst:
        dst.write(np.ones((1, 8, 8), "uint8"))
    readdir(None)
    seen = []
    real_open = rasterio.open

    def _open(*a, **kw):
        seen.append(get_gdal_config(KEY, normalize=False))
        return real_open(*a, **kw)

    monkeypatch.setattr(file_gbx, "_is_fuse_path", lambda p: True)
    monkeypatch.setattr(rasterio, "open", _open)
    assert file_gbx._stage_local_if_needed(str(path)) == (str(path), False)
    assert seen == ["EMPTY_DIR"]
    assert get_gdal_config(KEY, normalize=False) == "TRUE"
