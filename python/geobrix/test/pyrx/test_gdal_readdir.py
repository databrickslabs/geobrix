"""A light-tier open looks for sidecar files only where the folder has them.

To find a raster's sidecar files (external overviews, masks, ``.aux.xml``, world files)
GDAL lists the folder of the file on open, unless ``GDAL_DISABLE_READDIR_ON_OPEN`` says
otherwise. On a Volume that listing took 0.3 s to 10 s on a Serverless worker for a folder
of 745 hourly GeoTIFFs, at every open. ``configure_gdal_env`` sets the option to ``TRUE``
for the process (GDAL checks each sidecar by name), and ``open_raster``, the light tier's
open of a local or Volume file, lists each folder once per process and picks the option per
file: ``EMPTY_DIR`` (no sidecar look-up) where the listing shows no sidecar, ``TRUE`` where
GDAL finds the sidecars by name, ``NO`` (GDAL's own case-insensitive listing) otherwise.
The direct-read probe always uses ``EMPTY_DIR``. A value the user set decides every open.
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


@pytest.fixture(autouse=True)
def fresh_listings():
    _env._LISTINGS.clear()
    yield
    _env._LISTINGS.clear()


@pytest.fixture
def opens_seen(monkeypatch):
    """``(file name, GDAL_DISABLE_READDIR_ON_OPEN)`` at each ``rasterio.open`` of a file."""
    seen = []
    real_open = rasterio.open

    def _open(fp, *a, **kw):
        if (a[0] if a else kw.get("mode", "r")) == "r":  # the test's own writes aside
            seen.append(
                (os.path.basename(str(fp)), get_gdal_config(KEY, normalize=False))
            )
        return real_open(fp, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _open)
    return seen


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
def test_a_world_file_georeferences_a_virtual_tile(
    tmp_path, readdir, opens_seen, setting
):
    """The folder listing shows ``plain.tfw`` beside ``plain.tif``, a GeoTIFF with no
    georeferencing of its own, so a virtual tile's open (``open_tile`` and the point read of
    ``rst_sample``) runs with ``TRUE`` and GDAL finds the world file by name. ``EMPTY_DIR``
    set by the user, the control, hides it: the tile then has no georeferencing, so the row
    measures the sidecar lookup."""
    path, data = _world_file_tiff(tmp_path)
    readdir(setting)
    tile = _tile(path)
    # pixel (col 7, row 4): centre x = 500001 + 7*2, y = 5000099 - 4*2
    pt = shapely.to_wkb(shapely.Point(500015.0, 5000091.0))
    want = Affine(2.0, 0.0, 500000.0, 0.0, -2.0, 5000100.0)
    if setting is None:
        assert _georef(tile) == want
        assert prx._sample_udf.func(tile, pt) == [float(data[0, 4, 7])]
        assert {mode for name, mode in opens_seen if name == "plain.tif"} == {"TRUE"}
    else:
        assert _georef(tile) == Affine.identity()
        assert prx._sample_udf.func(tile, pt) is None  # outside the pixel grid


@pytest.mark.parametrize("setting", [None, "EMPTY_DIR"], ids=["TRUE", "EMPTY_DIR"])
def test_an_external_overview_is_found(tmp_path, readdir, setting, monkeypatch):
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
    assert _env.readdir_mode(path) == ("TRUE" if setting is None else None)
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


def _plain_tiff(path, georef=True):
    kw = (
        dict(crs="EPSG:32633", transform=from_origin(500000.0, 5000000.0, 10.0, 10.0))
        if georef
        else {}
    )
    with rasterio.Env(GDAL_PAM_ENABLED="NO"):
        with rasterio.open(
            path, "w", driver="GTiff", width=8, height=8, count=1, dtype="uint8", **kw
        ) as dst:
            dst.write(np.ones((1, 8, 8), "uint8"))
    return path


def test_a_file_with_no_sidecar_opens_without_any_sidecar_lookup(
    tmp_path, readdir, opens_seen
):
    """``a.tif`` has no sidecar in its folder (another GeoTIFF and a JSON file are there):
    every light-tier open of its virtual tile runs with ``EMPTY_DIR``. The folder is listed
    once for all of them."""
    a = _plain_tiff(tmp_path / "a.tif")
    _plain_tiff(tmp_path / "ab.tif")
    (tmp_path / "manifest.json").write_text("{}")
    readdir(None)
    before = _env._LISTINGS.listings
    tile = _tile(a)
    with ot.open_header(tile) as ds:
        assert ds.width == 8
    with ot._open(tile) as ds:
        assert ds.read(1).sum() == 64
    assert prx._sample_udf.func(
        tile, shapely.to_wkb(shapely.Point(500005.0, 4999995.0))
    )
    assert {m for n, m in opens_seen if n == "a.tif"} == {"EMPTY_DIR"}
    assert _env._LISTINGS.listings - before == 1


def test_a_case_mixed_sidecar_opens_with_gdals_listing(tmp_path, readdir, opens_seen):
    """``plain.Tfw``: GDAL's by-name checks try ``.tfw`` and ``.TFW`` only, so the open is
    left to GDAL's own listing (``NO``), which matches names case-insensitively."""
    path, data = _world_file_tiff(tmp_path)
    os.rename(tmp_path / "plain.tfw", tmp_path / "plain.Tfw")
    readdir(None)
    want = Affine(2.0, 0.0, 500000.0, 0.0, -2.0, 5000100.0)
    assert _georef(_tile(path)) == want
    assert {m for n, m in opens_seen if n == "plain.tif"} == {"NO"}


def test_a_file_written_after_the_listing_lists_the_folder_again(
    tmp_path, readdir, opens_seen
):
    """A file missing from the held listing lists its folder again, so a GeoTIFF and its
    world file written later are seen. A sidecar written later for a file the listing
    already holds is not seen until the folder is listed again."""
    readdir(None)
    a = _plain_tiff(tmp_path / "a.tif", georef=False)
    assert _georef(_tile(a)) == Affine.identity()
    (tmp_path / "a.tfw").write_text("\n".join(str(v) for v in _TFW) + "\n")
    assert _georef(_tile(a)) == Affine.identity()  # stale: a.tif was in the listing
    b = _plain_tiff(tmp_path / "b.tif", georef=False)
    (tmp_path / "b.tfw").write_text("\n".join(str(v) for v in _TFW) + "\n")
    want = Affine(2.0, 0.0, 500000.0, 0.0, -2.0, 5000100.0)
    assert _georef(_tile(b)) == want  # b.tif missing: listed again, b.tfw seen
    assert _georef(_tile(a)) == want  # the new listing holds a.tfw too
    modes = [m for n, m in opens_seen if n in ("a.tif", "b.tif")]
    assert modes == ["EMPTY_DIR", "EMPTY_DIR", "TRUE", "TRUE"]


def test_a_users_value_decides_every_open(tmp_path, readdir, opens_seen):
    """``NO`` in the environment, or a ``rasterio.Env`` around the call: no per-file
    choice is made, GDAL opens with the user's value."""
    a = _plain_tiff(tmp_path / "a.tif")
    readdir("NO")
    with ot.open_header(_tile(a)):
        pass
    readdir(None)
    with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="TRUE"):
        assert _env.readdir_mode(a) == "EMPTY_DIR"  # an Env with our own value
    with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="NO"):
        with ot.open_header(_tile(a)):
            pass
    assert [m for n, m in opens_seen if n == "a.tif"] == ["NO", "NO"]


@pytest.mark.parametrize(
    "name, folder, want",
    [
        ("x.tif", ["x.tif", "y.tif", "xy.tif", "manifest.json"], "EMPTY_DIR"),
        ("x.tif", ["x.tif", "x.tfw"], "TRUE"),
        (
            "x.tif",
            ["x.tif", "x.TFW", "x.tif.ovr", "x.tif.msk", "x.tif.aux.xml"],
            "TRUE",
        ),
        ("x.tif", ["x.tif", "x.tifw", "x.wld", "x.aux", "x.tif.AUX", "x.tab"], "TRUE"),
        ("x.TIF", ["x.TIF", "x.TFW"], "TRUE"),
        ("x.png", ["x.png", "x.pgw"], "TRUE"),
        ("x.tif", ["x.tif", "x.Tfw"], "NO"),
        ("x.tif", ["x.tif", "X.tfw"], "NO"),
        ("x.tif", ["x.tif", "x.png"], "NO"),
        ("x.tif", ["x.tif", "x_rpc.txt"], "NO"),
        ("x.tif", ["x.tif", "METADATA.DIM"], "NO"),
        ("x.tif", ["x.tif", "LC08_MTL.txt"], "NO"),
        ("x.tif", ["x.tif", "DIM_x.XML"], "NO"),
        ("x.tif", ["x.tif", "y.tif", "y.tif.aux.xml", "y.tif.ovr"], "EMPTY_DIR"),
        ("x.tif", ["x.tif", "x.tif.aux.xml", "y.tif.aux.xml"], "TRUE"),
        ("x.tif", ["x.tif", "x.XML"], "TRUE"),
    ],
)
def test_the_value_an_open_takes_from_the_folder_listing(name, folder, want):
    assert _env.readdir_mode_for(name, folder) == want


def test_paths_gdal_reads_through_a_vsi_handler_keep_the_process_value(readdir):
    readdir(None)
    for p in ("/vsis3/b/x.tif", "s3://b/x.tif", 'NETCDF:"/x.nc":t2m', "/vsimem/x.tif"):
        assert _env.readdir_mode(p) is None


def test_the_listings_held_are_capped(tmp_path, readdir, monkeypatch):
    """At most ``max_folders`` listings are held, the least recently used dropped first."""
    readdir(None)
    monkeypatch.setattr(_env._LISTINGS, "max_folders", 2)
    paths = []
    for i in range(3):
        (tmp_path / str(i)).mkdir()
        paths.append(_plain_tiff(tmp_path / str(i) / "a.tif"))
    for p in paths:
        assert _env.readdir_mode(p) == "EMPTY_DIR"
    held = list(_env._LISTINGS._names)
    assert held == [os.path.realpath(tmp_path / "1"), os.path.realpath(tmp_path / "2")]
