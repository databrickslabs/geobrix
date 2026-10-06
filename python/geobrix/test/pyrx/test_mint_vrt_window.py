"""Contract test for the per-tower VRT window used by the Wireless-Coverage viewshed.

`config_nb._window_vrt` mints one VRT per tower over the in-radius DSM tile *paths*; the
viewshed then reads that VRT as a single bytes-free DEM window. This pins the `mint_vrt`
contract that windowing relies on: a VRT over adjacent same-grid tiles reads back as one
raster spanning their union, in the tiles' CRS, resolvable from the written path.

(The Spark-side `_window_vrt` orchestration lives in config_nb — a notebook — and is
validated by the demo run; this is the pytest-able core it builds on.)
"""

import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds._mosaic import mint_vrt


def _write_tile(path, origin_x, origin_y, val, n=256, px=1.0):
    """Write an n×n single-band float32 GeoTIFF at (origin_x, origin_y) in EPSG:3857."""
    tr = from_origin(origin_x, origin_y, px, px)
    profile = dict(
        driver="GTiff",
        height=n,
        width=n,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=tr,
    )
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.full((n, n), val, "float32"), 1)


def test_mint_vrt_unions_adjacent_tiles(tmp_path):
    # 2×1 grid of 256 px tiles at 1 m -> union 512 wide × 256 tall.
    _write_tile(str(tmp_path / "t0.tif"), 0.0, 256.0, 10.0)
    _write_tile(str(tmp_path / "t1.tif"), 256.0, 256.0, 20.0)

    vrt = mint_vrt(
        [str(tmp_path / "t0.tif"), str(tmp_path / "t1.tif")],
        out=str(tmp_path / "win.vrt"),
    )
    assert os.path.exists(vrt)
    with rasterio.open(vrt) as ds:
        assert (ds.width, ds.height) == (512, 256)
        assert ds.crs.to_epsg() == 3857
        a = ds.read(1)
        assert a[0, 0] == 10.0  # left tile
        assert a[0, 500] == 20.0  # right tile


def test_mint_vrt_out_survives_denied_utime(tmp_path, monkeypatch):
    # Regression: minting to a UC Volume FUSE path failed because shutil.move fell back
    # to copy2 -> copystat -> os.utime(), which the FUSE mount rejects with
    # "Operation not permitted". mint_vrt(out=...) must write content only (no utime),
    # so it still succeeds when utime is denied.
    _write_tile(str(tmp_path / "t0.tif"), 0.0, 256.0, 10.0)
    _write_tile(str(tmp_path / "t1.tif"), 256.0, 256.0, 20.0)

    def _deny_utime(*args, **kwargs):
        raise PermissionError("[Errno 1] Operation not permitted")

    monkeypatch.setattr(os, "utime", _deny_utime)
    out = mint_vrt(
        [str(tmp_path / "t0.tif"), str(tmp_path / "t1.tif")],
        out=str(tmp_path / "win.vrt"),
    )
    with rasterio.open(out) as ds:
        assert (ds.width, ds.height) == (512, 256)
        assert ds.read(1)[0, 500] == 20.0
