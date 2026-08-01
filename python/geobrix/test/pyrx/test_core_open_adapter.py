"""_open adapter: v1 struct / v2 materialized / v2 virtual / raw bytes / VirtualTile."""

import numpy as np
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from .conftest import make_geotiff_bytes  # existing fixture (v1 bytes)


def test_open_raw_bytes():
    with ot._open(make_geotiff_bytes(width=4, height=3)) as ds:
        assert ds.width == 4 and ds.height == 3


def test_open_v1_struct():
    tile = {
        "cellid": 0,
        "raster": make_geotiff_bytes(width=5, height=2),
        "metadata": {},
    }
    with ot._open(tile) as ds:
        assert ds.width == 5 and ds.height == 2


def test_open_v2_materialized_struct():
    tile = {
        "cellid": 0,
        "raster": make_geotiff_bytes(width=6, height=6),
        "path": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    with ot._open(tile) as ds:
        assert ds.width == 6


def test_open_v2_virtual_struct(tmp_path):
    p = str(tmp_path / "r.tif")
    prof = dict(
        driver="GTiff",
        width=512,
        height=512,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(10, 50, 0.001, 0.001),
        nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(512 * 512, dtype="float32").reshape(512, 512), 1)
    tile = {
        "cellid": 0,
        "raster": None,
        "path": p,
        "window": {"col_off": 0, "row_off": 0, "width": 256, "height": 256},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    with ot._open(tile) as ds:
        assert (ds.width, ds.height) == (256, 256)


def test_open_virtualtile_passthrough():
    vt = VirtualTile(cellid=0, raster=make_geotiff_bytes(width=3, height=3))
    with ot._open(vt) as ds:
        assert ds.width == 3


def test_open_all_list(tmp_path):
    tiles = [
        {"cellid": 0, "raster": make_geotiff_bytes(width=w, height=2), "metadata": {}}
        for w in (2, 3)
    ]
    with ot._open_all(tiles) as dss:
        assert [d.width for d in dss] == [2, 3]
