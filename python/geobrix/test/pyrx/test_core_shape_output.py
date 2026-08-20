import os

import numpy as np
import pytest
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from .conftest import make_geotiff_bytes


def test_materialize_true_forces_bytes(tmp_path):
    p = str(tmp_path / "r.tif")  # make a virtual tile
    prof = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(10, 50, 0.001, 0.001),
        nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
    vt = VirtualTile(cellid=7, path=p, window=(0, 0, 8, 8))
    out = ot.shape_output(vt, materialize=True)
    assert out.raster is not None and not out.is_virtual()


def test_materialize_noop_on_materialized():
    vt = VirtualTile(cellid=1, raster=make_geotiff_bytes())
    out = ot.shape_output(vt, materialize=True)
    assert out.raster == vt.raster


def test_virtualize_dir_writes_and_returns_virtual(tmp_path):
    vt = VirtualTile(cellid=3, raster=make_geotiff_bytes(width=4, height=4))
    out = ot.shape_output(vt, virtualize_dir=str(tmp_path))
    assert out.raster is None and out.path is not None
    assert os.path.exists(out.path)
    # round-trips
    with ot.open_tile(out) as ds:
        assert ds.width == 4


def test_virtualize_prefix_in_name(tmp_path):
    vt = VirtualTile(cellid=9, raster=make_geotiff_bytes())
    out = ot.shape_output(vt, virtualize_dir=str(tmp_path), virtualize_prefix="run1")
    assert os.path.basename(out.path).startswith("run1_")


def test_conflict_raises():
    vt = VirtualTile(cellid=1, raster=make_geotiff_bytes())
    with pytest.raises(ValueError):
        ot.shape_output(vt, virtualize_dir="/x", materialize=True)


def test_auto_noop():
    vt = VirtualTile(cellid=1, raster=make_geotiff_bytes())
    assert ot.shape_output(vt) is vt


def test_materialize_stamps_tile_byte_size(tmp_path):
    """Materialized output carries tile_byte_size in metadata."""
    p = str(tmp_path / "r.tif")
    prof = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(10, 50, 0.001, 0.001),
        nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
    vt = VirtualTile(cellid=7, path=p, window=(0, 0, 8, 8))
    out = ot.shape_output(vt, materialize=True)
    assert out.raster is not None
    # tile_byte_size should be in metadata and equal to the raster byte length
    assert "tile_byte_size" in out.metadata
    tile_byte_size = int(out.metadata["tile_byte_size"])
    assert tile_byte_size == len(out.raster)
    assert tile_byte_size > 0


def test_virtualize_dir_stamps_path_file_size(tmp_path):
    """Virtualized output carries path_file_size in metadata."""
    vt = VirtualTile(cellid=3, raster=make_geotiff_bytes(width=4, height=4))
    out = ot.shape_output(vt, virtualize_dir=str(tmp_path))
    assert out.raster is None and out.path is not None
    assert os.path.exists(out.path)
    # path_file_size should be in metadata and equal to the written file size
    assert "path_file_size" in out.metadata
    path_file_size = int(out.metadata["path_file_size"])
    actual_file_size = os.path.getsize(out.path)
    assert path_file_size == actual_file_size
    assert path_file_size > 0
