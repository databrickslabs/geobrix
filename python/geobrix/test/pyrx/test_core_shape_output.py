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


def _pixels(tile):
    with ot.open_tile(tile) as ds:
        return ds.read(), ds.transform, ds.crs


def test_virtualize_dir_windowed_part_opens_to_its_own_pixels(tmp_path):
    """A materialized part cut from a parent carries ``window`` = its position in
    the PARENT. The file written by ``virtualize_dir`` holds only the part, so the
    virtual tile must open to the part's pixels, not to a parent-sized offset
    inside the part's file.
    """
    part = make_geotiff_bytes(width=16, height=8, epsg=3857)
    vt = VirtualTile(cellid=5, raster=part, window=(32, 24, 16, 8))
    want_px, want_tf, want_crs = _pixels(vt)

    out = ot.shape_output(vt, virtualize_dir=str(tmp_path))

    assert out.raster is None and out.is_virtual()
    got_px, got_tf, got_crs = _pixels(out)
    assert got_px.shape == want_px.shape
    assert np.array_equal(got_px, want_px)
    assert got_tf == want_tf and got_crs == want_crs
    # The window addresses the written file, whose origin is its own corner.
    assert out.window == (0, 0, 16, 8)
    # Distinct parts of one cellid still get distinct files.
    other = VirtualTile(cellid=5, raster=part, window=(0, 0, 16, 8))
    out2 = ot.shape_output(other, virtualize_dir=str(tmp_path))
    assert out2.path != out.path


def test_virtualize_dir_part_with_source_path_provenance(tmp_path):
    """A materialized tile that keeps its source ``path`` (whole-input retile
    output) is rewritten to the new file, not left pointing at the source."""
    src = tmp_path / "src.tif"
    src.write_bytes(make_geotiff_bytes(width=40, height=40, epsg=4326))
    part = make_geotiff_bytes(width=10, height=6, epsg=3857)
    vt = VirtualTile(cellid=2, raster=part, path=str(src), window=(12, 20, 10, 6))
    want_px, _, want_crs = _pixels(vt)

    out = ot.shape_output(vt, virtualize_dir=str(tmp_path / "out"))

    assert out.path != str(src) and os.path.dirname(out.path) == str(tmp_path / "out")
    got_px, _, got_crs = _pixels(out)
    assert np.array_equal(got_px, want_px) and got_crs == want_crs
