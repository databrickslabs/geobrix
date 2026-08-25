"""Pure-function tests for pyrx Operations ports: tryopen, setsrid, band,
asformat, buildoverviews, sample."""

import numpy as np
import pytest
import shapely.wkb
from shapely.geometry import LineString, Point

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import edit
from databricks.labs.gbx.pyrx.core import ops as ops_core

from .conftest import make_geotiff_bytes


# --- try_open ---------------------------------------------------------------
def test_try_open_true_on_valid_bytes():
    assert ops_core.try_open(make_geotiff_bytes()) is True


def test_try_open_false_on_garbage_bytes():
    assert ops_core.try_open(b"this is not a raster") is False


def test_try_open_false_on_none():
    assert ops_core.try_open(None) is False


# --- set_srid ---------------------------------------------------------------
def test_set_srid_stamps_crs_without_reprojecting():
    src = make_geotiff_bytes(width=4, height=3, epsg=4326)
    with _serde.open_tile(src) as ds:
        src_data = ds.read()
        src_transform = ds.transform
        out = edit.set_srid(ds, 27700)
    with _serde.open_tile(out) as o:
        assert o.crs.to_epsg() == 27700
        # pixels unchanged
        assert np.array_equal(o.read(), src_data)
        # geotransform unchanged
        assert o.transform == src_transform


def test_set_srid_rejects_negative():
    # Spec R2 dumb-storage: srid >= 0. Only a NEGATIVE srid is rejected.
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            edit.set_srid(ds, -1)


def test_set_srid_zero_clears_crs():
    # srid == 0 means "no CRS": clears the CRS metadata, no raise (R2 semantics).
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        cleared = edit.set_srid(ds, 0)
    with _serde.open_tile(cleared) as out:
        assert out.crs is None


# --- band -------------------------------------------------------------------
def test_band_extracts_single_band():
    src = make_geotiff_bytes(width=4, height=3, count=3)
    with _serde.open_tile(src) as ds:
        band2_src = ds.read(2)
        src_transform = ds.transform
        src_crs = ds.crs
        out = edit.band(ds, 2)
    with _serde.open_tile(out) as o:
        assert o.count == 1
        assert np.array_equal(o.read(1), band2_src)
        assert o.transform == src_transform
        assert o.crs == src_crs


def test_band_out_of_range_raises():
    with _serde.open_tile(make_geotiff_bytes(count=2)) as ds:
        with pytest.raises(ValueError):
            edit.band(ds, 3)
        with pytest.raises(ValueError):
            edit.band(ds, 0)


# --- as_format --------------------------------------------------------------
def test_as_format_gtiff_to_gtiff_reopens():
    src = make_geotiff_bytes(width=4, height=3)
    with _serde.open_tile(src) as ds:
        out = ops_core.as_format(ds, "GTiff")
    with _serde.open_tile(out) as o:
        assert o.driver == "GTiff"
        assert (o.width, o.height) == (4, 3)


def test_as_format_to_png():
    # PNG is in rasterio's bundled GDAL; uint8 single-band converts cleanly.
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    data = np.arange(12, dtype="uint8").reshape(3, 4)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = ops_core.as_format(ds, "PNG")
    with MemoryFile(out) as mf:
        with mf.open() as o:
            assert o.driver == "PNG"


def test_as_format_bad_driver_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            ops_core.as_format(ds, "NOT_A_REAL_DRIVER")


# --- build_overviews --------------------------------------------------------
def test_build_overviews_embeds_levels():
    # Need a large-enough raster so decimation factors are meaningful.
    src = make_geotiff_bytes(width=64, height=64)
    with _serde.open_tile(src) as ds:
        out = ops_core.build_overviews(ds, [2, 4], "nearest")
    with _serde.open_tile(out) as o:
        assert o.overviews(1) == [2, 4]


def test_build_overviews_default_resampling():
    src = make_geotiff_bytes(width=64, height=64)
    with _serde.open_tile(src) as ds:
        out = ops_core.build_overviews(ds, [2])
    with _serde.open_tile(out) as o:
        assert o.overviews(1) == [2]


def test_build_overviews_rejects_empty_levels():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            ops_core.build_overviews(ds, [])


def test_build_overviews_rejects_level_below_2():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            ops_core.build_overviews(ds, [1])


def test_build_overviews_rejects_bad_resampling():
    with _serde.open_tile(make_geotiff_bytes(width=64, height=64)) as ds:
        with pytest.raises(ValueError):
            ops_core.build_overviews(ds, [2], "not_a_method")


# --- sample -----------------------------------------------------------------
def test_sample_returns_per_band_pixel_values():
    # raster origin (10,50), 0.5px, 4x3. Center of pixel (col=1,row=0) is
    # world (10.75, 49.75). band1 value there = arange index 1 = 1.0;
    # band2 = 101.0.
    src = make_geotiff_bytes(width=4, height=3, count=2)
    pt = shapely.wkb.dumps(Point(10.75, 49.75))
    with _serde.open_tile(src) as ds:
        vals = ops_core.sample(ds, pt)
    assert vals == [1.0, 101.0]


def test_sample_non_point_raises():
    line = shapely.wkb.dumps(LineString([(10.5, 49.5), (11.0, 49.0)]))
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            ops_core.sample(ds, line)
