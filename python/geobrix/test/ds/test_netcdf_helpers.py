"""Unit tests for CF NetCDF helpers (no Spark)."""

import logging

import numpy as np
import pytest
from netCDF4 import Dataset

from databricks.labs.gbx.ds import _netcdf


def _write_regular_grid(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]  # descending (north-up)
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("ch4", "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def _write_points(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("obs", 5)
        lat = ds.createVariable("latitude", "f8", ("obs",))
        lon = ds.createVariable("longitude", "f8", ("obs",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 50.1, 50.2, 50.3, 50.4]
        lon[:] = [10.0, 10.1, 10.2, 10.3, 10.4]
        v = ds.createVariable("value", "f4", ("obs",))
        v[:] = np.arange(5, dtype="float32")


def _write_curvilinear(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("y", 2)
        ds.createDimension("x", 3)
        lat = ds.createVariable("latitude", "f8", ("y", "x"))
        lon = ds.createVariable("longitude", "f8", ("y", "x"))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.array([[50.0, 50.0, 50.0], [49.0, 49.0, 49.0]])
        lon[:] = np.array([[10.0, 11.0, 12.0], [10.0, 11.0, 12.0]])
        v = ds.createVariable("ch4", "f4", ("y", "x"))
        v[:] = np.arange(6, dtype="float32").reshape(2, 3)


def test_classify_grid(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf.classify(ds, "ch4") == _netcdf.GRID


def test_classify_points(tmp_path):
    p = str(tmp_path / "pts.nc")
    _write_points(p)
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf.classify(ds, "value") == _netcdf.POINTS


def test_classify_curvilinear(tmp_path):
    p = str(tmp_path / "curv.nc")
    _write_curvilinear(p)
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf.classify(ds, "ch4") == _netcdf.CURVILINEAR


def test_grid_transform_crs_north_up(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        transform, crs = _netcdf.grid_transform_crs(ds, "ch4")
    assert crs == "EPSG:4326"
    # origin at (lon min - half px, lat max + half px); px = 0.5
    assert transform.a == pytest.approx(0.5)  # x pixel size
    assert transform.e == pytest.approx(-0.5)  # y pixel size (north-up => negative)
    assert transform.c == pytest.approx(9.75)  # ulx = 10.0 - 0.25
    assert transform.f == pytest.approx(50.25)  # uly = 50.0 + 0.25


def test_array_2d_is_north_up(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        arr = _netcdf.array_2d(ds, "ch4")
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4))


def _write_grid_with_time(path, ntime=3):
    """Regular grid with a leading time dim of size ntime; slice t is all-t."""
    with Dataset(path, "w") as ds:
        ds.createDimension("time", ntime)
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("ch4", "f4", ("time", "lat", "lon"), fill_value=-9999.0)
        v[:] = np.stack([np.full((3, 4), t, dtype="float32") for t in range(ntime)])


def test_array_2d_warns_and_reads_first_slice_on_multi_time(tmp_path, caplog):
    """A leading time dim > 1 → array_2d reads slice 0 (unchanged) AND warns."""
    p = str(tmp_path / "time.nc")
    _write_grid_with_time(p, ntime=3)
    with _netcdf.open_dataset(p, None) as ds:
        with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
            arr = _netcdf.array_2d(ds, "ch4")
    # Behavior unchanged: only the first time slice (all zeros) is read.
    np.testing.assert_allclose(arr, np.zeros((3, 4), dtype="float32"))
    # The silent drop is now surfaced: a warning names the dim and its size.
    msgs = [r.getMessage() for r in caplog.records]
    assert any(
        "time" in m and "size 3" in m for m in msgs
    ), f"expected a dropped-dimension warning naming time/size 3; got {msgs}"


def test_array_2d_no_warn_on_singleton_leading_dim(tmp_path, caplog):
    """A size-1 leading dim (e.g. S5P time=1) is squeezed silently — no warning."""
    p = str(tmp_path / "time1.nc")
    _write_grid_with_time(p, ntime=1)
    with _netcdf.open_dataset(p, None) as ds:
        with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
            arr = _netcdf.array_2d(ds, "ch4")
    np.testing.assert_allclose(arr, np.zeros((3, 4), dtype="float32"))
    assert not [
        r for r in caplog.records if "leading dimension" in r.getMessage()
    ], "a size-1 leading dim should not warn"


def test_point_arrays_flatten(tmp_path):
    p = str(tmp_path / "pts.nc")
    _write_points(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["value"])
    assert srid == "4326"
    assert lon.shape == (5,) and lat.shape == (5,)
    np.testing.assert_allclose(attrs["value"], np.arange(5, dtype="float32"))


def test_point_arrays_curvilinear_ravel(tmp_path):
    p = str(tmp_path / "curv.nc")
    _write_curvilinear(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["ch4"])
    assert lon.shape == (6,) and lat.shape == (6,)
    np.testing.assert_allclose(attrs["ch4"], np.arange(6, dtype="float32"))


def test_point_arrays_grid_meshgrid(tmp_path):
    # A regular grid coerced to points: lon(4) x lat(3) -> 12 aligned points.
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["ch4"])
    assert lon.shape == (12,) and lat.shape == (12,) and attrs["ch4"].shape == (12,)
    # first cell is (lon=10.0, lat=50.0)
    assert lon[0] == pytest.approx(10.0) and lat[0] == pytest.approx(50.0)


def test_np_to_spark_types():
    from pyspark.sql.types import DoubleType, FloatType, IntegerType

    assert isinstance(_netcdf.np_to_spark(np.dtype("float32")), FloatType)
    assert isinstance(_netcdf.np_to_spark(np.dtype("float64")), DoubleType)
    assert isinstance(_netcdf.np_to_spark(np.dtype("int32")), IntegerType)


# --- readable_variables / select_variables -----------------------------------


def _grid_two_vars(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        for name in ("ch4", "co"):
            v = ds.createVariable(name, "f4", ("lat", "lon"), fill_value=-9999.0)
            v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def test_readable_variables_raster_enumerates_all_grids(tmp_path):
    f = tmp_path / "g.nc"
    _grid_two_vars(str(f))
    with _netcdf.open_dataset(str(f), None) as ds:
        assert sorted(_netcdf.readable_variables(ds, "raster")) == ["ch4", "co"]
        # coordinate variables are never returned
        assert "lat" not in _netcdf.readable_variables(ds, "raster")


def test_select_variables_absent_option_returns_all(tmp_path):
    f = tmp_path / "g.nc"
    _grid_two_vars(str(f))
    with _netcdf.open_dataset(str(f), None) as ds:
        assert sorted(_netcdf.select_variables(ds, {}, "raster")) == ["ch4", "co"]


def test_select_variables_filters_to_named(tmp_path):
    f = tmp_path / "g.nc"
    _grid_two_vars(str(f))
    with _netcdf.open_dataset(str(f), None) as ds:
        assert _netcdf.select_variables(ds, {"variable": "co"}, "raster") == ["co"]
