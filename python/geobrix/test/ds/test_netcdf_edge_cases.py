"""netcdf_gbx edge cases: a one-pixel axis, projected axes in feet, and vector mode over a stack of slices."""

import logging

import numpy as np
import pytest
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.core.crs import crs_from_cf
from databricks.labs.gbx.ds import _netcdf

from .test_netcdf_cf_crs import (
    CELL_KM,
    HRAP,
    _read,
    _transform_crs,
    _vector_rows,
    _write_projected,
)

US_FT = 1200.0 / 3937.0  # metres per US survey foot


def _write_geo(path, lons, lats, bounds=None):
    """A geographic grid (lat descending), ``bounds`` = (lon_bnds, lat_bnds) as (n, 2) arrays for the CF bounds."""
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", len(lats))
        ds.createDimension("lon", len(lons))
        ds.createDimension("nv", 2)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name, lon.standard_name = "latitude", "longitude"
        lat.units, lon.units = "degrees_north", "degrees_east"
        lat[:], lon[:] = lats, lons
        if bounds is not None:
            lon.bounds, lat.bounds = "lon_bnds", "lat_bnds"
            ds.createVariable("lon_bnds", "f8", ("lon", "nv"))[:] = bounds[0]
            ds.createVariable("lat_bnds", "f8", ("lat", "nv"))[:] = bounds[1]
        ds.createVariable("data", "f4", ("lat", "lon"))[:] = np.arange(
            len(lats) * len(lons), dtype="f4"
        ).reshape(len(lats), len(lons))
    return path


def _geo_transform(path):
    with _netcdf.open_dataset(path, None) as ds:
        return _netcdf.grid_transform_crs(ds, "data")


# --- a grid one pixel wide ---------------------------------------------------------------------------------------


def test_a_one_pixel_wide_grid_takes_its_pixel_size_from_the_cf_bounds(tmp_path):
    p = _write_geo(
        str(tmp_path / "narrow.nc"),
        lons=[10.25],
        lats=[50.75, 50.25, 49.75],
        bounds=([[10.0, 10.5]], [[51.0, 50.5], [50.5, 50.0], [50.0, 49.5]]),
    )
    t, crs = _geo_transform(p)
    assert crs == "EPSG:4326"
    assert (t.a, t.e) == (0.5, -0.5)  # x from the bounds, y from the two rows
    assert (t.c, t.f) == (10.0, 51.0)


def test_a_one_pixel_tall_grid_takes_its_pixel_size_from_the_cf_bounds(tmp_path):
    p = _write_geo(
        str(tmp_path / "short.nc"),
        lons=[10.25, 10.75],
        lats=[50.25],
        bounds=([[10.0, 10.5], [10.5, 11.0]], [[50.5, 50.0]]),
    )
    t, _ = _geo_transform(p)
    assert (t.a, t.e) == (0.5, -0.5)
    assert (t.c, t.f) == (10.0, 50.5)


def test_a_one_pixel_grid_reads_in_raster_mode(tmp_path):
    p = _write_geo(
        str(tmp_path / "one.nc"),
        lons=[10.25],
        lats=[50.25],
        bounds=([[10.0, 10.5]], [[50.5, 50.0]]),
    )
    ((_, tile),) = _read(p, variable="data")
    with MemoryFile(bytes(tile[1])) as mf, mf.open() as rds:
        assert (rds.width, rds.height) == (1, 1)
        assert tuple(rds.bounds) == (10.0, 50.0, 10.5, 50.5)


def test_a_one_pixel_wide_grid_with_no_bounds_is_refused_with_a_fix(tmp_path):
    p = _write_geo(str(tmp_path / "nobounds.nc"), lons=[10.25], lats=[50.5, 50.0])
    with pytest.raises(ValueError, match="one value") as err:
        _geo_transform(p)
    msg = str(err.value)
    assert "'lon'" in msg and "bounds" in msg and "Fix:" in msg


def test_a_grid_of_two_or_more_pixels_is_unchanged(tmp_path):
    p = _write_geo(str(tmp_path / "ok.nc"), lons=[1.0, 1.5, 2.0], lats=[3.0, 2.0])
    t, _ = _geo_transform(p)
    assert (t.a, t.e, t.c, t.f) == (0.5, -1.0, 0.75, 3.5)


# --- projected axes in feet ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "unit,metres",
    [
        ("ft", 0.3048),
        ("foot", 0.3048),
        ("feet", 0.3048),
        ("US_survey_foot", US_FT),
        ("US_survey_feet", US_FT),
    ],
)
def test_axes_in_feet_are_scaled_to_metres(tmp_path, unit, metres):
    # HRAP is a metre CRS: a cell of CELL_KM "feet" is CELL_KM * metres m.
    p = _write_projected(
        str(tmp_path / "ft.nc"), units=unit, extra={"crs_wkt": crs_from_cf(HRAP)}
    )
    t, _ = _transform_crs(p)
    assert t.a == pytest.approx(CELL_KM * metres, rel=1e-12)
    assert t.e == pytest.approx(-CELL_KM * metres, rel=1e-12)


def test_survey_feet_on_a_survey_foot_crs_need_no_scale(tmp_path):
    # EPSG:2264 is in US survey feet: an axis in US_survey_foot is already in its unit.
    p = _write_projected(
        str(tmp_path / "ft2264.nc"),
        mapping={},
        units="US_survey_foot",
        extra={"spatial_epsg": 2264},
    )
    t, crs = _transform_crs(p)
    assert crs == "EPSG:2264"
    assert t.a == pytest.approx(CELL_KM, rel=1e-9)


def test_an_unknown_unit_is_still_refused_with_the_same_message(tmp_path):
    p = _write_projected(str(tmp_path / "furlong.nc"), units="furlong")
    with pytest.raises(ValueError, match="not metres or kilometres") as err:
        _transform_crs(p)
    assert "set the axis units to m or km" in str(err.value)


# --- vector mode over a stack of slices -------------------------------------------------------------------------


def _write_swath_stack(path, nt, dim_order=("time", "y", "x")):
    with Dataset(path, "w") as ds:
        ds.createDimension("time", nt)
        ds.createDimension("y", 3)
        ds.createDimension("x", 4)
        lat = ds.createVariable("lat", "f8", ("y", "x"))
        lon = ds.createVariable("lon", "f8", ("y", "x"))
        lat.standard_name, lon.standard_name = "latitude", "longitude"
        lat.units, lon.units = "degrees_north", "degrees_east"
        lat[:] = 45.0 + np.arange(12).reshape(3, 4) * 0.01
        lon[:] = -100.0 + np.arange(12).reshape(3, 4) * 0.01
        v = ds.createVariable("temp", "f4", dim_order)
        v.coordinates = "lat lon"
        v[:] = np.arange(nt * 12, dtype="f4").reshape(nt, 3, 4)
    return path


def test_vector_mode_over_a_stack_warns_and_still_takes_slice_zero(tmp_path, caplog):
    p = _write_swath_stack(str(tmp_path / "stack.nc"), nt=3)
    with caplog.at_level(logging.WARNING, logger=_netcdf.__name__):
        _, rows = _vector_rows(p)
    assert [r[0] for r in rows] == list(range(12))  # slice 0, as before
    texts = [r.getMessage() for r in caplog.records if "temp" in r.getMessage()]
    assert len(texts) == 1
    assert "'time'" in texts[0] and "index 0" in texts[0] and "3" in texts[0]


def test_vector_mode_over_one_slice_stays_quiet(tmp_path, caplog):
    p = _write_swath_stack(str(tmp_path / "one.nc"), nt=1)
    with caplog.at_level(logging.WARNING, logger=_netcdf.__name__):
        _, rows = _vector_rows(p)
    assert len(rows) == 12
    assert not [r for r in caplog.records if "temp" in r.getMessage()]
