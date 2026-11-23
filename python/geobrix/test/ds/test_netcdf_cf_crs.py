"""netcdf_gbx raster mode: the CRS comes from the file (CF grid_mapping); EPSG:4326 is the default for
geographic grids only; any other grid with no CRS is refused."""

import numpy as np
import pytest
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.core.crs import crs_from_cf, get_transformer
from databricks.labs.gbx.ds import _netcdf
from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
from databricks.labs.gbx.ds.raster import _FilePartition

# NWS Stage IV (HRAP): polar stereographic true at 60 N, central meridian 105 W, sphere of radius 6,371,200 m.
HRAP = {
    "grid_mapping_name": "polar_stereographic",
    "straight_vertical_longitude_from_pole": -105.0,
    "latitude_of_projection_origin": 90.0,
    "standard_parallel": 60.0,
    "earth_radius": 6371200.0,
}
CELL_KM = (
    4.7625  # the HRAP cell, in kilometres (x and y are in km, as in the NWS files)
)
# True scale at the standard parallel: the point (105 W, 60 N) is R * cos(60 deg) = 3,185,600 m from the pole, on the
# central meridian, which runs along -y on the north-polar plane. A wrong radius or a wrong parallel moves it.
TRUE_SCALE_POINT = (0.0, -3185600.0)


def _assert_hrap(crs):
    x, y = get_transformer("EPSG:4326", crs).transform(-105.0, 60.0)
    assert x == pytest.approx(TRUE_SCALE_POINT[0], abs=1.0)
    assert y == pytest.approx(TRUE_SCALE_POINT[1], abs=1.0)


def _write_projected(path, mapping=HRAP, aux_latlon=False):
    """A 3 x 4 projected grid: 1-D x / y in km (y ascending, the south row first), optionally with 2-D lat / lon
    next to them. Pixel value = file row * 10 + col."""
    with Dataset(path, "w") as ds:
        ds.createDimension("y", 3)
        ds.createDimension("x", 4)
        x = ds.createVariable("x", "f8", ("x",))
        y = ds.createVariable("y", "f8", ("y",))
        x.standard_name, x.units = "projection_x_coordinate", "km"
        y.standard_name, y.units = "projection_y_coordinate", "km"
        x[:] = 100.0 + CELL_KM * np.arange(4)
        y[:] = -3000.0 + CELL_KM * np.arange(3)
        if mapping is not None:
            g = ds.createVariable("polar_stereographic", "i4")
            for key, value in mapping.items():
                setattr(g, key, value)
        if aux_latlon:
            lat = ds.createVariable("lat", "f8", ("y", "x"))
            lon = ds.createVariable("lon", "f8", ("y", "x"))
            lat.standard_name, lat.units = "latitude", "degrees_north"
            lon.standard_name, lon.units = "longitude", "degrees_east"
            lat[:] = np.full((3, 4), 45.0)
            lon[:] = np.full((3, 4), -100.0)
        v = ds.createVariable("precip", "f4", ("y", "x"), fill_value=-9999.0)
        if mapping is not None:
            v.grid_mapping = "polar_stereographic"
        if aux_latlon:
            v.coordinates = "lat lon"
        v[:] = (np.arange(3)[:, None] * 10 + np.arange(4)[None, :]).astype("f4")
    return path


def _write_curvilinear(path):
    """2-D lat / lon only (a swath-like grid), no CRS anywhere."""
    with Dataset(path, "w") as ds:
        ds.createDimension("y", 3)
        ds.createDimension("x", 4)
        lat = ds.createVariable("lat", "f8", ("y", "x"))
        lon = ds.createVariable("lon", "f8", ("y", "x"))
        lat.standard_name, lon.standard_name = "latitude", "longitude"
        lat[:] = 45.0 + np.arange(12).reshape(3, 4) * 0.01
        lon[:] = -100.0 + np.arange(12).reshape(3, 4) * 0.01
        ds.createVariable("precip", "f4", ("y", "x"))[:] = np.zeros((3, 4), dtype="f4")
    return path


def _read(path):
    return list(
        NetcdfRasterReader({"path": path, "variable": "precip"}).read(
            _FilePartition(path, -1)
        )
    )


@pytest.mark.parametrize("aux_latlon", [False, True])
def test_a_cf_grid_mapping_gives_the_crs_and_a_metre_transform(tmp_path, aux_latlon):
    p = _write_projected(str(tmp_path / "hrap.nc"), aux_latlon=aux_latlon)
    ((_, tile),) = _read(p)
    with MemoryFile(bytes(tile[1])) as mf, mf.open() as rds:
        arr, t = rds.read(1), rds.transform
    np.testing.assert_array_equal(
        arr[0], [20, 21, 22, 23]
    )  # north-up: the last file row first
    assert tile[7] != "EPSG:4326"
    _assert_hrap(tile[7])
    # x / y are in km and the CRS is in metres: the pixel is 4762.5 m; the origin is the outer corner of the first cell.
    assert t.a == pytest.approx(4762.5) and t.e == pytest.approx(-4762.5)
    assert t.c == pytest.approx(100_000.0 - 2381.25, abs=0.01)
    assert t.f == pytest.approx((-3000.0 + 2 * CELL_KM) * 1000.0 + 2381.25, abs=0.01)


@pytest.mark.parametrize("aux_latlon", [False, True])
def test_a_projected_grid_with_no_crs_is_refused(tmp_path, aux_latlon):
    p = _write_projected(
        str(tmp_path / "nocrs.nc"), mapping=None, aux_latlon=aux_latlon
    )
    with pytest.raises(ValueError, match="no CRS") as err:
        _read(p)
    assert "set crs_wkt on the grid mapping, or read it with raster_gbx" in str(
        err.value
    )


def test_a_grid_with_2d_lat_lon_and_no_crs_is_refused(tmp_path):
    p = _write_curvilinear(str(tmp_path / "swath.nc"))
    with (
        _netcdf.open_dataset(p, None) as ds,
        pytest.raises(ValueError, match="no CRS") as err,
    ):
        _netcdf.grid_transform_crs(ds, "precip")
    assert "read it with raster_gbx" in str(err.value)


def test_a_geographic_grid_with_no_crs_stays_epsg_4326(tmp_path):
    p = str(tmp_path / "geo.nc")
    with Dataset(p, "w") as ds:
        ds.createDimension("lat", 2)
        ds.createDimension("lon", 3)
        ds.createVariable("lat", "f8", ("lat",))[:] = [10.5, 10.0]
        ds.createVariable("lon", "f8", ("lon",))[:] = [5.0, 5.5, 6.0]
        ds["lat"].standard_name, ds["lon"].standard_name = "latitude", "longitude"
        ds.createVariable("data", "f4", ("lat", "lon"))[:] = np.zeros(
            (2, 3), dtype="f4"
        )
    with _netcdf.open_dataset(p, None) as ds:
        assert _netcdf._crs_string(ds, "data") == "EPSG:4326"
        _, crs = _netcdf.grid_transform_crs(ds, "data")
    assert crs == "EPSG:4326"


def test_crs_from_cf_builds_the_hrap_crs():
    crs = crs_from_cf(HRAP)
    assert isinstance(crs, str)
    _assert_hrap(crs)


def test_crs_from_cf_takes_numpy_attribute_values():
    _assert_hrap(
        crs_from_cf(
            {
                k: (np.float64(v) if not isinstance(v, str) else v)
                for k, v in HRAP.items()
            }
        )
    )


def test_crs_from_cf_refuses_attributes_that_name_no_mapping():
    with pytest.raises(ValueError, match="grid_mapping_name"):
        crs_from_cf({"earth_radius": 6371200.0})


def test_crs_from_cf_refuses_an_unknown_mapping_with_a_fix():
    with pytest.raises(ValueError, match="crs_wkt"):
        crs_from_cf({"grid_mapping_name": "no_such_mapping"})
