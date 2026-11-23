"""netcdf_gbx raster mode: the CRS comes from the file (CF grid_mapping); EPSG:4326 is the default for
geographic grids only; any other grid with no CRS is refused. A projected axis is scaled to the unit of its CRS.
Vector mode reads a projected grid from its 2-D lat / lon, and refuses one that has none.
"""

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
# The HRAP cell, in kilometres (x and y are in km, as in the NWS files).
CELL_KM = 4.7625
# True scale at the standard parallel: the point (105 W, 60 N) is R * cos(60 deg) = 3,185,600 m from the pole, on the
# central meridian, which runs along -y on the north-polar plane. A wrong radius or a wrong parallel moves it.
TRUE_SCALE_POINT = (0.0, -3185600.0)
# The 2-D aux lat / lon offsets: row / 10 + col / 100 degrees, so every cell has its own position.
AUX_STEP = np.arange(3)[:, None] / 10.0 + np.arange(4)[None, :] / 100.0
FILE_VALUES = np.arange(3)[:, None] * 10 + np.arange(4)[None, :]


def _assert_hrap(crs):
    x, y = get_transformer("EPSG:4326", crs).transform(-105.0, 60.0)
    assert x == pytest.approx(TRUE_SCALE_POINT[0], abs=1.0)
    assert y == pytest.approx(TRUE_SCALE_POINT[1], abs=1.0)


def _assert_hrap_metre_transform(t):
    # x / y are in km and the CRS is in metres: the pixel is 4762.5 m; the origin is the outer corner of the first cell.
    assert t.a == pytest.approx(4762.5) and t.e == pytest.approx(-4762.5)
    assert t.c == pytest.approx(100_000.0 - 2381.25, abs=0.01)
    assert t.f == pytest.approx((-3000.0 + 2 * CELL_KM) * 1000.0 + 2381.25, abs=0.01)


def _write_projected(path, mapping=HRAP, aux_latlon=False, units="km", extra=None):
    """A 3 x 4 projected grid: 1-D x / y in ``units`` (y ascending, the south row first), optionally with 2-D lat / lon
    next to them (45 + AUX_STEP, -100 + AUX_STEP). Pixel value = file row * 10 + col. ``mapping`` and ``extra``
    (``crs_wkt``, ``spatial_epsg``) are the attributes of the grid-mapping variable; None writes no such variable.
    """
    with Dataset(path, "w") as ds:
        ds.createDimension("y", 3)
        ds.createDimension("x", 4)
        x = ds.createVariable("x", "f8", ("x",))
        y = ds.createVariable("y", "f8", ("y",))
        x.standard_name = "projection_x_coordinate"
        y.standard_name = "projection_y_coordinate"
        if units is not None:
            x.units, y.units = units, units
        x[:] = 100.0 + CELL_KM * np.arange(4)
        y[:] = -3000.0 + CELL_KM * np.arange(3)
        if mapping is not None:
            g = ds.createVariable("polar_stereographic", "i4")
            for key, value in {**mapping, **(extra or {})}.items():
                setattr(g, key, value)
        if aux_latlon:
            lat = ds.createVariable("lat", "f8", ("y", "x"))
            lon = ds.createVariable("lon", "f8", ("y", "x"))
            lat.standard_name, lat.units = "latitude", "degrees_north"
            lon.standard_name, lon.units = "longitude", "degrees_east"
            lat[:] = 45.0 + AUX_STEP
            lon[:] = -100.0 + AUX_STEP
        v = ds.createVariable("precip", "f4", ("y", "x"), fill_value=-9999.0)
        if mapping is not None:
            v.grid_mapping = "polar_stereographic"
        if aux_latlon:
            v.coordinates = "lat lon"
        v[:] = FILE_VALUES.astype("f4")
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


def _read(path, **options):
    reader = NetcdfRasterReader({"path": path, "variable": "precip", **options})
    return list(reader.read(_FilePartition(path, -1)))


def _transform_crs(path):
    with _netcdf.open_dataset(path, None) as ds:
        return _netcdf.grid_transform_crs(ds, "precip")


@pytest.mark.parametrize("aux_latlon", [False, True])
def test_a_cf_grid_mapping_gives_the_crs_and_a_metre_transform(tmp_path, aux_latlon):
    p = _write_projected(str(tmp_path / "hrap.nc"), aux_latlon=aux_latlon)
    ((_, tile),) = _read(p)
    with MemoryFile(bytes(tile[1])) as mf, mf.open() as rds:
        arr, t = rds.read(1), rds.transform
    # North-up: the last file row first.
    np.testing.assert_array_equal(arr[0], [20, 21, 22, 23])
    assert tile[7] != "EPSG:4326"
    _assert_hrap(tile[7])
    _assert_hrap_metre_transform(t)


def test_crs_wkt_with_km_axes_gives_a_metre_transform(tmp_path):
    # CF 1.7+ producers write crs_wkt next to the grid_mapping_name attributes: crs_wkt wins, and the km axes still
    # scale to the metres of that CRS.
    p = _write_projected(str(tmp_path / "wkt.nc"), extra={"crs_wkt": crs_from_cf(HRAP)})
    ((_, tile),) = _read(p)
    with MemoryFile(bytes(tile[1])) as mf, mf.open() as rds:
        _assert_hrap_metre_transform(rds.transform)
    _assert_hrap(tile[7])


def test_km_axes_scale_to_the_unit_of_the_crs(tmp_path):
    # EPSG:2264 (North Carolina State Plane) is in US survey feet: a 4.7625 km cell is 4762.5 / 0.3048006096 ft.
    p = _write_projected(
        str(tmp_path / "ft.nc"), mapping={}, extra={"spatial_epsg": 2264}
    )
    t, crs = _transform_crs(p)
    assert crs == "EPSG:2264"
    assert t.a == pytest.approx(4762.5 / 0.3048006096, rel=1e-9)


def test_axes_with_no_units_are_in_the_unit_of_the_crs(tmp_path):
    p = _write_projected(
        str(tmp_path / "nounits.nc"), units=None, extra={"crs_wkt": crs_from_cf(HRAP)}
    )
    t, _ = _transform_crs(p)
    assert t.a == pytest.approx(CELL_KM)


def test_an_axis_unit_it_does_not_know_is_refused_with_a_fix(tmp_path):
    p = _write_projected(str(tmp_path / "furlong.nc"), units="furlong")
    with pytest.raises(ValueError, match="not metres or kilometres") as err:
        _transform_crs(p)
    assert "set the axis units to m or km" in str(err.value)
    assert "the CRS then fixes the unit" not in str(err.value)


@pytest.mark.parametrize("aux_latlon", [False, True])
def test_a_projected_grid_with_no_crs_is_refused(tmp_path, aux_latlon):
    p = _write_projected(
        str(tmp_path / "nocrs.nc"), mapping=None, aux_latlon=aux_latlon
    )
    with pytest.raises(ValueError, match="no CRS") as err:
        _read(p)
    assert "add a CF grid mapping variable" in str(err.value)
    # GDAL finds no CRS in it either: raster_gbx is no fix.
    assert "raster_gbx" not in str(err.value)


def test_a_grid_with_2d_lat_lon_and_no_crs_is_refused(tmp_path):
    p = _write_curvilinear(str(tmp_path / "swath.nc"))
    with pytest.raises(ValueError, match="no CRS") as err:
        _transform_crs(p)
    assert "mode=vector" in str(err.value)


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


def test_bbox_in_epsg_4326_cuts_a_projected_grid(tmp_path):
    p = _write_projected(str(tmp_path / "hrap.nc"))
    t, crs = _transform_crs(p)
    # The centres of north-up (row 0, col 1) and (row 1, col 2), in lon / lat: the bbox is their envelope.
    xs, ys = zip(*(t * (c + 0.5, r + 0.5) for r, c in ((0, 1), (1, 2))))
    lons, lats = get_transformer(crs, "EPSG:4326").transform(list(xs), list(ys))
    ((_, tile),) = _read(
        p, bbox=f"{min(lons)},{min(lats)},{max(lons)},{max(lats)}", bboxCrs="EPSG:4326"
    )
    assert tile[8]["sourceWindow"] == "1,0,2,2"
    with MemoryFile(bytes(tile[1])) as mf, mf.open() as rds:
        # North-up rows 0..1 are file rows 2..1.
        np.testing.assert_array_equal(rds.read(1), [[21, 22], [11, 12]])


def _vector_rows(path, **options):
    from databricks.labs.gbx.ds._netcdf_vector import (
        NetcdfVectorReader,
        _NcFilePartition,
    )

    reader = NetcdfVectorReader({"path": path, **options})
    return reader.schema(), list(reader.read(_NcFilePartition(path)))


def test_vector_mode_reads_a_projected_grid_from_its_2d_lat_lon(tmp_path):
    import shapely

    p = _write_projected(str(tmp_path / "aux.nc"), aux_latlon=True)
    schema, rows = _vector_rows(p)
    assert [f.name for f in schema.fields] == [
        "precip",
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
    ]
    assert len(rows) == 12
    points = shapely.from_wkb([r[1] for r in rows])
    np.testing.assert_array_equal([r[0] for r in rows], FILE_VALUES.ravel())
    np.testing.assert_allclose(shapely.get_x(points), (-100.0 + AUX_STEP).ravel())
    np.testing.assert_allclose(shapely.get_y(points), (45.0 + AUX_STEP).ravel())
    assert {r[2] for r in rows} == {"4326"}


@pytest.mark.parametrize("options", [{}, {"variable": "precip"}])
def test_vector_mode_refuses_a_projected_grid_with_no_2d_lat_lon(tmp_path, options):
    p = _write_projected(str(tmp_path / "proj.nc"))
    with pytest.raises(
        ValueError, match="this is a projected grid: read it in raster mode"
    ):
        _vector_rows(p, **options)


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
