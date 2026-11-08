"""What the netcdf_gbx readers make of CF files: grids as tiles, points and curvilinear grids as points."""

import logging

import numpy as np
import pytest
from netCDF4 import Dataset
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader
from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
from databricks.labs.gbx.ds.raster import _FilePartition


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


def raster_rows(path, **options):
    """(variable, transform, crs, pixels) of each tile the raster reader yields."""
    reader = NetcdfRasterReader({"path": path, **options})
    out = []
    for source, (_, raster_bytes, meta) in reader.read(_FilePartition(path, -1)):
        with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as ds:
            out.append((source.split(":")[-1], ds.transform, str(ds.crs), ds.read(1)))
    return out


def vector_rows(path, **options):
    """(schema, rows) of the vector reader; a row is (*variables, wkb, srid, proj)."""
    reader = NetcdfVectorReader({"path": path, **options})
    return reader.schema(), [
        r for part in reader.partitions() for r in reader.read(part)
    ]


def test_a_regular_grid_is_a_north_up_tile_in_wgs84(tmp_path):
    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    ((var, transform, crs, pixels),) = raster_rows(p)
    assert (var, crs) == ("ch4", "EPSG:4326")
    # origin at (lon min - half px, lat max + half px); px = 0.5, north-up => negative y size
    assert (transform.a, transform.e, transform.c, transform.f) == pytest.approx(
        (0.5, -0.5, 9.75, 50.25)
    )
    np.testing.assert_allclose(pixels, np.arange(12, dtype="float32").reshape(3, 4))


def test_a_south_up_grid_is_flipped_to_north_up(tmp_path):
    p = str(tmp_path / "south_up.nc")
    with Dataset(p, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [49.0, 49.5, 50.0]  # ascending: the first row is the southern one
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        ds.createVariable("ch4", "f4", ("lat", "lon"))[:] = np.arange(
            12, dtype="float32"
        ).reshape(3, 4)
    ((_, transform, _, pixels),) = raster_rows(p)
    np.testing.assert_allclose(
        pixels, np.arange(12, dtype="float32").reshape(3, 4)[::-1]
    )
    assert (transform.e, transform.f) == pytest.approx((-0.5, 50.25))


@pytest.mark.parametrize("ntime", [3, 1], ids=["many_steps_warn", "one_step_is_silent"])
def test_a_leading_time_dim_reads_its_first_slice_and_warns_when_it_drops_steps(
    tmp_path, caplog, ntime
):
    p = str(tmp_path / "time.nc")
    _write_grid_with_time(p, ntime=ntime)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        ((_, _, _, pixels),) = raster_rows(p)
    np.testing.assert_allclose(pixels, np.zeros((3, 4), dtype="float32"))
    msgs = [
        r.getMessage() for r in caplog.records if "leading dimension" in r.getMessage()
    ]
    assert bool(msgs) is (ntime > 1)
    assert not msgs or any("time" in m and "size 3" in m for m in msgs), msgs


# file -> (variable, points, first point (lon, lat), values in order) as the vector reader flattens it
POINTS = {
    "points": (_write_points, "value", 5, (10.0, 50.0), np.arange(5)),
    "curvilinear_ravel": (_write_curvilinear, "ch4", 6, (10.0, 50.0), np.arange(6)),
}


@pytest.mark.parametrize(
    "write,name,n,first,values", POINTS.values(), ids=POINTS.keys()
)
def test_the_vector_reader_flattens_points_and_curvilinear_grids(
    tmp_path, write, name, n, first, values
):
    import shapely

    p = str(tmp_path / "f.nc")
    write(p)
    schema, rows = vector_rows(p, variable=name)
    assert schema.fieldNames() == [name, "geom_0", "geom_0_srid", "geom_0_srid_proj"]
    assert len(rows) == n
    assert {r[-2] for r in rows} == {"4326"} and {r[-1] for r in rows} == {"EPSG:4326"}
    g = shapely.from_wkb(rows[0][-3])
    assert (g.x, g.y) == pytest.approx(first)
    np.testing.assert_allclose([r[0] for r in rows], values)


def test_a_points_file_has_no_raster_and_a_grid_has_no_points(tmp_path):
    pts, grid = str(tmp_path / "pts.nc"), str(tmp_path / "grid.nc")
    _write_points(pts)
    _write_regular_grid(grid)
    assert raster_rows(pts) == []
    assert vector_rows(grid)[1] == []


def test_point_arrays_coerces_a_regular_grid_to_a_meshgrid(tmp_path):
    """Kept as a kernel test: the readers never reach it (the vector reader takes no grid variable, see above),
    so no read exercises this branch of point_arrays."""
    from databricks.labs.gbx.ds import _netcdf

    p = str(tmp_path / "grid.nc")
    _write_regular_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        lon, lat, attrs, srid = _netcdf.point_arrays(ds, ["ch4"])
    assert srid == "4326"
    assert lon.shape == (12,) and lat.shape == (12,) and attrs["ch4"].shape == (12,)
    assert lon[0] == pytest.approx(10.0) and lat[0] == pytest.approx(
        50.0
    )  # the first cell is (10.0, 50.0)


@pytest.mark.parametrize(
    "dtype,spark_type",
    [("f4", FloatType), ("f8", DoubleType), ("i4", IntegerType)],
    ids=["float", "double", "int"],
)
def test_attribute_columns_take_the_variables_type(tmp_path, dtype, spark_type):
    p = str(tmp_path / "t.nc")
    with Dataset(p, "w") as ds:
        ds.createDimension("obs", 2)
        for name in ("latitude", "longitude"):
            v = ds.createVariable(name, "f8", ("obs",))
            v.standard_name = name
            v[:] = [1.0, 2.0]
        ds.createVariable("value", dtype, ("obs",))[:] = [1, 2]
    schema, _ = vector_rows(p, variable="value")
    assert isinstance(schema["value"].dataType, spark_type)


# --- which variables a read takes ---------------------------------------------


@pytest.mark.parametrize(
    "options,want",
    [({}, ["ch4", "co"]), ({"variable": "co"}, ["co"])],
    ids=["all_grids_never_the_coordinates", "named_variable"],
)
def test_a_read_takes_every_grid_variable_or_the_named_one(tmp_path, options, want):
    p = str(tmp_path / "g.nc")
    _grid_two_vars(p)
    assert sorted(v for v, *_ in raster_rows(p, **options)) == want
