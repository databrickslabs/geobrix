"""Integration tests for the netcdf_gbx DataSource (uses local Spark)."""

import numpy as np
import pytest
import shapely
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
from databricks.labs.gbx.pyrx import _serde

EXPECTED_METADATA_KEYS = {
    "path",
    "sourcePath",
    "driver",
    "format",
    "last_command",
    "last_error",
    "all_parents",
    "size",
    "compression",
    "isZipped",
    "isSubset",
}


def _write_regular_grid(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("ch4", "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


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


def _write_points(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("obs", 5)
        lat = ds.createVariable("latitude", "f8", ("obs",))
        lon = ds.createVariable("longitude", "f8", ("obs",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 50.1, 50.2, 50.3, 50.4]
        lon[:] = [10.0, 10.1, 10.2, 10.3, 10.4]
        v = ds.createVariable("ch4", "f4", ("obs",))
        v[:] = np.arange(5, dtype="float32")
        qa = ds.createVariable("qa_value", "i4", ("obs",))
        qa[:] = np.array([0, 1, 0, 1, 1], dtype="int32")


def _write_regular_grid_two(path):
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


# --- raster mode ---------------------------------------------------


def test_raster_schema_matches_tile_schema():
    ds = NetcdfGbxDataSource(options={"path": "/tmp/none", "variable": "ch4"})
    schema = ds.schema()
    assert [f.name for f in schema.fields] == ["source", "tile"]
    assert schema["tile"].dataType == _serde.TILE_SCHEMA


def test_raster_read_round_trip(spark, tmp_path):
    f = tmp_path / "grid.nc"
    _write_regular_grid(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").option("variable", "ch4").load(str(f))
    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["tile"]["cellid"] == -1
    assert EXPECTED_METADATA_KEYS <= set(row["tile"]["metadata"].keys())
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
        assert out.crs.to_epsg() == 4326
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_raster_bare_load_returns_all_grid_variables(spark, tmp_path):
    f = tmp_path / "grid2.nc"
    _write_regular_grid_two(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").load(str(f))  # NO variable option
    rows = df.collect()
    assert len(rows) == 2  # one tile per grid variable
    sources = sorted(r["source"] for r in rows)
    assert sources[0].endswith(":ch4") and sources[1].endswith(":co")


def test_raster_variable_option_filters(spark, tmp_path):
    f = tmp_path / "grid2.nc"
    _write_regular_grid_two(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").option("variable", "co").load(str(f))
    rows = df.collect()
    assert len(rows) == 1 and rows[0]["source"].endswith(":co")


def test_raster_mode_skips_curvilinear_variable(spark, tmp_path):
    f = tmp_path / "curv.nc"
    _write_curvilinear(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    # bare load: curvilinear var is not a readable GRID -> zero rows, no error
    df = spark.read.format("netcdf_gbx").load(str(f))
    assert df.count() == 0


# --- vector mode ---------------------------------------------------


def test_vector_schema_columns(tmp_path):
    from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

    f = tmp_path / "pts.nc"
    _write_points(str(f))
    reader = NetcdfVectorReader({"path": str(f), "variables": "ch4,qa_value"})
    schema = reader.schema()
    assert [fld.name for fld in schema.fields] == [
        "ch4",
        "qa_value",
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
    ]
    from pyspark.sql.types import BinaryType, FloatType, IntegerType, StringType

    assert isinstance(schema["ch4"].dataType, FloatType)
    assert isinstance(schema["qa_value"].dataType, IntegerType)
    assert isinstance(schema["geom_0"].dataType, BinaryType)
    assert isinstance(schema["geom_0_srid"].dataType, StringType)


def test_vector_schema_reads_no_variable_data(tmp_path, monkeypatch):
    """schema() takes each variable's type from its metadata: it never reads (and
    decodes) a selected variable's data, which for a large file is all of it."""
    from xarray.backends import netCDF4_

    from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

    read = []
    real = netCDF4_.NetCDF4ArrayWrapper._getitem

    def spy(self, key):
        read.append(self.variable_name)
        return real(self, key)

    monkeypatch.setattr(netCDF4_.NetCDF4ArrayWrapper, "_getitem", spy)
    f = tmp_path / "pts.nc"
    _write_points(str(f))
    schema = NetcdfVectorReader({"path": str(f), "variables": "ch4,qa_value"}).schema()
    assert schema.fieldNames()[:2] == ["ch4", "qa_value"]
    assert not {"ch4", "qa_value"} & set(read), read


def test_vector_read_dsg_points(spark, tmp_path):
    f = tmp_path / "pts.nc"
    _write_points(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4,qa_value")
        .load(str(f))
    )
    rows = df.orderBy("ch4").collect()
    assert len(rows) == 5
    assert rows[0]["geom_0_srid"] == "4326"
    pt = shapely.from_wkb(bytes(rows[0]["geom_0"]))
    assert pt.x == pytest.approx(10.0) and pt.y == pytest.approx(50.0)
    assert rows[1]["qa_value"] == 1  # ch4==1 -> qa 1


def test_vector_reader_types_a_time_a_date_and_a_flag_as_their_cf_attributes_say(
    tmp_path,
):
    """A CF time variable reads as a timestamp (UTC), a whole-day count in days as a date, a variable that
    flags 0 / 1 as ``false true`` as a boolean, and a NULL stays NULL: not epoch nanoseconds as
    text and a float, which is what the decoded values would give. An int8 with no flag
    attributes is still an integer."""
    import datetime

    from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

    f = tmp_path / "cf.nc"
    with Dataset(str(f), "w") as ds:
        ds.createDimension("obs", 2)
        for name, v in (("latitude", [50.0, 51.0]), ("longitude", [10.0, 11.0])):
            var = ds.createVariable(name, "f8", ("obs",))
            var.standard_name = name
            var[:] = v
        seen = ds.createVariable("seen", "f8", ("obs",), fill_value=-1e30)
        seen.units, seen.calendar = (
            "seconds since 1970-01-01 00:00:00",
            "proleptic_gregorian",
        )
        seen[:] = np.ma.masked_array([1772368215.25, 0.0], mask=[False, True])
        day = ds.createVariable("day", "i4", ("obs",), fill_value=-9999)
        day.units, day.calendar = (
            "days since 1970-01-01 00:00:00",
            "proleptic_gregorian",
        )
        day[:] = np.ma.masked_array([20513, 0], mask=[False, True])
        # a day count that is not whole days, or whose base is not midnight, has a time of day
        half = ds.createVariable("half", "f8", ("obs",), fill_value=-1e30)
        half.units, half.calendar = ("days since 2020-01-01", "proleptic_gregorian")
        half[:] = np.ma.masked_array([0.5, 0.0], mask=[False, True])
        noon = ds.createVariable("noon", "i4", ("obs",))
        noon.units, noon.calendar = (
            "days since 2000-01-01 12:00:00",
            "proleptic_gregorian",
        )
        noon[:] = [0, 1]
        scaled = ds.createVariable("scaled", "i2", ("obs",))
        scaled.units, scaled.calendar = ("days since 2020-01-01", "proleptic_gregorian")
        scaled.scale_factor = 0.5
        scaled[:] = [0.5, 1.0]  # packed as the integers 1 and 2
        ok = ds.createVariable("ok", "i1", ("obs",), fill_value=-127)
        ok.flag_values, ok.flag_meanings = np.array([0, 1], dtype="i1"), "false true"
        ok[:] = np.ma.masked_array([1, 0], mask=[False, True])
        ds.createVariable("qa", "i1", ("obs",))[:] = [3, 4]

    reader = NetcdfVectorReader({"path": str(f)})
    types = {fld.name: fld.dataType.simpleString() for fld in reader.schema().fields}
    assert [
        types[c] for c in ("seen", "day", "half", "noon", "scaled", "ok", "qa")
    ] == [
        "timestamp",
        "date",
        "timestamp",
        "timestamp",
        "timestamp",
        "boolean",
        "int",
    ]
    (part,) = reader.partitions()
    names = [fld.name for fld in reader.schema().fields]
    rows = [dict(zip(names, r)) for r in reader.read(part)]
    utc = datetime.timezone.utc
    assert [r["seen"] for r in rows] == [
        datetime.datetime(2026, 3, 1, 12, 30, 15, 250000, tzinfo=utc),
        None,
    ]
    assert [r["day"] for r in rows] == [datetime.date(2026, 3, 1), None]
    assert [r["half"] for r in rows] == [
        datetime.datetime(2020, 1, 1, 12, tzinfo=utc),
        None,
    ]
    assert [r["noon"] for r in rows] == [
        datetime.datetime(2000, 1, 1, 12, tzinfo=utc),
        datetime.datetime(2000, 1, 2, 12, tzinfo=utc),
    ]
    assert [r["scaled"] for r in rows] == [
        datetime.datetime(2020, 1, 1, 12, tzinfo=utc),
        datetime.datetime(2020, 1, 2, tzinfo=utc),
    ]
    assert [r["ok"] for r in rows] == [True, None]
    assert [r["qa"] for r in rows] == [3, 4]


def test_vector_read_curvilinear_to_points(spark, tmp_path):
    f = tmp_path / "curv.nc"
    _write_curvilinear(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4")
        .load(str(f))
    )
    assert df.count() == 6  # one point per cell (2x3)


def test_vector_bare_load_returns_all_dsg_variables(spark, tmp_path):
    f = tmp_path / "pts.nc"
    _write_points(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = (
        spark.read.format("netcdf_gbx").option("mode", "vector").load(str(f))
    )  # no variables
    assert {"ch4", "qa_value"}.issubset(set(df.columns))
    assert df.count() == 5


# --- registration --------------------------------------------------


def test_register_exposes_netcdf_gbx(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register

    f = tmp_path / "grid.nc"
    _write_regular_grid(str(f))
    register(spark, only=["netcdf"])
    df = spark.read.format("netcdf_gbx").option("variable", "ch4").load(str(f))
    assert df.count() == 1
