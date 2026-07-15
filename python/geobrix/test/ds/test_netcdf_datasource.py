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


# --- raster mode (Task 3) ---------------------------------------------------


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
    assert set(row["tile"]["metadata"].keys()) == EXPECTED_METADATA_KEYS
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
        assert out.crs.to_epsg() == 4326
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_raster_mode_rejects_curvilinear(tmp_path):
    f = tmp_path / "curv.nc"
    _write_curvilinear(str(f))
    from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
    from databricks.labs.gbx.ds.raster import _FilePartition

    reader = NetcdfRasterReader({"path": str(f), "variable": "ch4"})
    with pytest.raises(ValueError, match="vector"):
        list(reader.read(_FilePartition(str(f), reader.size_mib)))


# --- vector mode (Task 4) ---------------------------------------------------


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


# --- registration (Task 5) --------------------------------------------------


def test_register_exposes_netcdf_gbx(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register

    f = tmp_path / "grid.nc"
    _write_regular_grid(str(f))
    register(spark, only=["netcdf"])
    df = spark.read.format("netcdf_gbx").option("variable", "ch4").load(str(f))
    assert df.count() == 1
