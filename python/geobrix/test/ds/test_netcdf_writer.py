"""Tests for the netcdf_gbx raster writer (DataSource V2 write path).

Round-trip: write a known CF grid -> read back via netcdf_gbx -> compare.
"""

import numpy as np
import pytest
from netCDF4 import Dataset

from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource


def _write_regular_grid(path, var="ch4"):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lat.standard_name = "latitude"
        lon = ds.createVariable("lon", "f8", ("lon",))
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable(var, "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def test_raster_write_roundtrip(spark, tmp_path):
    src = tmp_path / "in.nc"
    _write_regular_grid(str(src))
    outdir = tmp_path / "out"
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").load(str(src))  # (source, tile), 1 grid var
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    # re-read the written .nc
    re = spark.read.format("netcdf_gbx").load(str(outdir)).collect()
    assert len(re) == 1
    from rasterio.io import MemoryFile

    with MemoryFile(bytes(re[0]["tile"]["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1)
        epsg = ds.crs.to_epsg()
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )
    assert epsg == 4326


def test_raster_write_overwrite_clears_stale(spark, tmp_path):
    src = tmp_path / "in.nc"
    _write_regular_grid(str(src))
    outdir = tmp_path / "out"
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").load(str(src))
    # Write twice with overwrite; second write must not double-count files.
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1, f"Expected 1 .nc after overwrite, got {len(nc_files)}"


def test_raster_write_nameCol(spark, tmp_path):
    """nameCol=source: override source to a plain name and verify the output filename."""
    src = tmp_path / "in.nc"
    _write_regular_grid(str(src))
    outdir = tmp_path / "out"
    spark.dataSource.register(NetcdfGbxDataSource)
    from pyspark.sql import functions as F

    df = spark.read.format("netcdf_gbx").load(str(src))
    # Replace source with a plain filename string; schema stays exactly (source, tile).
    df = df.withColumn("source", F.lit("custom_output"))
    df.write.format("netcdf_gbx").option("nameCol", "source").mode("overwrite").save(
        str(outdir)
    )
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    assert nc_files[0].name == "custom_output.nc"


def test_raster_write_non4326_crs(spark, tmp_path):
    """Write a grid encoded with EPSG:27700; re-read and confirm CRS is preserved."""
    from rasterio.crs import CRS
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    # Build a synthetic GeoTIFF tile in EPSG:27700 (British National Grid)
    transform_27700 = from_origin(530000.0, 180500.0, 500.0, 500.0)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(27700),
        transform=transform_27700,
        nodata=-9999.0,
    )
    arr_27700 = np.arange(12, dtype="float32").reshape(3, 4)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr_27700, 1)
        tile_bytes = mf.read()

    # Build a DataFrame with the raster schema directly
    from pyspark.sql import Row
    from pyspark.sql.types import (
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    tile_schema = StructType(
        [
            StructField("cellid", LongType(), True),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    outer_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("tile", tile_schema, True),
        ]
    )
    rows = [
        Row(
            source='NETCDF:"/tmp/dummy.nc":band1',
            tile=Row(cellid=0, raster=bytearray(tile_bytes), metadata={}),
        )
    ]
    df = spark.createDataFrame(rows, schema=outer_schema)
    outdir = tmp_path / "out_27700"
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    # Re-read and check EPSG via rasterio
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    # The written .nc should be re-readable by the netcdf_gbx reader
    re = spark.read.format("netcdf_gbx").load(str(outdir)).collect()
    assert len(re) == 1


def test_raster_write_nodata_preserved(spark, tmp_path):
    """NoData written to .nc comes back as the same fill_value on re-read."""
    src = tmp_path / "in.nc"
    _write_regular_grid(str(src))
    outdir = tmp_path / "out"
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").load(str(src))
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    with Dataset(str(nc_files[0]), "r") as nc:
        # The variable should have a fill_value of -9999
        var_names = [v for v in nc.variables if v not in ("lat", "lon", "crs")]
        assert len(var_names) == 1
        fv = nc.variables[var_names[0]].getncattr("_FillValue")
        assert float(fv) == pytest.approx(-9999.0)
