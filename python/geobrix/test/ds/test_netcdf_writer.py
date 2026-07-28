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
    """Writer emits a crs variable with spatial_epsg when source tile CRS != 4326.

    The netcdf_gbx reader only classifies geographic grids, so a projected-CRS tile
    cannot round-trip through the reader. Instead, we assert directly on the written
    .nc that the crs variable exists and carries the correct spatial_epsg attribute,
    which _netcdf._crs_string reads back on re-open.
    """
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
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    # Assert the crs variable exists and spatial_epsg == 27700.
    # _netcdf._crs_string reads getattr(v, "spatial_epsg", None) to recover the EPSG.
    with Dataset(str(nc_files[0]), "r") as nc:
        assert (
            "crs" in nc.variables
        ), "Expected a 'crs' grid_mapping variable for non-4326 CRS"
        crs_var = nc.variables["crs"]
        assert hasattr(
            crs_var, "spatial_epsg"
        ), "crs variable must carry spatial_epsg attribute"
        assert int(crs_var.spatial_epsg) == 27700
        # Pixels must also match exactly.
        data_vars = [v for v in nc.variables if v not in ("lat", "lon", "crs")]
        assert len(data_vars) == 1
        np.testing.assert_array_equal(
            np.array(nc.variables[data_vars[0]][:]), arr_27700
        )


def test_raster_write_varNameCol(spark, tmp_path):
    """varNameCol overrides the output variable name (and drives the stem filename)."""
    src = tmp_path / "in.nc"
    _write_regular_grid(str(src))
    outdir = tmp_path / "out"
    spark.dataSource.register(NetcdfGbxDataSource)
    from pyspark.sql import functions as F

    df = spark.read.format("netcdf_gbx").load(str(src))
    # Override source so it carries no variable selector, forcing varNameCol to drive both.
    df = df.withColumn("source", F.lit("no_selector_here"))
    df.write.format("netcdf_gbx").option("varNameCol", "source").mode("overwrite").save(
        str(outdir)
    )
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    # varNameCol value "no_selector_here" drives both variable name and stem.
    assert nc_files[0].name == "no_selector_here.nc"
    with Dataset(str(nc_files[0]), "r") as nc:
        assert (
            "no_selector_here" in nc.variables
        ), f"Expected variable 'no_selector_here', got {list(nc.variables)}"


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


# ---------------------------------------------------------------------------
# Vector writer tests
# ---------------------------------------------------------------------------


def test_vector_write_roundtrip(spark, tmp_path):
    import shapely
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("ch4", FloatType(), True),
            StructField("qa_value", IntegerType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    pts = [
        (
            float(i),
            i % 2,
            bytes(shapely.to_wkb(shapely.Point(10.0 + i * 0.1, 50.0 + i * 0.1))),
            "4326",
            "EPSG:4326",
        )
        for i in range(5)
    ]
    df = spark.createDataFrame(pts, schema)
    spark.dataSource.register(NetcdfGbxDataSource)
    outdir = tmp_path / "vout"
    # coalesce(1): single partition -> single .nc, deterministic round-trip.
    (
        df.coalesce(1)
        .write.format("netcdf_gbx")
        .option("mode", "vector")
        .mode("overwrite")
        .save(str(outdir))
    )
    re = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4,qa_value")
        .load(str(outdir))
        .orderBy("ch4")
        .collect()
    )
    assert len(re) == 5
    pt0 = shapely.from_wkb(bytes(re[0]["geom_0"]))
    assert pt0.x == pytest.approx(10.0) and pt0.y == pytest.approx(50.0)
    assert re[1]["qa_value"] == 1


def test_vector_write_featuretype_and_obs(spark, tmp_path):
    """Output .nc has featureType='point' global attr and obs dim of correct size."""
    import shapely
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("temp", FloatType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    pts = [
        (
            float(i),
            bytes(shapely.to_wkb(shapely.Point(5.0 + i, 45.0 + i))),
            "4326",
            "EPSG:4326",
        )
        for i in range(3)
    ]
    df = spark.createDataFrame(pts, schema)
    spark.dataSource.register(NetcdfGbxDataSource)
    outdir = tmp_path / "vft"
    # coalesce(1): force a single partition so there is exactly 1 output .nc.
    (
        df.coalesce(1)
        .write.format("netcdf_gbx")
        .option("mode", "vector")
        .mode("overwrite")
        .save(str(outdir))
    )
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    with Dataset(str(nc_files[0]), "r") as nc:
        assert nc.featureType == "point"
        assert "obs" in nc.dimensions
        assert nc.dimensions["obs"].size == 3


def test_vector_write_empty_partition_no_file(tmp_path):
    """Empty iterator -> no file written and paths=[]."""
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        StringType,
        StructField,
        StructType,
    )

    from databricks.labs.gbx.ds._write_netcdf import NetcdfVectorGbxWriter

    schema = StructType(
        [
            StructField("val", FloatType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    outdir = str(tmp_path / "empty_out")
    writer = NetcdfVectorGbxWriter({"path": outdir}, schema, overwrite=False)
    msg = writer.write(iter([]))
    assert msg.paths == []
    import os

    assert not os.path.exists(outdir) or not list(
        __import__("glob").glob(os.path.join(outdir, "*.nc"))
    )


def test_vector_write_nameCol(spark, tmp_path):
    """nameCol drives the output filename."""
    import shapely
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("val", FloatType(), True),
            StructField("filename", StringType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    pts = [
        (
            float(i),
            "my_sensor_data",
            bytes(shapely.to_wkb(shapely.Point(10.0 + i, 50.0 + i))),
            "4326",
            "EPSG:4326",
        )
        for i in range(2)
    ]
    df = spark.createDataFrame(pts, schema)
    spark.dataSource.register(NetcdfGbxDataSource)
    outdir = tmp_path / "named"
    # coalesce(1): single partition so nameCol from first row drives the one file.
    (
        df.coalesce(1)
        .write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("nameCol", "filename")
        .mode("overwrite")
        .save(str(outdir))
    )
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1
    assert nc_files[0].name == "my_sensor_data.nc"


def test_vector_write_nullable_attrs(spark, tmp_path):
    """Nullable attribute columns (None rows) write and round-trip without crash.

    Policy:
    - FloatType/DoubleType: None -> NaN fill (_FillValue=NaN); non-null values
      survive exactly.
    - IntegerType/LongType: None -> netCDF4.default_fillvals sentinel
      (_FillValue=<int fill>); non-null values survive exactly.
    """
    import math

    import netCDF4
    import shapely
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("qa_int", IntegerType(), True),
            StructField("ch4_dbl", DoubleType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    # row 0: qa_int=10, ch4_dbl=1.5
    # row 1: qa_int=None, ch4_dbl=None  (the null row)
    # row 2: qa_int=20, ch4_dbl=3.5
    rows = [
        (
            10,
            1.5,
            bytes(shapely.to_wkb(shapely.Point(10.0, 50.0))),
            "4326",
            "EPSG:4326",
        ),
        (
            None,
            None,
            bytes(shapely.to_wkb(shapely.Point(11.0, 51.0))),
            "4326",
            "EPSG:4326",
        ),
        (
            20,
            3.5,
            bytes(shapely.to_wkb(shapely.Point(12.0, 52.0))),
            "4326",
            "EPSG:4326",
        ),
    ]
    df = spark.createDataFrame(rows, schema)
    spark.dataSource.register(NetcdfGbxDataSource)
    outdir = tmp_path / "nullable_out"
    # coalesce(1): single partition so all 3 rows land in one .nc.
    (
        df.coalesce(1)
        .write.format("netcdf_gbx")
        .option("mode", "vector")
        .mode("overwrite")
        .save(str(outdir))
    )
    nc_files = list(outdir.glob("*.nc"))
    assert len(nc_files) == 1

    with netCDF4.Dataset(str(nc_files[0]), "r") as nc:
        qa = nc.variables["qa_int"][:]  # MaskedArray
        ch4 = nc.variables["ch4_dbl"][:]  # MaskedArray
        int_fill = int(nc.variables["qa_int"]._FillValue)

    import numpy.ma as ma

    # Non-null int values survive exactly.
    assert int(qa[0]) == 10
    assert int(qa[2]) == 20
    # Null int cell is masked (CF fill sentinel); underlying fill value matches.
    assert ma.is_masked(qa[1])
    assert int_fill == netCDF4.default_fillvals["i4"]

    # Non-null float values survive exactly.
    assert float(ch4[0]) == pytest.approx(1.5)
    assert float(ch4[2]) == pytest.approx(3.5)
    # Null float cell is masked (NaN fill); underlying data is NaN.
    assert ma.is_masked(ch4[1]) or math.isnan(float(ch4.data[1]))


# ---------------------------------------------------------------------------
# singleFile mode tests
# ---------------------------------------------------------------------------


def test_vector_write_singlefile_one_nc(spark, tmp_path):
    import os

    import shapely
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("ch4", FloatType(), True),
            StructField("qa_value", IntegerType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    pts = [
        (
            float(i),
            i % 2,
            bytes(shapely.to_wkb(shapely.Point(10.0 + i * 0.1, 50.0 + i * 0.1))),
            "4326",
            "EPSG:4326",
        )
        for i in range(12)
    ]
    df = spark.createDataFrame(pts, schema).repartition(4)  # multiple partitions
    spark.dataSource.register(NetcdfGbxDataSource)
    out = tmp_path / "vout_single"
    (
        df.write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("singleFile", "true")
        .mode("overwrite")
        .save(str(out))
    )
    ncs = [f for f in os.listdir(str(out)) if f.endswith(".nc")]
    assert len(ncs) == 1, f"expected ONE .nc, got {ncs}"
    re = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4,qa_value")
        .load(str(out))
        .orderBy("ch4")
        .collect()
    )
    assert len(re) == 12
    pt0 = shapely.from_wkb(bytes(re[0]["geom_0"]))
    assert pt0.x == pytest.approx(10.0) and pt0.y == pytest.approx(50.0)
