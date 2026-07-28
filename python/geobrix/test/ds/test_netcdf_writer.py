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


# ---------------------------------------------------------------------------
# Raster singleFile mode tests (merge distinct same-grid vars -> one CF .nc)
# ---------------------------------------------------------------------------


def _grid_tile_bytes(width, height, *, epsg=4326, origin=(10.0, 50.0), res=0.5):
    """Build an in-memory north-up GeoTIFF tile and return its bytes."""
    from rasterio.crs import CRS
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(origin[0], origin[1], res, res)
    arr = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(epsg),
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr, 1)
        return mf.read(), arr


def _raster_df(spark, rows):
    """Build a (source, tile) DataFrame from (source, tile_bytes) pairs."""
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
    spark_rows = [
        Row(
            source=src,
            tile=Row(cellid=0, raster=bytearray(tb), metadata={}),
        )
        for src, tb in rows
    ]
    return spark.createDataFrame(spark_rows, schema=outer_schema)


def test_raster_write_singlefile_multivar(spark, tmp_path):
    """Two DISTINCT vars (tas, pr) on the SAME grid -> one .nc with both."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    tas_bytes, tas_arr = _grid_tile_bytes(4, 3)
    pr_bytes, pr_arr = _grid_tile_bytes(4, 3)
    df = _raster_df(
        spark,
        [('NETCDF:"f":tas', tas_bytes), ('NETCDF:"f":pr', pr_bytes)],
    ).coalesce(1)
    out = tmp_path / "rout_single"
    (
        df.write.format("netcdf_gbx")
        .option("singleFile", "true")
        .mode("overwrite")
        .save(str(out))
    )
    ncs = [f for f in os.listdir(str(out)) if f.endswith(".nc")]
    assert len(ncs) == 1, f"expected ONE .nc, got {ncs}"
    with Dataset(os.path.join(str(out), ncs[0])) as nc:
        assert "tas" in nc.variables and "pr" in nc.variables
        assert nc.variables["tas"].dimensions == ("lat", "lon")
        assert nc.variables["pr"].dimensions == ("lat", "lon")
        assert "lat" in nc.variables and "lon" in nc.variables
        np.testing.assert_array_equal(np.array(nc.variables["tas"][:]), tas_arr)
        np.testing.assert_array_equal(np.array(nc.variables["pr"][:]), pr_arr)


def test_raster_write_singlefile_incompatible_grid_errors(spark, tmp_path):
    """Two vars on DIFFERENT grids + singleFile -> ValueError -> rst_merge_agg."""
    spark.dataSource.register(NetcdfGbxDataSource)
    tas_bytes, _ = _grid_tile_bytes(4, 3)
    pr_bytes, _ = _grid_tile_bytes(5, 6)  # different grid size
    df = _raster_df(
        spark,
        [('NETCDF:"f":tas', tas_bytes), ('NETCDF:"f":pr', pr_bytes)],
    ).coalesce(1)
    out = tmp_path / "rout_incompat"
    with pytest.raises(Exception) as e:
        (
            df.write.format("netcdf_gbx")
            .option("singleFile", "true")
            .mode("overwrite")
            .save(str(out))
        )
    assert "rst_merge_agg" in str(e.value)


def test_raster_write_singlefile_duplicate_var_errors(spark, tmp_path):
    """Two window-tiles of the SAME var + same grid + singleFile -> ValueError."""
    spark.dataSource.register(NetcdfGbxDataSource)
    a_bytes, _ = _grid_tile_bytes(4, 3)
    b_bytes, _ = _grid_tile_bytes(4, 3)
    df = _raster_df(
        spark,
        [('NETCDF:"f":tas', a_bytes), ('NETCDF:"f":tas', b_bytes)],
    ).coalesce(1)
    out = tmp_path / "rout_dup"
    with pytest.raises(Exception) as e:
        (
            df.write.format("netcdf_gbx")
            .option("singleFile", "true")
            .mode("overwrite")
            .save(str(out))
        )
    assert "rst_merge_agg" in str(e.value)


def test_raster_write_singlefile_non4326_and_fillvalue(spark, tmp_path):
    """RASTER singleFile: two DISTINCT vars on the SAME non-4326 grid merge into
    one .nc that carries the projected EPSG (crs.spatial_epsg) AND preserves the
    per-var _FillValue from the source tile NoData."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    tas_bytes, tas_arr = _grid_tile_bytes(
        4, 3, epsg=27700, origin=(530000.0, 180500.0), res=500.0
    )
    pr_bytes, pr_arr = _grid_tile_bytes(
        4, 3, epsg=27700, origin=(530000.0, 180500.0), res=500.0
    )
    df = _raster_df(
        spark,
        [('NETCDF:"f":tas', tas_bytes), ('NETCDF:"f":pr', pr_bytes)],
    ).coalesce(1)
    out = tmp_path / "rout_single_27700"
    (
        df.write.format("netcdf_gbx")
        .option("singleFile", "true")
        .mode("overwrite")
        .save(str(out))
    )
    ncs = [f for f in os.listdir(str(out)) if f.endswith(".nc")]
    assert len(ncs) == 1, f"expected ONE .nc, got {ncs}"
    with Dataset(os.path.join(str(out), ncs[0])) as nc:
        assert "crs" in nc.variables
        assert int(nc.variables["crs"].spatial_epsg) == 27700
        assert "tas" in nc.variables and "pr" in nc.variables
        # _FillValue must survive the singleFile merge for the data vars.
        assert float(nc.variables["tas"].getncattr("_FillValue")) == pytest.approx(
            -9999.0
        )
        assert float(nc.variables["pr"].getncattr("_FillValue")) == pytest.approx(
            -9999.0
        )
        np.testing.assert_array_equal(np.array(nc.variables["tas"][:]), tas_arr)


# ---------------------------------------------------------------------------
# Vector singleFile nullable-attribute coverage
# ---------------------------------------------------------------------------


def test_vector_singlefile_nullable_attrs(spark, tmp_path):
    """Nullable attribute columns round-trip through the singleFile merge path.

    Same policy as parts-mode: float None -> NaN fill, int None -> CF sentinel.
    """
    import math
    import os

    import netCDF4
    import numpy.ma as ma
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
    df = spark.createDataFrame(rows, schema).repartition(2)
    spark.dataSource.register(NetcdfGbxDataSource)
    out = tmp_path / "vnull_single"
    (
        df.write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("singleFile", "true")
        .mode("overwrite")
        .save(str(out))
    )
    ncs = [f for f in os.listdir(str(out)) if f.endswith(".nc")]
    assert len(ncs) == 1, f"expected ONE .nc, got {ncs}"
    with netCDF4.Dataset(os.path.join(str(out), ncs[0]), "r") as nc:
        assert nc.dimensions["obs"].size == 3
        qa = nc.variables["qa_int"][:]
        ch4 = nc.variables["ch4_dbl"][:]
        int_fill = int(nc.variables["qa_int"]._FillValue)
    # exactly one masked int + one masked float (the null row).
    assert int(ma.count_masked(qa)) == 1
    assert int_fill == netCDF4.default_fillvals["i4"]
    good_ints = sorted(int(v) for v in qa.compressed())
    assert good_ints == [10, 20]
    good_floats = sorted(float(v) for v in ch4.compressed())
    assert good_floats == [pytest.approx(1.5), pytest.approx(3.5)]
    assert int(ma.count_masked(ch4)) == 1 or any(
        math.isnan(float(x)) for x in np.array(ch4.data)
    )


# ---------------------------------------------------------------------------
# merge / keepParts / fileName / partPrefix
# ---------------------------------------------------------------------------


def _vector_schema():
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("ch4", FloatType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )


def _vector_points(n):
    import shapely

    return [
        (
            float(i),
            bytes(shapely.to_wkb(shapely.Point(10.0 + i, 50.0 + i))),
            "4326",
            "EPSG:4326",
        )
        for i in range(n)
    ]


def test_vector_merge_dir_no_rerun(spark, tmp_path):
    """Write parts-mode, then merge the dir passing a DIFFERENT DataFrame.

    The merge must ignore the passed DataFrame (no re-run), fold the on-disk
    parts into ONE .nc, and delete the parts (keepParts default false).
    """
    import os

    import shapely

    spark.dataSource.register(NetcdfGbxDataSource)
    schema = _vector_schema()
    df = spark.createDataFrame(_vector_points(6), schema).repartition(3)
    outdir = tmp_path / "vmerge"
    df.write.format("netcdf_gbx").option("mode", "vector").mode("overwrite").save(
        str(outdir)
    )
    parts_before = [f for f in os.listdir(str(outdir)) if f.endswith(".nc")]
    assert len(parts_before) >= 1

    # DIFFERENT DataFrame: a single sentinel point that must NOT appear on merge.
    sentinel = [
        (999.0, bytes(shapely.to_wkb(shapely.Point(99.0, 99.0))), "4326", "EPSG:4326")
    ]
    other = spark.createDataFrame(sentinel, schema)
    (
        other.write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("merge", "true")
        .mode("overwrite")
        .save(str(outdir))
    )
    ncs = [f for f in os.listdir(str(outdir)) if f.endswith(".nc")]
    assert len(ncs) == 1, f"expected ONE merged .nc, got {ncs}"

    re = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", "ch4")
        .load(str(outdir))
        .orderBy("ch4")
        .collect()
    )
    vals = sorted(float(r["ch4"]) for r in re)
    assert vals == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    assert 999.0 not in vals  # proves the DataFrame was ignored


def test_merge_keepParts_true(spark, tmp_path):
    """keepParts=true keeps BOTH the merged file and the source parts."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    schema = _vector_schema()
    df = spark.createDataFrame(_vector_points(6), schema).repartition(3)
    outdir = tmp_path / "vkeep"
    df.write.format("netcdf_gbx").option("mode", "vector").mode("overwrite").save(
        str(outdir)
    )
    parts_before = sorted(f for f in os.listdir(str(outdir)) if f.endswith(".nc"))
    assert len(parts_before) >= 1

    df2 = spark.createDataFrame(_vector_points(1), schema)
    (
        df2.write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("merge", "true")
        .option("keepParts", "true")
        .mode("overwrite")
        .save(str(outdir))
    )
    # merged output named after the dir (case 2 of _resolve_single_file_output).
    assert (outdir / "vkeep.nc").exists()
    parts_after = sorted(
        f for f in os.listdir(str(outdir)) if f.endswith(".nc") and f != "vkeep.nc"
    )
    assert parts_after == parts_before  # parts survived


def test_merge_failure_preserves_parts(spark, tmp_path):
    """A failed merge (incompatible raster grids) leaves ALL parts intact and
    writes no valid-looking partial output, even with keepParts=false."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    tas_bytes, _ = _grid_tile_bytes(4, 3)
    pr_bytes, _ = _grid_tile_bytes(5, 6)  # incompatible grid
    df = _raster_df(
        spark,
        [('NETCDF:"f":tas', tas_bytes), ('NETCDF:"f":pr', pr_bytes)],
    )
    outdir = tmp_path / "rmerge_fail"
    df.write.format("netcdf_gbx").mode("overwrite").save(str(outdir))
    parts_before = sorted(f for f in os.listdir(str(outdir)) if f.endswith(".nc"))
    assert set(parts_before) == {"tas.nc", "pr.nc"}

    dummy, _ = _grid_tile_bytes(4, 3)
    df2 = _raster_df(spark, [('NETCDF:"f":ignored', dummy)])
    with pytest.raises(Exception) as e:
        (
            df2.write.format("netcdf_gbx")
            .option("merge", "true")
            .mode("overwrite")
            .save(str(outdir))
        )
    assert "rst_merge_agg" in str(e.value)
    parts_after = sorted(f for f in os.listdir(str(outdir)) if f.endswith(".nc"))
    assert parts_after == parts_before  # nothing lost
    assert not (outdir / "rmerge_fail.nc").exists()  # no partial output


def test_merge_empty_dir_errors(spark, tmp_path):
    """merge on a directory with no .nc files raises a clear ValueError."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    outdir = tmp_path / "empty_merge"
    os.makedirs(str(outdir), exist_ok=True)
    dummy, _ = _grid_tile_bytes(4, 3)
    df = _raster_df(spark, [('NETCDF:"f":x', dummy)])
    with pytest.raises(Exception) as e:
        (
            df.write.format("netcdf_gbx")
            .option("merge", "true")
            .mode("overwrite")
            .save(str(outdir))
        )
    msg = str(e.value).lower()
    assert "merge" in msg and "no .nc" in msg


def test_partPrefix(spark, tmp_path):
    """partPrefix controls the parts-mode filename stem (<partPrefix>-<uuid>.nc)."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    schema = _vector_schema()
    df = spark.createDataFrame(_vector_points(3), schema).coalesce(1)
    outdir = tmp_path / "vprefix"
    (
        df.write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("partPrefix", "myshard")
        .mode("overwrite")
        .save(str(outdir))
    )
    ncs = [f for f in os.listdir(str(outdir)) if f.endswith(".nc")]
    assert len(ncs) == 1
    assert ncs[0].startswith("myshard-"), ncs


def test_fileName_singlefile(spark, tmp_path):
    """fileName names the singleFile merged output."""
    import os

    spark.dataSource.register(NetcdfGbxDataSource)
    schema = _vector_schema()
    df = spark.createDataFrame(_vector_points(5), schema).repartition(3)
    outdir = tmp_path / "vfname"
    (
        df.write.format("netcdf_gbx")
        .option("mode", "vector")
        .option("singleFile", "true")
        .option("fileName", "combined")
        .mode("overwrite")
        .save(str(outdir))
    )
    ncs = [f for f in os.listdir(str(outdir)) if f.endswith(".nc")]
    assert ncs == ["combined.nc"], ncs
