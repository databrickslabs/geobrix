import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()

MODIS_B01 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
).resolve()
NETCDF = (
    HERE.parents[4]
    / "src/test/resources/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc"
).resolve()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    return spark


def test_rst_avg(spark):
    """Test getting average pixel value."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_avg(rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))).alias(
            "avg_value"
        )
    )

    result = df.collect()
    assert result is not None
    assert result[0]["avg_value"] is not None
    # rst_avg returns an array (one value per band)
    assert isinstance(result[0]["avg_value"], list)
    assert len(result[0]["avg_value"]) > 0
    assert result[0]["avg_value"][0] > 0


def test_rst_min_max(spark):
    """Test getting min and max pixel values."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(
        rx.rst_min(tile_col).alias("min_value"), rx.rst_max(tile_col).alias("max_value")
    )

    result = df.collect()
    assert result is not None
    assert result[0]["min_value"] is not None
    assert result[0]["max_value"] is not None
    # rst_min and rst_max return arrays (one value per band)
    assert isinstance(result[0]["min_value"], list)
    assert isinstance(result[0]["max_value"], list)
    assert len(result[0]["min_value"]) > 0
    assert len(result[0]["max_value"]) > 0
    assert result[0]["min_value"][0] <= result[0]["max_value"][0]


def test_rst_median(spark):
    """Test getting median pixel value."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_median(rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))).alias(
            "median_value"
        )
    )

    result = df.collect()
    assert result is not None
    assert result[0]["median_value"] is not None
    # rst_median returns an array (one value per band)
    assert isinstance(result[0]["median_value"], list)
    assert len(result[0]["median_value"]) > 0


def test_rst_dimensions(spark):
    """Test getting raster dimensions."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(
        rx.rst_width(tile_col).alias("width"),
        rx.rst_height(tile_col).alias("height"),
        rx.rst_pixelwidth(tile_col).alias("pixel_width"),
        rx.rst_pixelheight(tile_col).alias("pixel_height"),
        rx.rst_pixelcount(tile_col).alias("pixel_count"),
    )

    result = df.collect()
    assert result is not None
    assert result[0]["width"] > 0
    assert result[0]["height"] > 0
    assert result[0]["pixel_width"] > 0
    assert result[0]["pixel_height"] > 0
    # rst_pixelcount returns an array (one value per band)
    assert isinstance(result[0]["pixel_count"], list)
    # Note: pixel_count is total pixels, not width * height * bands
    assert result[0]["pixel_count"][0] > 0


def test_rst_georeference(spark):
    """Test getting georeference information."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(
        rx.rst_georeference(tile_col).alias("georeference"),
        rx.rst_upperleftx(tile_col).alias("upper_left_x"),
        rx.rst_upperlefty(tile_col).alias("upper_left_y"),
        rx.rst_scalex(tile_col).alias("scale_x"),
        rx.rst_scaley(tile_col).alias("scale_y"),
        rx.rst_skewx(tile_col).alias("skew_x"),
        rx.rst_skewy(tile_col).alias("skew_y"),
        rx.rst_rotation(tile_col).alias("rotation"),
    )

    result = df.collect()
    assert result is not None
    assert result[0]["georeference"] is not None
    assert result[0]["upper_left_x"] is not None
    assert result[0]["upper_left_y"] is not None
    assert result[0]["scale_x"] is not None
    assert result[0]["scale_y"] is not None
    # Skew and rotation should exist (might be 0)
    assert result[0]["skew_x"] is not None
    assert result[0]["skew_y"] is not None
    assert result[0]["rotation"] is not None


def test_rst_metadata(spark):
    """Test getting raster metadata."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(
        rx.rst_metadata(tile_col).alias("metadata"),
        rx.rst_bandmetadata(tile_col, f.lit(1)).alias("band_metadata"),
        rx.rst_format(tile_col).alias("format"),
        rx.rst_type(tile_col).alias("type"),
        rx.rst_srid(tile_col).alias("srid"),
    )

    result = df.collect()
    assert result is not None
    assert result[0]["metadata"] is not None
    assert result[0]["band_metadata"] is not None
    assert result[0]["format"] == "GTiff"
    assert result[0]["type"] is not None
    assert result[0]["srid"] is not None


def test_rst_bands(spark):
    """Test getting band information."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(rx.rst_numbands(tile_col).alias("num_bands"))

    result = df.collect()
    assert result is not None
    assert result[0]["num_bands"] > 0
    assert result[0]["num_bands"] == 1  # MODIS files are single band


def test_rst_nodata(spark):
    """Test getting NoData values."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(rx.rst_getnodata(tile_col).alias("nodata_values"))

    result = df.collect()
    assert result is not None
    # NoData values might be None or an array
    assert "nodata_values" in result[0].asDict()


def test_rst_boundingbox(spark):
    """Test getting bounding box."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(rx.rst_boundingbox(tile_col).alias("bbox"))

    result = df.collect()
    assert result is not None
    assert result[0]["bbox"] is not None


def test_rst_memsize(spark):
    """Test getting memory size."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(rx.rst_memsize(tile_col).alias("mem_size"))

    result = df.collect()
    assert result is not None
    assert result[0]["mem_size"] is not None
    assert result[0]["mem_size"] > 0


def test_rst_summary(spark):
    """Test getting raster summary."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    df = spark.range(1).select(rx.rst_summary(tile_col).alias("summary"))

    result = df.collect()
    assert result is not None
    assert result[0]["summary"] is not None


def test_rst_subdatasets(spark):
    """Test getting subdatasets from NetCDF."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(NETCDF)), f.lit("NetCDF"))

    df = spark.range(1).select(rx.rst_subdatasets(tile_col).alias("subdatasets"))

    result = df.collect()
    assert result is not None
    assert result[0]["subdatasets"] is not None


def test_rst_getsubdataset(spark):
    """Test extracting subdataset from NetCDF."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(NETCDF)), f.lit("NetCDF"))

    df = spark.range(1).select(
        rx.rst_getsubdataset(tile_col, f.lit("prAdjust")).alias("subdataset")
    )

    result = df.collect()
    assert result is not None
    assert result[0]["subdataset"] is not None


def test_all_accessors_together(spark):
    """Test all accessor functions together to ensure they work in combination."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff"))

    # Test that we can call multiple accessors in one select
    df = spark.range(1).select(
        tile_col.alias("tile"),
        rx.rst_avg(tile_col).alias("avg"),
        rx.rst_min(tile_col).alias("min"),
        rx.rst_max(tile_col).alias("max"),
        rx.rst_width(tile_col).alias("width"),
        rx.rst_height(tile_col).alias("height"),
        rx.rst_numbands(tile_col).alias("bands"),
        rx.rst_format(tile_col).alias("format"),
        rx.rst_srid(tile_col).alias("srid"),
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    # All values should be present
    row_dict = result[0].asDict()
    assert all(
        key in row_dict
        for key in ["avg", "min", "max", "width", "height", "bands", "format", "srid"]
    )
