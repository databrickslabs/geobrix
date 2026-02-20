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
MODIS_B02 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"
).resolve()
MODIS_B03 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF"
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


def test_rst_retile(spark):
    """Test retiling a raster."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_retile(
            rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")),
            f.lit(256),  # tile_width
            f.lit(256),  # tile_height
        ).alias("tiles")
    )

    result = df.collect()
    assert result is not None
    # rst_retile returns an array of tiles
    assert result[0]["tiles"] is not None
    assert isinstance(result[0]["tiles"], (list, tuple))
    # MODIS tile (2400x2400) split into 256x256 tiles should create multiple tiles
    assert len(result[0]["tiles"]) > 1


def test_rst_maketiles(spark):
    """Test making tiles based on size in MB."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_maketiles(
            rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")),
            f.lit(1),  # size_in_mb
        ).alias("tiles")
    )

    result = df.collect()
    assert result is not None
    # rst_maketiles returns an array of tiles
    assert result[0]["tiles"] is not None
    assert isinstance(result[0]["tiles"], (list, tuple))


def test_rst_tooverlappingtiles(spark):
    """Test creating overlapping tiles."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_tooverlappingtiles(
            rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")),
            f.lit(256),  # tile_width
            f.lit(256),  # tile_height
            f.lit(10),  # overlap in pixels
        ).alias("tiles")
    )

    result = df.collect()
    assert result is not None
    # rst_tooverlappingtiles returns an array of tiles
    assert result[0]["tiles"] is not None
    assert isinstance(result[0]["tiles"], (list, tuple))
    assert len(result[0]["tiles"]) > 1


def test_rst_separatebands(spark):
    """Test separating bands into individual rasters."""
    from databricks.labs.gbx.rasterx import functions as rx

    # First create a multi-band raster by combining single bands
    df = spark.createDataFrame(
        [(1, str(MODIS_B01)), (2, str(MODIS_B02)), (3, str(MODIS_B03))], ["id", "path"]
    )

    df = df.withColumn("tile", rx.rst_fromfile(f.col("path"), f.lit("GTiff")))

    # Merge them into a multi-band raster
    multiband_df = df.select(rx.rst_merge(f.collect_list("tile")).alias("multiband"))

    # Now separate the bands
    result_df = multiband_df.select(
        rx.rst_separatebands(f.col("multiband")).alias("bands")
    )

    result = result_df.collect()
    assert result is not None
    # rst_separatebands returns an array of single-band rasters
    assert result[0]["bands"] is not None
    assert isinstance(result[0]["bands"], (list, tuple))
    # Should separate into individual bands (3 in this case from merged multiband)
    assert len(result[0]["bands"]) >= 1


def test_rst_h3_tessellate(spark):
    """Test H3 tessellation of raster."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_h3_tessellate(
            rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")),
            f.lit(4),  # H3 resolution (reduced for performance)
        ).alias("h3_tiles")
    )

    result = df.collect()
    assert result is not None
    # rst_h3_tessellate returns an array of tiles
    assert result[0]["h3_tiles"] is not None
    assert isinstance(result[0]["h3_tiles"], (list, tuple))
    assert len(result[0]["h3_tiles"]) > 0


def test_rst_frombands(spark):
    """Test creating raster from multiple bands."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.createDataFrame(
        [(1, str(MODIS_B01)), (2, str(MODIS_B02)), (3, str(MODIS_B03))], ["id", "path"]
    )

    df = df.withColumn("tile", rx.rst_fromfile(f.col("path"), f.lit("GTiff")))

    result_df = df.select(
        rx.rst_frombands(f.collect_list("tile")).alias("multiband_raster")
    )

    result = result_df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["multiband_raster"] is not None


def test_rst_fromcontent(spark):
    """Test creating raster from binary content."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Read raster as binary content
    df = spark.read.format("binaryFile").load(str(MODIS_B01))

    result_df = df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("raster")
    )

    result = result_df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["raster"] is not None


def test_rst_fromfile(spark):
    """Test creating raster from file path."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("raster")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["raster"] is not None


def test_generators_workflow(spark):
    """Test a complete workflow using multiple generators."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Load raster
    df = spark.range(1).select(
        rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("original")
    )

    # Retile it
    df = df.select(
        f.col("original"),
        rx.rst_retile(f.col("original"), f.lit(512), f.lit(512)).alias("retiled"),
    )

    result = df.collect()
    assert result is not None
    assert result[0]["original"] is not None
    # Retiled returns an array of tiles
    assert result[0]["retiled"] is not None
    assert isinstance(result[0]["retiled"], (list, tuple))
    assert len(result[0]["retiled"]) > 1
