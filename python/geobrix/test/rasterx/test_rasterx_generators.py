import logging
from pathlib import Path
from test.rasterx._helpers import read_bytes, tile_from_path

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
    from databricks.labs.gbx import manager as gbx_manager

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

    gbx_manager.register_family(spark, ("gbx_rst_",))
    return spark


def test_rst_retile(spark):
    """Test retiling a raster."""
    # Served by the light level (or a heavy level that dispatches to it): needs the light deps, which the
    # heavyweight CI env does not have (test/conftest.py).
    pytest.importorskip("rasterio")
    pytest.importorskip("pandas")
    from databricks.labs.gbx.rasterx import functions as rx

    src = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
    )
    # rst_retile is a table function: one output row per sub-tile, beside the input row's columns
    rows = rx.rst_retile(src, "tile", 256, 256).collect()
    # MODIS tile (2400x2400) split into 256x256 tiles should create multiple tiles
    assert len(rows) > 1


def test_rst_maketiles(spark):
    """Test making tiles based on size in MB."""
    from databricks.labs.gbx.rasterx import functions as rx

    src = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
    )

    # a table function: one output row per tile, beside the input row's columns
    result = rx.rst_maketiles(src, "tile", 1).collect()  # size_in_mb = 1
    assert result
    assert result[0]["raster"] is not None


def test_rst_tooverlappingtiles(spark):
    """Test creating overlapping tiles."""
    from databricks.labs.gbx.rasterx import functions as rx

    src = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
    )

    # tile_width 256, tile_height 256, overlap 10 pixels: one row per overlapping tile
    result = rx.rst_tooverlappingtiles(src, "tile", 256, 256, 10).collect()
    assert len(result) > 1
    assert result[0]["raster"] is not None


def test_rst_separatebands(spark):
    """Test separating bands into individual rasters."""
    from databricks.labs.gbx.rasterx import functions as rx

    # First create a multi-band raster by combining single bands.
    # rst_fromfile is lightweight-only (issue #34); load bytes + gbx_rst_fromcontent.
    df = spark.createDataFrame(
        [
            (1, read_bytes(MODIS_B01)),
            (2, read_bytes(MODIS_B02)),
            (3, read_bytes(MODIS_B03)),
        ],
        ["id", "content"],
    )

    df = df.withColumn("tile", rx.rst_fromcontent(f.col("content"), f.lit("GTiff")))

    # Merge them into a multi-band raster
    multiband_df = df.select(rx.rst_merge(f.collect_list("tile")).alias("multiband"))

    # Now separate the bands: one row per band
    result = rx.rst_separatebands(multiband_df, "multiband").collect()
    assert len(result) >= 1
    assert result[0]["raster"] is not None


def test_rst_h3_tessellate(spark):
    """Test H3 tessellation of raster."""
    from databricks.labs.gbx.rasterx import functions as rx

    src = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
    )

    # H3 resolution 1 (coarse, for memory): one row per cell
    result = rx.rst_h3_tessellate(src, "tile", 1).collect()
    assert len(result) > 0
    assert result[0]["raster"] is not None


def test_rst_from_local_path(spark):
    """Test creating a raster tile from a local file path (heavy-native bytes path).

    rst_fromfile is lightweight-only (issue #34); the heavy tier loads the local
    tile's bytes and decodes via gbx_rst_fromcontent (see tile_from_path helper).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("raster")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["raster"] is not None


def test_generators_workflow(spark):
    """Test a complete workflow using multiple generators."""
    # Served by the light level (or a heavy level that dispatches to it): needs the light deps, which the
    # heavyweight CI env does not have (test/conftest.py).
    pytest.importorskip("rasterio")
    pytest.importorskip("pandas")
    from databricks.labs.gbx.rasterx import functions as rx

    # Load raster
    df = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("original")
    )

    # Retile it: one row per sub-tile, each beside its original tile
    result = rx.rst_retile(df, "original", 512, 512).collect()
    assert len(result) > 1
    assert result[0]["original"] is not None
