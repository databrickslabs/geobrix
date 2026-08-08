"""End-to-end Python tests for the CRS family wrappers: rst_crs, rst_setcrs, rst_transformcrs."""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

# An SRTM elevation tile shipped in the essential sample-data bundle.
SRTM_PATH = (
    "/Volumes/main/default/test-data/geobrix-examples/london/elevation/srtm_n51w001.tif"
)


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.appName("gbx-rasterx-crs-tests")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    return spark


def test_rst_crs_returns_string(spark):
    """rst_crs(tile) returns a non-null CRS string."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Load the SRTM tile (which has a known CRS).
    if not Path(SRTM_PATH).exists():
        pytest.skip(f"Sample data not found: {SRTM_PATH}")

    df = spark.read.format("binaryFile").load(SRTM_PATH).limit(1)
    tile_df = df.select(rx.rst_fromcontent(col("content"), lit("GTiff")).alias("tile"))

    # Apply rst_crs.
    result = tile_df.select(rx.rst_crs(col("tile")).alias("crs")).collect()
    assert len(result) == 1
    crs_value = result[0]["crs"]
    assert crs_value is not None, "rst_crs should return a non-null CRS string"
    assert isinstance(crs_value, str), f"Expected string, got {type(crs_value)}"
    # SRTM tiles typically have WGS84 (EPSG:4326).
    assert "4326" in crs_value or "4326" in str(crs_value).upper()


def test_rst_setcrs_bare_string_literal(spark):
    """rst_setcrs(tile, 'EPSG:4326') with a bare string literal should not resolve as a column name."""
    from databricks.labs.gbx.rasterx import functions as rx

    if not Path(SRTM_PATH).exists():
        pytest.skip(f"Sample data not found: {SRTM_PATH}")

    df = spark.read.format("binaryFile").load(SRTM_PATH).limit(1)
    tile_df = df.select(rx.rst_fromcontent(col("content"), lit("GTiff")).alias("tile"))

    # Apply rst_setcrs with a bare string — the _crs_col helper must lift it via f.lit
    # so it is NOT resolved as a column name.
    result = tile_df.select(
        rx.rst_setcrs(col("tile"), "EPSG:3857").alias("tile_out"),
        rx.rst_crs(rx.rst_setcrs(col("tile"), "EPSG:3857")).alias("crs_out"),
    ).collect()

    assert len(result) == 1
    crs_value = result[0]["crs_out"]
    assert crs_value is not None, "rst_crs on stamped tile should return non-null"
    assert "3857" in str(crs_value), f"Expected 3857 in CRS string, got {crs_value}"


def test_rst_transformcrs_reprojects(spark):
    """rst_transformcrs(tile, target_crs) should reproject and return a non-null tile."""
    from databricks.labs.gbx.rasterx import functions as rx

    if not Path(SRTM_PATH).exists():
        pytest.skip(f"Sample data not found: {SRTM_PATH}")

    df = spark.read.format("binaryFile").load(SRTM_PATH).limit(1)
    tile_df = df.select(rx.rst_fromcontent(col("content"), lit("GTiff")).alias("tile"))

    # Transform from WGS84 (4326) to Web Mercator (3857).
    result = tile_df.select(
        rx.rst_transformcrs(col("tile"), "EPSG:3857").alias("tile_out"),
        rx.rst_crs(rx.rst_transformcrs(col("tile"), "EPSG:3857")).alias("crs_out"),
    ).collect()

    assert len(result) == 1
    tile_value = result[0]["tile_out"]
    crs_value = result[0]["crs_out"]
    assert tile_value is not None, "rst_transformcrs should return a non-null tile"
    assert crs_value is not None, "CRS of transformed tile should be non-null"
    assert "3857" in str(crs_value), f"Expected 3857 in transformed CRS, got {crs_value}"
