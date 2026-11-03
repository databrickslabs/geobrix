"""End-to-end Python test for the web-mercator reprojection function.

A smoke / round-trip test — confirms the JVM binding fires. The tile functions (rst_tilexyz, rst_xyzpyramid)
are covered by the WEB_TILES oracle table.
"""

import logging
from pathlib import Path
from test.rasterx._helpers import tile_from_path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

MODIS_B01 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
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


def test_rst_to_webmercator_roundtrip(spark):
    """Tile is reprojected to EPSG:3857 — round-trip through rst_srid."""
    # Served by the light level (or a heavy level that dispatches to it): needs the light deps, which the
    # heavyweight CI env does not have (test/conftest.py).
    pytest.importorskip("rasterio")
    pytest.importorskip("pandas")
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_srid(
            rx.rst_to_webmercator(tile_from_path(rx, f, str(MODIS_B01), "GTiff"))
        ).alias("srid")
    )
    row = df.collect()[0]
    assert row["srid"] == 3857
