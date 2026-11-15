"""The tile struct the heavy level (the JAR) returns is the manifest's tile type, and its metadata names the source.

The oracle tables judge what a tile holds; this pins the shape of the struct itself on the JVM side, which the light
side's contract test (test/manager/test_contracts.py) cannot see.
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
    from test._heavy import pin_l1

    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark_session = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    pin_l1(spark_session, ["gbx_rst_fromcontent"])
    return spark_session


def test_heavy_tile_is_the_manifest_tile_type(spark):
    """The JVM tile type equals the manifest's TILE_SQL, field for field and in order."""
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
    )
    assert df.schema["tile"].dataType.simpleString() == TILE_SQL


def test_heavy_tile_metadata_names_the_source(spark):
    """A decoded file is a tile with its bytes, no cell, the driver, the extension without a dot and its byte size."""
    from databricks.labs.gbx.rasterx import functions as rx

    row = (
        spark.range(1)
        .select(tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile"))
        .select("tile.cellid", "tile.raster", "tile.metadata")
        .collect()[0]
    )
    assert row["cellid"] is None
    assert row["metadata"]["driver"] == "GTiff"
    assert row["metadata"]["extension"] == "tif"
    assert int(row["metadata"]["size"]) == len(row["raster"]) > 0
