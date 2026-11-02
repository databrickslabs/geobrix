"""End-to-end Python tests for quadbin_cellunion_agg.

Streams quadbin cell BIGINTs into the aggregator and asserts a non-null
BINARY geometry result is returned. Cells are obtained via the existing
quadbin_pointascell binding.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Quadbin cell math is served by the light level only: the heavyweight CI env (no light deps) skips this file.
pytest.importorskip("quadbin")
pytest.importorskip("pandas")

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[3] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=INFO,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def quadbin_registered(spark):
    """Register quadbin functions once for the session."""
    from databricks.labs.gbx.gridx.quadbin import functions as qx

    qx.register(spark)
    return qx


def _cell_at(spark, qx, lon: float, lat: float, z: int) -> int:
    """Compute a quadbin cell id via the SQL binding."""
    df = spark.createDataFrame([(lon, lat)], ["lon", "lat"])
    return df.select(
        qx.quadbin_pointascell(f.col("lon"), f.col("lat"), z).alias("cell")
    ).first()["cell"]


def test_quadbin_cellunion_agg_returns_binary(spark, quadbin_registered):
    """quadbin_cellunion_agg streams cell rows and returns a non-null BINARY geometry."""
    qx = quadbin_registered

    # Get a centre cell and a neighbour cell via kring (k=1 yields 9 cells).
    centre = _cell_at(spark, qx, 0.0, 0.0, 8)
    df_centre = spark.createDataFrame([(centre,)], ["cell"])
    ring = df_centre.select(qx.quadbin_kring(f.col("cell"), 1).alias("r")).first()["r"]
    # Use the centre and first neighbour — two distinct cells in the same group.
    neighbour = next(c for c in ring if c != centre)

    rows = [
        (1, centre),
        (1, neighbour),
    ]
    df = spark.createDataFrame(rows, ["key", "cell"])

    out = (
        df.groupBy("key")
        .agg(qx.quadbin_cellunion_agg(f.col("cell")).alias("union_geom"))
        .collect()
    )
    assert len(out) == 1
    assert out[0]["union_geom"] is not None
    assert isinstance(out[0]["union_geom"], (bytes, bytearray))
    assert len(out[0]["union_geom"]) > 0
