"""End-to-end Python tests for the 3 web-mercator tile functions.

One smoke / round-trip test per function — confirms the JVM bindings fire and
the Long-overload eval entry points accept PySpark int inputs (LongType).
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
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_srid(
            rx.rst_to_webmercator(tile_from_path(rx, f, str(MODIS_B01), "GTiff"))
        ).alias("srid")
    )
    row = df.collect()[0]
    assert row["srid"] == 3857


def test_rst_tilexyz_returns_png_bytes(spark):
    """Out-of-extent tile still returns non-null PNG bytes (transparent fallback)."""
    from databricks.labs.gbx.rasterx import functions as rx

    # z=10, x=0, y=0 is the upper-left corner of the world — way outside MODIS h10v07.
    df = spark.range(1).select(
        rx.rst_tilexyz(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
            10,
            0,
            0,
        ).alias("bytes")
    )
    row = df.collect()[0]
    assert row["bytes"] is not None
    assert len(row["bytes"]) > 0
    # PNG magic header
    assert bytes(row["bytes"][:4]) == b"\x89PNG"


def test_rst_xyzpyramid_emits_rows(spark):
    """Pyramid generator emits at least one (z, x, y, bytes) row at z=4."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Generators are top-level in Spark 4.0 — invoke directly in select(), no f.explode wrap.
    df = spark.range(1).select(
        rx.rst_xyzpyramid(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
            4,
            4,
        ).alias("t")
    )
    rows = df.collect()
    assert len(rows) >= 1
    # Each row's "t" is the (z, x, y, bytes) inner struct (the generator emits a single
    # "tile" column per row, which the .alias("t") above renames to "t").
    for r in rows:
        t = r["t"]
        assert t["z"] == 4
        assert t["x"] is not None
        assert t["y"] is not None
        assert t["bytes"] is not None
        assert bytes(t["bytes"][:4]) == b"\x89PNG"
