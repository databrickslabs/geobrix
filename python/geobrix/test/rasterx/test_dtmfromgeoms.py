"""End-to-end Python tests for rst_dtmfromgeoms and rst_dtmfromgeoms_agg.

Exercises the full PySpark call_function -> registered UDF -> Scala execute path.
"""

import logging
import struct
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


def _point_z_wkb(x: float, y: float, z: float) -> bytes:
    """ISO WKB for a 3D point (byte_order=1 LE, type=1001 Point Z, then x,y,z)."""
    return struct.pack("<BI", 1, 1001) + struct.pack("<ddd", x, y, z)


def _linestring_z_wkb(coords: list) -> bytes:
    """ISO WKB for a 3D linestring (type=1002 LineString Z)."""
    header = struct.pack("<BII", 1, 1002, len(coords))
    pts = b"".join(struct.pack("<ddd", x, y, z) for (x, y, z) in coords)
    return header + pts


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


def test_rst_dtmfromgeoms_agg_wkb_breaklines(spark):
    """Regression: agg decodes a non-empty WKB breakline array (the untested decode path)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as F

    bl = _linestring_z_wkb([(0.0, 50.0, 0.0), (100.0, 50.0, 0.0)])
    rows = [
        (1, "POINT Z (0 0 5)"),
        (1, "POINT Z (100 0 205)"),
        (1, "POINT Z (0 100 305)"),
        (1, "POINT Z (100 100 505)"),
    ]
    df = spark.createDataFrame(rows, ["region", "pt"])
    out = (
        df.groupBy("region")
        .agg(
            F.rst_dtmfromgeoms_agg(
                f.col("pt"),
                f.array(
                    f.lit(bl).cast("binary")
                ),  # non-empty WKB breakline array (constant)
                f.lit(0.0),
                f.lit(0.01),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(100.0),
                f.lit(100.0),
                f.lit(10),
                f.lit(10),
                f.lit(32633),
            ).alias("dtm")
        )
        .collect()
    )
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None


def test_rst_dtmfromgeoms_agg_returns_tile(spark):
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as F

    rows = [
        (1, "POINT Z (0 0 5)"),
        (1, "POINT Z (100 0 205)"),
        (1, "POINT Z (0 100 305)"),
        (1, "POINT Z (100 100 505)"),
    ]
    df = spark.createDataFrame(rows, ["region", "pt"])
    out = (
        df.groupBy("region")
        .agg(
            F.rst_dtmfromgeoms_agg(
                f.col("pt"),
                f.array().cast("array<string>"),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(100.0),
                f.lit(100.0),
                f.lit(10),
                f.lit(10),
                f.lit(32633),
            ).alias("dtm")
        )
        .collect()
    )
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None
