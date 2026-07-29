"""End-to-end Python test for the Wave 2 vector<->raster bridge functions.

One round-trip test: rasterize a square polygon, then polygonize the resulting
tile, and assert the burn value survives. This confirms the JVM bindings fire
and that both functions interoperate end-to-end.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


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


def test_rasterize_polygonize_roundtrip(spark):
    """Rasterize a square then polygonize -> burn value survives on >= 1 feature.

    WKB hex below encodes POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)).
    """
    sq_wkb_hex = (
        "01030000000100000005000000"
        "00000000000000000000000000000000"
        "00000000000024400000000000000000"
        "00000000000024400000000000002440"
        "00000000000000000000000000002440"
        "00000000000000000000000000000000"
    )
    df = spark.sql(f"""
        SELECT gbx_rst_polygonize(
            gbx_rst_rasterize(unhex('{sq_wkb_hex}'),
                              42.0, 0.0, 0.0, 10.0, 10.0, 100, 100, 4326)
        ) AS features
        """)
    out = df.collect()
    assert len(out) == 1
    features = out[0]["features"]
    assert len(features) > 0
    assert any(abs(feat["value"] - 42.0) < 1e-6 for feat in features)
    # Each emitted feature should carry non-empty WKB.
    assert all(
        feat["geom_wkb"] is not None and len(feat["geom_wkb"]) > 0 for feat in features
    )
