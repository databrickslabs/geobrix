"""Python round-trip test for ``gbx_st_asmvt_pyramid``.

Confirms the JVM binding fires, that the Long-overload eval entry points accept
PySpark int inputs (LongType), and that the per-tile MVT bytes carry the
configured layer name. Builder logic (zoom guards, per-tile clip math) is
already covered in Scala by ``MvtPyramidBuilderTest``.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import call_function, col, lit, struct

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


@pytest.fixture(scope="module")
def spark():
    from test._heavy import heavy_register

    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.appName("gbx-vectorx-pyramid-tests")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")

    heavy_register(s, ("gbx_st_",))  # the JVM expressions
    yield s


def _polygon_wkb_30deg_band() -> bytes:
    """WKB for a rectangle spanning lon -30..+30 / lat 10..20 (straddles the prime meridian)."""
    # POLYGON((-30 10, 30 10, 30 20, -30 20, -30 10))
    import struct as _s

    header = bytes.fromhex("01030000000100000005000000")
    coords = [(-30.0, 10.0), (30.0, 10.0), (30.0, 20.0), (-30.0, 20.0), (-30.0, 10.0)]
    body = b"".join(_s.pack("<dd", x, y) for x, y in coords)
    return header + body


def test_st_asmvt_pyramid_emits_rows(spark):
    """Pyramid generator emits one row per intersecting tile across z=2..2."""
    rect_wkb = _polygon_wkb_30deg_band()
    df = spark.createDataFrame([(rect_wkb, "region-a", 1)], ["geom_wkb", "name", "id"])
    # Generators are top-level in Spark 4.0 - invoke directly in select(), no f.explode wrap.
    out = df.select(
        call_function(
            "gbx_st_asmvt_pyramid",
            col("geom_wkb"),
            struct(col("name"), col("id")),
            lit(2),
            lit(2),
            lit("regions"),
        ).alias("t")
    ).collect()
    # At z=2 the rectangle straddles the prime meridian — tiles x=1 and x=2 in the y=1 row.
    assert len(out) == 2
    for r in out:
        t = r["t"]
        assert t["z"] == 2
        assert t["x"] is not None
        assert t["y"] is not None
        assert t["mvt_bytes"] is not None and len(t["mvt_bytes"]) > 0
        assert b"regions" in bytes(t["mvt_bytes"])
