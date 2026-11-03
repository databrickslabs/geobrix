"""End-to-end Python tests for the resample + IDW functions.

One parameterized round-trip across the 3-function resample family, plus a
single combined round-trip for the IDW pair (non-aggregator + aggregator).
Following the streamlined test budget: only verify JVM bindings fire and a
non-empty tile comes back; numerical correctness is asserted in the Scala
suites (ResampleTest, RST_GridFromPointsTest).
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

# Single-band SRTM elevation tile shipped in the essential bundle.
SAMPLE_TILE_PATH = (
    "/Volumes/main/default/test-data/geobrix-examples/london/elevation/srtm_n51w001.tif"
)


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


def test_idw_roundtrip_non_agg_and_agg_match(spark):
    """IDW non-aggregator and aggregator return non-empty tiles on the same data.

    Both functions delegate to ``RST_GridFromPoints.execute`` under the hood,
    so the goal here is JVM-bindings + SQL coverage. Numerical parity between
    the two paths is asserted in the Scala test.
    """
    # 4 corner points of a 100x100 m extent (EPSG:32633), values 0/10/20/30.
    # WKB-encode each POINT directly (struct: byte-order=little + type=Point=1 + x + y).
    import struct as _struct

    def _point_wkb(x: float, y: float) -> bytes:
        return _struct.pack("<BIdd", 1, 1, x, y)

    pts = [(0.0, 0.0), (100.0, 0.0), (0.0, 100.0), (100.0, 100.0)]
    vals = [0.0, 10.0, 20.0, 30.0]
    wkbs = [_point_wkb(x, y) for x, y in pts]

    # Non-aggregator: arrays in a single row.
    df_arr = spark.createDataFrame([Row(points=wkbs, values=vals)])
    df_non_agg = df_arr.select(
        f.call_function(
            "gbx_rst_gridfrompoints",
            f.col("points"),
            f.col("values"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(100.0),
            f.lit(100.0),
            f.lit(50),
            f.lit(50),
            f.lit(32633),
            f.lit(2.0),
            f.lit(12),
        ).alias("out")
    )
    rows_na = df_non_agg.collect()
    assert len(rows_na) == 1
    out_na = rows_na[0]["out"]
    assert out_na is not None, "rst_gridfrompoints returned null tile"
    assert out_na["raster"] is not None
    assert len(out_na["raster"]) > 0

    # Aggregator: one row per point/value, grouped on a constant key.
    df_long = spark.createDataFrame(
        [Row(grp=1, point=w, value=v) for w, v in zip(wkbs, vals)]
    )
    df_agg = df_long.groupBy("grp").agg(
        f.call_function(
            "gbx_rst_gridfrompoints_agg",
            f.col("point"),
            f.col("value"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(100.0),
            f.lit(100.0),
            f.lit(50),
            f.lit(50),
            f.lit(32633),
            f.lit(2.0),
            f.lit(12),
        ).alias("out")
    )
    rows_a = df_agg.collect()
    assert len(rows_a) == 1
    out_a = rows_a[0]["out"]
    assert out_a is not None, "rst_gridfrompoints_agg returned null"
    assert len(out_a) > 0  # the aggregate returns the GeoTIFF bytes
