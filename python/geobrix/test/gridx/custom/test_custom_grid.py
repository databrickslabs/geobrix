"""Python tests for the 7 gbx_custom_* grid functions.

Registers the SQL functions, builds small DataFrames, evaluates each
Column wrapper, and asserts on collected rows.

WKB is built via struct.pack (ISO WKB, little-endian) — shapely is not
available in the CI test environment.
"""

import logging
import struct
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

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
def custom_registered(spark):
    """Register custom grid functions once for all tests."""
    from databricks.labs.gbx.gridx.custom import functions as cx

    cx.register(spark)
    return cx


def _point_wkb(x: float, y: float) -> bytes:
    """ISO WKB for a 2-D Point (type=1, little-endian)."""
    return struct.pack("<BI", 1, 1) + struct.pack("<dd", x, y)


def _polygon_wkb(x0: float, y0: float, x1: float, y1: float) -> bytes:
    """ISO WKB for a rectangular Polygon (type=3, little-endian)."""
    coords = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    header = struct.pack("<BII I", 1, 3, 1, 5)
    points = b"".join(struct.pack("<dd", x, y) for x, y in coords)
    return header + points


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_grid(cx, spark):
    """Return a single-row DataFrame with a materialised grid struct column.

    Grid: extent [0,100] x [0,100], 2 splits, 10x10 root cells, SRID 32633.
    All numeric args are integers as required by the Scala expression.
    """
    df = spark.createDataFrame([(1,)], ["dummy"])
    return df.select(cx.custom_grid(0, 100, 0, 100, 2, 10, 10, 32633).alias("grid"))


def _get_cell(cx, spark) -> int:
    """Compute the cell id for point (5, 5) on the test grid."""
    point_wkb = _point_wkb(5.0, 5.0)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df2 = spark.createDataFrame([(point_wkb, grid_val)], ["pt", "grid"])
    row = df2.select(
        cx.custom_pointascell(f.col("pt").cast("binary"), f.col("grid"), 0).alias(
            "cell"
        )
    ).first()
    return row["cell"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_custom_grid_returns_struct(spark, custom_registered):
    """custom_grid should return a non-null struct."""
    cx = custom_registered
    grid_df = _make_grid(cx, spark)
    row = grid_df.first()
    assert row["grid"] is not None


def test_custom_pointascell(spark, custom_registered):
    """point (5,5) inside [0,100]x[0,100] should yield a non-null BIGINT cell."""
    cx = custom_registered
    cell = _get_cell(cx, spark)
    assert cell is not None
    assert isinstance(cell, int)


def test_custom_cellaswkb(spark, custom_registered):
    """Cell footprint returned as non-null BINARY."""
    cx = custom_registered
    cell = _get_cell(cx, spark)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(cell, grid_val)], ["cell", "grid"])
    row = df.select(
        cx.custom_cellaswkb(f.col("cell"), f.col("grid")).alias("wkb")
    ).first()
    wkb = row["wkb"]
    assert wkb is not None
    assert isinstance(wkb, (bytes, bytearray))
    assert len(wkb) > 0


def test_custom_cellaswkt(spark, custom_registered):
    """Cell footprint returned as a POLYGON WKT string."""
    cx = custom_registered
    cell = _get_cell(cx, spark)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(cell, grid_val)], ["cell", "grid"])
    row = df.select(
        cx.custom_cellaswkt(f.col("cell"), f.col("grid")).alias("wkt")
    ).first()
    wkt = row["wkt"]
    assert wkt is not None
    assert isinstance(wkt, str)
    assert wkt.upper().startswith("POLYGON")


def test_custom_centroid(spark, custom_registered):
    """Cell centroid returned as non-null BINARY."""
    cx = custom_registered
    cell = _get_cell(cx, spark)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(cell, grid_val)], ["cell", "grid"])
    row = df.select(cx.custom_centroid(f.col("cell"), f.col("grid")).alias("c")).first()
    c = row["c"]
    assert c is not None
    assert isinstance(c, (bytes, bytearray))
    assert len(c) > 0


def test_custom_polyfill(spark, custom_registered):
    """Polygon covering [0,30]x[0,30] at resolution 0 should fill 9 cells (3x3)."""
    cx = custom_registered
    poly_wkb = _polygon_wkb(0.0, 0.0, 30.0, 30.0)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(poly_wkb, grid_val)], ["geom", "grid"])
    row = df.select(
        cx.custom_polyfill(f.col("geom").cast("binary"), f.col("grid"), 0).alias(
            "cells"
        )
    ).first()
    cells = row["cells"]
    assert cells is not None
    assert len(cells) == 9


def test_custom_kring(spark, custom_registered):
    """kring with k=1 should return a non-empty array of cells."""
    cx = custom_registered
    cell = _get_cell(cx, spark)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(cell, grid_val)], ["cell", "grid"])
    row = df.select(
        cx.custom_kring(f.col("cell"), f.col("grid"), 1).alias("ring")
    ).first()
    ring = row["ring"]
    assert ring is not None
    assert len(ring) >= 1


def test_custom_geomkringexplode(spark, custom_registered):
    """custom_geomkringexplode should return a Column."""
    cx = custom_registered
    poly_wkb = _polygon_wkb(0.0, 0.0, 30.0, 30.0)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(poly_wkb, grid_val)], ["geom", "grid"])
    # Test that the function returns a Column (can be used in select)
    result = df.select(
        cx.custom_geomkringexplode(
            f.col("geom").cast("binary"), f.col("grid"), 0, 1
        ).alias("cells")
    ).collect()
    assert len(result) >= 0  # Should not error


def test_custom_geomkloopexplode(spark, custom_registered):
    """custom_geomkloopexplode should return a Column."""
    cx = custom_registered
    poly_wkb = _polygon_wkb(0.0, 0.0, 30.0, 30.0)
    grid_df = _make_grid(cx, spark)
    grid_val = grid_df.first()["grid"]

    df = spark.createDataFrame([(poly_wkb, grid_val)], ["geom", "grid"])
    # Test that the function returns a Column (can be used in select)
    result = df.select(
        cx.custom_geomkloopexplode(
            f.col("geom").cast("binary"), f.col("grid"), 0, 1
        ).alias("cells")
    ).collect()
    assert len(result) >= 0  # Should not error
