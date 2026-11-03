"""Python smoke tests for TIN generator bindings.

Confirms that ``gbx_st_triangulate``, ``gbx_st_interpolateelevationbbox``, and
``gbx_st_interpolateelevationgeom`` fire through the JVM bindings and produce
non-null geometry rows when invoked as top-level generators in a ``select()``.

Detailed triangulation math is covered by the Scala expression unit tests
(``ST_TriangulateTest``, ``ST_InterpolateElevationBBoxTest``). These tests
exercise the Python → Spark → JVM path only.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import call_function, col, lit
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


@pytest.fixture(scope="module")
def spark():
    from test._heavy import heavy_register

    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.appName("gbx-vectorx-tin-tests")
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# 4 corners of a 10x10 square with Z=0 (same as Scala unit test)
_SQUARE_CORNERS_WKT = [
    "POINT Z (0 0 0)",
    "POINT Z (10 0 0)",
    "POINT Z (0 10 0)",
    "POINT Z (10 10 0)",
]

_SPLIT_FINDER = "NONENCROACHING"
_MERGE_TOL = 0.01
_SNAP_TOL = 0.01

# Explicit schema for the common (pts, breaks, merge_tol, snap_tol, finder) row.
# PySpark cannot infer the type of an empty Python list [], so we declare it explicitly.
_TIN_SCHEMA = StructType(
    [
        StructField("pts", ArrayType(StringType()), nullable=False),
        StructField("breaks", ArrayType(StringType()), nullable=False),
        StructField("merge_tol", DoubleType(), nullable=False),
        StructField("snap_tol", DoubleType(), nullable=False),
        StructField("finder", StringType(), nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# test_st_triangulate
# ---------------------------------------------------------------------------


def test_st_triangulate_emits_triangle_rows(spark):
    """Triangulate returns at least 1 triangle WKB row for a non-collinear square."""
    # Build a single-row df: points_wkt (array of WKT strings), breaklines (empty array).
    # Explicit schema required because PySpark cannot infer the type of an empty Python list.
    df = spark.createDataFrame(
        [(_SQUARE_CORNERS_WKT, [], _MERGE_TOL, _SNAP_TOL, _SPLIT_FINDER)],
        schema=_TIN_SCHEMA,
    )
    # Generators are top-level in Spark 4.0 — invoke directly in select(), no explode.
    out = df.select(
        call_function(
            "gbx_st_triangulate",
            col("pts"),
            col("breaks"),
            col("merge_tol"),
            col("snap_tol"),
            col("finder"),
        ).alias("t")
    ).collect()

    assert len(out) >= 1, f"Expected >= 1 triangle rows, got {len(out)}"
    for r in out:
        # PySpark unwraps a single-field struct to the field value directly.
        # r["t"] is the WKB bytearray of the triangle polygon.
        tri = r["t"]
        assert tri is not None
        assert len(tri) > 0


# ---------------------------------------------------------------------------
# test_st_interpolateelevationbbox
# ---------------------------------------------------------------------------


def test_st_interpolateelevationbbox_emits_elevation_rows(spark):
    """BBox grid interpolation returns 100 Z-point rows for a 10x10 grid over a 100x100 square."""
    # 4 corners of 100x100 square, z=0 everywhere (flat plane)
    pts_wkt = [
        "POINT Z (0 0 0)",
        "POINT Z (100 0 0)",
        "POINT Z (0 100 0)",
        "POINT Z (100 100 0)",
    ]
    bbox_schema = StructType(
        [
            StructField("pts", ArrayType(StringType()), nullable=False),
            StructField("breaks", ArrayType(StringType()), nullable=False),
            StructField("merge_tol", DoubleType(), nullable=False),
            StructField("snap_tol", DoubleType(), nullable=False),
            StructField("finder", StringType(), nullable=False),
        ]
    )
    df = spark.createDataFrame(
        [(pts_wkt, [], _MERGE_TOL, _SNAP_TOL, _SPLIT_FINDER)],
        schema=bbox_schema,
    )
    out = df.select(
        call_function(
            "gbx_st_interpolateelevationbbox",
            col("pts"),
            col("breaks"),
            col("merge_tol"),
            col("snap_tol"),
            col("finder"),
            lit(0.0),  # xmin
            lit(0.0),  # ymin
            lit(100.0),  # xmax
            lit(100.0),  # ymax
            lit(10),  # width_px
            lit(10),  # height_px
            lit(32633),  # srid
        ).alias("t")
    ).collect()

    assert len(out) == 100, f"Expected 100 elevation rows, got {len(out)}"
    for r in out:
        # PySpark unwraps a single-field struct to the field value directly.
        # r["t"] is the WKB bytearray of the Z-valued elevation point.
        pt = r["t"]
        assert pt is not None
        assert len(pt) > 0


# ---------------------------------------------------------------------------
# test_st_interpolateelevationgeom
# ---------------------------------------------------------------------------


def test_st_interpolateelevationgeom_emits_elevation_rows(spark):
    """Origin-grid interpolation returns >= 1 Z-point row for a 3x3 grid over a 10x10 square."""
    df = spark.createDataFrame(
        [(_SQUARE_CORNERS_WKT, [], _MERGE_TOL, _SNAP_TOL, _SPLIT_FINDER)],
        schema=_TIN_SCHEMA,
    )
    # grid_origin as WKT string (no SRID prefix — plain WKT, SRID will be 0)
    out = df.select(
        call_function(
            "gbx_st_interpolateelevationgeom",
            col("pts"),
            col("breaks"),
            col("merge_tol"),
            col("snap_tol"),
            col("finder"),
            lit("POINT (1 1)"),  # grid_origin: inside the 10x10 square
            lit(3),  # grid_cols
            lit(3),  # grid_rows
            lit(3.0),  # cell_size_x  (1,4,7 → all inside [0,10])
            lit(3.0),  # cell_size_y
        ).alias("t")
    ).collect()

    assert len(out) >= 1, f"Expected >= 1 elevation rows, got {len(out)}"
    for r in out:
        # PySpark unwraps a single-field struct to the field value directly.
        # r["t"] is the WKB bytearray of the Z-valued elevation point.
        pt = r["t"]
        assert pt is not None
        assert len(pt) > 0
