"""The JVM TIN expressions take the optional trailing ``mode`` parameter.

The Scala/SQL ``gbx_st_triangulate`` / ``gbx_st_interpolateelevation{bbox,geom}``
expressions are arity-overloaded with a trailing ``mode`` arg
(``'constrained'`` default, ``'conforming'`` Steiner). These tests call the
installed JVM companions by SQL name, with and without ``mode``, and assert both
modes are reachable and the shorter arity still works.

Heavy generators are top-level Generator Columns in Spark 4.0: invoke directly
in ``select(...)`` (the single-field struct unwraps to the WKB value).

Heavy requires the geobrix JAR (Scala/JTS). The JAR is present in the
geobrix-dev Docker container; this test auto-skips when the JAR is not staged
under ``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/vectorx/test_tin_mode_wrapper.py \\
        --with-integration --log heavy-mode-wrapper.log
"""

import logging
from pathlib import Path
from test._heavy import heavy_register  # noqa: E402

import pytest
from pyspark.sql.functions import call_function, col, lit
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/vectorx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark():

    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: it has no effect if a JVM (and therefore
    # a Spark session) is already live in this process. Skip instead of producing a
    # misleading failure when another test suite already created a JAR-free session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run this test in isolation: "
                "gbx:test:python --path "
                "python/geobrix/test/vectorx/test_tin_mode_wrapper.py "
                "--with-integration"
            )

    s = (
        SparkSession.builder.appName("gbx-vectorx-tin-mode-wrapper")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")

    heavy_register(s, ("gbx_st_",))
    yield s


# 4 corners of a 10x10 square with Z=0 (same as the other heavy TIN tests).
_SQUARE_CORNERS_WKT = [
    "POINT Z (0 0 0)",
    "POINT Z (10 0 0)",
    "POINT Z (0 10 0)",
    "POINT Z (10 10 0)",
]
_SPLIT_FINDER = "NONENCROACHING"
_MERGE_TOL = 0.01
_SNAP_TOL = 0.01

# PySpark cannot infer the type of an empty Python list [], so declare explicitly.
_TIN_SCHEMA = StructType(
    [
        StructField("pts", ArrayType(StringType()), nullable=False),
        StructField("breaks", ArrayType(StringType()), nullable=False),
        StructField("merge_tol", DoubleType(), nullable=False),
        StructField("snap_tol", DoubleType(), nullable=False),
        StructField("finder", StringType(), nullable=False),
    ]
)


def _tin_df(spark):
    return spark.createDataFrame(
        [(_SQUARE_CORNERS_WKT, [], _MERGE_TOL, _SNAP_TOL, _SPLIT_FINDER)],
        schema=_TIN_SCHEMA,
    )


# ---------------------------------------------------------------------------
# st_triangulate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["constrained", "conforming"])
def test_st_triangulate_expression_mode(spark, mode):
    """The JVM expression takes mode for both 'constrained' and 'conforming'."""
    out = (
        _tin_df(spark)
        .select(
            call_function(
                "gbx_st_triangulate",
                col("pts"),
                col("breaks"),
                col("merge_tol"),
                col("snap_tol"),
                col("finder"),
                lit(mode),
            ).alias("t")
        )
        .collect()
    )
    assert len(out) >= 1, f"mode={mode}: expected >= 1 triangle rows, got {len(out)}"
    for r in out:
        assert r["t"] is not None and len(r["t"]) > 0


def test_st_triangulate_expression_default_mode(spark):
    """Default (no mode arg) still works — back-compat with the 5-arg arity."""
    out = (
        _tin_df(spark)
        .select(
            call_function(
                "gbx_st_triangulate",
                col("pts"),
                col("breaks"),
                col("merge_tol"),
                col("snap_tol"),
                col("finder"),
            ).alias("t")
        )
        .collect()
    )
    assert len(out) >= 1, f"default mode: expected >= 1 triangle rows, got {len(out)}"


# ---------------------------------------------------------------------------
# st_interpolateelevationbbox
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["constrained", "conforming"])
def test_st_interpolateelevationbbox_expression_mode(spark, mode):
    """The JVM bbox expression takes mode for both modes and emits rows."""
    out = (
        _tin_df(spark)
        .select(
            call_function(
                "gbx_st_interpolateelevationbbox",
                col("pts"),
                col("breaks"),
                col("merge_tol"),
                col("snap_tol"),
                col("finder"),
                lit(0.0),  # xmin
                lit(0.0),  # ymin
                lit(10.0),  # xmax
                lit(10.0),  # ymax
                lit(5),  # width_px
                lit(5),  # height_px
                lit(32633),  # srid
                lit(mode),
            ).alias("t")
        )
        .collect()
    )
    assert len(out) >= 1, f"mode={mode}: expected >= 1 elevation rows, got {len(out)}"
    for r in out:
        assert r["t"] is not None and len(r["t"]) > 0


def test_st_interpolateelevationbbox_expression_default_mode(spark):
    """Default (no mode arg) still works — back-compat with the 12-arg arity."""
    out = (
        _tin_df(spark)
        .select(
            call_function(
                "gbx_st_interpolateelevationbbox",
                col("pts"),
                col("breaks"),
                col("merge_tol"),
                col("snap_tol"),
                col("finder"),
                lit(0.0),
                lit(0.0),
                lit(10.0),
                lit(10.0),
                lit(5),
                lit(5),
                lit(32633),
            ).alias("t")
        )
        .collect()
    )
    assert len(out) >= 1, f"default mode: expected >= 1 elevation rows, got {len(out)}"


# ---------------------------------------------------------------------------
# st_interpolateelevationgeom
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["constrained", "conforming"])
def test_st_interpolateelevationgeom_expression_mode(spark, mode):
    """The JVM origin-grid expression takes mode for both modes and emits rows."""
    out = (
        _tin_df(spark)
        .select(
            call_function(
                "gbx_st_interpolateelevationgeom",
                col("pts"),
                col("breaks"),
                col("merge_tol"),
                col("snap_tol"),
                col("finder"),
                lit("POINT (1 1)"),  # grid_origin inside the 10x10 square
                lit(3),  # grid_cols
                lit(3),  # grid_rows
                lit(3.0),  # cell_size_x (1,4,7 all inside [0,10])
                lit(3.0),  # cell_size_y
                lit(mode),
            ).alias("t")
        )
        .collect()
    )
    assert len(out) >= 1, f"mode={mode}: expected >= 1 elevation rows, got {len(out)}"
    for r in out:
        assert r["t"] is not None and len(r["t"]) > 0


def test_st_interpolateelevationgeom_expression_default_mode(spark):
    """Default (no mode arg) still works — back-compat with the 10-arg arity."""
    out = (
        _tin_df(spark)
        .select(
            call_function(
                "gbx_st_interpolateelevationgeom",
                col("pts"),
                col("breaks"),
                col("merge_tol"),
                col("snap_tol"),
                col("finder"),
                lit("POINT (1 1)"),
                lit(3),
                lit(3),
                lit(3.0),
                lit(3.0),
            ).alias("t")
        )
        .collect()
    )
    assert len(out) >= 1, f"default mode: expected >= 1 elevation rows, got {len(out)}"
