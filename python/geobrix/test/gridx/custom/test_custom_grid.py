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


def _rows(df):
    return sorted(tuple(r) for r in df.collect())


@pytest.mark.parametrize("name", ["geomkringexplode", "geomkloopexplode"])
def test_custom_geom_explode_wrappers_match_sql(spark, custom_registered, name):
    """The heavy wrappers bind geom/grid/resolution/k[/mode/coverage] in the SQL
    function's positional order: same rows as the SQL call, with defaults and
    with an explicit non-default mode + coverage (which change the result)."""
    cx = custom_registered
    wrapper = getattr(cx, f"custom_{name}")
    grid_val = _make_grid(cx, spark).first()["grid"]
    df = spark.createDataFrame(
        [(_polygon_wkb(0.0, 0.0, 30.0, 30.0), grid_val)], ["geom", "grid"]
    )
    geom = f.col("geom").cast("binary")
    got = _rows(df.select(wrapper(geom, f.col("grid"), 0, 1)))
    want = _rows(df.selectExpr(f"gbx_custom_{name}(cast(geom as binary), grid, 0, 1)"))
    assert got and got == want
    got_in = _rows(df.select(wrapper(geom, f.col("grid"), 0, 1, "boundary-in", "core")))
    want_in = _rows(
        df.selectExpr(
            f"gbx_custom_{name}(cast(geom as binary), grid, 0, 1, 'boundary-in', 'core')"
        )
    )
    assert got_in == want_in and want_in != want
