"""TDD tests for gbx_rst_binpoints (scalar) and gbx_rst_binpoints_agg (grouped agg).

These are Spark-level tests: they exercise the registered SQL names and the
public Python API (rst_binpoints / rst_binpoints_agg), verifying the full
UDF-layer plumbing on top of the Spark-free binning core (test_core_binning.py).

Row layout for the shared 2×2 fixture
--------------------------------------
Grid: [0,2]×[0,2], 2 pixels wide × 2 pixels tall, EPSG:2227.
Raster row 0 = top (high y); raster convention means row = floor((ymax-y)/cellH).
- Point (0.5, 1.5) is in the TOP-LEFT  cell → arr[0,0].
- Point (1.5, 0.5) is in the BOTTOM-RIGHT cell → arr[1,1].
"""

import pytest
from pyspark.sql import functions as F
from rasterio.io import MemoryFile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_first_band(gtiff_bytes: bytes):
    """Return (arr, nodata) from a single-band GTiff bytes object."""
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        return ds.read(1), ds.nodata


# ---------------------------------------------------------------------------
# Step 1 / Step 2 tests — RED before implementation, GREEN after
# ---------------------------------------------------------------------------


def test_binpoints_agg_max(spark):
    """Grouped-agg via SQL expr: max of two points in one cell, one in another."""
    from databricks.labs.gbx.pyrx import functions as fns

    fns.register(spark)

    rows = [(0.5, 1.5, 10.0), (0.5, 1.5, 25.0), (1.5, 0.5, 7.0)]
    df = spark.createDataFrame(rows, "x double, y double, z double")
    out = (
        df.groupBy()
        .agg(
            F.expr(
                "gbx_rst_binpoints_agg(x, y, z, 0.0, 0.0, 2.0, 2.0, 2, 2, 2227, 'max')"
                " AS r"
            )
        )
        .collect()[0]["r"]
    )
    assert out is not None, "expected non-null BINARY result from gbx_rst_binpoints_agg"
    arr, nodata = _read_first_band(bytes(out))
    assert arr.shape == (2, 2)
    assert arr[0, 0] == pytest.approx(25.0), "top-left cell must be max(10,25)"
    assert arr[1, 1] == pytest.approx(7.0), "bottom-right cell must be 7.0"
    assert arr[0, 1] == pytest.approx(nodata), "empty cell must be NoData"
    assert arr[1, 0] == pytest.approx(nodata), "empty cell must be NoData"


# ---------------------------------------------------------------------------
# Step 5 tests — additional coverage
# ---------------------------------------------------------------------------


def test_binpoints_agg_empty_group(spark):
    """An empty group (no rows) must return None, not raise."""
    from databricks.labs.gbx.pyrx import functions as fns

    fns.register(spark)

    schema = "x double, y double, z double, g string"
    df = spark.createDataFrame([], schema)
    rows = (
        df.groupBy("g")
        .agg(
            F.expr(
                "gbx_rst_binpoints_agg(x, y, z, 0.0, 0.0, 2.0, 2.0, 2, 2, 4326, 'max')"
                " AS r"
            )
        )
        .collect()
    )
    # An empty DataFrame produces no groups → zero rows, not a null row.
    assert len(rows) == 0, "empty input must produce zero output rows"


def test_binpoints_agg_mean(spark):
    """statistic='mean': average over two identical-cell points."""
    from databricks.labs.gbx.pyrx import functions as fns

    fns.register(spark)

    # Two points in top-left cell: mean(4, 8) = 6.
    rows = [(0.25, 1.5, 4.0), (0.75, 1.5, 8.0)]
    df = spark.createDataFrame(rows, "x double, y double, z double")
    out = (
        df.groupBy()
        .agg(
            F.expr(
                "gbx_rst_binpoints_agg(x, y, z, 0.0, 0.0, 2.0, 2.0, 2, 2, 4326, 'mean')"
                " AS r"
            )
        )
        .collect()[0]["r"]
    )
    arr, nodata = _read_first_band(bytes(out))
    assert arr[0, 0] == pytest.approx(6.0), "mean(4,8) must equal 6.0"
    assert arr[0, 1] == pytest.approx(nodata)
    assert arr[1, 0] == pytest.approx(nodata)
    assert arr[1, 1] == pytest.approx(nodata)


def test_binpoints_scalar_python_api(spark):
    """Scalar form (rst_binpoints): collect-array columns -> one tile per row."""
    from databricks.labs.gbx.pyrx import functions as fns

    # One row: three-point array in the same cell layout as test_binpoints_agg_max.
    rows = [([0.5, 0.5, 1.5], [1.5, 1.5, 0.5], [10.0, 25.0, 7.0])]
    df = spark.createDataFrame(
        rows,
        "x array<double>, y array<double>, z array<double>",
    )
    result_df = df.select(
        fns.rst_binpoints(
            "x",
            "y",
            "z",
            F.lit(0.0),
            F.lit(0.0),
            F.lit(2.0),
            F.lit(2.0),
            F.lit(2),
            F.lit(2),
            F.lit(2227),
        ).alias("tile")
    )
    row = result_df.collect()[0]["tile"]
    assert row is not None, "rst_binpoints must return a tile struct"
    raster_bytes = bytes(row["raster"])
    arr, nodata = _read_first_band(raster_bytes)
    assert arr.shape == (2, 2)
    assert arr[0, 0] == pytest.approx(25.0)
    assert arr[1, 1] == pytest.approx(7.0)


def test_binpoints_scalar_empty_array(spark):
    """Scalar UDF with empty arrays must return None (no crash)."""
    from databricks.labs.gbx.pyrx import functions as fns

    rows = [([], [], [])]
    df = spark.createDataFrame(
        rows,
        "x array<double>, y array<double>, z array<double>",
    )
    result_df = df.select(
        fns.rst_binpoints(
            "x",
            "y",
            "z",
            F.lit(0.0),
            F.lit(0.0),
            F.lit(2.0),
            F.lit(2.0),
            F.lit(2),
            F.lit(2),
            F.lit(4326),
        ).alias("tile")
    )
    row = result_df.collect()[0]["tile"]
    assert row is None, "empty array input must return None"
