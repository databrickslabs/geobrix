"""Unit tests for the per-tower raster-viewshed pipeline.

Two test groups:

1. **Input guards** (pure-Python, no Spark): exercises ``_validate_towers_inputs``
   and the ``observer_height`` scalar/column resolver.

2. **Public API** (local Spark, no GDAL): exercises ``rst_viewshed_towers`` up to
   the integration boundary — the point where worker-side UDFs would need a GDAL
   stack and real DSM raster files.

Integration boundary
--------------------
``rst_viewshed_towers`` calls ``dsm_tiles_df.select(...).collect()`` eagerly on the
driver (no GDAL — just reads tile-index rows from a synthetic Spark DataFrame).
It then builds a lazy Spark query plan.  The per-tower UDFs (``_mint_window_vrt``,
``rst_fromfile``, ``rst_viewshed``, ``rst_histogram``) run only when ``.collect()``
is invoked on the result and require a GDAL stack + real DSM COG files.

The tests below inspect the **lazy plan** (schema, column names) without calling
``.collect()``, so they run in any Python environment with PySpark.

The Serverless integration run (``test_h3_viewshed_spike.py`` + on-cluster WC
notebooks) covers post-boundary worker execution.
"""

import pytest
from pyspark.sql import Column
from pyspark.sql.types import DoubleType, LongType

from databricks.labs.gbx.pyrx.core.analysis import (
    _observer_height_col,
    _validate_towers_inputs,
)
from databricks.labs.gbx.pyrx.functions import rst_viewshed_towers


# --- _validate_towers_inputs ------------------------------------------------
def test_rejects_tiles_df_without_path():
    with pytest.raises(ValueError, match="path"):
        _validate_towers_inputs(tiles_cols=["tx", "ty"], radius_m=2500.0)


def test_rejects_nonpositive_radius():
    with pytest.raises(ValueError, match="radius"):
        _validate_towers_inputs(tiles_cols=["path", "tx", "ty"], radius_m=0.0)


def test_rejects_negative_radius():
    with pytest.raises(ValueError, match="radius"):
        _validate_towers_inputs(tiles_cols=["path", "tx", "ty"], radius_m=-10.0)


def test_rejects_missing_tile_index_columns():
    with pytest.raises(ValueError, match="tx"):
        _validate_towers_inputs(tiles_cols=["path"], radius_m=2500.0)


def test_path_remedy_names_the_dsm_cog_dir():
    # The remedy must steer the caller to the DSM COG directory, because
    # materialized DSM STRUCT tables carry a NULL .path.
    with pytest.raises(ValueError, match="(?i)dsm"):
        _validate_towers_inputs(tiles_cols=["tx", "ty"], radius_m=2500.0)


def test_accepts_valid_inputs():
    _validate_towers_inputs(
        tiles_cols=["path", "tx", "ty"], radius_m=2500.0
    )  # no raise


def test_accepts_extra_columns():
    _validate_towers_inputs(
        tiles_cols=["path", "tx", "ty", "crs", "cellid"], radius_m=1000.0
    )  # no raise


# --- _observer_height_col ---------------------------------------------------
# These need an active SparkContext to *build* a Column (classic PySpark asserts
# a live context in F.col/F.lit). Requesting the ``spark`` fixture supplies one;
# no pipeline is executed — the Column is only constructed, never collected.
def test_observer_height_scalar_becomes_literal_column(spark):
    col = _observer_height_col(30.0)
    assert isinstance(col, Column)


def test_observer_height_int_scalar_becomes_literal_column(spark):
    col = _observer_height_col(30)
    assert isinstance(col, Column)


def test_observer_height_str_becomes_column_reference(spark):
    col = _observer_height_col("mast_height_m")
    assert isinstance(col, Column)


# =============================================================================
# Public API — rst_viewshed_towers (local Spark, no GDAL)
# =============================================================================
#
# These tests call the PUBLIC function directly.  They cover every code-path
# reachable before the integration boundary (see module docstring).
#
# Passing ``num_partitions=1`` skips the one genuinely eager Spark action that
# the function fires when ``num_partitions`` is None:
#   ``base.select("tower_id").distinct().count()``
# The only remaining eager action — ``dsm_tiles_df.select(...).collect()`` —
# reads column values from a synthetic DataFrame and needs no GDAL.
# =============================================================================


def _towers_df(spark):
    """Two synthetic tower rows with EPSG:3857-style projected coordinates."""
    return spark.createDataFrame(
        [(1, 537000.0, 182000.0), (2, 538500.0, 183500.0)],
        ["tower_cellid", "x", "y"],
    )


def _dsm_tiles_df(spark):
    """Minimal tile manifest — synthetic (non-existent) paths, non-null so the
    driver-side tile map is populated.  Workers never open these paths because
    the tests do NOT call .collect() on the result."""
    return spark.createDataFrame(
        [("/fake/dsm_53_18.tif", 53, 18), ("/fake/dsm_54_18.tif", 54, 18)],
        ["path", "tx", "ty"],
    )


def _call_towers(spark, **overrides):
    """Invoke rst_viewshed_towers with project-typical defaults."""
    kw = dict(
        towers_df=_towers_df(spark),
        dsm_tiles_df=_dsm_tiles_df(spark),
        radius_m=2500.0,
        observer_height=30.0,
        x0=0.0,
        y0=0.0,
        tile_m=10000.0,
        num_partitions=1,  # skip the .distinct().count() eager action
    )
    kw.update(overrides)
    return rst_viewshed_towers(**kw)


# --- Schema / column-name tests (no .collect() → no GDAL) -------------------


def test_rst_viewshed_towers_returns_dataframe(spark):
    """rst_viewshed_towers returns a DataFrame object without error."""
    from pyspark.sql import DataFrame

    result = _call_towers(spark)
    assert isinstance(result, DataFrame)


def test_rst_viewshed_towers_schema_columns(spark):
    """Output carries exactly the six contract columns in contract order."""
    result = _call_towers(spark)
    assert result.columns == [
        "tower_cellid",
        "observer_x",
        "observer_y",
        "visible_px",
        "total_px",
        "frac",
    ]


def test_rst_viewshed_towers_schema_types(spark):
    """visible_px / total_px are LongType; frac is DoubleType."""
    result = _call_towers(spark)
    type_map = {f.name: type(f.dataType) for f in result.schema}
    assert type_map["visible_px"] is LongType
    assert type_map["total_px"] is LongType
    assert type_map["frac"] is DoubleType


def test_rst_viewshed_towers_no_reproj_columns_by_default(spark):
    """Without to_crs the reproj stats columns are absent."""
    result = _call_towers(spark)
    assert "visible_px_reproj" not in result.columns
    assert "total_px_reproj" not in result.columns


def test_rst_viewshed_towers_reproj_columns_with_to_crs(spark):
    """With to_crs=4326 the result gains visible_px_reproj / total_px_reproj."""
    result = _call_towers(spark, to_crs=4326)
    assert "visible_px_reproj" in result.columns
    assert "total_px_reproj" in result.columns
    # Reproj counters must also be long
    type_map = {f.name: type(f.dataType) for f in result.schema}
    assert type_map["visible_px_reproj"] is LongType
    assert type_map["total_px_reproj"] is LongType


# --- observer_height: scalar vs column name ----------------------------------


def test_rst_viewshed_towers_observer_height_scalar_float(spark):
    """A float scalar for observer_height builds a valid plan (no error)."""
    result = _call_towers(spark, observer_height=30.0)
    assert "visible_px" in result.columns


def test_rst_viewshed_towers_observer_height_int_scalar(spark):
    """An int scalar for observer_height (coerced to float internally) builds a valid plan."""
    result = _call_towers(spark, observer_height=30)
    assert "visible_px" in result.columns


def test_rst_viewshed_towers_observer_height_column_name(spark):
    """A column-name string for observer_height builds a valid plan."""
    towers_with_height = spark.createDataFrame(
        [(1, 537000.0, 182000.0, 30.0), (2, 538500.0, 183500.0, 45.0)],
        ["tower_cellid", "x", "y", "mast_height_m"],
    )
    result = _call_towers(
        spark,
        towers_df=towers_with_height,
        observer_height="mast_height_m",
    )
    assert "visible_px" in result.columns


# --- Input validation (eager guards, no Spark plan needed for some) ----------


def test_rst_viewshed_towers_tile_m_zero_raises(spark):
    """tile_m=0 is rejected before any Spark plan is built."""
    with pytest.raises(ValueError, match="tile_m"):
        _call_towers(spark, tile_m=0.0)


def test_rst_viewshed_towers_tile_m_negative_raises(spark):
    """tile_m<0 is rejected before any Spark plan is built."""
    with pytest.raises(ValueError, match="tile_m"):
        _call_towers(spark, tile_m=-500.0)


def test_rst_viewshed_towers_null_paths_raises(spark):
    """When every path in dsm_tiles_df is NULL, a ValueError with 'dsm' context is raised."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    null_tiles_df = spark.createDataFrame(
        [(None, 0, 0)],
        schema=StructType(
            [
                StructField("path", StringType(), nullable=True),
                StructField("tx", IntegerType(), nullable=False),
                StructField("ty", IntegerType(), nullable=False),
            ]
        ),
    )
    with pytest.raises(ValueError, match="(?i)dsm"):
        _call_towers(spark, dsm_tiles_df=null_tiles_df)


# --- Custom column-name options ----------------------------------------------


def test_rst_viewshed_towers_custom_tower_id_col(spark):
    """tower_id_col controls the name of the identity column in the output."""
    towers_df = spark.createDataFrame([(1, 537000.0, 182000.0)], ["site_id", "x", "y"])
    result = rst_viewshed_towers(
        towers_df,
        _dsm_tiles_df(spark),
        radius_m=2500.0,
        observer_height=30.0,
        x0=0.0,
        y0=0.0,
        tile_m=10000.0,
        tower_id_col="site_id",
        num_partitions=1,
    )
    assert result.columns[0] == "site_id"


def test_rst_viewshed_towers_custom_xy_cols(spark):
    """x_col / y_col control which input columns become observer_x / observer_y."""
    towers_df = spark.createDataFrame(
        [(1, 537000.0, 182000.0)], ["tower_cellid", "easting", "northing"]
    )
    result = rst_viewshed_towers(
        towers_df,
        _dsm_tiles_df(spark),
        radius_m=2500.0,
        observer_height=30.0,
        x0=0.0,
        y0=0.0,
        tile_m=10000.0,
        x_col="easting",
        y_col="northing",
        num_partitions=1,
    )
    assert "observer_x" in result.columns
    assert "observer_y" in result.columns
