"""Unit tests for the per-tower raster-viewshed pipeline input guards.

Pure-Python (no Spark session): exercises the ``_validate_towers_inputs`` guard
and the ``observer_height`` scalar/column resolver that ``rst_viewshed_towers``
builds on. The Spark pipeline itself is validated by the Serverless integration
run, not here.
"""

import pytest
from pyspark.sql import Column

from databricks.labs.gbx.pyrx.core.analysis import (
    _observer_height_col,
    _validate_towers_inputs,
)


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
