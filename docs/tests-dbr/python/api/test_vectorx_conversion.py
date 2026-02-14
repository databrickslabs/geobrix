"""
Tests for VectorX Conversion and Complete Example (DBR integration)

Validates examples in docs/docs/api/scala.mdx#conversion-functions and #complete-example-2.
Requires Databricks Runtime. Skips on open-source Spark.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import vectorx_conversion


def test_vectorx_conversion_constants():
    """Constants for Scala API doc are defined."""
    assert hasattr(vectorx_conversion, "VECTORX_CONVERSION_EXAMPLE")
    assert hasattr(vectorx_conversion, "VECTORX_CONVERSION_EXAMPLE_output")
    assert "st_geomfromwkb" in vectorx_conversion.VECTORX_CONVERSION_EXAMPLE
    assert "st_legacyaswkb" in vectorx_conversion.VECTORX_CONVERSION_EXAMPLE


def test_sql_vectorx_legacy_conversion_constants():
    """SQL constants for docs/docs/api/sql.mdx#legacy-geometry-conversion are defined."""
    assert hasattr(vectorx_conversion, "SQL_VECTORX_LEGACY_CONVERSION")
    assert hasattr(vectorx_conversion, "SQL_VECTORX_LEGACY_CONVERSION_output")
    assert "st_geomfromwkb" in vectorx_conversion.SQL_VECTORX_LEGACY_CONVERSION
    assert "gbx_st_legacyaswkb" in vectorx_conversion.SQL_VECTORX_LEGACY_CONVERSION


def test_vectorx_complete_example_constants():
    """Constants for Scala API VectorX Complete Example are defined."""
    assert hasattr(vectorx_conversion, "VECTORX_COMPLETE_EXAMPLE")
    assert hasattr(vectorx_conversion, "VECTORX_COMPLETE_EXAMPLE_output")
    assert "st_isvalid" in vectorx_conversion.VECTORX_COMPLETE_EXAMPLE
    assert "st_area" in vectorx_conversion.VECTORX_COMPLETE_EXAMPLE


def test_vectorx_conversion_example_run(spark):
    """Integration test: run conversion when legacy table exists (DBR only)."""
    try:
        legacy = spark.table("legacy_mosaic_table")
    except Exception as e:
        if "Table or view not found" in str(e) or "LEGACY_MOSAIC_TABLE" in str(e).upper():
            pytest.skip("legacy_mosaic_table not present; integration test requires DBR and test table")
        raise
    try:
        result = vectorx_conversion.vectorx_conversion_example(spark, "legacy_mosaic_table")
        assert result is not None
        assert "geometry" in result.columns
    except Exception as e:
        if "st_geomfromwkb" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            pytest.skip(f"Databricks Runtime required: {e}")
        raise


def test_vectorx_complete_example_run(spark):
    """Integration test: run complete migration when legacy table exists (DBR only)."""
    try:
        legacy = spark.table("legacy_mosaic_geometries")
    except Exception as e:
        if "Table or view not found" in str(e):
            pytest.skip("legacy_mosaic_geometries not present; integration test requires DBR and test table")
        raise
    try:
        result = vectorx_conversion.vectorx_complete_example(spark, "legacy_mosaic_geometries")
        assert result is not None
        assert "geometry" in result.columns
        assert "is_valid" in result.columns
        assert "area" in result.columns
    except Exception as e:
        if "st_geomfromwkb" in str(e) or "st_isvalid" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            pytest.skip(f"Databricks Runtime required: {e}")
        raise
