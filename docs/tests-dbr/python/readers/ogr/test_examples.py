"""Tests for OGR Reader DBR Examples

Validates examples in docs/docs/readers/ogr.mdx#databricks-integration.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import examples


def test_convert_to_geometry(spark, sample_nyc_subway_shp):
    """Test WKB to GEOMETRY conversion."""
    try:
        result = examples.convert_to_geometry(spark, sample_nyc_subway_shp)
        assert 'geometry' in result.columns
        assert result.count() > 2000
    except Exception as e:
        if "st_geomfromwkb" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            pytest.skip(f"DBR not available: {e}")
        raise


def test_sql_constant():
    """Test SQL constant is defined."""
    assert hasattr(examples, 'SQL_READ_SHAPEFILE')
    assert 'st_geomfromwkb' in examples.SQL_READ_SHAPEFILE
