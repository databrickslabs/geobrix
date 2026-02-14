"""Tests for Shapefile Examples"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import shapefile_examples


def test_read_shapefile(spark, sample_nyc_subway_shp):
    """Test basic shapefile read."""
    result = shapefile_examples.read_shapefile(spark, sample_nyc_subway_shp)
    assert result is not None
    assert result.count() > 2000


def test_read_with_options(spark, sample_nyc_subway_shp):
    """Test shapefile read with chunk size option."""
    result = shapefile_examples.read_with_options(spark, sample_nyc_subway_shp)
    assert result is not None
    assert result.count() > 2000


def test_sql_constant():
    """Test SQL constant is defined."""
    assert hasattr(shapefile_examples, 'SQL_SHAPEFILE')
    assert 'shapefile_ogr.' in shapefile_examples.SQL_SHAPEFILE


def test_output_constants():
    """Shapefile reader doc: output constants for Example output blocks."""
    for name in ("READ_SHAPEFILE_output", "READ_WITH_OPTIONS_output", "SQL_SHAPEFILE_output"):
        assert hasattr(shapefile_examples, name), f"missing {name}"
        assert isinstance(getattr(shapefile_examples, name), str)
        assert len(getattr(shapefile_examples, name).strip()) > 0
