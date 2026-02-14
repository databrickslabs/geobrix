"""
Tests for GeoJSON Reader Examples

Validates code examples shown in docs/docs/readers/geojson.mdx
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import geojson_examples


def test_read_geojson(spark, sample_nyc_boroughs):
    """Test basic GeoJSON read with multi=false."""
    result = geojson_examples.read_geojson(spark, sample_nyc_boroughs)
    assert result is not None
    assert result.count() == 5  # NYC has 5 boroughs
    assert 'geom_0' in result.columns


def test_read_geojsonseq(spark):
    """Test GeoJSONSeq read with NYC boroughs."""
    path = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojsonl"
    result = geojson_examples.read_geojsonseq(spark, path)
    assert result is not None
    assert result.count() == 5  # NYC has 5 boroughs
    assert 'geom_0' in result.columns


def test_sql_constant():
    """Test SQL constant is defined and valid."""
    assert hasattr(geojson_examples, 'SQL_GEOJSON')
    assert 'geojson_ogr.' in geojson_examples.SQL_GEOJSON
    assert 'SELECT' in geojson_examples.SQL_GEOJSON


def test_output_constants():
    """GeoJSON reader doc: output constants for Example output blocks."""
    for name in ("READ_GEOJSON_output", "READ_GEOJSONSEQ_output", "SQL_GEOJSON_output"):
        assert hasattr(geojson_examples, name), f"missing {name}"
        assert isinstance(getattr(geojson_examples, name), str)
        assert len(getattr(geojson_examples, name).strip()) > 0
