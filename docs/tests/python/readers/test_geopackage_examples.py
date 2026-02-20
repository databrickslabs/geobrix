"""
Tests for GeoPackage Reader Examples

Validates code examples shown in docs/docs/readers/geopackage.mdx
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import geopackage_examples
from path_config import SAMPLE_DATA_BASE, MIN_BOROUGHS, MAX_BOROUGHS


def test_read_geopackage(spark):
    """Test basic GeoPackage read."""
    path = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
    result = geopackage_examples.read_geopackage(spark, path)
    assert result is not None
    assert result.count() > 0
    # GeoPackage typically has 'shape' column for geometry
    columns = result.columns
    assert any('shape' in col.lower() or 'geom' in col.lower() for col in columns)


def test_read_specific_layer(spark):
    """Test reading specific layer from GeoPackage."""
    path = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
    result = geopackage_examples.read_specific_layer(spark, path, "boroughs")
    assert result is not None
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


def test_sql_constant():
    """Test SQL constant is defined and valid."""
    assert hasattr(geopackage_examples, 'SQL_GEOPACKAGE')
    assert 'gpkg_ogr.' in geopackage_examples.SQL_GEOPACKAGE
    assert 'SELECT' in geopackage_examples.SQL_GEOPACKAGE


def test_output_constants():
    """GeoPackage reader doc: output constants for Example output blocks."""
    for name in ("READ_GEOPACKAGE_output", "READ_SPECIFIC_LAYER_output", "SQL_GEOPACKAGE_output"):
        assert hasattr(geopackage_examples, name), f"missing {name}"
        assert isinstance(getattr(geopackage_examples, name), str)
        assert len(getattr(geopackage_examples, name).strip()) > 0
