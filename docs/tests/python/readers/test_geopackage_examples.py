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
    # The geometry column is named as the layer names it (`geom` in the sample file),
    # with its `_srid` and `_srid_proj` companions: the page's schema and output.
    columns = result.columns
    assert columns[-3:] == ["geom", "geom_srid", "geom_srid_proj"], columns
    assert "geom_0" not in columns
    assert _output_columns(geopackage_examples.READ_GEOPACKAGE_output) == columns


def _output_columns(output):
    """Column names of a displayed ``show()`` output block."""
    return [c.strip() for c in output.splitlines()[1].strip("|").split("|")]


def test_read_specific_layer(spark):
    """Test reading specific layer from GeoPackage."""
    path = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
    result = geopackage_examples.read_specific_layer(spark, path, "boroughs")
    assert result is not None
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"
    assert _output_columns(geopackage_examples.READ_SPECIFIC_LAYER_output) == result.columns


def test_sql_constant():
    """Test SQL constant is defined and valid."""
    assert hasattr(geopackage_examples, 'SQL_GEOPACKAGE')
    assert 'gpkg_ogr.' in geopackage_examples.SQL_GEOPACKAGE
    cols = _output_columns(geopackage_examples.SQL_GEOPACKAGE_output)
    assert cols == _output_columns(geopackage_examples.READ_GEOPACKAGE_output)
    assert 'SELECT' in geopackage_examples.SQL_GEOPACKAGE


def test_output_constants():
    """GeoPackage reader doc: output constants for Example output blocks."""
    for name in ("READ_GEOPACKAGE_output", "READ_SPECIFIC_LAYER_output", "SQL_GEOPACKAGE_output"):
        assert hasattr(geopackage_examples, name), f"missing {name}"
        assert isinstance(getattr(geopackage_examples, name), str)
        assert len(getattr(geopackage_examples, name).strip()) > 0
