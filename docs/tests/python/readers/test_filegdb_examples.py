"""
Tests for File Geodatabase Reader Examples

These tests verify that the code examples in the documentation are valid.

Run:
    pytest docs/tests/python/readers/test_filegdb_examples.py -v
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import filegdb_examples
from path_config import SAMPLE_DATA_BASE

# Sample data path at runtime (path_config)
SAMPLE_FILEGDB = f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"

@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("FileGDBExamplesTest").getOrCreate()


def test_read_filegdb(spark):
    """Test basic File Geodatabase read - validates READ_FILEGDB constant."""
    result = filegdb_examples.read_filegdb(spark, SAMPLE_FILEGDB)
    assert result is not None
    assert result.count() > 0
    # FileGDB typically uses SHAPE for geometry column
    columns_lower = [c.lower() for c in result.columns]
    assert any('shape' in c for c in columns_lower) or 'geom_0' in result.columns


def test_read_with_layer(spark):
    """Test reading specific feature class - validates READ_WITH_LAYER constant."""
    result = filegdb_examples.read_with_layer(spark, SAMPLE_FILEGDB, layer="NYC_Boroughs")
    assert result is not None
    # Should return data if layer exists, or handle gracefully if not


def test_sql_constant():
    """Test SQL constant is defined and valid."""
    assert hasattr(filegdb_examples, 'SQL_FILEGDB')
    assert 'file_gdb_ogr.' in filegdb_examples.SQL_FILEGDB
    assert 'SELECT' in filegdb_examples.SQL_FILEGDB


def test_output_constants():
    """FileGDB reader doc: output constants for Example output blocks."""
    for name in ("READ_FILEGDB_output", "READ_WITH_LAYER_output", "SQL_FILEGDB_output"):
        assert hasattr(filegdb_examples, name), f"missing {name}"
        assert isinstance(getattr(filegdb_examples, name), str)
        assert len(getattr(filegdb_examples, name).strip()) > 0
