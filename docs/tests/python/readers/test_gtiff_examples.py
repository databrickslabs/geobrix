"""
Tests for GeoTIFF Reader Examples

These tests verify that the code examples in the documentation are valid.

Run:
    pytest docs/tests/python/readers/test_gtiff_examples.py -v
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import gtiff_examples

# Sample data paths
SAMPLE_GTIFF = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"

@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("GTiffExamplesTest").getOrCreate()


def test_read_gtiff(spark):
    """Test basic GeoTIFF read - validates READ_GTIFF constant."""
    result = gtiff_examples.read_gtiff(spark, SAMPLE_GTIFF)
    assert result is not None
    assert result.count() > 0
    assert 'tile' in result.columns


def test_read_with_options(spark):
    """Test GeoTIFF read with options - validates READ_WITH_OPTIONS constant."""
    result = gtiff_examples.read_with_options(spark, SAMPLE_GTIFF)
    assert result is not None
    assert result.count() > 0


def test_sql_constant():
    """Test SQL constant is defined and valid."""
    assert hasattr(gtiff_examples, 'SQL_GTIFF')
    assert 'gtiff_gdal.' in gtiff_examples.SQL_GTIFF
    assert 'SELECT' in gtiff_examples.SQL_GTIFF


def test_output_constants():
    """GeoTIFF reader doc: output constants for Example output blocks (one-copy)."""
    for name in ("READ_GTIFF_output", "READ_WITH_OPTIONS_output", "SQL_GTIFF_output"):
        assert hasattr(gtiff_examples, name), f"missing {name}"
        assert isinstance(getattr(gtiff_examples, name), str)
        assert len(getattr(gtiff_examples, name).strip()) > 0


def test_gtiff_vs_gdal_and_cog_constants():
    """One-copy: comparison and COG snippets exist and use Volumes path."""
    assert hasattr(gtiff_examples, 'GTIFF_VS_GDAL')
    assert hasattr(gtiff_examples, 'COG_EXAMPLE')
    assert "geobrix_samples" in gtiff_examples.GTIFF_VS_GDAL
    assert "geobrix_samples" in gtiff_examples.COG_EXAMPLE
