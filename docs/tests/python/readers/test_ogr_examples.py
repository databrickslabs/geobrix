"""Tests for OGR Reader Examples"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import ogr_examples
from path_config import MIN_BOROUGHS, MAX_BOROUGHS


def test_read_ogr(spark, sample_nyc_boroughs):
    """Test basic OGR read."""
    result = ogr_examples.read_ogr(spark, sample_nyc_boroughs)
    assert result is not None
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


def test_read_with_driver(spark, sample_nyc_boroughs):
    """Test OGR read with explicit driver name."""
    result = ogr_examples.read_with_driver(spark, sample_nyc_boroughs)
    assert result is not None
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


def test_sql_constant():
    """Test SQL constant is defined."""
    assert ogr_examples.SQL_OGR is not None
    assert "ogr." in ogr_examples.SQL_OGR
    assert "SELECT" in ogr_examples.SQL_OGR


def test_output_constants():
    """OGR reader doc: output constants for Example output blocks (one-copy)."""
    for name in ("READ_OGR_output", "READ_WITH_DRIVER_output", "SQL_OGR_output"):
        assert hasattr(ogr_examples, name), f"missing {name}"
        assert isinstance(getattr(ogr_examples, name), str)
        assert len(getattr(ogr_examples, name).strip()) > 0


def test_named_vs_ogr_constant():
    """One-copy: Named vs OGR snippet uses Volumes path."""
    assert hasattr(ogr_examples, "NAMED_VS_OGR")
    assert "geobrix_samples" in ogr_examples.NAMED_VS_OGR
