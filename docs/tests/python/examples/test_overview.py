"""
GeoBrix Examples Overview Tests

Tests for examples overview code.
"""

import pytest
import sys
from pathlib import Path
import importlib.util

# Import the module under test - explicitly load from current directory to avoid conflicts
overview_path = Path(__file__).parent / "overview.py"
spec = importlib.util.spec_from_file_location("overview", overview_path)
overview = importlib.util.module_from_spec(spec)
spec.loader.exec_module(overview)


def test_example_read_catalog_rasters(spark):
    """Test raster cataloging example (structure check)."""
    assert callable(overview.example_read_catalog_rasters)


def test_example_spatial_aggregation_bng(spark):
    """Test BNG spatial aggregation example (structure check)."""
    assert callable(overview.example_spatial_aggregation_bng)


def test_example_migrate_from_mosaic(spark):
    """Test Mosaic migration example (structure check)."""
    assert callable(overview.example_migrate_from_mosaic)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
