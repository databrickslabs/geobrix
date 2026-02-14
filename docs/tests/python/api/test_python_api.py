"""
Tests for Python API Reference Examples

These are structure tests to validate that the example functions are properly defined.
"""
import pytest
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import python_api


def test_verify_installation():
    """Test installation verification (structure check)."""
    assert callable(python_api.verify_installation)


def test_import_pattern_rasterx(spark):
    """Test RasterX import pattern (structure check)."""
    assert callable(python_api.import_pattern_rasterx)


def test_import_pattern_gridx(spark):
    """Test GridX import pattern (structure check)."""
    assert callable(python_api.import_pattern_gridx)


def test_import_pattern_vectorx(spark):
    """Test VectorX import pattern (structure check)."""
    assert callable(python_api.import_pattern_vectorx)


def test_rst_boundingbox_example(spark):
    """Test rst_boundingbox example (structure check)."""
    assert callable(python_api.rst_boundingbox_example)


def test_rst_width_example(spark):
    """Test rst_width example (structure check)."""
    assert callable(python_api.rst_width_example)


def test_rst_height_example(spark):
    """Test rst_height example (structure check)."""
    assert callable(python_api.rst_height_example)


def test_rst_numbands_example(spark):
    """Test rst_numbands example (structure check)."""
    assert callable(python_api.rst_numbands_example)


def test_rst_metadata_example(spark):
    """Test rst_metadata example (structure check)."""
    assert callable(python_api.rst_metadata_example)


def test_rst_srid_example(spark):
    """Test rst_srid example (structure check)."""
    assert callable(python_api.rst_srid_example)


# rst_clip_example: runs in docs/tests; DBR-specific variants were consolidated.


def test_rasterx_complete_example(spark):
    """Test RasterX complete example (structure check)."""
    assert callable(python_api.rasterx_complete_example)


def test_bng_cellarea_example(spark):
    """Test BNG cellarea example (structure check)."""
    assert callable(python_api.bng_cellarea_example)


def test_bng_pointtocell_example(spark):
    """Test BNG pointtocell example (structure check)."""
    assert callable(python_api.bng_pointtocell_example)


def test_gridx_complete_example(spark):
    """Test GridX complete example (structure check)."""
    assert callable(python_api.gridx_complete_example)


def test_st_legacyaswkb_example(spark):
    """Test st_legacyaswkb example (structure check)."""
    assert callable(python_api.st_legacyaswkb_example)


def test_dataframe_select_operations(spark):
    """Test DataFrame select operations (structure check)."""
    assert callable(python_api.dataframe_select_operations)


def test_dataframe_filter_operations(spark):
    """Test DataFrame filter operations (structure check)."""
    assert callable(python_api.dataframe_filter_operations)


def test_dataframe_withcolumn_operations(spark):
    """Test DataFrame withColumn operations (structure check)."""
    assert callable(python_api.dataframe_withcolumn_operations)


def test_using_with_sql(spark):
    """Test using with SQL (structure check)."""
    assert callable(python_api.using_with_sql)


def test_type_hints_and_ide_support(spark):
    """Test type hints and IDE support (structure check)."""
    assert callable(python_api.type_hints_and_ide_support)


def test_error_handling_example(spark):
    """Test error handling example (structure check)."""
    assert callable(python_api.error_handling_example)
