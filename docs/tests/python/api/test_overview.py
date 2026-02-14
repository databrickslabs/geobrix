"""
Tests for API Reference Overview Examples

These are structure tests to validate that the example functions are properly defined.
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


def test_register_packages_python(spark):
    """Test package registration (structure check)."""
    assert callable(overview.register_packages_python)


def test_sql_show_functions():
    """Test SQL SHOW FUNCTIONS constant."""
    assert isinstance(overview.SQL_SHOW_FUNCTIONS, str)
    assert "SHOW FUNCTIONS" in overview.SQL_SHOW_FUNCTIONS


def test_pattern_import_register(spark):
    """Test import and register pattern (structure check)."""
    assert callable(overview.pattern_import_register)


def test_pattern_mixed_language(spark):
    """Test mixed language usage pattern (structure check)."""
    assert callable(overview.pattern_mixed_language)


def test_pattern_chaining_operations(spark):
    """Test chaining operations pattern (structure check)."""
    assert callable(overview.pattern_chaining_operations)


def test_error_check_function_availability(spark):
    """Test function availability check (structure check)."""
    assert callable(overview.error_check_function_availability)


def test_error_functions_not_registered(spark):
    """Test functions not registered solution (structure check)."""
    assert callable(overview.error_functions_not_registered)


def test_error_import_errors():
    """Test import error solution (structure check)."""
    assert callable(overview.error_import_errors)


def test_performance_register_once(spark):
    """Test register once performance tip (structure check)."""
    assert callable(overview.performance_register_once)


def test_performance_use_dataframe_api(spark):
    """Test DataFrame API performance tip (structure check)."""
    assert callable(overview.performance_use_dataframe_api)


def test_performance_batch_operations(spark):
    """Test batch operations performance tip (structure check)."""
    assert callable(overview.performance_batch_operations)
