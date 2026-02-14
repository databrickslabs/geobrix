"""
Tests for VectorX Function Reference Examples

Validates the single st_legacyaswkb example (legacy point to WKB).
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import vectorx_functions


def test_vectorx_setup_example():
    """Common setup example exists and has output constant."""
    assert hasattr(vectorx_functions, 'vectorx_setup_example')
    assert callable(vectorx_functions.vectorx_setup_example)
    assert hasattr(vectorx_functions, 'vectorx_setup_example_output')


def test_st_legacyaswkb_sql_example_constant():
    """SQL example constant exists for docs."""
    assert hasattr(vectorx_functions, 'ST_LEGACYASWKB_SQL_EXAMPLE')
    assert 'gbx_st_legacyaswkb' in vectorx_functions.ST_LEGACYASWKB_SQL_EXAMPLE
    assert hasattr(vectorx_functions, 'ST_LEGACYASWKB_SQL_EXAMPLE_output')


def test_st_legacyaswkb_python_example_callable(spark):
    """st_legacyaswkb_python_example is defined and callable."""
    assert callable(vectorx_functions.st_legacyaswkb_python_example)


def test_st_legacyaswkb_python_example_executes(spark):
    """st_legacyaswkb_python_example runs and returns one row with wkb column."""
    result = vectorx_functions.st_legacyaswkb_python_example(spark)
    rows = result.collect()
    assert len(rows) == 1
    assert "wkb" in result.columns
    assert rows[0]["wkb"] is not None
