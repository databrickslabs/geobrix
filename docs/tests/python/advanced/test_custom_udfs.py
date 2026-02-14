"""
Tests for custom UDFs examples.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import custom_udfs


def test_eval_method_standard_usage(spark):
    """Test standard eval method usage (structure check)."""
    assert callable(custom_udfs.eval_method_standard_usage)


def test_basic_python_udf_example(spark):
    """Test basic Python UDF (structure check)."""
    assert callable(custom_udfs.basic_python_udf_example)


def test_integration_test_example():
    """Test integration testing pattern (structure check)."""
    assert callable(custom_udfs.integration_test_example)


def test_conditional_processing_example(spark):
    """Common pattern: conditional processing (structure check)."""
    assert callable(custom_udfs.conditional_processing_example)


def test_chained_processing_example(spark):
    """Common pattern: chained processing (structure check)."""
    assert callable(custom_udfs.chained_processing_example)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
