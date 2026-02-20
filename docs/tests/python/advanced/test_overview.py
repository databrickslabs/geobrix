"""
Tests for advanced overview examples.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import overview


def test_spark_expressions_standard_usage(spark):
    """Test standard Spark expressions usage (structure check)."""
    assert callable(overview.spark_expressions_standard_usage)


def test_end_to_end_advanced_pipeline(spark):
    """Test end-to-end pipeline (structure check)."""
    assert callable(overview.end_to_end_advanced_pipeline)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
