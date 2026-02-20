"""
Tests for DBR-only Synthetic Points example (st_* functions).

Documentation: docs/docs/sample-data/additional.mdx § Synthetic Points (Vector)
Run on DBR: pytest docs/tests-dbr/python/sample_data/test_additional.py -v
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import additional


@pytest.mark.structure
def test_synthetic_points_constant():
    """Synthetic Points snippet is defined and uses st_* (DBR-only)."""
    assert isinstance(additional.SYNTHETIC_POINTS, str)
    assert "st_point" in additional.SYNTHETIC_POINTS
    assert "st_astext" in additional.SYNTHETIC_POINTS
    assert "synthetic_points" in additional.SYNTHETIC_POINTS


@pytest.mark.structure
def test_synthetic_points_output_constant():
    """Output constant for doc display exists."""
    assert hasattr(additional, "SYNTHETIC_POINTS_output")
    assert isinstance(additional.SYNTHETIC_POINTS_output, str)
    assert "synthetic points" in additional.SYNTHETIC_POINTS_output
