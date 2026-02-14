"""
Tests for Setup page code examples (constants = payload only in docs).

Documentation: docs/docs/sample-data/setup.mdx
Run: pytest docs/tests/python/sample_data/test_setup.py -v
"""
import pytest
from . import setup as sample_data_setup


@pytest.mark.structure
def test_setup_configure_storage_constant():
    code = sample_data_setup.SETUP_CONFIGURE_STORAGE
    assert isinstance(code, str)
    assert "SAMPLE_DATA_PATH" in code
    assert "pathlib" in code
    assert "CATALOG" in code and "VOLUME" in code
    assert sample_data_setup.SETUP_CONFIGURE_STORAGE_output.strip()
