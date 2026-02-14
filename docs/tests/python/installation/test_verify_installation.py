"""
Tests for installation verification snippets (one-copy doc).
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import verify_installation


def test_verify_python_installation_constant():
    """Installation doc: Python verification snippet is defined."""
    assert hasattr(verify_installation, "VERIFY_PYTHON_INSTALLATION")
    s = verify_installation.VERIFY_PYTHON_INSTALLATION
    assert "rx.register(spark)" in s
    assert "SHOW FUNCTIONS" in s and "gbx_rst_*" in s


def test_verify_sql_functions_constant():
    """Installation doc: SQL verification snippet is defined."""
    assert hasattr(verify_installation, "VERIFY_SQL_FUNCTIONS")
    s = verify_installation.VERIFY_SQL_FUNCTIONS
    assert "SHOW FUNCTIONS LIKE 'gbx_*'" in s
    assert "DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox" in s


def test_verify_output_constants():
    """Installation doc: example output constants for Verification section."""
    assert hasattr(verify_installation, "VERIFY_PYTHON_INSTALLATION_output")
    assert hasattr(verify_installation, "VERIFY_SQL_FUNCTIONS_output")
    assert "function" in verify_installation.VERIFY_PYTHON_INSTALLATION_output
    assert "gbx_rst" in verify_installation.VERIFY_SQL_FUNCTIONS_output
