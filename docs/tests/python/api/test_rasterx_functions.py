"""
Test structure of RasterX functions documentation examples.
Only tests for code that is used in docs/docs/api/rasterx-functions.mdx.
"""

import sys
from pathlib import Path

try:
    from . import rasterx_functions
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_functions
    except ModuleNotFoundError:
        rasterx_functions = None


def test_rasterx_setup_example():
    """Common setup example is used in RasterX Function Reference doc."""
    assert rasterx_functions is not None
    assert hasattr(rasterx_functions, 'rasterx_setup_example')
    assert callable(rasterx_functions.rasterx_setup_example)
    assert hasattr(rasterx_functions, 'rasterx_setup_example_output')
