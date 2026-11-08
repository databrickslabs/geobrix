"""
Tests for the RasterX setup example used in docs/docs/api/raster-functions.mdx.

The per-function examples of that page run in test_rasterx_functions_sql.py and the *_python_light.py test files.
"""

try:
    from . import rasterx_functions
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_functions
    except ModuleNotFoundError:
        rasterx_functions = None


def test_rasterx_setup_example():
    """The common setup example and its output constant exist."""
    assert rasterx_functions is not None
    assert callable(rasterx_functions.rasterx_setup_example)
    assert hasattr(rasterx_functions, "rasterx_setup_example_output")
