"""
Test structure of RasterX functions documentation examples.
Only tests for code that is used in docs/docs/api/rasterx-functions.mdx.
"""

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
    assert hasattr(rasterx_functions, "rasterx_setup_example")
    assert callable(rasterx_functions.rasterx_setup_example)
    assert hasattr(rasterx_functions, "rasterx_setup_example_output")


# ---------------------------------------------------------------------------
# Heavy-Python per-function tests (all use in-memory synthetic data)
# ---------------------------------------------------------------------------


def test_rst_avg_python_heavy_example(spark):
    """rst_avg returns a list of per-band float averages."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_avg_python_heavy_example(spark)
    assert isinstance(result, list), f"Expected list, got {type(result)}"
    assert len(result) >= 1
    assert isinstance(result[0], float), f"Expected float in list, got {type(result[0])}"


def test_rst_boundingbox_python_heavy_example(spark):
    """rst_boundingbox returns non-null WKB binary bytes."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_boundingbox_python_heavy_example(spark)
    assert result is not None, "Expected non-null bbox"
    assert len(result) > 0, "Expected non-empty bbox bytes"


def test_rst_numbands_python_heavy_example(spark):
    """rst_numbands returns a positive integer."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_numbands_python_heavy_example(spark)
    assert isinstance(result, int) and result >= 1


def test_rst_width_python_heavy_example(spark):
    """rst_width returns 4 for the synthetic 4-pixel-wide raster."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_width_python_heavy_example(spark)
    assert result == 4, f"Expected width 4, got {result}"


def test_rst_fromfile_python_heavy_example(spark):
    """rst_fromfile (heavy) reads a temp-file tile and returns its width."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_fromfile_python_heavy_example(spark)
    assert result == 4, f"Expected width 4, got {result}"


def test_heavy_output_constants_exist():
    """All heavy-tier *_output constants are present and non-empty strings."""
    assert rasterx_functions is not None
    output_names = [
        "rst_avg_python_heavy_example_output",
        "rst_boundingbox_python_heavy_example_output",
        "rst_numbands_python_heavy_example_output",
        "rst_width_python_heavy_example_output",
        "rst_fromfile_python_heavy_example_output",
    ]
    for name in output_names:
        assert hasattr(rasterx_functions, name), f"Missing output constant: {name}"
        val = getattr(rasterx_functions, name)
        assert isinstance(val, str) and val.strip(), f"Output constant is empty: {name}"
