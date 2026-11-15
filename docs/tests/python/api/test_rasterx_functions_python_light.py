"""
Tests for the light (pyrx) tier per-function RasterX examples.

Ensures all examples in rasterx_functions_python_light.py are executable
and produce real, valid results. No JAR is required — examples use in-memory
synthetic GeoTIFF data.
"""

try:
    from . import rasterx_functions_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_functions_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


def test_rst_fromfile_python_light_example(spark):
    """rst_fromfile returns a valid tile; width is 4 for the synthetic raster."""
    assert light_examples is not None
    result = light_examples.rst_fromfile_python_example(spark)
    assert result == 4, f"Expected width 4, got {result}"


def test_output_constants_exist():
    """All *_output constants are present and non-empty strings."""
    assert light_examples is not None
    output_names = [
        "rst_fromfile_python_example_output",
    ]
    for name in output_names:
        assert hasattr(light_examples, name), f"Missing output constant: {name}"
        val = getattr(light_examples, name)
        assert isinstance(val, str) and val.strip(), f"Output constant is empty: {name}"
