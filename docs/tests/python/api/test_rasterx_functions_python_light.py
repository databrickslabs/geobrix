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


def test_rst_avg_python_light_example(spark):
    """rst_avg returns a list of per-band float averages."""
    assert light_examples is not None
    result = light_examples.rst_avg_python_light_example(spark)
    assert isinstance(result, list), f"Expected list, got {type(result)}"
    assert len(result) >= 1
    assert isinstance(result[0], float), f"Expected float in list, got {type(result[0])}"


def test_rst_boundingbox_python_light_example(spark):
    """rst_boundingbox returns non-null WKB binary bytes for the bounding polygon."""
    assert light_examples is not None
    result = light_examples.rst_boundingbox_python_light_example(spark)
    assert result is not None, "Expected non-null bbox"
    assert len(result) > 0, "Expected non-empty bbox bytes"


def test_rst_numbands_python_light_example(spark):
    """rst_numbands returns 1 for a single-band synthetic raster."""
    assert light_examples is not None
    result = light_examples.rst_numbands_python_light_example(spark)
    assert result == 1, f"Expected 1 band, got {result}"


def test_rst_width_python_light_example(spark):
    """rst_width returns 4 for the 4-pixel-wide synthetic raster."""
    assert light_examples is not None
    result = light_examples.rst_width_python_light_example(spark)
    assert result == 4, f"Expected width 4, got {result}"


def test_rst_fromfile_python_light_example(spark):
    """rst_fromfile returns a valid tile; width is 4 for the synthetic raster."""
    assert light_examples is not None
    result = light_examples.rst_fromfile_python_light_example(spark)
    assert result == 4, f"Expected width 4, got {result}"


def test_output_constants_exist():
    """All *_output constants are present and non-empty strings."""
    assert light_examples is not None
    output_names = [
        "rst_avg_python_light_example_output",
        "rst_boundingbox_python_light_example_output",
        "rst_numbands_python_light_example_output",
        "rst_width_python_light_example_output",
        "rst_fromfile_python_light_example_output",
    ]
    for name in output_names:
        assert hasattr(light_examples, name), f"Missing output constant: {name}"
        val = getattr(light_examples, name)
        assert isinstance(val, str) and val.strip(), f"Output constant is empty: {name}"
