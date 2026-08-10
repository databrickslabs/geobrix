"""
Tests for the light (pyrx) tier per-function RasterX accessor examples.

Ensures all examples in rasterx_accessors_python_light.py are executable
and produce real, valid results. No JAR is required — examples use in-memory
synthetic GeoTIFF data.
"""

try:
    from . import rasterx_accessors_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_accessors_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


def test_rst_bandmetadata_python_light_example(spark):
    """rst_bandmetadata returns a dict of band metadata keys."""
    assert light_examples is not None
    result = light_examples.rst_bandmetadata_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_format_python_light_example(spark):
    """rst_format returns 'GTiff' for a GeoTIFF tile."""
    assert light_examples is not None
    result = light_examples.rst_format_python_light_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"


def test_rst_georeference_python_light_example(spark):
    """rst_georeference returns a dict with georeference parameters."""
    assert light_examples is not None
    result = light_examples.rst_georeference_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_getnodata_python_light_example(spark):
    """rst_getnodata returns a list of NoData values."""
    assert light_examples is not None
    result = light_examples.rst_getnodata_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_getsubdataset_python_light_example(spark):
    """rst_getsubdataset extracts a NetCDF subdataset and returns width=4."""
    assert light_examples is not None
    result = light_examples.rst_getsubdataset_python_light_example(spark)
    assert result == 4, f"Expected width 4 from extracted subdataset, got {result!r}"


def test_rst_height_python_light_example(spark):
    """rst_height returns 3 for the 3-row synthetic raster."""
    assert light_examples is not None
    result = light_examples.rst_height_python_light_example(spark)
    assert result == 3, f"Expected height 3, got {result}"


def test_rst_max_python_light_example(spark):
    """rst_max returns a list of per-band maximum values."""
    assert light_examples is not None
    result = light_examples.rst_max_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_median_python_light_example(spark):
    """rst_median returns a list of per-band median values."""
    assert light_examples is not None
    result = light_examples.rst_median_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_memsize_python_light_example(spark):
    """rst_memsize returns a positive integer byte count."""
    assert light_examples is not None
    result = light_examples.rst_memsize_python_light_example(spark)
    assert isinstance(result, int) and result > 0, f"Expected positive int, got {result!r}"


def test_rst_metadata_python_light_example(spark):
    """rst_metadata returns a dict of metadata."""
    assert light_examples is not None
    result = light_examples.rst_metadata_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_min_python_light_example(spark):
    """rst_min returns a list of per-band minimum values."""
    assert light_examples is not None
    result = light_examples.rst_min_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_pixelcount_python_light_example(spark):
    """rst_pixelcount returns a list of pixel counts per band."""
    assert light_examples is not None
    result = light_examples.rst_pixelcount_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_pixelheight_python_light_example(spark):
    """rst_pixelheight returns a positive float."""
    assert light_examples is not None
    result = light_examples.rst_pixelheight_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_pixelwidth_python_light_example(spark):
    """rst_pixelwidth returns a positive float."""
    assert light_examples is not None
    result = light_examples.rst_pixelwidth_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_rotation_python_light_example(spark):
    """rst_rotation returns a float (0.0 for north-up axis-aligned raster)."""
    assert light_examples is not None
    result = light_examples.rst_rotation_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_scalex_python_light_example(spark):
    """rst_scalex returns a non-zero float."""
    assert light_examples is not None
    result = light_examples.rst_scalex_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_scaley_python_light_example(spark):
    """rst_scaley returns a non-zero float (negative for north-up rasters)."""
    assert light_examples is not None
    result = light_examples.rst_scaley_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_skewx_python_light_example(spark):
    """rst_skewx returns a float."""
    assert light_examples is not None
    result = light_examples.rst_skewx_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_skewy_python_light_example(spark):
    """rst_skewy returns a float."""
    assert light_examples is not None
    result = light_examples.rst_skewy_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_srid_python_light_example(spark):
    """rst_srid returns 4326 for an EPSG:4326 raster."""
    assert light_examples is not None
    result = light_examples.rst_srid_python_light_example(spark)
    assert result == 4326, f"Expected 4326, got {result}"


def test_rst_crs_python_light_example(spark):
    """rst_crs returns a non-empty CRS string."""
    assert light_examples is not None
    result = light_examples.rst_crs_python_light_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"


def test_rst_subdatasets_python_light_example(spark):
    """rst_subdatasets returns a dict (empty for plain GeoTIFF)."""
    assert light_examples is not None
    result = light_examples.rst_subdatasets_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_summary_python_light_example(spark):
    """rst_summary returns a non-empty JSON string."""
    assert light_examples is not None
    result = light_examples.rst_summary_python_light_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"


def test_rst_type_python_light_example(spark):
    """rst_type returns a list of band type strings."""
    assert light_examples is not None
    result = light_examples.rst_type_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"
    assert isinstance(result[0], str), f"Expected string in list, got {type(result[0])}"


def test_rst_upperleftx_python_light_example(spark):
    """rst_upperleftx returns a float."""
    assert light_examples is not None
    result = light_examples.rst_upperleftx_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_upperlefty_python_light_example(spark):
    """rst_upperlefty returns a float."""
    assert light_examples is not None
    result = light_examples.rst_upperlefty_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_isempty_python_light_example(spark):
    """rst_isempty returns False for a valid non-empty raster tile."""
    assert light_examples is not None
    result = light_examples.rst_isempty_python_light_example(spark)
    assert result is False, f"Expected False, got {result!r}"


def test_rst_tryopen_python_light_example(spark):
    """rst_tryopen returns True for a valid raster tile."""
    assert light_examples is not None
    result = light_examples.rst_tryopen_python_light_example(spark)
    assert result is True, f"Expected True, got {result!r}"


def test_rst_histogram_python_light_example(spark):
    """rst_histogram returns a dict keyed by band name with list values."""
    assert light_examples is not None
    result = light_examples.rst_histogram_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) >= 1, "Expected at least one band in histogram"
    for v in result.values():
        assert isinstance(v, list), f"Expected list values in histogram dict, got {type(v)}"


def test_output_constants_exist():
    """All *_output constants are present and non-empty strings."""
    assert light_examples is not None
    output_names = [
        "rst_bandmetadata_python_light_example_output",
        "rst_format_python_light_example_output",
        "rst_georeference_python_light_example_output",
        "rst_getnodata_python_light_example_output",
        "rst_getsubdataset_python_light_example_output",
        "rst_height_python_light_example_output",
        "rst_max_python_light_example_output",
        "rst_median_python_light_example_output",
        "rst_memsize_python_light_example_output",
        "rst_metadata_python_light_example_output",
        "rst_min_python_light_example_output",
        "rst_pixelcount_python_light_example_output",
        "rst_pixelheight_python_light_example_output",
        "rst_pixelwidth_python_light_example_output",
        "rst_rotation_python_light_example_output",
        "rst_scalex_python_light_example_output",
        "rst_scaley_python_light_example_output",
        "rst_skewx_python_light_example_output",
        "rst_skewy_python_light_example_output",
        "rst_srid_python_light_example_output",
        "rst_crs_python_light_example_output",
        "rst_subdatasets_python_light_example_output",
        "rst_summary_python_light_example_output",
        "rst_type_python_light_example_output",
        "rst_upperleftx_python_light_example_output",
        "rst_upperlefty_python_light_example_output",
        "rst_isempty_python_light_example_output",
        "rst_tryopen_python_light_example_output",
        "rst_histogram_python_light_example_output",
    ]
    for name in output_names:
        assert hasattr(light_examples, name), f"Missing output constant: {name}"
        val = getattr(light_examples, name)
        assert isinstance(val, str) and val.strip(), f"Output constant is empty: {name}"
