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


# ---------------------------------------------------------------------------
# Accessor family — heavy-tier tests
# ---------------------------------------------------------------------------


def test_rst_bandmetadata_python_heavy_example(spark):
    """rst_bandmetadata returns a dict of band metadata."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_bandmetadata_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_format_python_heavy_example(spark):
    """rst_format returns a non-empty format string."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_format_python_heavy_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"


def test_rst_georeference_python_heavy_example(spark):
    """rst_georeference returns a dict with georeference parameters."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_georeference_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_getnodata_python_heavy_example(spark):
    """rst_getnodata returns a list of NoData values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_getnodata_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_getsubdataset_python_heavy_example(spark):
    """rst_getsubdataset extracts a NetCDF subdataset and returns width=4."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_getsubdataset_python_heavy_example(spark)
    assert result == 4, f"Expected width 4 from extracted subdataset, got {result!r}"


def test_rst_height_python_heavy_example(spark):
    """rst_height returns 3 for the 3-row synthetic raster."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_height_python_heavy_example(spark)
    assert result == 3, f"Expected height 3, got {result}"


def test_rst_max_python_heavy_example(spark):
    """rst_max returns a list of per-band maximum values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_max_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_median_python_heavy_example(spark):
    """rst_median returns a list of per-band median values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_median_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_memsize_python_heavy_example(spark):
    """rst_memsize returns a positive integer."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_memsize_python_heavy_example(spark)
    assert isinstance(result, int) and result > 0, f"Expected positive int, got {result!r}"


def test_rst_metadata_python_heavy_example(spark):
    """rst_metadata returns a dict."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_metadata_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_min_python_heavy_example(spark):
    """rst_min returns a list of per-band minimum values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_min_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_pixelcount_python_heavy_example(spark):
    """rst_pixelcount returns a list of pixel counts."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_pixelcount_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"


def test_rst_pixelheight_python_heavy_example(spark):
    """rst_pixelheight returns a positive float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_pixelheight_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_pixelwidth_python_heavy_example(spark):
    """rst_pixelwidth returns a positive float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_pixelwidth_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_rotation_python_heavy_example(spark):
    """rst_rotation returns a float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_rotation_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_scalex_python_heavy_example(spark):
    """rst_scalex returns a non-zero float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_scalex_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_scaley_python_heavy_example(spark):
    """rst_scaley returns a float (negative for north-up rasters)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_scaley_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_skewx_python_heavy_example(spark):
    """rst_skewx returns a float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_skewx_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_skewy_python_heavy_example(spark):
    """rst_skewy returns a float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_skewy_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_srid_python_heavy_example(spark):
    """rst_srid returns 4326 for an EPSG:4326 raster."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_srid_python_heavy_example(spark)
    assert result == 4326, f"Expected 4326, got {result}"


def test_rst_crs_python_heavy_example(spark):
    """rst_crs returns a non-empty CRS string."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_crs_python_heavy_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"


def test_rst_subdatasets_python_heavy_example(spark):
    """rst_subdatasets returns a dict (empty for plain GeoTIFF)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_subdatasets_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_summary_python_heavy_example(spark):
    """rst_summary returns a non-empty JSON string."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_summary_python_heavy_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"


def test_rst_type_python_heavy_example(spark):
    """rst_type returns a list of band type strings."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_type_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"
    assert isinstance(result[0], str), f"Expected string in list, got {type(result[0])}"


def test_rst_upperleftx_python_heavy_example(spark):
    """rst_upperleftx returns a float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_upperleftx_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_upperlefty_python_heavy_example(spark):
    """rst_upperlefty returns a float."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_upperlefty_python_heavy_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"


def test_rst_isempty_python_heavy_example(spark):
    """rst_isempty returns False for a valid non-empty tile."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_isempty_python_heavy_example(spark)
    assert result is False, f"Expected False, got {result!r}"


def test_rst_tryopen_python_heavy_example(spark):
    """rst_tryopen returns True for a valid tile."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tryopen_python_heavy_example(spark)
    assert result is True, f"Expected True, got {result!r}"


def test_rst_histogram_python_heavy_example(spark):
    """rst_histogram returns a dict keyed by band name with list values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_histogram_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) >= 1, "Expected at least one band in histogram"
    for v in result.values():
        assert isinstance(v, list), f"Expected list values in histogram dict, got {type(v)}"


def test_accessor_heavy_output_constants_exist():
    """All accessor heavy-tier *_output constants are present and non-empty strings."""
    assert rasterx_functions is not None
    output_names = [
        "rst_bandmetadata_python_heavy_example_output",
        "rst_format_python_heavy_example_output",
        "rst_georeference_python_heavy_example_output",
        "rst_getnodata_python_heavy_example_output",
        "rst_getsubdataset_python_heavy_example_output",
        "rst_height_python_heavy_example_output",
        "rst_max_python_heavy_example_output",
        "rst_median_python_heavy_example_output",
        "rst_memsize_python_heavy_example_output",
        "rst_metadata_python_heavy_example_output",
        "rst_min_python_heavy_example_output",
        "rst_pixelcount_python_heavy_example_output",
        "rst_pixelheight_python_heavy_example_output",
        "rst_pixelwidth_python_heavy_example_output",
        "rst_rotation_python_heavy_example_output",
        "rst_scalex_python_heavy_example_output",
        "rst_scaley_python_heavy_example_output",
        "rst_skewx_python_heavy_example_output",
        "rst_skewy_python_heavy_example_output",
        "rst_srid_python_heavy_example_output",
        "rst_crs_python_heavy_example_output",
        "rst_subdatasets_python_heavy_example_output",
        "rst_summary_python_heavy_example_output",
        "rst_type_python_heavy_example_output",
        "rst_upperleftx_python_heavy_example_output",
        "rst_upperlefty_python_heavy_example_output",
        "rst_isempty_python_heavy_example_output",
        "rst_tryopen_python_heavy_example_output",
        "rst_histogram_python_heavy_example_output",
    ]
    for name in output_names:
        assert hasattr(rasterx_functions, name), f"Missing output constant: {name}"
        val = getattr(rasterx_functions, name)
        assert isinstance(val, str) and val.strip(), f"Output constant is empty: {name}"
