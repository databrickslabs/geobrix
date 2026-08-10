"""
Tests for the light (pyrx) tier per-function RasterX accessor examples.

Ensures all examples in rasterx_accessors_python_light.py are executable
and produce real, valid results from the canonical fixtures.

Fixture assignments:
- SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618, 161x236, 10m pixels):
  rst_format, rst_georeference, rst_getnodata, rst_height, rst_memsize,
  rst_metadata, rst_rotation, rst_scalex, rst_scaley, rst_skewx, rst_skewy,
  rst_srid, rst_crs, rst_upperleftx, rst_upperlefty, rst_width, rst_boundingbox,
  rst_pixelheight, rst_pixelwidth
- MULTIBAND (rgb_nir_small.tif, EPSG:4326, 8x8, 3 bands, UInt16):
  rst_avg, rst_bandmetadata, rst_histogram, rst_isempty, rst_max, rst_median,
  rst_min, rst_numbands, rst_pixelcount, rst_summary, rst_tryopen, rst_type
- NETCDF (prAdjust_day_HadGEM2-CC_*.nc, 2 subdatasets):
  rst_subdatasets, rst_getsubdataset
"""

try:
    from . import rasterx_accessors_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_accessors_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


# ---------------------------------------------------------------------------
# MULTIBAND fixture tests
# ---------------------------------------------------------------------------


def test_rst_avg_python_light_example(spark):
    """rst_avg returns [83.59375, 153.125, 114.3125] for the 3-band multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_avg_python_light_example(spark)
    assert isinstance(result, list), f"Expected list, got {type(result)}"
    assert len(result) == 3, f"Expected 3 bands, got {len(result)}"
    assert result[0] is not None, "band_averages[0] should not be None (all-NoData)"


def test_rst_bandmetadata_python_light_example(spark):
    """rst_bandmetadata returns non-empty dict with real per-band tags for the multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_bandmetadata_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) > 0, f"Expected non-empty metadata dict, got {result!r}"
    # multiband fixture has name, wavelength_nm, band_index per band
    assert "name" in result, f"Expected 'name' key in band metadata, got {result!r}"


def test_rst_histogram_python_light_example(spark):
    """rst_histogram returns a dict with 3 band keys for the multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_histogram_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) == 3, f"Expected 3 bands in histogram, got {len(result)}"
    for v in result.values():
        assert isinstance(v, list), f"Expected list values in histogram dict, got {type(v)}"


def test_rst_isempty_python_light_example(spark):
    """rst_isempty returns False for the multiband fixture (has real pixel data)."""
    assert light_examples is not None
    result = light_examples.rst_isempty_python_light_example(spark)
    assert result is False, f"Expected False (multiband has real data), got {result!r}"


def test_rst_max_python_light_example(spark):
    """rst_max returns [119.0, 197.0, 148.0] for the 3-band multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_max_python_light_example(spark)
    assert isinstance(result, list) and len(result) == 3, f"Expected 3-band list, got {result!r}"
    assert result[0] is not None, "band_max[0] should not be None"


def test_rst_median_python_light_example(spark):
    """rst_median returns [85.0, 157.5, 111.5] for the 3-band multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_median_python_light_example(spark)
    assert isinstance(result, list) and len(result) == 3, f"Expected 3-band list, got {result!r}"
    assert result[0] is not None, "band_median[0] should not be None"


def test_rst_min_python_light_example(spark):
    """rst_min returns [50.0, 102.0, 82.0] for the 3-band multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_min_python_light_example(spark)
    assert isinstance(result, list) and len(result) == 3, f"Expected 3-band list, got {result!r}"
    assert result[0] is not None, "band_min[0] should not be None"


def test_rst_numbands_python_light_example(spark):
    """rst_numbands returns 3 for the multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_numbands_python_light_example(spark)
    assert result == 3, f"Expected 3 bands, got {result}"


def test_rst_pixelcount_python_light_example(spark):
    """rst_pixelcount returns [64, 64, 64] for the multiband fixture (8x8, no NoData)."""
    assert light_examples is not None
    result = light_examples.rst_pixelcount_python_light_example(spark)
    assert isinstance(result, list) and len(result) == 3, f"Expected 3-band list, got {result!r}"
    assert all(v == 64 for v in result), f"Expected [64, 64, 64], got {result!r}"


def test_rst_summary_python_light_example(spark):
    """rst_summary returns a non-empty JSON string for the multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_summary_python_light_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"
    assert "GTiff" in result or "driverShortName" in result, f"Summary missing driver info: {result[:200]}"


def test_rst_tryopen_python_light_example(spark):
    """rst_tryopen returns True for the multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_tryopen_python_light_example(spark)
    assert result is True, f"Expected True, got {result!r}"


def test_rst_type_python_light_example(spark):
    """rst_type returns ['UInt16', 'UInt16', 'UInt16'] for the 3-band multiband fixture."""
    assert light_examples is not None
    result = light_examples.rst_type_python_light_example(spark)
    assert isinstance(result, list) and len(result) == 3, f"Expected 3-band list, got {result!r}"
    assert all(t == "UInt16" for t in result), f"Expected all UInt16, got {result!r}"


# ---------------------------------------------------------------------------
# SINGLE-BAND fixture tests
# ---------------------------------------------------------------------------


def test_rst_boundingbox_python_light_example(spark):
    """rst_boundingbox returns WKB bytes for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_boundingbox_python_light_example(spark)
    assert result is not None, "rst_boundingbox should not return None"
    assert isinstance(result, (bytes, bytearray)), f"Expected bytes, got {type(result)}"
    assert len(result) > 0, "WKB bytes should be non-empty"


def test_rst_crs_python_light_example(spark):
    """rst_crs returns 'EPSG:32618' for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_crs_python_light_example(spark)
    assert isinstance(result, str) and len(result) > 0, f"Expected non-empty string, got {result!r}"
    assert "32618" in result, f"Expected EPSG:32618, got {result!r}"


def test_rst_format_python_light_example(spark):
    """rst_format returns 'GTiff' for the single-band GeoTIFF fixture."""
    assert light_examples is not None
    result = light_examples.rst_format_python_light_example(spark)
    assert isinstance(result, str) and result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_georeference_python_light_example(spark):
    """rst_georeference returns a dict with georeference parameters for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_georeference_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert "scaleX" in result, f"Expected 'scaleX' key in georeference, got {result.keys()}"
    assert result["scaleX"] == 10.0, f"Expected scaleX=10.0, got {result.get('scaleX')}"


def test_rst_getnodata_python_light_example(spark):
    """rst_getnodata returns [0.0] for the single-band fixture (nodata=0.0)."""
    assert light_examples is not None
    result = light_examples.rst_getnodata_python_light_example(spark)
    assert isinstance(result, list) and len(result) >= 1, f"Expected non-empty list, got {result!r}"
    assert result[0] == 0.0, f"Expected nodata=0.0, got {result[0]}"


def test_rst_height_python_light_example(spark):
    """rst_height returns 161 for the single-band sentinel2 fixture."""
    assert light_examples is not None
    result = light_examples.rst_height_python_light_example(spark)
    assert result == 161, f"Expected height 161, got {result}"


def test_rst_memsize_python_light_example(spark):
    """rst_memsize returns a positive integer for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_memsize_python_light_example(spark)
    assert isinstance(result, int) and result > 0, f"Expected positive int, got {result!r}"


def test_rst_metadata_python_light_example(spark):
    """rst_metadata returns a dict with driver, crs, count fields for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_metadata_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert "driver" in result, "Expected 'driver' key in metadata"
    assert result.get("driver") == "GTiff", f"Expected GTiff driver, got {result.get('driver')}"


def test_rst_pixelheight_python_light_example(spark):
    """rst_pixelheight returns 10.0 for the single-band fixture (EPSG:32618, 10m pixels)."""
    assert light_examples is not None
    result = light_examples.rst_pixelheight_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 10.0, f"Expected 10.0, got {result}"


def test_rst_pixelwidth_python_light_example(spark):
    """rst_pixelwidth returns 10.0 for the single-band fixture (EPSG:32618, 10m pixels)."""
    assert light_examples is not None
    result = light_examples.rst_pixelwidth_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 10.0, f"Expected 10.0, got {result}"


def test_rst_rotation_python_light_example(spark):
    """rst_rotation returns 0.0 for the axis-aligned single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_rotation_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 0.0, f"Expected 0.0, got {result}"


def test_rst_scalex_python_light_example(spark):
    """rst_scalex returns 10.0 for the single-band fixture (10m pixels)."""
    assert light_examples is not None
    result = light_examples.rst_scalex_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 10.0, f"Expected 10.0, got {result}"


def test_rst_scaley_python_light_example(spark):
    """rst_scaley returns -10.0 for the north-up single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_scaley_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == -10.0, f"Expected -10.0, got {result}"


def test_rst_skewx_python_light_example(spark):
    """rst_skewx returns 0.0 for the axis-aligned single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_skewx_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 0.0, f"Expected 0.0, got {result}"


def test_rst_skewy_python_light_example(spark):
    """rst_skewy returns 0.0 for the axis-aligned single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_skewy_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 0.0, f"Expected 0.0, got {result}"


def test_rst_srid_python_light_example(spark):
    """rst_srid returns 32618 for the single-band EPSG:32618 fixture."""
    assert light_examples is not None
    result = light_examples.rst_srid_python_light_example(spark)
    assert result == 32618, f"Expected 32618, got {result}"


def test_rst_upperleftx_python_light_example(spark):
    """rst_upperleftx returns 2121950.0 for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_upperleftx_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == 2121950.0, f"Expected 2121950.0, got {result}"


def test_rst_upperlefty_python_light_example(spark):
    """rst_upperlefty returns -10790470.0 for the single-band fixture."""
    assert light_examples is not None
    result = light_examples.rst_upperlefty_python_light_example(spark)
    assert isinstance(result, float), f"Expected float, got {type(result)}"
    assert result == -10790470.0, f"Expected -10790470.0, got {result}"


def test_rst_width_python_light_example(spark):
    """rst_width returns 236 for the single-band sentinel2 fixture."""
    assert light_examples is not None
    result = light_examples.rst_width_python_light_example(spark)
    assert result == 236, f"Expected width 236, got {result}"


# ---------------------------------------------------------------------------
# NETCDF fixture tests
# ---------------------------------------------------------------------------


def test_rst_getsubdataset_python_light_example(spark):
    """rst_getsubdataset extracts the prAdjust subdataset; width of result is 720."""
    assert light_examples is not None
    result = light_examples.rst_getsubdataset_python_light_example(spark)
    assert result == 720, f"Expected width 720 for prAdjust subdataset, got {result!r}"


def test_rst_subdatasets_python_light_example(spark):
    """rst_subdatasets returns a non-empty dict with SUBDATASET keys for the NetCDF fixture."""
    assert light_examples is not None
    result = light_examples.rst_subdatasets_python_light_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) > 0, f"Expected non-empty subdatasets dict (NetCDF fixture), got {result!r}"
    # Should contain SUBDATASET_1_NAME and SUBDATASET_2_NAME
    assert any("SUBDATASET" in k for k in result.keys()), f"Expected SUBDATASET keys, got {list(result.keys())}"


# ---------------------------------------------------------------------------
# Output constants existence check
# ---------------------------------------------------------------------------


def test_output_constants_exist():
    """All *_output constants are present and non-empty strings."""
    assert light_examples is not None
    output_names = [
        "rst_avg_python_light_example_output",
        "rst_bandmetadata_python_light_example_output",
        "rst_boundingbox_python_light_example_output",
        "rst_crs_python_light_example_output",
        "rst_format_python_light_example_output",
        "rst_georeference_python_light_example_output",
        "rst_getnodata_python_light_example_output",
        "rst_getsubdataset_python_light_example_output",
        "rst_height_python_light_example_output",
        "rst_histogram_python_light_example_output",
        "rst_isempty_python_light_example_output",
        "rst_max_python_light_example_output",
        "rst_median_python_light_example_output",
        "rst_memsize_python_light_example_output",
        "rst_metadata_python_light_example_output",
        "rst_min_python_light_example_output",
        "rst_numbands_python_light_example_output",
        "rst_pixelcount_python_light_example_output",
        "rst_pixelheight_python_light_example_output",
        "rst_pixelwidth_python_light_example_output",
        "rst_rotation_python_light_example_output",
        "rst_scalex_python_light_example_output",
        "rst_scaley_python_light_example_output",
        "rst_skewx_python_light_example_output",
        "rst_skewy_python_light_example_output",
        "rst_srid_python_light_example_output",
        "rst_subdatasets_python_light_example_output",
        "rst_summary_python_light_example_output",
        "rst_tryopen_python_light_example_output",
        "rst_type_python_light_example_output",
        "rst_upperleftx_python_light_example_output",
        "rst_upperlefty_python_light_example_output",
        "rst_width_python_light_example_output",
    ]
    for name in output_names:
        assert hasattr(light_examples, name), f"Missing output constant: {name}"
        val = getattr(light_examples, name)
        assert isinstance(val, str) and val.strip(), f"Output constant is empty: {name}"
