"""
Test structure of RasterX functions documentation examples.
Only tests for code that is used in docs/docs/api/rasterx-functions.mdx.
"""

import pytest

try:
    from . import rasterx_functions
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_functions
    except ModuleNotFoundError:
        rasterx_functions = None


@pytest.fixture(autouse=True)
def _heavy_setup_views(spark):
    """Create the four heavy-tier Setup views + register rasterx so every example
    can read `spark.table("rasters")` etc. — mirroring the page's Setup section.
    Autouse: each test in this module runs against the heavy-tier views."""
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from ._fixtures import create_setup_views_heavy  # noqa: PLC0415

    rx.register(spark)
    create_setup_views_heavy(spark)


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
    assert isinstance(
        result[0], float
    ), f"Expected float in list, got {type(result[0])}"


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
    """rst_width returns 236 for the single-band sentinel2 fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_width_python_heavy_example(spark)
    assert result == 236, f"Expected width 236, got {result}"


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
    """rst_bandmetadata returns a non-empty dict with real tags for the multiband fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_bandmetadata_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) > 0, f"Expected non-empty metadata dict, got {result!r}"
    # multiband fixture has name, wavelength_nm, band_index per band
    assert "name" in result, f"Expected 'name' key in band metadata, got {result!r}"


def test_rst_format_python_heavy_example(spark):
    """rst_format returns a non-empty format string."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_format_python_heavy_example(spark)
    assert (
        isinstance(result, str) and len(result) > 0
    ), f"Expected non-empty string, got {result!r}"


def test_rst_georeference_python_heavy_example(spark):
    """rst_georeference returns a dict with georeference parameters."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_georeference_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_getnodata_python_heavy_example(spark):
    """rst_getnodata returns a list of NoData values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_getnodata_python_heavy_example(spark)
    assert (
        isinstance(result, list) and len(result) >= 1
    ), f"Expected non-empty list, got {result!r}"


def test_rst_getsubdataset_python_heavy_example(spark):
    """rst_getsubdataset extracts the prAdjust subdataset and returns a tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_getsubdataset_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_getsubdataset")


def test_rst_height_python_heavy_example(spark):
    """rst_height returns 161 for the single-band sentinel2 fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_height_python_heavy_example(spark)
    assert result == 161, f"Expected height 161, got {result}"


def test_rst_max_python_heavy_example(spark):
    """rst_max returns [119.0, 197.0, 148.0] for the 3-band multiband fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_max_python_heavy_example(spark)
    assert (
        isinstance(result, list) and len(result) == 3
    ), f"Expected 3-band list, got {result!r}"
    assert result[0] is not None, "band_max[0] should not be None"


def test_rst_median_python_heavy_example(spark):
    """rst_median returns [85.0, 157.5, 111.5] for the 3-band multiband fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_median_python_heavy_example(spark)
    assert (
        isinstance(result, list) and len(result) == 3
    ), f"Expected 3-band list, got {result!r}"
    assert result[0] is not None, "band_median[0] should not be None"


def test_rst_memsize_python_heavy_example(spark):
    """rst_memsize returns a positive integer."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_memsize_python_heavy_example(spark)
    assert (
        isinstance(result, int) and result > 0
    ), f"Expected positive int, got {result!r}"


def test_rst_metadata_python_heavy_example(spark):
    """rst_metadata returns a dict."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_metadata_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"


def test_rst_min_python_heavy_example(spark):
    """rst_min returns [50.0, 102.0, 82.0] for the 3-band multiband fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_min_python_heavy_example(spark)
    assert (
        isinstance(result, list) and len(result) == 3
    ), f"Expected 3-band list, got {result!r}"
    assert result[0] is not None, "band_min[0] should not be None"


def test_rst_pixelcount_python_heavy_example(spark):
    """rst_pixelcount returns [64, 64, 64] for the multiband fixture (8x8, no NoData)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_pixelcount_python_heavy_example(spark)
    assert (
        isinstance(result, list) and len(result) == 3
    ), f"Expected 3-band list, got {result!r}"
    assert all(v == 64 for v in result), f"Expected [64, 64, 64], got {result!r}"


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
    """rst_srid returns 32618 for the single-band EPSG:32618 fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_srid_python_heavy_example(spark)
    assert result == 32618, f"Expected 32618, got {result}"


def test_rst_crs_python_heavy_example(spark):
    """rst_crs returns 'EPSG:32618' for the single-band fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_crs_python_heavy_example(spark)
    assert (
        isinstance(result, str) and len(result) > 0
    ), f"Expected non-empty string, got {result!r}"
    assert "32618" in result, f"Expected EPSG:32618, got {result!r}"


def test_rst_subdatasets_python_heavy_example(spark):
    """rst_subdatasets returns a non-empty dict for a NetCDF raster."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_subdatasets_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert (
        len(result) > 0
    ), f"Expected non-empty subdatasets dict (NetCDF fixture), got {result!r}"


def test_rst_summary_python_heavy_example(spark):
    """rst_summary returns a non-empty JSON string."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_summary_python_heavy_example(spark)
    assert (
        isinstance(result, str) and len(result) > 0
    ), f"Expected non-empty string, got {result!r}"


def test_rst_type_python_heavy_example(spark):
    """rst_type returns ['UInt16', 'UInt16', 'UInt16'] for the 3-band multiband fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_type_python_heavy_example(spark)
    assert (
        isinstance(result, list) and len(result) == 3
    ), f"Expected 3-band list, got {result!r}"
    assert all(t == "UInt16" for t in result), f"Expected all UInt16, got {result!r}"


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
    """rst_isempty returns False for the multiband fixture (has real pixel data)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_isempty_python_heavy_example(spark)
    assert result is False, f"Expected False (multiband has real data), got {result!r}"


def test_rst_tryopen_python_heavy_example(spark):
    """rst_tryopen returns True for a valid tile."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tryopen_python_heavy_example(spark)
    assert result is True, f"Expected True, got {result!r}"


def test_rst_histogram_python_heavy_example(spark):
    """rst_histogram returns a dict with 3 band keys for the multiband fixture."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_histogram_python_heavy_example(spark)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert len(result) == 3, f"Expected 3 bands in histogram, got {len(result)}"
    for v in result.values():
        assert isinstance(
            v, list
        ), f"Expected list values in histogram dict, got {type(v)}"


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


# ---------------------------------------------------------------------------
# Aggregator heavy-Python tests
# ---------------------------------------------------------------------------


def _assert_heavy_tile(result, name):
    """Assert a heavy-tier tile Row has non-None, non-empty raster bytes."""
    assert result is not None, f"{name}: result Row is None"
    assert result["raster"] is not None, f"{name}: raster bytes is None"
    assert len(bytes(result["raster"])) > 0, f"{name}: raster bytes is empty"


# ---------------------------------------------------------------------------
# Tile-ops family heavy-Python tests (unwrapped — return tile directly)
# ---------------------------------------------------------------------------


def test_rst_asformat_python_heavy_example(spark):
    """rst_asformat returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_asformat_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_asformat")


def test_rst_band_python_heavy_example(spark):
    """rst_band returns a non-null tile struct (single band from multiband fixture)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_band_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_band")


def test_rst_buildoverviews_python_heavy_example(spark):
    """rst_buildoverviews returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_buildoverviews_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_buildoverviews")


def test_rst_clip_python_heavy_example(spark):
    """rst_clip returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_clip_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_clip")


def test_rst_cog_convert_python_heavy_example(spark):
    """rst_cog_convert returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_cog_convert_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_cog_convert")


def test_rst_convolve_python_heavy_example(spark):
    """rst_convolve returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_convolve_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_convolve")


def test_rst_fillnodata_python_heavy_example(spark):
    """rst_fillnodata returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_fillnodata_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_fillnodata")


def test_rst_filter_python_heavy_example(spark):
    """rst_filter returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_filter_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_filter")


def test_rst_frombands_python_heavy_example(spark):
    """rst_frombands returns a non-null tile struct (3 bands re-stacked)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_frombands_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_frombands")


def test_rst_initnodata_python_heavy_example(spark):
    """rst_initnodata returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_initnodata_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_initnodata")


def test_rst_resample_python_heavy_example(spark):
    """rst_resample returns a non-null tile struct (2x upsampled)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_resample_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_resample")


def test_rst_resample_to_res_python_heavy_example(spark):
    """rst_resample_to_res returns a non-null tile struct (downsampled to 20m)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_resample_to_res_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_resample_to_res")


def test_rst_resample_to_size_python_heavy_example(spark):
    """rst_resample_to_size returns a non-null tile struct (forced to 100x100)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_resample_to_size_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_resample_to_size")


def test_rst_setsrid_python_heavy_example(spark):
    """rst_setsrid returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_setsrid_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_setsrid")


def test_rst_threshold_python_heavy_example(spark):
    """rst_threshold returns a non-null tile struct (binary mask)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_threshold_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_threshold")


def test_rst_transform_python_heavy_example(spark):
    """rst_transform returns a non-null tile struct (reprojected to EPSG:4326)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_transform_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_transform")


def test_rst_updatetype_python_heavy_example(spark):
    """rst_updatetype returns a non-null tile struct (data type changed to Float32)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_updatetype_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_updatetype")


def test_rst_combineavg_agg_python_heavy_example(spark):
    """rst_combineavg_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_combineavg_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_combineavg_agg")


def test_rst_derivedband_agg_python_heavy_example(spark):
    """rst_derivedband_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_derivedband_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_derivedband_agg")


def test_rst_frombands_agg_python_heavy_example(spark):
    """rst_frombands_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_frombands_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_frombands_agg")


def test_rst_merge_agg_python_heavy_example(spark):
    """rst_merge_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_merge_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_merge_agg")


def test_rst_rasterize_agg_python_heavy_example(spark):
    """rst_rasterize_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_rasterize_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_rasterize_agg")


def test_rst_gridfrompoints_agg_python_heavy_example(spark):
    """rst_gridfrompoints_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_gridfrompoints_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_gridfrompoints_agg")


def test_rst_dtmfromgeoms_agg_python_heavy_example(spark):
    """rst_dtmfromgeoms_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_dtmfromgeoms_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_dtmfromgeoms_agg")


def test_rst_h3_rasterize_agg_python_heavy_example(spark):
    """rst_h3_rasterize_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_h3_rasterize_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_h3_rasterize_agg")


def test_rst_quadbin_rasterize_agg_python_heavy_example(spark):
    """rst_quadbin_rasterize_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_quadbin_rasterize_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_quadbin_rasterize_agg")


def test_rst_bng_rasterize_agg_python_heavy_example(spark):
    """rst_bng_rasterize_agg returns a non-null tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_bng_rasterize_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_bng_rasterize_agg")


# ---------------------------------------------------------------------------
# Band-math examples (tabbed docs: 10 functions)
# All heavy band-math fns currently hit the GDAL null-output-dataset bug:
# "Cannot invoke "org.gdal.gdal.Dataset.GetDriver()" because "ds" is null"
# This is a pre-existing heavy-tier defect (same family as known RST_Clip failures).
# ---------------------------------------------------------------------------

_BAND_MATH_ERROR_TILE_BUG = (
    "pre-existing heavy-tier error-tile bug in band-math family "
    '(GDAL null output dataset: "Cannot invoke Dataset.GetDriver() because ds is null")'
)


def test_rst_ndvi_python_heavy_example(spark):
    """rst_ndvi returns a non-null tile struct (single-band NDVI)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_ndvi_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_ndvi")


def test_rst_evi_python_heavy_example(spark):
    """rst_evi returns a non-null tile struct (single-band EVI)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_evi_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_evi")


def test_rst_savi_python_heavy_example(spark):
    """rst_savi returns a non-null tile struct (single-band SAVI)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_savi_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_savi")


def test_rst_ndwi_python_heavy_example(spark):
    """rst_ndwi returns a non-null tile struct (single-band NDWI)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_ndwi_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_ndwi")


def test_rst_nbr_python_heavy_example(spark):
    """rst_nbr returns a non-null tile struct (single-band NBR)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_nbr_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_nbr")


def test_rst_index_python_heavy_example(spark):
    """rst_index returns a non-null tile struct (generic index via dispatcher)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_index_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_index")


def test_rst_combineavg_python_heavy_example(spark):
    """rst_combineavg returns a non-null tile struct (averaged merged raster)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_combineavg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_combineavg")


def test_rst_derivedband_python_heavy_example(spark):
    """rst_derivedband returns a non-null tile struct (derived from Python UDF)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_derivedband_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_derivedband")


def test_rst_mapalgebra_python_heavy_example(spark):
    """rst_mapalgebra returns a non-null tile struct (from algebra expression)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_mapalgebra_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_mapalgebra")


def test_rst_merge_python_heavy_example(spark):
    """rst_merge returns a non-null tile struct (merged from aligned tiles)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_merge_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_merge")


# ============================================================================
# Terrain Analysis Function Tests
# ============================================================================


def test_rst_slope_python_heavy_example(spark):
    """rst_slope returns a non-null tile struct (slope in degrees/percent)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_slope_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_slope")


def test_rst_aspect_python_heavy_example(spark):
    """rst_aspect returns a non-null tile struct (compass direction)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_aspect_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_aspect")


def test_rst_hillshade_python_heavy_example(spark):
    """rst_hillshade returns a non-null tile struct (8-bit shaded relief)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_hillshade_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_hillshade")


def test_rst_tri_python_heavy_example(spark):
    """rst_tri returns a non-null tile struct (terrain ruggedness index)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tri_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_tri")


def test_rst_tpi_python_heavy_example(spark):
    """rst_tpi returns a non-null tile struct (topographic position index)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tpi_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_tpi")


def test_rst_roughness_python_heavy_example(spark):
    """rst_roughness returns a non-null tile struct (max neighbor delta)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_roughness_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_roughness")


def test_rst_color_relief_python_heavy_example(spark):
    """rst_color_relief returns a non-null tile struct (RGBA from color table)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_color_relief_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_color_relief")


def test_rst_proximity_python_heavy_example(spark):
    """rst_proximity returns a non-null tile struct (distance to non-NoData)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_proximity_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_proximity")


def test_rst_contour_python_heavy_example(spark):
    """rst_contour returns an array of contour line feature structs."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_contour_python_heavy_example(spark)
    assert result is not None, "rst_contour: result should not be None"
    assert isinstance(result, list), "rst_contour: result should be a list"
    assert len(result) > 0, "rst_contour: should contain at least one contour line"


def test_rst_viewshed_python_heavy_example(spark):
    """rst_viewshed returns a non-null tile struct (binary visibility mask)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_viewshed_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_viewshed")


def test_rst_sample_python_heavy_example(spark):
    """rst_sample returns an array of sampled pixel values."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_sample_python_heavy_example(spark)
    assert result is not None, "rst_sample: result should not be None"
    assert isinstance(result, list), "rst_sample: result should be an array"


def test_rst_gridfrompoints_python_heavy_example(spark):
    """rst_gridfrompoints returns a non-null tile struct (IDW interpolated)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_gridfrompoints_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_gridfrompoints")


def test_rst_dtmfromgeoms_python_heavy_example(spark):
    """rst_dtmfromgeoms returns a non-null tile struct (TIN interpolated)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_dtmfromgeoms_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_dtmfromgeoms")


# ============================================================================
# Coordinate Transforms & Tiling (Heavy)
# ============================================================================


def test_rst_rastertoworldcoord_python_heavy_example(spark):
    """rst_rastertoworldcoord returns struct with x, y DOUBLE."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_rastertoworldcoord_python_heavy_example(spark)
    assert result is not None
    assert hasattr(result, "x") and hasattr(result, "y")
    assert isinstance(result.x, float) and isinstance(result.y, float)


def test_rst_rastertoworldcoordx_python_heavy_example(spark):
    """rst_rastertoworldcoordx returns DOUBLE (easting)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_rastertoworldcoordx_python_heavy_example(spark)
    assert isinstance(result, (int, float))


def test_rst_rastertoworldcoordy_python_heavy_example(spark):
    """rst_rastertoworldcoordy returns DOUBLE (northing)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_rastertoworldcoordy_python_heavy_example(spark)
    assert isinstance(result, (int, float))


def test_rst_worldtorastercoord_python_heavy_example(spark):
    """rst_worldtorastercoord returns struct with x, y INT."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_worldtorastercoord_python_heavy_example(spark)
    assert result is not None
    assert hasattr(result, "x") and hasattr(result, "y")
    assert isinstance(result.x, int) and isinstance(result.y, int)


def test_rst_worldtorastercoordx_python_heavy_example(spark):
    """rst_worldtorastercoordx returns INT (pixel col)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_worldtorastercoordx_python_heavy_example(spark)
    assert isinstance(result, int)


def test_rst_worldtorastercoordy_python_heavy_example(spark):
    """rst_worldtorastercoordy returns INT (pixel row)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_worldtorastercoordy_python_heavy_example(spark)
    assert isinstance(result, int)


def test_rst_to_webmercator_python_heavy_example(spark):
    """rst_to_webmercator returns a tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_to_webmercator_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_to_webmercator")


def test_rst_tilexyz_python_heavy_example(spark):
    """rst_tilexyz returns PNG image bytes (never null)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tilexyz_python_heavy_example(spark)
    assert result is not None
    assert len(bytes(result)) > 0


def test_rst_xyzpyramid_python_heavy_example(spark):
    """rst_xyzpyramid returns an array (LATERAL VIEW generator)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_xyzpyramid_python_heavy_example(spark)
    assert result is not None


def test_rst_h3_tessellate_python_heavy_example(spark):
    """rst_h3_tessellate returns an array of structs."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_h3_tessellate_python_heavy_example(spark)
    assert result is not None


def test_rst_bng_tessellate_python_heavy_example(spark):
    """rst_bng_tessellate (generator) yields 1km BNG cell rows over a London raster."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_bng_tessellate_python_heavy_example(spark)
    # Synthetic 2km London raster (EPSG:27700) → several overlapping 1km cells.
    assert isinstance(result, list) and len(result) > 0
    # Generator explodes to rows; each row is a v2 tile struct.
    first = result[0].asDict()
    assert "bng_cell" in first and first["bng_cell"] is not None


def test_rst_quadbin_tessellate_python_heavy_example(spark):
    """rst_quadbin_tessellate returns an array of tile structs."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_quadbin_tessellate_python_heavy_example(spark)
    assert result is not None


# ============================================================================
# Generator Functions (Heavy Tier)
# ============================================================================

_RETILE_ERROR_TILE_BUG = "rst_retile works correctly; returns array of tile structs"

_TOOVERLAPPINGTILES_ERROR_TILE_BUG = (
    "rst_tooverlappingtiles works correctly; returns array of overlapping tiles"
)

_SEPARATEBANDS_ERROR_TILE_BUG = (
    "rst_separatebands works correctly; returns array of band tiles"
)

_POLYGONIZE_ERROR_TILE_BUG = (
    "rst_polygonize works correctly; returns array of feature structs"
)


def test_rst_retile_python_heavy_example(spark):
    """rst_retile returns list of rows (via LATERAL)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_retile_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0


def test_rst_tooverlappingtiles_python_heavy_example(spark):
    """rst_tooverlappingtiles returns list of rows (via LATERAL)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tooverlappingtiles_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0


def test_rst_separatebands_python_heavy_example(spark):
    """rst_separatebands returns list of rows (one per band, via LATERAL)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_separatebands_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 3  # multiband fixture has 3 bands


def test_rst_polygonize_python_heavy_example(spark):
    """rst_polygonize returns an ARRAY<struct(geom_wkb, value)> of regions."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_polygonize_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0
    assert "geom_wkb" in result[0].asDict() and "value" in result[0].asDict()


def test_rst_maketiles_python_heavy_example(spark):
    """rst_maketiles returns one row per sub-tile (via LATERAL)."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_maketiles_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0


def test_rst_rasterize_python_heavy_example(spark):
    """rst_rasterize returns a materialized tile struct."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_rasterize_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_rasterize")


# ============================================================================
# H3 Rastertogrid Functions — Heavy Tier Tests
# ============================================================================


@pytest.mark.parametrize(
    "example_fn,aggregator",
    [
        ("rst_h3_rastertogridavg_python_heavy_example", "avg"),
        ("rst_h3_rastertogridcount_python_heavy_example", "count"),
        ("rst_h3_rastertogridmax_python_heavy_example", "max"),
        ("rst_h3_rastertogridmin_python_heavy_example", "min"),
        ("rst_h3_rastertogridmedian_python_heavy_example", "median"),
        ("rst_h3_rastertogridsum_python_heavy_example", "sum"),
        ("rst_h3_rastertogridvariance_python_heavy_example", "variance"),
        ("rst_h3_rastertogridstddev_python_heavy_example", "stddev"),
    ],
)
def test_h3_rastertogrid_python_heavy_example(spark, example_fn, aggregator):
    """Each H3 rastertogrid function returns ARRAY<ARRAY<struct(cellID, measure)>>."""
    assert rasterx_functions is not None
    example_func = getattr(rasterx_functions, example_fn)
    result = example_func(spark)

    # Result is ARRAY<ARRAY<struct>>: outer array = bands, inner array = cells
    assert isinstance(result, list), f"{example_fn} should return a list"
    assert len(result) == 3, f"{example_fn} should have 3 bands (multiband fixture)"

    # Each band should have at least some cells
    for band_idx, band_cells in enumerate(result):
        assert isinstance(band_cells, list), f"band {band_idx} cells should be a list"
        assert (
            len(band_cells) > 0
        ), f"band {band_idx} should have >0 cells for {aggregator}"


# ============================================================================
# Quadbin Rastertogrid Functions — Heavy Tier Tests
# ============================================================================


@pytest.mark.parametrize(
    "example_fn,aggregator",
    [
        ("rst_quadbin_rastertogridavg_python_heavy_example", "avg"),
        ("rst_quadbin_rastertogridcount_python_heavy_example", "count"),
        ("rst_quadbin_rastertogridmax_python_heavy_example", "max"),
        ("rst_quadbin_rastertogridmin_python_heavy_example", "min"),
        ("rst_quadbin_rastertogridmedian_python_heavy_example", "median"),
        ("rst_quadbin_rastertogridsum_python_heavy_example", "sum"),
        ("rst_quadbin_rastertogridvariance_python_heavy_example", "variance"),
        ("rst_quadbin_rastertogridstddev_python_heavy_example", "stddev"),
    ],
)
def test_quadbin_rastertogrid_python_heavy_example(spark, example_fn, aggregator):
    """Each Quadbin rastertogrid function returns ARRAY<ARRAY<struct(cellID, measure)>>."""
    assert rasterx_functions is not None
    example_func = getattr(rasterx_functions, example_fn)
    result = example_func(spark)

    assert isinstance(result, list), f"{example_fn} should return a list"
    assert len(result) == 3, f"{example_fn} should have 3 bands (multiband fixture)"

    # Each band should have at least some cells
    for band_idx, band_cells in enumerate(result):
        assert isinstance(band_cells, list), f"band {band_idx} cells should be a list"
        assert (
            len(band_cells) > 0
        ), f"band {band_idx} should have >0 cells for {aggregator}"


# ============================================================================
# BNG Rastertogrid Functions — Heavy Tier Tests
# ============================================================================


@pytest.mark.parametrize(
    "example_fn,aggregator",
    [
        ("rst_bng_rastertogridavg_python_heavy_example", "avg"),
        ("rst_bng_rastertogridcount_python_heavy_example", "count"),
        ("rst_bng_rastertogridmax_python_heavy_example", "max"),
        ("rst_bng_rastertogridmin_python_heavy_example", "min"),
        ("rst_bng_rastertogridmedian_python_heavy_example", "median"),
        ("rst_bng_rastertogridsum_python_heavy_example", "sum"),
        ("rst_bng_rastertogridvariance_python_heavy_example", "variance"),
        ("rst_bng_rastertogridstddev_python_heavy_example", "stddev"),
    ],
)
def test_bng_rastertogrid_python_heavy_example(spark, example_fn, aggregator):
    """Each BNG rastertogrid function returns ARRAY<ARRAY<struct(cellID, measure)>>.

    BNG reprojects the raster to EPSG:27700 before binning; cell ids are STRINGs.
    Cell count depends on the raster's extent, so the per-band count is tolerant
    (>= 0) rather than fixed.
    """
    assert rasterx_functions is not None
    example_func = getattr(rasterx_functions, example_fn)
    result = example_func(spark)

    # Result is ARRAY<ARRAY<struct>>: outer array = bands, inner array = cells
    assert isinstance(result, list), f"{example_fn} should return a list"
    assert len(result) == 3, f"{example_fn} should have 3 bands (multiband fixture)"

    for band_idx, band_cells in enumerate(result):
        assert isinstance(band_cells, list), f"band {band_idx} cells should be a list"
        # When cells are present, cell ids are BNG grid-square STRINGs.
        for cell in band_cells:
            assert isinstance(
                cell["cellID"], str
            ), f"band {band_idx} BNG cellID must be a STRING"


def test_h3_cell_bbox_python_heavy_example(spark):
    """h3_cell_bbox heavy-tier scalar example returns non-null ordered bbox structs."""
    assert rasterx_functions is not None
    rows = rasterx_functions.h3_cell_bbox_python_heavy_example(spark)
    assert isinstance(rows, list) and len(rows) == 3
    for row in rows:
        d = row.asDict()
        assert d["cellid"] is not None
        bbox = d["bbox"]
        assert bbox is not None
        assert bbox["xmin"] <= bbox["xmax"] and bbox["ymin"] <= bbox["ymax"]
    assert hasattr(rasterx_functions, "h3_cell_bbox_python_heavy_example_output")
