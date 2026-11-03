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


def test_rst_binpoints_agg_python_heavy_example(spark):
    """rst_binpoints_agg returns a non-null Float32 tile struct from scalar point rows."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_binpoints_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_binpoints_agg")


def test_rst_custom_rasterize_agg_python_heavy_example(spark):
    """rst_custom_rasterize_agg returns a non-null tile struct from custom-grid cell rows."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_custom_rasterize_agg_python_heavy_example(spark)
    _assert_heavy_tile(result, "rst_custom_rasterize_agg")


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


















# ============================================================================
# Terrain Analysis Function Tests
# ============================================================================




# ============================================================================
# Coordinate Transforms & Tiling (Heavy)
# ============================================================================






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
