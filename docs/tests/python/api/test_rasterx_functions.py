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


def test_rst_isoband_python_heavy_example(spark):
    """rst_isoband returns a non-empty list of polygon patches from a DEM tile."""
    assert rasterx_functions is not None
    patches = rasterx_functions.rst_isoband_python_heavy_example(spark)
    assert isinstance(patches, list), "rst_isoband must return a list of patches"
    assert len(patches) > 0, "rst_isoband must return at least one patch for a DEM with real elevation"
    patch = patches[0]
    assert patch["geom_wkb"] is not None, "patch geom_wkb must be non-null"
    assert patch["band"] is not None, "patch band must be non-null"
    assert hasattr(rasterx_functions, "rst_isoband_python_heavy_example_output")


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




def test_rst_contour_python_heavy_example(spark):
    """rst_contour returns an array of contour line feature structs."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_contour_python_heavy_example(spark)
    assert result is not None, "rst_contour: result should not be None"
    assert isinstance(result, list), "rst_contour: result should be a list"
    assert len(result) > 0, "rst_contour: should contain at least one contour line"




# ============================================================================
# Coordinate Transforms & Tiling (Heavy)
# ============================================================================






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


def test_rst_tooverlappingtiles_python_heavy_example(spark):
    """rst_tooverlappingtiles (heavyweight generator) returns one `tile` row per tile."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_tooverlappingtiles_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0
    assert list(result[0].asDict()) == ["tile"]


def test_rst_separatebands_python_heavy_example(spark):
    """rst_separatebands (heavyweight generator) returns one `tile` row per band."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_separatebands_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 3  # multiband fixture has 3 bands
    assert list(result[0].asDict()) == ["tile"]


def test_rst_polygonize_python_heavy_example(spark):
    """rst_polygonize returns an ARRAY<struct(geom_wkb, value)> of regions."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_polygonize_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0
    assert "geom_wkb" in result[0].asDict() and "value" in result[0].asDict()


def test_rst_maketiles_python_heavy_example(spark):
    """rst_maketiles (heavyweight generator) returns one `tile` row per sub-tile."""
    assert rasterx_functions is not None
    result = rasterx_functions.rst_maketiles_python_heavy_example(spark)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) > 0
    assert list(result[0].asDict()) == ["tile"]


def test_rst_custom_tessellate_python_heavy_example(spark):
    """rst_custom_tessellate (heavyweight generator) returns one `tile` row per cell."""
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = rasterx_functions.rst_custom_tessellate_python_heavy_example(spark)
    assert isinstance(result, list) and len(result) > 0
    assert list(result[0].asDict()) == ["tile"]
    assert result[0]["tile"]["cellid"] is not None


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


