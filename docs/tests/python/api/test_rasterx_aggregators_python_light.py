"""
Tests for the light (pyrx) tier per-function RasterX aggregator examples.

Ensures all examples in rasterx_aggregators_python_light.py are executable
and produce real, valid results.

All *_agg examples return a tile struct STRUCT<cellid, raster, metadata>.
Assertions verify: result is not None, result["raster"] is non-empty bytes.

Fixture: MULTI-TILE (rgb_nir_small.tif split into 3 per-band rows, same grid).
  Used for: rst_combineavg_agg, rst_frombands_agg, rst_derivedband_agg, rst_merge_agg.

Synthesized rows for: rst_rasterize_agg, rst_gridfrompoints_agg, rst_dtmfromgeoms_agg,
  rst_h3_rasterize_agg, rst_quadbin_rasterize_agg, rst_bng_rasterize_agg.
"""

import pytest

try:
    from . import rasterx_aggregators_python_light as agg_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_aggregators_python_light as agg_examples
    except ModuleNotFoundError:
        agg_examples = None


@pytest.fixture(autouse=True)
def _light_setup_views(spark):
    """Register pyrx + create the four light-tier Setup views once per test.
    Aggregator examples build their own multi-row input, but registration is now
    the fixture's job (examples no longer call rx.register)."""
    from databricks.labs.gbx import manager as gbx_manager
    from ._fixtures import create_setup_views_light  # noqa: PLC0415

    gbx_manager.register_family(spark, ('gbx_rst_', 'gbx_pmtiles_'), level="L3")
    create_setup_views_light(spark)


def _assert_tile(result, name):
    """Assert an aggregate's BINARY result (or the tile an example made of it with rst_fromcontent) is a non-empty GeoTIFF."""
    assert result is not None, f"{name}: result is None"
    raw = bytes(result["raster"] if hasattr(result, "asDict") else result)
    assert len(raw) > 0, f"{name}: raster bytes is empty"
    assert raw[:2] in (b"II", b"MM"), f"{name}: not a GeoTIFF"


# ---------------------------------------------------------------------------
# Multi-band tile tests (rgb_nir_small.tif fixture)
# ---------------------------------------------------------------------------


def test_rst_combineavg_agg_python_example(spark):
    """rst_combineavg_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_combineavg_agg_python_example(spark)
    _assert_tile(result, "rst_combineavg_agg")


def test_rst_derivedband_agg_python_example(spark):
    """rst_derivedband_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_derivedband_agg_python_example(spark)
    _assert_tile(result, "rst_derivedband_agg")


def test_rst_frombands_agg_python_example(spark):
    """rst_frombands_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_frombands_agg_python_example(spark)
    _assert_tile(result, "rst_frombands_agg")


def test_rst_merge_agg_python_example(spark):
    """rst_merge_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_merge_agg_python_example(spark)
    _assert_tile(result, "rst_merge_agg")


# ---------------------------------------------------------------------------
# Synthesized-row tests (geometry / point / cellid fixtures)
# ---------------------------------------------------------------------------


def test_rst_rasterize_agg_python_example(spark):
    """rst_rasterize_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_rasterize_agg_python_example(spark)
    _assert_tile(result, "rst_rasterize_agg")


def test_rst_gridfrompoints_agg_python_example(spark):
    """rst_gridfrompoints_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_gridfrompoints_agg_python_example(spark)
    _assert_tile(result, "rst_gridfrompoints_agg")


def test_rst_dtmfromgeoms_agg_python_example(spark):
    """rst_dtmfromgeoms_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_dtmfromgeoms_agg_python_example(spark)
    _assert_tile(result, "rst_dtmfromgeoms_agg")


def test_rst_h3_rasterize_agg_python_example(spark):
    """rst_h3_rasterize_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_h3_rasterize_agg_python_example(spark)
    _assert_tile(result, "rst_h3_rasterize_agg")


def test_rst_quadbin_rasterize_agg_python_example(spark):
    """rst_quadbin_rasterize_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_quadbin_rasterize_agg_python_example(spark)
    _assert_tile(result, "rst_quadbin_rasterize_agg")


def test_rst_bng_rasterize_agg_python_example(spark):
    """rst_bng_rasterize_agg returns non-empty GeoTIFF bytes."""
    assert agg_examples is not None
    result = agg_examples.rst_bng_rasterize_agg_python_example(spark)
    _assert_tile(result, "rst_bng_rasterize_agg")


def test_rst_binpoints_agg_python_example(spark):
    """rst_binpoints_agg returns non-empty GeoTIFF bytes from scalar point rows."""
    assert agg_examples is not None
    result = agg_examples.rst_binpoints_agg_python_example(spark)
    _assert_tile(result, "rst_binpoints_agg")


def test_rst_custom_rasterize_agg_python_example(spark):
    """rst_custom_rasterize_agg returns non-empty GeoTIFF bytes from custom-grid cell rows."""
    assert agg_examples is not None
    result = agg_examples.rst_custom_rasterize_agg_python_example(spark)
    _assert_tile(result, "rst_custom_rasterize_agg")
