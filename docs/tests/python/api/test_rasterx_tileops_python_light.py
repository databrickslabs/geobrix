"""
Tests for the light (pyrx) tier per-function RasterX tile-ops and constructor examples.

Ensures all examples in rasterx_tileops_python_light.py are executable and
produce real, valid results from the canonical fixtures.

Tile-returning functions (unwrapped) assert: result is a non-None Row whose
`raster` field carries non-empty encoded bytes (the fixtures load via
`rst_fromcontent`/`rst_fromfile`, so the light tier returns materialized v2
tiles — `raster` populated, `path` null). Constructors that verify via a
scalar accessor keep their original assertion pattern.

Fixture assignments:
- SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618, 161x236, 10m pixels):
  rst_asformat, rst_buildoverviews, rst_clip, rst_cog_convert, rst_convolve,
  rst_fillnodata, rst_filter, rst_initnodata, rst_resample, rst_resample_to_res,
  rst_resample_to_size, rst_setcrs, rst_setsrid, rst_threshold, rst_transform,
  rst_transformcrs, rst_updatetype
- MULTIBAND (rgb_nir_small.tif, EPSG:4326, 8x8, 3 bands, UInt16):
  rst_band, rst_frombands
- CONSTRUCTORS (produce a tile from bytes/path):
  rst_fromcontent, rst_fromfile (scalar-accessor pattern retained)
  rst_frombands (returns tile struct)
"""

import pytest

try:
    from . import rasterx_tileops_python_light as tileops_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_tileops_python_light as tileops_examples
    except ModuleNotFoundError:
        tileops_examples = None


@pytest.fixture(autouse=True)
def _light_setup_views(spark):
    """Register pyrx + create the four light-tier Setup views once per test, so
    single-tile examples can read `spark.table("rasters")` etc."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from ._fixtures import create_setup_views_light  # noqa: PLC0415

    rx.register(spark)
    create_setup_views_light(spark)


def _assert_materialized_tile(result, name):
    """Assert a light-tier v2 tile Row is non-None with populated raster bytes.

    The shared fixtures load tiles via ``rst_fromcontent``/``rst_fromfile``,
    which produce **materialized** v2 tiles: the ``raster`` field carries the
    encoded bytes and the ``path`` provenance field is null. (Virtual tiles —
    ``path`` populated, ``raster`` lazy — arise only from force-output params.)
    """
    assert result is not None, f"{name}: result Row is None"
    assert result["raster"] is not None, f"{name}: raster bytes should be populated"
    assert len(bytes(result["raster"])) > 0, f"{name}: raster bytes should be non-empty"


# ---------------------------------------------------------------------------
# SINGLE-BAND fixture tests — tile-returning (unwrapped)
# ---------------------------------------------------------------------------


def test_rst_asformat_python_light_example(spark):
    """rst_asformat returns a materialized tile with populated raster bytes."""
    assert tileops_examples is not None
    result = tileops_examples.rst_asformat_python_light_example(spark)
    _assert_materialized_tile(result, "rst_asformat")


def test_rst_buildoverviews_python_light_example(spark):
    """rst_buildoverviews returns a materialized tile with populated raster bytes."""
    assert tileops_examples is not None
    result = tileops_examples.rst_buildoverviews_python_light_example(spark)
    _assert_materialized_tile(result, "rst_buildoverviews")


def test_rst_clip_python_light_example(spark):
    """rst_clip returns a materialized tile with populated raster bytes."""
    assert tileops_examples is not None
    result = tileops_examples.rst_clip_python_light_example(spark)
    _assert_materialized_tile(result, "rst_clip")


def test_rst_cog_convert_python_light_example(spark):
    """rst_cog_convert returns a materialized tile with populated raster bytes."""
    assert tileops_examples is not None
    result = tileops_examples.rst_cog_convert_python_light_example(spark)
    _assert_materialized_tile(result, "rst_cog_convert")


def test_rst_convolve_python_light_example(spark):
    """rst_convolve with a 3x3 identity kernel returns a materialized tile."""
    assert tileops_examples is not None
    result = tileops_examples.rst_convolve_python_light_example(spark)
    _assert_materialized_tile(result, "rst_convolve")


def test_rst_fillnodata_python_light_example(spark):
    """rst_fillnodata returns a materialized tile with populated raster bytes."""
    assert tileops_examples is not None
    result = tileops_examples.rst_fillnodata_python_light_example(spark)
    _assert_materialized_tile(result, "rst_fillnodata")


def test_rst_filter_python_light_example(spark):
    """rst_filter with a 3x3 median kernel returns a materialized tile."""
    assert tileops_examples is not None
    result = tileops_examples.rst_filter_python_light_example(spark)
    _assert_materialized_tile(result, "rst_filter")


def test_rst_initnodata_python_light_example(spark):
    """rst_initnodata returns a materialized tile with populated raster bytes."""
    assert tileops_examples is not None
    result = tileops_examples.rst_initnodata_python_light_example(spark)
    _assert_materialized_tile(result, "rst_initnodata")


def test_rst_resample_python_light_example(spark):
    """rst_resample returns a materialized tile (2x upsampled)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_resample_python_light_example(spark)
    _assert_materialized_tile(result, "rst_resample")


def test_rst_resample_to_res_python_light_example(spark):
    """rst_resample_to_res returns a materialized tile (downsampled to 20m)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_resample_to_res_python_light_example(spark)
    _assert_materialized_tile(result, "rst_resample_to_res")


def test_rst_resample_to_size_python_light_example(spark):
    """rst_resample_to_size returns a materialized tile (forced to 100x100)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_resample_to_size_python_light_example(spark)
    _assert_materialized_tile(result, "rst_resample_to_size")


def test_rst_setcrs_python_light_example(spark):
    """rst_setcrs stamps EPSG:32618 onto the tile without reprojecting."""
    assert tileops_examples is not None
    result = tileops_examples.rst_setcrs_python_light_example(spark)
    assert (
        isinstance(result, str) and "32618" in result
    ), f"Expected CRS containing '32618', got {result!r}"


def test_rst_setsrid_python_light_example(spark):
    """rst_setsrid returns a materialized tile with updated SRID."""
    assert tileops_examples is not None
    result = tileops_examples.rst_setsrid_python_light_example(spark)
    _assert_materialized_tile(result, "rst_setsrid")


def test_rst_threshold_python_light_example(spark):
    """rst_threshold returns a materialized tile (binary mask)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_threshold_python_light_example(spark)
    _assert_materialized_tile(result, "rst_threshold")


def test_rst_transform_python_light_example(spark):
    """rst_transform returns a materialized tile (reprojected to EPSG:4326)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_transform_python_light_example(spark)
    _assert_materialized_tile(result, "rst_transform")


def test_rst_transformcrs_python_light_example(spark):
    """rst_transformcrs('EPSG:3857') reprojects the tile to Web Mercator."""
    assert tileops_examples is not None
    result = tileops_examples.rst_transformcrs_python_light_example(spark)
    assert (
        isinstance(result, str) and "3857" in result
    ), f"Expected CRS containing '3857', got {result!r}"


def test_rst_updatetype_python_light_example(spark):
    """rst_updatetype returns a materialized tile (type-converted to Float32)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_updatetype_python_light_example(spark)
    _assert_materialized_tile(result, "rst_updatetype")


# ---------------------------------------------------------------------------
# MULTIBAND fixture tests — tile-returning (unwrapped)
# ---------------------------------------------------------------------------


def test_rst_band_python_light_example(spark):
    """rst_band returns a materialized tile (single band extracted from multiband fixture)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_band_python_light_example(spark)
    _assert_materialized_tile(result, "rst_band")


# ---------------------------------------------------------------------------
# Constructor tests
# ---------------------------------------------------------------------------


def test_rst_fromcontent_python_light_example(spark):
    """rst_fromcontent builds a GTiff tile from binary content via binaryFile reader."""
    assert tileops_examples is not None
    result = tileops_examples.rst_fromcontent_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_frombands_python_light_example(spark):
    """rst_frombands returns a materialized tile (3 per-band tiles stacked into a 3-band tile)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_frombands_python_light_example(spark)
    _assert_materialized_tile(result, "rst_frombands")


def test_rst_fromfile_python_light_example(spark):
    """rst_fromfile defaults to a VIRTUAL v2 tile: raster null, path + window set."""
    assert tileops_examples is not None
    tile = tileops_examples.rst_fromfile_python_light_example(spark)
    assert tile is not None, "rst_fromfile returned null for a readable path"
    assert tile["raster"] is None, "virtual tile must have raster=None (bytes-free)"
    assert tile["path"] is not None, "virtual tile must carry the source path"
    assert tile["window"] is not None, "virtual tile must carry the whole-file window"
