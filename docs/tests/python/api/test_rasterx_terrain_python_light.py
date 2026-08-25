"""
Tests for the light (pyrx) tier terrain analysis examples.

Ensures all examples in rasterx_terrain_python_light.py are executable and
produce real, valid materialized v2 tiles from the DEM fixture.

All terrain functions return tile structs or array-of-structs. The light tier
materializes via rst_fromcontent, so tiles have raster bytes populated and path null.
"""

import pytest

try:
    from . import rasterx_terrain_python_light as terrain_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_terrain_python_light as terrain_examples
    except ModuleNotFoundError:
        terrain_examples = None


def _assert_materialized_tile(result, name):
    """Assert a light-tier v2 tile Row is non-None with populated raster bytes.

    The shared fixtures load tiles via ``rst_fromcontent``,
    which produce **materialized** v2 tiles: the ``raster`` field carries the
    encoded bytes and the ``path`` provenance field is null.
    """
    assert result is not None, f"{name}: result Row is None"
    assert result["raster"] is not None, f"{name}: raster bytes should be populated"
    assert len(bytes(result["raster"])) > 0, f"{name}: raster bytes should be non-empty"


# ============================================================================
# Terrain Analysis Tests
# ============================================================================


def test_rst_slope_python_light_example(spark):
    """rst_slope returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_slope_python_light_example(spark)
    _assert_materialized_tile(result, "rst_slope")


def test_rst_aspect_python_light_example(spark):
    """rst_aspect returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_aspect_python_light_example(spark)
    _assert_materialized_tile(result, "rst_aspect")


def test_rst_hillshade_python_light_example(spark):
    """rst_hillshade returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_hillshade_python_light_example(spark)
    _assert_materialized_tile(result, "rst_hillshade")


def test_rst_tri_python_light_example(spark):
    """rst_tri returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_tri_python_light_example(spark)
    _assert_materialized_tile(result, "rst_tri")


def test_rst_tpi_python_light_example(spark):
    """rst_tpi returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_tpi_python_light_example(spark)
    _assert_materialized_tile(result, "rst_tpi")


def test_rst_roughness_python_light_example(spark):
    """rst_roughness returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_roughness_python_light_example(spark)
    _assert_materialized_tile(result, "rst_roughness")


def test_rst_color_relief_python_light_example(spark):
    """rst_color_relief returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_color_relief_python_light_example(spark)
    _assert_materialized_tile(result, "rst_color_relief")


def test_rst_proximity_python_light_example(spark):
    """rst_proximity returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_proximity_python_light_example(spark)
    _assert_materialized_tile(result, "rst_proximity")


def test_rst_contour_python_light_example(spark):
    """rst_contour returns an array of contour line feature structs."""
    assert terrain_examples is not None
    result = terrain_examples.rst_contour_python_light_example(spark)
    assert result is not None, "rst_contour: result should not be None"
    assert isinstance(
        result, list
    ), "rst_contour: result should be a list of contour features"
    assert (
        len(result) > 0
    ), "rst_contour: result should contain at least one contour line"


def test_rst_viewshed_python_light_example(spark):
    """rst_viewshed returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_viewshed_python_light_example(spark)
    _assert_materialized_tile(result, "rst_viewshed")


def test_rst_sample_python_light_example(spark):
    """rst_sample returns an array of sampled pixel values."""
    assert terrain_examples is not None
    result = terrain_examples.rst_sample_python_light_example(spark)
    assert result is not None, "rst_sample: result should not be None"
    assert isinstance(result, list), "rst_sample: result should be an array of values"


def test_rst_gridfrompoints_python_light_example(spark):
    """rst_gridfrompoints returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_gridfrompoints_python_light_example(spark)
    _assert_materialized_tile(result, "rst_gridfrompoints")


def test_rst_dtmfromgeoms_python_light_example(spark):
    """rst_dtmfromgeoms returns a materialized tile with populated raster bytes."""
    assert terrain_examples is not None
    result = terrain_examples.rst_dtmfromgeoms_python_light_example(spark)
    _assert_materialized_tile(result, "rst_dtmfromgeoms")
