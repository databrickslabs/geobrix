"""
Tests for the light (pyrx) tier band-math / spectral index examples.

Ensures all examples in rasterx_bandmath_python_light.py are executable and
produce real, valid materialized v2 tiles from the multiband fixture.

All band-math functions return tile structs. The light tier materializes via
rst_fromcontent, so tiles have raster bytes populated and path null.
"""

try:
    from . import rasterx_bandmath_python_light as bandmath_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_bandmath_python_light as bandmath_examples
    except ModuleNotFoundError:
        bandmath_examples = None


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
# Spectral Index Tests
# ============================================================================


def test_rst_ndvi_python_light_example(spark):
    """rst_ndvi returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_ndvi_python_light_example(spark)
    _assert_materialized_tile(result, "rst_ndvi")


def test_rst_evi_python_light_example(spark):
    """rst_evi returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_evi_python_light_example(spark)
    _assert_materialized_tile(result, "rst_evi")


def test_rst_savi_python_light_example(spark):
    """rst_savi returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_savi_python_light_example(spark)
    _assert_materialized_tile(result, "rst_savi")


def test_rst_ndwi_python_light_example(spark):
    """rst_ndwi returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_ndwi_python_light_example(spark)
    _assert_materialized_tile(result, "rst_ndwi")


def test_rst_nbr_python_light_example(spark):
    """rst_nbr returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_nbr_python_light_example(spark)
    _assert_materialized_tile(result, "rst_nbr")


def test_rst_index_python_light_example(spark):
    """rst_index returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_index_python_light_example(spark)
    _assert_materialized_tile(result, "rst_index")


# ============================================================================
# Multi-tile Operation Tests
# ============================================================================


def test_rst_combineavg_python_light_example(spark):
    """rst_combineavg returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_combineavg_python_light_example(spark)
    _assert_materialized_tile(result, "rst_combineavg")


def test_rst_derivedband_python_light_example(spark):
    """rst_derivedband returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_derivedband_python_light_example(spark)
    _assert_materialized_tile(result, "rst_derivedband")


def test_rst_mapalgebra_python_light_example(spark):
    """rst_mapalgebra returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_mapalgebra_python_light_example(spark)
    _assert_materialized_tile(result, "rst_mapalgebra")


def test_rst_merge_python_light_example(spark):
    """rst_merge returns a materialized tile with populated raster bytes."""
    assert bandmath_examples is not None
    result = bandmath_examples.rst_merge_python_light_example(spark)
    _assert_materialized_tile(result, "rst_merge")
