"""
Tests for the light (pyrx) tier per-function RasterX tile-ops and constructor examples.

Ensures all examples in rasterx_tileops_python_light.py are executable and
produce real, valid results from the canonical fixtures.

Fixture assignments:
- SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618, 161x236, 10m pixels):
  rst_asformat, rst_clip, rst_convolve, rst_cog_convert, rst_fillnodata,
  rst_filter, rst_initnodata, rst_resample, rst_resample_to_res, rst_resample_to_size,
  rst_setcrs, rst_setsrid, rst_threshold, rst_transform, rst_transformcrs,
  rst_updatetype, rst_buildoverviews
- MULTIBAND (rgb_nir_small.tif, EPSG:4326, 8x8, 3 bands, UInt16):
  rst_band, rst_frombands
- CONSTRUCTORS (produce a tile from bytes/path):
  rst_fromcontent, rst_frombands, rst_fromfile
"""

try:
    from . import rasterx_tileops_python_light as tileops_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_tileops_python_light as tileops_examples
    except ModuleNotFoundError:
        tileops_examples = None


# ---------------------------------------------------------------------------
# SINGLE-BAND fixture tests
# ---------------------------------------------------------------------------


def test_rst_asformat_python_light_example(spark):
    """rst_asformat returns 'GTiff' when converting the single-band fixture to GTiff."""
    assert tileops_examples is not None
    result = tileops_examples.rst_asformat_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_buildoverviews_python_light_example(spark):
    """rst_buildoverviews returns a tile whose format is 'GTiff'."""
    assert tileops_examples is not None
    result = tileops_examples.rst_buildoverviews_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_clip_python_light_example(spark):
    """rst_clip returns a tile in GTiff format when clipping with a NYC EWKT polygon."""
    assert tileops_examples is not None
    result = tileops_examples.rst_clip_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_cog_convert_python_light_example(spark):
    """rst_cog_convert returns a tile whose format is 'GTiff' (COG is a valid GeoTIFF)."""
    assert tileops_examples is not None
    result = tileops_examples.rst_cog_convert_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_convolve_python_light_example(spark):
    """rst_convolve with a 3x3 identity kernel returns a tile in GTiff format."""
    assert tileops_examples is not None
    result = tileops_examples.rst_convolve_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_fillnodata_python_light_example(spark):
    """rst_fillnodata returns a tile in GTiff format."""
    assert tileops_examples is not None
    result = tileops_examples.rst_fillnodata_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_filter_python_light_example(spark):
    """rst_filter with a 3x3 median kernel returns a tile in GTiff format."""
    assert tileops_examples is not None
    result = tileops_examples.rst_filter_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_initnodata_python_light_example(spark):
    """rst_initnodata returns a tile in GTiff format."""
    assert tileops_examples is not None
    result = tileops_examples.rst_initnodata_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_resample_python_light_example(spark):
    """rst_resample(2.0, bilinear) doubles the width from 236 to 472 px."""
    assert tileops_examples is not None
    result = tileops_examples.rst_resample_python_light_example(spark)
    assert result == 472, f"Expected width 472 (2x of 236), got {result!r}"


def test_rst_resample_to_res_python_light_example(spark):
    """rst_resample_to_res(20m, 20m) halves the width from 236 to 118 px."""
    assert tileops_examples is not None
    result = tileops_examples.rst_resample_to_res_python_light_example(spark)
    assert result == 118, f"Expected width 118 (10m->20m), got {result!r}"


def test_rst_resample_to_size_python_light_example(spark):
    """rst_resample_to_size(100, 100) returns a tile of exactly 100 px wide."""
    assert tileops_examples is not None
    result = tileops_examples.rst_resample_to_size_python_light_example(spark)
    assert result == 100, f"Expected width 100, got {result!r}"


def test_rst_setcrs_python_light_example(spark):
    """rst_setcrs stamps EPSG:32618 onto the tile without reprojecting."""
    assert tileops_examples is not None
    result = tileops_examples.rst_setcrs_python_light_example(spark)
    assert (
        isinstance(result, str) and "32618" in result
    ), f"Expected CRS containing '32618', got {result!r}"


def test_rst_setsrid_python_light_example(spark):
    """rst_setsrid stamps SRID 32618 onto the tile without reprojecting."""
    assert tileops_examples is not None
    result = tileops_examples.rst_setsrid_python_light_example(spark)
    assert result == 32618, f"Expected SRID 32618, got {result!r}"


def test_rst_threshold_python_light_example(spark):
    """rst_threshold(>0) returns a tile in GTiff format."""
    assert tileops_examples is not None
    result = tileops_examples.rst_threshold_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_transform_python_light_example(spark):
    """rst_transform(4326) reprojects the tile to EPSG:4326."""
    assert tileops_examples is not None
    result = tileops_examples.rst_transform_python_light_example(spark)
    assert result == 4326, f"Expected SRID 4326, got {result!r}"


def test_rst_transformcrs_python_light_example(spark):
    """rst_transformcrs('EPSG:3857') reprojects the tile to Web Mercator."""
    assert tileops_examples is not None
    result = tileops_examples.rst_transformcrs_python_light_example(spark)
    assert (
        isinstance(result, str) and "3857" in result
    ), f"Expected CRS containing '3857', got {result!r}"


def test_rst_updatetype_python_light_example(spark):
    """rst_updatetype('Float32') returns a tile in GTiff format."""
    assert tileops_examples is not None
    result = tileops_examples.rst_updatetype_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


# ---------------------------------------------------------------------------
# MULTIBAND fixture tests
# ---------------------------------------------------------------------------


def test_rst_band_python_light_example(spark):
    """rst_band(1) from the 3-band multiband fixture returns a single-band tile."""
    assert tileops_examples is not None
    result = tileops_examples.rst_band_python_light_example(spark)
    assert result == 1, f"Expected 1 band after rst_band extraction, got {result!r}"


# ---------------------------------------------------------------------------
# Constructor tests
# ---------------------------------------------------------------------------


def test_rst_fromcontent_python_light_example(spark):
    """rst_fromcontent builds a GTiff tile from binary content via binaryFile reader."""
    assert tileops_examples is not None
    result = tileops_examples.rst_fromcontent_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"


def test_rst_frombands_python_light_example(spark):
    """rst_frombands re-stacks 3 per-band tiles into a 3-band tile."""
    assert tileops_examples is not None
    result = tileops_examples.rst_frombands_python_light_example(spark)
    assert result == 3, f"Expected 3 bands after rst_frombands, got {result!r}"


def test_rst_fromfile_python_light_example(spark):
    """rst_fromfile loads a GTiff tile from the canonical single-band path."""
    assert tileops_examples is not None
    result = tileops_examples.rst_fromfile_python_light_example(spark)
    assert result == "GTiff", f"Expected 'GTiff', got {result!r}"
