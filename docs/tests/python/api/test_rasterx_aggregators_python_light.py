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


# ---------------------------------------------------------------------------
# Array forms and key-partitioned table functions: hold the pixels, not just the bytes
# ---------------------------------------------------------------------------

_NODATA = -9999.0


def _geotiff(content):
    """(crs, pixels) of GeoTIFF bytes or a tile struct: the first band as a list of rows."""
    import rasterio

    raw = content["raster"] if hasattr(content, "asDict") else content
    with rasterio.MemoryFile(bytes(raw)) as mem, mem.open() as ds:
        assert ds.nodata == _NODATA
        return ds.crs.to_string(), ds.read(1).tolist()


@pytest.mark.parametrize(
    "name, crs, pixels",
    [
        (
            "rst_bng_rasterize",
            "EPSG:27700",
            [[4.0, _NODATA, _NODATA], [3.0, 2.0, _NODATA]],
        ),
        ("rst_h3_rasterize", "EPSG:4326", [[2.0, _NODATA], [1.0, 3.0]]),
        ("rst_quadbin_rasterize", "EPSG:4326", [[1.0, 2.0, 3.0]]),
        ("rst_custom_rasterize", "EPSG:27700", [[3.0, _NODATA], [1.0, 2.0]]),
    ],
)
def test_rasterize_array_python_examples(spark, name, crs, pixels):
    """Each array form burns its cells at the pixels of their centroids."""
    assert agg_examples is not None
    result = getattr(agg_examples, f"{name}_python_example")(spark)
    assert _geotiff(result) == (crs, pixels)


def test_rst_binpoints_partial_python_example(spark):
    """The key counts the column from the west and the row from the north; the east edge has none."""
    assert agg_examples is not None
    rows = agg_examples.rst_binpoints_partial_python_example(spark)
    got = {
        (r["x"], r["y"]): (None if r["px"] is None else (r["px"]["col"], r["px"]["row"]))
        for r in rows
    }
    assert got == {(1.5, 8.5): (1, 1), (0.5, 0.5): (0, 9), (0.0, 10.0): (0, 0), (10.0, 5.0): None}


def test_rst_binpoints_finalize_python_example(spark):
    """The tile holds the maximum of each pixel's points."""
    assert agg_examples is not None
    tile = agg_examples.rst_binpoints_finalize_python_example(spark)
    assert _geotiff(tile) == ("EPSG:4326", [[5.0, 7.0]])


def test_rst_bng_rasterize_tvf_python_example(spark):
    """One tile per key, with a repeated cell burning its largest value."""
    assert agg_examples is not None
    rows = agg_examples.rst_bng_rasterize_tvf_python_example(spark)
    assert {r["key"]: _geotiff(r["raster"]) for r in rows} == {
        "a": ("EPSG:27700", [[_NODATA] * 3, [3.0, 2.0, _NODATA]]),
        "b": ("EPSG:27700", [[4.0, _NODATA, _NODATA], [_NODATA] * 3]),
    }


def test_rst_merge_tvf_python_example(spark):
    """The two tiles of one key merge into a 4 x 2 mosaic."""
    assert agg_examples is not None
    rows = agg_examples.rst_merge_tvf_python_example(spark)
    assert [r["key"] for r in rows] == ["a"]
    assert _geotiff(rows[0]["raster"]) == (
        "EPSG:4326",
        [[1.0, 1.0, 2.0, 2.0], [1.0, 1.0, 2.0, 2.0]],
    )
