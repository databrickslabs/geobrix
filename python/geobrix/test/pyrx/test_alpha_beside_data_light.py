"""The light-only checks of rasters with an alpha band beside data bands, or masked by their own mask band.

These functions have the light level alone (histogram, the band statistics, isoband, the derived band),
so they need no JAR and run in the light suite; the cross-tier parity of the same rasters, which needs
the JAR, is ``test_parity_alpha_beside_data.py``, whose fixtures and collectors these reuse.
"""

import pytest

pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (the lightweight tier required: the env wheel, or pip install geobrix)",
)
pytest.importorskip(
    "shapely",
    reason="shapely not installed (the lightweight tier required: the env wheel, or pip install geobrix)",
)

from test.pyrx.test_parity_alpha_beside_data import (  # noqa: E402
    _STRIPED,
    _assert_band_statistics,
    _masked_rgb_nir,
    _raster_of,
    _single_tile,
    _sql_rows,
    _striped,
    _vector_rows,
)


@pytest.mark.parametrize(
    "name", ["gray_x_alpha", "rgb_x_alpha", "rgba", "masked_rgb_nir"]
)
def test_histogram_over_the_pixels_valid_for_each_band(spark, name):
    """Per band, the histogram counts the pixels valid for it. The range [0, 8) holds the data values
    (0 and 7) and not the alpha band's 255."""
    raster = _raster_of(name)
    sql = "SELECT gbx_rst_histogram(tile, 8, 0.0, 8.0, false) AS h FROM _alpha_t"
    light = dict(_sql_rows(spark, raster, "light", sql)[0]["h"])
    # band 1 is 0 over rows 1 and 2 and 7 over rows 0 and 3 of the pattern; an alpha layout counts rows 1 and 3
    # (4 zeros and 4 sevens), the masked raster all rows but column 0 (6 and 6)
    want = (6, 6) if name == "masked_rgb_nir" else (4, 4)
    assert (light["band_1"][0], light["band_1"][7]) == want, light["band_1"]


def test_statistics_of_a_raster_with_its_own_mask_parity(spark):
    """pixel count, min, max, mean per band: the masked column is left out (heavy's multi-dimensional
    statistics ignored the mask band). Column 0 is hidden, so each band counts 12 pixels; the colour
    bands hold 7 over two rows and 0 over two (mean 3.5), the last band 255 over two rows and 0 over
    two (mean 127.5)."""
    want = [(12, 0, 7, 3.5)] * 3 + [(12, 0, 255, 127.5)]
    _assert_band_statistics(spark, _masked_rgb_nir(), want)


@pytest.mark.parametrize("kind", ["isoband"])
@pytest.mark.parametrize("name", _STRIPED)
def test_isoband_leaves_out_transparent_and_masked_pixels(spark, name, kind):
    """isoband has the light level alone: the transparent and masked rows stay out of its polygons."""
    raster = _striped(name)
    light = _vector_rows(spark, raster, "light", kind)
    assert light, "isoband gave no polygon"
    assert sum(a for a, _ in light) == 24.0, "the hidden rows must stay out"


_FIRST_FUNC = (
    "def first(in_ar, out_ar, xoff, yoff, xsize, ysize, rxsize, rysize, radius, gt, **kw):\n"
    "    out_ar[:] = in_ar[0]\n"
)


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "masked_rgb_nir"])
def test_derivedband_is_invalid_where_the_input_is(spark, name):
    """The single band derived from a raster has no alpha band of its own: the light level (the only one)
    writes the input's validity as its own mask band, and the input's values where it is valid.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core._nodata import valid_mask

    raster = _raster_of(name)

    def derive(m, t):
        from pyspark.sql import functions as f

        return f.call_function(
            "gbx_rst_derivedband", t, f.lit(_FIRST_FUNC), f.lit("first")
        )

    out = _single_tile(spark, raster, "light", derive)
    with MemoryFile(out) as mf, mf.open() as ds:
        valid, data = valid_mask(ds), ds.read(1)
    with MemoryFile(raster) as mf, mf.open() as src:
        want_valid, first = valid_mask(src), src.read(1)
    assert (valid == want_valid).all()
    assert (data.astype("float64")[valid] == first.astype("float64")[valid]).all()
    assert valid.any() and not valid.all()
