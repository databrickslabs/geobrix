import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import mapalgebra


def _ras(value, w=2, h=2):
    data = np.full((h, w), float(value), dtype="float32")
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def test_mapalgebra_add_two():
    out = mapalgebra.mapalgebra([_ras(1.0), _ras(2.0)], "A + B")
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "float32"
        assert np.allclose(o.read(1), 3.0)


def test_mapalgebra_scalar_on_one():
    out = mapalgebra.mapalgebra([_ras(4.0)], "A * 2")
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 8.0)


def test_mapalgebra_normalized_diff():
    out = mapalgebra.mapalgebra([_ras(10.0), _ras(4.0)], "(A - B) / (A + B)")
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 6.0 / 14.0, atol=1e-5)


def _ras_arr(data):
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


def test_mapalgebra_masks_input_nodata_and_sets_output_nodata():
    a = np.full((3, 3), 5.0, dtype="float32")
    a[1, 1] = -9999.0
    out = mapalgebra.mapalgebra([_ras_arr(a)], "A * 2")
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert o.nodata == -9999.0  # output nodata now set (was None)
        assert r[1, 1] == -9999.0  # input sentinel masked
        assert r[0, 0] == 10.0  # 5*2 elsewhere


# ---------------------------------------------------------------------------
# JSON-spec parity: the light tier accepts the SAME gdal_calc JSON envelope as
# the heavy/SQL tier ('{"calc": "..."}'), extracting `calc` for numexpr, so a
# single spec works verbatim across all four doc tabs.
# ---------------------------------------------------------------------------


def test_mapalgebra_json_spec_scalar():
    """A JSON envelope with `calc` is honored identically to the bare string."""
    out = mapalgebra.mapalgebra([_ras(4.0)], '{"calc": "A * 2"}')
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 8.0)


def test_mapalgebra_json_spec_matches_bare_string():
    """The heavy JSON form and the bare numexpr string produce identical output."""
    bare = mapalgebra.mapalgebra([_ras(10.0), _ras(4.0)], "(A - B) / (A + B)")
    js = mapalgebra.mapalgebra([_ras(10.0), _ras(4.0)], '{"calc": "(A - B) / (A + B)"}')
    with _serde.open_tile(bare) as ob, _serde.open_tile(js) as oj:
        assert np.allclose(ob.read(1), oj.read(1))


def test_mapalgebra_json_spec_whitespace_and_no_space_calc():
    """`{"calc":"A*2"}` (no spaces) parses the same as the spaced form."""
    out = mapalgebra.mapalgebra([_ras(4.0)], '{"calc":"A*2"}')
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 8.0)


def test_mapalgebra_json_missing_calc_raises():
    """A JSON object without `calc` is a clear error, not a silent numexpr fail."""
    import pytest

    with pytest.raises(ValueError, match="calc"):
        mapalgebra.mapalgebra([_ras(4.0)], '{"extra_options": "--type=Float32"}')


def test_mapalgebra_extra_options_raises():
    """`extra_options` is gdal_calc-only CLI plumbing the light tier cannot honor;
    it raises clearly rather than being silently dropped."""
    import pytest

    with pytest.raises(ValueError, match="extra_options|not supported"):
        mapalgebra.mapalgebra(
            [_ras(4.0)], '{"calc": "A*2", "extra_options": "--type=Float32"}'
        )


# ---------------------------------------------------------------------------
# Per-variable band / raster selection (gdal_calc A_band / A_index parity).
# The whole point: NDVI = (NIR - Red)/(NIR + Red) from ONE multiband raster,
# without decomposing it into separate single-band tiles first.
# ---------------------------------------------------------------------------


def _ras_bands(band_values, w=2, h=2):
    """Multiband GTiff: band i (1-based) filled with band_values[i-1]."""
    count = len(band_values)
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=count,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for i, v in enumerate(band_values, start=1):
                dst.write(np.full((h, w), float(v), dtype="float32"), i)
        return mf.read()


def test_mapalgebra_ndvi_from_one_multiband_raster_via_index_and_band():
    """(A-B)/(A+B) reading band 4 (NIR) and band 3 (Red) of a SINGLE raster.

    Faithful gdal_calc form: A and B both map to raster 0 via A_index/B_index,
    with A_band=4, B_band=3 — exactly `gdal_calc -A in --A_band=4 -B in --B_band=3`.
    """
    # 4-band raster: band3 (Red)=4.0, band4 (NIR)=10.0 → (10-4)/(10+4) = 6/14.
    ras = _ras_bands([1.0, 2.0, 4.0, 10.0])
    spec = '{"calc": "(A - B) / (A + B)", "A_index": 0, "B_index": 0, "A_band": 4, "B_band": 3}'
    out = mapalgebra.mapalgebra([ras], spec)
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 6.0 / 14.0, atol=1e-5)


def test_mapalgebra_band_selection_positional_two_copies():
    """The same NDVI via the positional form: pass the raster twice and use
    A_band/B_band (no *_index) — A→raster0 band4, B→raster1 band3."""
    ras = _ras_bands([1.0, 2.0, 4.0, 10.0])
    spec = '{"calc": "(A - B) / (A + B)", "A_band": 4, "B_band": 3}'
    out = mapalgebra.mapalgebra([ras, ras], spec)
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 6.0 / 14.0, atol=1e-5)


def test_mapalgebra_band_out_of_range_raises():
    """A band past the raster's band count is a clear ValueError (not an opaque
    rasterio IndexError from deep in read) — mirrors the raster-index message."""
    import pytest

    ras = _ras_bands([1.0, 2.0])  # 2 bands
    with pytest.raises(ValueError, match=r"A_band=5 is out of range.*2 band"):
        mapalgebra.mapalgebra([ras], '{"calc": "A", "A_band": 5}')


def test_mapalgebra_raster_index_out_of_range_raises():
    """An A_index past the number of provided rasters is a clear ValueError."""
    import pytest

    with pytest.raises(ValueError, match="index|raster"):
        mapalgebra.mapalgebra([_ras(4.0)], '{"calc": "A", "A_index": 3}')
