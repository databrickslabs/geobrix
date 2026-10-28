"""Focal-op semantics tests: pyrx focal.filt / focal.convolve must match the
heavyweight rasterx GDALBlock contract -- NoData-aware skip + edge window-shrink
(filter) / zero-pad (convolve), kernel applied UN-flipped (correlation).

These complement the smoke-level focal goldens in test_core_bandmath.py.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from scipy import ndimage

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import focal

from . import _alpha_layouts as al


def _tile(data, nodata=-9999.0):
    """Single-band float32 GTiff bytes from a 2-D array (nodata=None => undeclared)."""
    data = np.asarray(data, dtype="float32")
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
    )
    if nodata is not None:
        profile["nodata"] = nodata
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def _open(raster_bytes):
    """Materialize tile bytes into an open dataset whose lifetime we control."""
    mf = MemoryFile(raster_bytes)
    return mf.open()


def _read_band(raster_bytes):
    with _serde.open_tile(raster_bytes) as o:
        return o.read(1).astype("float64")


def _read_nodata(raster_bytes):
    with _serde.open_tile(raster_bytes) as o:
        return o.nodata


# focal.filt / focal.convolve accept an OPEN dataset (see functions.py bindings),
# so wrap _tile bytes in an open dataset before passing through.
def _run(fn, raster_bytes, *args):
    with _serde.open_tile(raster_bytes) as ds:
        return fn(ds, *args)


def test_convolve_edge_is_zero_padded():
    # the positions off the raster add 0, as the heavyweight's GDALBlock.convolveAt sums
    # only the in-bounds neighbours: corners 4, edges 6, interior 9 for a 3x3 of ones
    data = np.ones((4, 4), dtype="float64")
    k = np.ones((3, 3), dtype="float64")
    out = _read_band(_run(focal.convolve, _tile(data, nodata=None), k.tolist()))
    assert np.allclose(out, ndimage.correlate(data, k, mode="constant", cval=0.0))
    assert out[0, 0] == 4 and out[0, 1] == 6 and out[1, 1] == 9
    # and the edge is not replicated
    assert not np.allclose(out, ndimage.correlate(data, k, mode="nearest"))


def test_convolve_asymmetric_kernel_is_unflipped_correlation():
    data = np.arange(25, dtype="float64").reshape(5, 5)
    k = np.array([[0, 0, 0], [0, 0, 1], [0, 0, 0]], dtype="float64")
    out = _read_band(_run(focal.convolve, _tile(data, nodata=None), k.tolist()))
    assert np.allclose(out, ndimage.correlate(data, k, mode="constant", cval=0.0))
    assert not np.allclose(out, ndimage.convolve(data, k, mode="constant", cval=0.0))


def test_filter_mean_shrinks_window_at_edge():
    data = np.arange(16, dtype="float64").reshape(4, 4)
    box = np.ones((3, 3))
    num = ndimage.correlate(data, box, mode="constant", cval=0.0)
    cnt = ndimage.correlate(np.ones_like(data), box, mode="constant", cval=0.0)
    expected = (num / cnt).astype("float32")
    out = _read_band(_run(focal.filt, _tile(data, nodata=None), 3, "mean"))
    assert np.allclose(out, expected, atol=1e-5)


def test_filter_mean_of_a_uint8_raster_marks_empty_windows_with_the_float_nodata():
    """A mean is Float32 whatever the input type, so -9999 fits: a uint8 raster with no NoData of its
    own marks an all-transparent window -9999, not 0 (which a true mean of 0 would equal).
    """
    data = np.zeros((2, 6, 6), dtype="uint8")
    data[-1] = 255
    data[-1, 2, 3] = 0  # one transparent pixel: its 1x1 window holds no valid pixel
    raw = al.layout_bytes("gray_alpha", data=data, transform=from_origin(0, 6, 1, 1))
    out = _read_band(_run(focal.filt, raw, 1, "mean"))
    assert out[2, 3] == -9999.0
    assert (np.delete(out.ravel(), 2 * 6 + 3) == 0.0).all()


def test_filter_mean_skips_nodata_neighbor():
    data = np.array([[1.0, 1.0, 1.0], [1.0, 5.0, 1.0], [1.0, 1.0, 1.0]])
    out = _read_band(_run(focal.filt, _tile(data, nodata=5.0), 3, "mean"))
    # nodata center excluded -> mean of valid {1,1,1} = 1.0
    assert abs(out[0, 0] - 1.0) < 1e-6


def test_all_nodata_window_yields_nodata():
    data = np.full((3, 3), 9.0)
    res = _run(focal.filt, _tile(data, nodata=9.0), 3, "median")
    band = _read_band(res)
    nd = _read_nodata(res)
    # every pixel's window all-invalid -> nodata
    assert np.all(band == nd)


def test_filter_min_shrinks_window_at_edge():
    # min over VALID in-bounds neighbors only (window shrinks at the border):
    # matches the NaN-aware shrink (cval=nan excludes out-of-bounds).
    data = np.arange(16, dtype="float64").reshape(4, 4)
    expected = ndimage.generic_filter(
        data, np.nanmin, size=3, mode="constant", cval=np.nan
    )
    out = _read_band(_run(focal.filt, _tile(data, nodata=None), 3, "min"))
    assert np.allclose(out, expected)


def test_filter_max_shrinks_window_at_edge():
    data = np.arange(16, dtype="float64").reshape(4, 4)
    expected = ndimage.generic_filter(
        data, np.nanmax, size=3, mode="constant", cval=np.nan
    )
    out = _read_band(_run(focal.filt, _tile(data, nodata=None), 3, "max"))
    assert np.allclose(out, expected)


def test_filter_min_max_skip_nodata_neighbor():
    # the NoData center must be excluded from a corner's min/max window
    data = np.array([[2.0, 2.0, 2.0], [2.0, -9999.0, 2.0], [2.0, 8.0, 2.0]])
    mn = _read_band(_run(focal.filt, _tile(data, nodata=-9999.0), 3, "min"))
    mx = _read_band(_run(focal.filt, _tile(data, nodata=-9999.0), 3, "max"))
    # corner [0,0] sees valid {2,2,2} (center -9999 skipped) -> min=max=2.0
    assert abs(mn[0, 0] - 2.0) < 1e-6
    assert abs(mx[0, 0] - 2.0) < 1e-6


# --- identity: vectorized min/max/median == old per-pixel generic_filter ---
#
# The old slow path used scipy.ndimage.generic_filter with a per-pixel nan-aware
# Python callback (np.nanmin/nanmax/nanmedian) and a separate valid-count NoData
# fill. These tests reconstruct that reference inline and assert focal.filt's
# vectorized replacement reproduces it bit-for-bit (NoData cells included) over
# random arrays + validity masks, all-invalid windows, and border-mask cases.
# The heavy-vs-light benchmark gates rst_filter on this consistency, so output
# identity is the contract.

_NAN_AGG_REF = {"min": np.nanmin, "max": np.nanmax, "median": np.nanmedian}


def _reference_filt(data, valid, size, op, nd):
    """Old generic_filter-based result: nan-aware agg over the window, with the
    valid-count NoData fill (all-invalid window -> nd)."""
    arr = np.where(valid, data, np.nan)
    with np.errstate(invalid="ignore"):
        res = ndimage.generic_filter(
            arr, _NAN_AGG_REF[op], size=size, mode="constant", cval=np.nan
        )
    box = np.ones((size, size), dtype="float64")
    cnt = ndimage.correlate(valid.astype("float64"), box, mode="constant", cval=0.0)
    invalid = cnt == 0
    return np.where(invalid, nd, res)


def test_filt_vectorized_matches_generic_filter_reference():
    rng = np.random.default_rng(20260608)
    nd = -9999.0
    for op in ("min", "max", "median"):
        for size in (3, 5):
            data = rng.uniform(-50.0, 50.0, size=(40, 40)).astype("float64")
            valid = rng.random((40, 40)) > 0.25  # ~25% NoData
            # encode invalid cells AS the nodata sentinel so read_masked masks them
            tile_data = np.where(valid, data, nd)
            expected = _reference_filt(data, valid, size, op, nd)
            out = _read_band(_run(focal.filt, _tile(tile_data, nodata=nd), size, op))
            np.testing.assert_allclose(
                out, expected, rtol=0, atol=1e-4, err_msg=f"{op} size={size}"
            )


@pytest.mark.filterwarnings(
    "ignore:All-NaN slice:RuntimeWarning"
)  # the reference's nan-aware window
def test_filt_vectorized_all_invalid_window():
    # a fully-NoData block -> every window all-invalid -> nd everywhere
    nd = -9999.0
    for op in ("min", "max", "median"):
        for size in (3, 5):
            data = np.full((6, 6), nd, dtype="float64")
            valid = np.zeros((6, 6), dtype=bool)
            expected = _reference_filt(np.zeros((6, 6)), valid, size, op, nd)
            out = _read_band(_run(focal.filt, _tile(data, nodata=nd), size, op))
            np.testing.assert_allclose(out, expected, rtol=0, atol=1e-4)
            assert np.all(out == nd)


def test_filt_vectorized_border_mask_edge_shrink():
    # NoData ring around a valid interior -> locks edge-shrink + skip behavior
    nd = -9999.0
    base = np.arange(64, dtype="float64").reshape(8, 8)
    valid = np.ones((8, 8), dtype=bool)
    valid[0, :] = valid[-1, :] = valid[:, 0] = valid[:, -1] = False
    tile_data = np.where(valid, base, nd)
    for op in ("min", "max", "median"):
        for size in (3, 5):
            expected = _reference_filt(base, valid, size, op, nd)
            out = _read_band(_run(focal.filt, _tile(tile_data, nodata=nd), size, op))
            np.testing.assert_allclose(
                out, expected, rtol=0, atol=1e-4, err_msg=f"{op} size={size}"
            )
