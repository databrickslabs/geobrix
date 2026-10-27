"""Spark-free focal ops via scipy.ndimage, matching rasterx GDALBlock semantics:
NoData-aware skip + edge window-shrink (filter) / zero-padded edge (convolve).

GDALBlock.valuesAt (avg/min/max/median): a neighbor contributes only if it is
in-bounds AND in-mask AND not-NoData; out-of-bounds neighbors are skipped so the
window shrinks at edges; if no neighbor is valid the output pixel is NoData.
GDALBlock.convolveAt: ``sum += value * kernel(i)(j)`` over in-bounds valid
neighbors only (others contribute 0, no renormalization), kernel applied
UN-flipped (correlation).
"""

import warnings

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
from scipy import ndimage

from databricks.labs.gbx.pyrx.core._nodata import (
    fallback_nodata,
    has_alpha,
    nodata_fits_dtype,
    read_valid,
)
from databricks.labs.gbx.pyrx.core.compression import write_gtiff


def _nodata_value(ds, default: float = -9999.0) -> float:
    """The source's NoData, else *default* when the band type can hold it, else the type's fallback
    (``fallback_nodata``): a ``uint8`` raster cannot carry -9999."""
    if ds.nodata is not None:
        return ds.nodata
    return (
        default
        if nodata_fits_dtype(default, ds.dtypes[0])
        else fallback_nodata(ds.dtypes[0])
    )


def _out_profile(ds, dtype: str, nd: float, set_nodata: bool) -> dict:
    """Source profile minus block/tiling options that break small outputs."""
    profile = ds.profile.copy()
    for key in ("blockxsize", "blockysize", "tiled", "interleave"):
        profile.pop(key, None)
    profile.update(driver="GTiff", dtype=dtype)
    if set_nodata:
        profile.update(nodata=nd)
    return profile


def filt(ds, kernel_size, operation: str) -> bytes:
    """Apply a focal min/max/mean/median filter per band, GDALBlock semantics.

    Output dtype: ``mean`` -> Float32; ``min``/``max``/``median`` -> input dtype.
    All-invalid windows become NoData. A neighbour is valid by ``_nodata.band_valid_mask``: the
    band's own mask and, for a raster with an alpha band, opaque, so transparent pixels are skipped
    like NoData at any band count.
    """
    size = int(kernel_size)
    op = str(operation)
    box = np.ones((size, size), dtype="float64")
    nd = _nodata_value(ds, default=-9999.0)
    out_bands, any_invalid = [], False
    for i in range(1, ds.count + 1):
        data, valid = read_valid(ds, i)
        if op == "mean":
            num = ndimage.correlate(data * valid, box, mode="constant", cval=0.0)
            cnt = ndimage.correlate(
                valid.astype("float64"), box, mode="constant", cval=0.0
            )
            res = np.where(cnt > 0, num / np.where(cnt > 0, cnt, 1.0), nd)
            invalid = cnt == 0
        else:
            # Vectorized min/max/median over valid neighbors only. Out-of-bounds
            # and invalid (masked/NoData) neighbors are skipped so the window
            # shrinks at edges -- identical to a nan-aware agg over the window,
            # but without scipy.generic_filter's per-pixel Python callback (the
            # ~14x rst_filter regression). See the identity tests in
            # test_core_focal.py asserting bit-for-bit parity with the old path.
            if op == "min":
                # min ignoring invalid == min with invalid/OOB -> +inf
                filled = np.where(valid, data, np.inf).astype("float64")
                res = ndimage.minimum_filter(
                    filled, size=size, mode="constant", cval=np.inf
                )
            elif op == "max":
                filled = np.where(valid, data, -np.inf).astype("float64")
                res = ndimage.maximum_filter(
                    filled, size=size, mode="constant", cval=-np.inf
                )
            else:  # median (assumes odd `size`, which is the norm for filters)
                arr = np.where(valid, data, np.nan).astype("float64")
                pad = size // 2
                padded = np.pad(arr, pad, mode="constant", constant_values=np.nan)
                windows = sliding_window_view(padded, (size, size))
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", RuntimeWarning)
                    res = np.nanmedian(windows, axis=(-2, -1))
            # All-invalid window -> NoData. Use the valid-count (not isfinite),
            # since min/max emit +/-inf for an all-invalid window.
            cnt = ndimage.correlate(
                valid.astype("float64"), box, mode="constant", cval=0.0
            )
            invalid = cnt == 0
            res = np.where(invalid, nd, res)
        any_invalid = any_invalid or bool(invalid.any())
        out_bands.append(res)
    out_dtype = "float32" if op == "mean" else ds.dtypes[0]
    out = np.stack(out_bands)
    # a raster with an alpha band declares none: a NoData would shadow it (the all-invalid windows
    # are written with the value ``nd`` all the same)
    profile = _out_profile(
        ds, out_dtype, nd, (any_invalid or ds.nodata is not None) and not has_alpha(ds)
    )
    return write_gtiff(profile, out.astype(out_dtype), alpha_like=ds.colorinterp)


def convolve(ds, kernel) -> bytes:
    """2-D correlation (UN-flipped kernel) per band, zero-padded edge.

    Mirrors GDALBlock.convolveAt: ``sum`` starts at 0 and accumulates
    ``value * kernel(i)(j)`` over in-bounds valid neighbors; invalid / NoData neighbors
    contribute 0 (via ``data * valid``) and so do the positions off the raster
    (``mode="constant", cval=0.0``), with no weight renormalization, and the returned
    sum is emitted verbatim (convolveAt never yields NoData). Output dtype is Float64.
    The result equals the heavyweight's over the whole raster, edges included.
    """
    k = np.asarray(kernel, dtype="float64")
    nd = _nodata_value(ds, default=-9999.0)
    out_bands = []
    for i in range(1, ds.count + 1):
        data, valid = read_valid(ds, i)
        # data * valid zeroes out masked / NoData neighbors so they add 0,
        # exactly matching the heavyweight's skip-and-keep-sum behavior.
        res = ndimage.correlate(data * valid, k, mode="constant", cval=0.0)
        out_bands.append(res)
    out = np.stack(out_bands)
    profile = _out_profile(ds, "float64", nd, ds.nodata is not None)
    return write_gtiff(profile, out, alpha_like=ds.colorinterp)
