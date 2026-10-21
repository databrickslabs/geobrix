"""Shared NoData / edge-mask helpers for pyrx neighborhood + band-math ops.

Matches the heavyweight (GDAL) semantics: read each band with its validity mask,
propagate input NoData through a 3x3 window AND mark the 1-px kernel border as
NoData (one binary_erosion with border_value=0), and emit GTiff bytes that set
those pixels to the output NoData value.
"""

from __future__ import annotations

import numpy as np
from scipy import ndimage

from databricks.labs.gbx.pyrx.core.compression import write_gtiff

# Mirrors heavy GDAL.getNoDataConstant (rasterx/gdal/GDAL.scala:36-50) exactly, so both tiers
# blank with the same value when a source declares no nodata. UInt32 is 2*Int.MaxValue there.
_FALLBACK_NODATA = {
    "uint8": 0.0,
    "uint16": 65535.0,
    "int16": -32768.0,
    "uint32": 4294967294.0,
    "int32": -2147483648.0,
    "float32": float(np.finfo(np.float32).min),
    "float64": float(np.finfo(np.float64).min),
}


def fallback_nodata(dtype: str) -> float:
    """The nodata value to stamp when a raster of *dtype* declares none."""
    return _FALLBACK_NODATA.get(np.dtype(dtype).name, 0.0)


def nodata_fits_dtype(nodata: float, dtype: str) -> bool:
    """Return True if *nodata* is representable in *dtype*.

    Float dtypes always fit (rasterio allows any float nodata for float bands).
    Integer dtypes are checked via numpy's iinfo range.
    """
    try:
        dt = np.dtype(dtype)
        if np.issubdtype(dt, np.integer):
            info = np.iinfo(dt)
            return info.min <= nodata <= info.max
        return True  # float dtypes: always fits
    except Exception:
        return True  # unknown dtype: don't block


def read_masked(ds, band: int = 1):
    """Return (data: float64 2-D, valid: bool 2-D) for *band*.

    `valid` comes from ds.read_masks (0 = invalid/nodata, 255 = valid). When the
    band has no declared nodata, read_masks is all-255 -> valid all-True -> no
    masking, mirroring the heavyweight's "mask only when nodata is declared".
    """
    data = ds.read(int(band)).astype("float64")
    valid = ds.read_masks(int(band)) != 0
    return data, valid


def propagate_invalid(valid: np.ndarray, size: int = 3) -> np.ndarray:
    """Invalid where ANY pixel in the size x size window is invalid OR out-of-array.

    binary_erosion with border_value=0 treats out-of-bounds as invalid, so the
    result includes BOTH the input-NoData propagation and the 1-px border ring
    that gdal.DEMProcessing produces by default.
    """
    structure = np.ones((size, size), dtype=bool)
    eroded = ndimage.binary_erosion(valid, structure=structure, border_value=0)
    return ~eroded


def emit(ds, arr: np.ndarray, nodata: float, invalid: np.ndarray, dtype: str) -> bytes:
    """Write *arr* as a single-band GTiff in memory, setting NoData where *invalid*
    (or non-finite). Float dtypes keep values; uint8 is clipped/rounded to [0,255]."""
    invalid = invalid | ~np.isfinite(arr)
    if dtype == "uint8":
        base = np.clip(np.round(arr), 0, 255)
    else:
        base = arr.astype("float64")
    out = np.where(invalid, nodata, base).astype(dtype)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype=dtype, nodata=nodata)
    return write_gtiff(profile, out[np.newaxis, ...])
