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


def alpha_index(colorinterp):
    """0-based index of the alpha band in *colorinterp*, or ``None`` when there is none.

    THE alpha rule of both tiers (heavy ``GDAL.alphaBandIndex``): a band is the alpha band
    only when it is labelled Alpha AND is the LAST band, as gdalwarp reads a source. A
    band labelled Alpha anywhere else is ordinary data.
    """
    from rasterio.enums import ColorInterp

    labels = list(colorinterp)
    return len(labels) - 1 if labels and labels[-1] == ColorInterp.alpha else None


def subset_colorinterp(colorinterp, indexes):
    """The colour interpretation of the band subset *indexes* (1-based, in output order) of a
    dataset labelled *colorinterp*: the labels a copy of those bands is written with.

    THE band-subset rule of both tiers (heavy ``GDAL.subsetAlphaIndex``). A subset keeps the
    Alpha label only when ALL of these hold: the selected band is the source's alpha band
    (:func:`alpha_index`), it is selected LAST, and at least one data band is selected with it.
    Otherwise every selected band is data (a band labelled Alpha is relabelled undefined), so a
    lone alpha band, which the user selects for its values, is valid wherever its own NoData or
    mask says so, and an alpha label on any other band stays data.
    """
    from rasterio.enums import ColorInterp

    labels = list(colorinterp)
    source_alpha = alpha_index(labels)
    chosen = [i - 1 for i in indexes]
    keeps = (
        source_alpha is not None
        and chosen[-1] == source_alpha
        and any(i != source_alpha for i in chosen[:-1])
    )
    return [
        (
            ColorInterp.alpha
            if keeps and pos == len(chosen) - 1
            else (
                ColorInterp.undefined if labels[i] == ColorInterp.alpha else labels[i]
            )
        )
        for pos, i in enumerate(chosen)
    ]


def has_alpha(ds) -> bool:
    """True when the last band of *ds* is labelled alpha (see :func:`alpha_index`)."""
    return alpha_index(ds.colorinterp) is not None


def has_own_mask(ds) -> bool:
    """True when a band of *ds* is masked by its own per-dataset mask band that is not
    derived from an alpha band: an internal or external GTiff mask, as JPEG/YCbCr imagery
    often has (heavy ``GDAL.hasOwnMask``, the sources heavy ``WarpSource`` reads through a
    mask view). A warp drops such a mask once a source NoData is set, and it is not carried
    by writing the pixels alone, so ops that rewrite or warp the pixels apply it.

    A raster may have an alpha band AND an own mask: GDAL then reports the own mask only (it
    outranks the mask it would derive from the alpha band), so the alpha band is still read
    as values (:func:`valid_mask`) and the mask is carried beside it. A mask GDAL derives
    from an alpha band is flagged ``alpha`` as well as ``per_dataset``, and is not an own
    mask: the alpha band carries it.
    """
    from rasterio.enums import MaskFlags

    return any(
        MaskFlags.per_dataset in flags and MaskFlags.alpha not in flags
        for flags in ds.mask_flag_enums
    )


def opaque_mask(ds, window=None):
    """``(rows, cols)`` bool array of the pixels whose alpha is above 0, or ``None`` when
    *ds* has no alpha band (:func:`alpha_index`). 0, negative and NaN are transparent.
    """
    a = alpha_index(ds.colorinterp)
    return None if a is None else ds.read(a + 1, window=window) > 0


def band_valid_mask(ds, index: int, window=None) -> np.ndarray:
    """``(rows, cols)`` bool array: True where a pixel of band *index* (1-based) of *ds* is
    valid for that band.

    The per-band form of :func:`valid_mask`, for the reducers and statistics that judge each
    band alone (a pixel that is NoData in one band still counts for another). A data band is
    valid where its own mask says so (NoData, the raster's own mask band) and, when *ds* has
    an alpha band, where the pixel is opaque: GDAL derives a mask from alpha only for 2- and
    4-band rasters, so this makes a 3-band gray+x+alpha raster agree with an RGBA one. The
    alpha band itself is judged by its own mask only, as GDAL does: its values over every
    pixel, which is how an opacity is summarised.
    """
    valid = ds.read_masks(index, window=window) != 0
    a = alpha_index(ds.colorinterp)
    if a is None or index - 1 == a:
        return valid
    return valid & opaque_mask(ds, window=window)


def valid_mask(ds, window=None) -> np.ndarray:
    """``(rows, cols)`` bool array: True where a pixel of *ds* is valid.

    THE validity rule of both tiers (heavy ``ValidMask``), applied for every band count.
    A pixel is valid when ALL of these hold:

    * its alpha value is above 0, when *ds* has an alpha band (:func:`alpha_index`);
    * some data band (any band but the alpha band) is valid by its own mask (heavy
      ``anyValid``): ``read_masks``, so a NaN NoData is recognised and a source's own
      per-dataset mask band (an internal or external GTiff mask) is honoured. A raster
      that is only an alpha band has no data band to ask, and that clause holds.

    GDAL derives a mask from the alpha band only for 2- and 4-band rasters, and drops it
    when a NoData is declared over the alpha band; reading the alpha values here makes a
    3-band gray+x+alpha or a 6-band RGB+extra+alpha raster, and a raster that declares a
    NoData, mask as gdalwarp does. (Not ``DatasetReader.dataset_mask``: it takes band 4 of
    any 4-band raster whose band 1 is red as the mask, alpha or not.)
    """
    masks = ds.read_masks(window=window) != 0
    a = alpha_index(ds.colorinterp)
    if a is None:
        return masks.any(axis=0)
    opaque = opaque_mask(ds, window=window)
    valid = masks[:a].any(axis=0) if a else np.ones(masks.shape[1:], dtype=bool)
    return valid & opaque


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
