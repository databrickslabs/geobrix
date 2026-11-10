"""Spark-free vector<->raster bridge ops. rasterize: build a raster from a
geometry. fill_nodata: interpolate across NoData. Both return GTiff bytes."""

import numpy as np
import shapely.wkb
from rasterio.features import rasterize as _rasterize
from rasterio.features import shapes as _shapes
from rasterio.fill import fillnodata as _fillnodata
from rasterio.transform import from_bounds
from shapely.geometry import shape as _shape

from databricks.labs.gbx.pyrx.core._nodata import band_valid_mask
from databricks.labs.gbx.pyrx.core.compression import write_gtiff

_NODATA = -9999.0


def rasterize_working_bytes(width_px: int, height_px: int) -> int:
    """An upper estimate of the memory :func:`rasterize_geom` holds at its peak: the burned grid, its copy for the
    encoder and the encoded bytes (24 bytes a pixel)."""
    return int(width_px) * int(height_px) * 24


def rasterize_geom(
    geom_wkb: bytes,
    value,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    out_srid=None,
    out_crs=None,
) -> bytes:
    """Burn a geometry (WKB/EWKB) into a new raster at the given extent/size.

    Pixels inside the geometry get *value*; outside pixels get NoData (-9999.0).
    Returns GTiff bytes.

    Output CRS (Rule 2): ``out_crs`` (string) wins over ``out_srid`` (int); both
    set -> error; neither -> the geometry's own source CRS carried through (or
    CRS-less). The geometry is reprojected from its source CRS (embedded EWKB
    SRID) into the output CRS before burning, so a geometry in one CRS and an
    output declared in another is correct — not garbage (Rule-2 reprojection).
    """
    from databricks.labs.gbx.pyrx.core.crs import (
        crs_to_canonical,
        get_transformer,
        resolve_crs,
        resolve_source_crs,
    )

    geom = shapely.wkb.loads(bytes(geom_wkb))
    width_px = int(width_px)
    height_px = int(height_px)

    # Rule 1 source CRS (embedded SRID only — rasterize has no source param).
    src_crs = resolve_source_crs(shapely.get_srid(geom))
    # Rule 2 target CRS: out_crs wins over out_srid (both -> error); neither ->
    # the source CRS carried through (NOT a forced default).
    if out_srid is not None and out_crs is not None:
        raise ValueError("rst_rasterize: provide out_srid OR out_crs, not both")
    if out_crs is not None:
        tgt_crs = resolve_crs(out_crs)
    elif out_srid is not None:
        tgt_crs = resolve_crs(out_srid)
    else:
        tgt_crs = src_crs  # carry source through; may be None (CRS-less)

    # Reproject the geometry source -> target before burning (never on CRS-less).
    if src_crs is not None and tgt_crs is not None and src_crs != tgt_crs:
        from shapely.ops import transform as _shapely_transform

        tr = get_transformer(src_crs, tgt_crs)
        geom = _shapely_transform(lambda xs, ys: tr.transform(xs, ys), geom)

    transform = from_bounds(
        float(xmin), float(ymin), float(xmax), float(ymax), width_px, height_px
    )
    arr = _rasterize(
        [(geom, float(value))],
        out_shape=(height_px, width_px),
        transform=transform,
        fill=_NODATA,
        dtype="float64",
    )
    profile = dict(
        driver="GTiff",
        width=width_px,
        height=height_px,
        count=1,
        dtype="float64",
        crs=crs_to_canonical(tgt_crs),  # None -> CRS-less output
        transform=transform,
        nodata=_NODATA,
    )
    return write_gtiff(profile, arr[np.newaxis, ...])


def polygonize(ds, band: int = 1, connectedness: int = 4):
    """Extract vector polygons from contiguous equal-value regions of a band.

    Returns a list of (geom_wkb: bytes, value: float). NoData, masked and
    transparent pixels are excluded (``_nodata.band_valid_mask``). ``connectedness`` is 4 or 8.

    Mirrors heavyweight ``gbx_rst_polygonize`` (GDAL ``GDALPolygonize``), which
    is the *integer* polygonize: it reads the band as ``Int32``, so a float
    band is ROUNDED to the nearest integer (GDAL's float->int RasterIO uses
    round-to-nearest, not truncation) before contiguous equal-value regions are
    grouped. Grouping on raw float equality (as ``rasterio.features.shapes``
    does natively) would emit one polygon per pixel on any continuous band --
    both wrong vs the GDAL contract and ~100x slower. We round to int32 first to
    match GDAL exactly (a bare truncating cast collapses a [0,1] field to a
    single 0-region and diverges from heavy's 1664/6835 regions).
    """
    b = int(band)
    arr = ds.read(b)
    msk = band_valid_mask(
        ds, b
    )  # NoData, masked or transparent -> excluded from shapes
    # GDALPolygonize reads the band as Int32: GDAL's float->int RasterIO rounds
    # to nearest, so np.round (round-half-to-even) reproduces it bit-for-bit.
    arr = np.round(arr).astype("int32")
    out = []
    for geom_dict, value in _shapes(
        arr, mask=msk, connectivity=int(connectedness), transform=ds.transform
    ):
        out.append((shapely.wkb.dumps(_shape(geom_dict)), float(value)))
    return out


def check_breaks(breaks) -> None:
    """ValueError naming the element when ``breaks`` holds a NULL (a parameter error, found before any tile is read)."""
    for i, b in enumerate(breaks):
        if b is None:
            raise ValueError(
                f"rst_isoband: breaks must not contain NULL; element {i} is NULL. "
                "Fix: remove the NULL from the breaks array."
            )


def check_ascending(breaks) -> None:
    """ValueError when ``breaks`` is not strictly ascending."""
    if not np.all(np.diff(np.asarray(breaks, dtype="float64")) > 0):
        raise ValueError("breaks must be strictly ascending")


def isoband(ds, breaks, band=1):
    """Reclassify a band into value bins ``[breaks[i], breaks[i+1])`` and return one
    ``(geom_wkb, band_index, lower, upper)`` tuple per CONTIGUOUS patch.

    NoData, masked and transparent pixels are excluded (``_nodata.band_valid_mask``). *breaks* must be strictly
    ascending; values below ``breaks[0]`` or >= ``breaks[-1]`` are dropped.

    Args:
        ds:     Open rasterio DatasetReader.
        breaks: 1-D sequence of N+1 strictly-ascending boundary values that
                define N bands. ``[0, 5, 10]`` creates bands [0,5) and [5,10).
        band:   1-based band index to read (default 1).

    Returns:
        List of ``(geom_wkb: bytes, band_index: int, lower: float, upper: float)``
        — one entry per contiguous equal-band polygon. ``geom_wkb`` is a WKB
        Polygon (2-D, raster CRS).
    """
    brk = np.asarray(breaks, dtype="float64")
    check_ascending(brk)
    arr = ds.read(int(band))
    msk = band_valid_mask(ds, int(band))  # False where NoData, masked or transparent
    # np.digitize(right=False) maps a value v to index i such that breaks[i-1] <= v < breaks[i].
    # Subtract 1 → band index in [0, len(brk)-2] for in-range values; 0 maps below-range → -1,
    # and len(brk)-1 maps >= breaks[-1] → len(brk)-2+1 = out-of-range.
    idx = np.digitize(arr, brk, right=False) - 1
    valid = (idx >= 0) & (idx <= len(brk) - 2) & msk
    idx = np.where(valid, idx, -1).astype("int32")
    out = []
    for geom_dict, v in _shapes(
        idx, mask=(idx >= 0), connectivity=4, transform=ds.transform
    ):
        bi = int(v)
        out.append(
            (
                shapely.wkb.dumps(_shape(geom_dict)),
                bi,
                float(brk[bi]),
                float(brk[bi + 1]),
            )
        )
    return out


def fill_nodata(ds, max_search_dist=None, smoothing_iter=None) -> bytes:
    """Interpolate across NoData gaps in a raster dataset.

    *ds* is an open rasterio DatasetReader. Returns GTiff bytes with NoData
    pixels filled by interpolation from their neighbours.

    A last band that is an alpha band (with another band before it) is the tile's mask, never a band of values: each
    other band is filled where it is not valid or is transparent, from its valid, opaque pixels, and the alpha band
    keeps its values, except that a transparent pixel the search reached in every band becomes opaque (the largest
    alpha of the tile's opaque pixels). A pixel not reached in every band stays
    transparent. The GDAL spec engine's fill does the same (``engine.run._fill_under_alpha``).
    """
    from rasterio.enums import ColorInterp

    msd = 100.0 if max_search_dist is None else float(max_search_dist)
    smi = 0 if smoothing_iter is None else int(smoothing_iter)
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    alpha = (
        ds.count if ds.count >= 2 and ds.colorinterp[-1] == ColorInterp.alpha else None
    )
    a = ds.read(alpha) if alpha else None
    opaque = a > 0 if alpha else None  # 0, negative and NaN are transparent
    reached = np.ones((ds.height, ds.width), bool) if alpha else None
    bands = []
    for i in range(1, ds.count + 1):
        if i == alpha:
            continue
        band = ds.read(i)
        msk = ds.read_masks(i)  # 0 where NoData, 255 where valid
        if alpha:
            msk = np.where((msk != 0) & opaque, np.uint8(255), np.uint8(0))
            ones = (msk != 0).astype("uint8")
            reached &= _fillnodata(ones, mask=ones, max_search_distance=msd) != 0
        bands.append(
            _fillnodata(
                band, mask=msk, max_search_distance=msd, smoothing_iterations=smi
            )
        )
    if alpha:
        # the tile's own full opacity: the largest alpha of its opaque pixels (a filled pixel is reached from one)
        top = a[opaque].max() if opaque.any() else 0
        bands.append(np.where(reached & ~opaque, top, a).astype(a.dtype))
    data = np.stack(bands)
    return write_gtiff(profile, data, alpha_like=ds.colorinterp)
