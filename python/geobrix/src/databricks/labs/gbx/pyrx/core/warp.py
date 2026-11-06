"""Spark-free reproject/warp helpers (rasterio.warp). Tile-returning: each
function returns new GTiff bytes."""

import threading
from collections import OrderedDict

import numpy as np
import rasterio
import rasterio.windows
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform, reproject

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core._nodata import (
    fallback_nodata,
    has_alpha,
    has_own_mask,
)
from databricks.labs.gbx.pyrx.core._util import resampling_enum


def _fill_plan(ds):
    """The warp NoData plan of ``ds`` when its warp fill is marked NoData, else ``None``.

    Returns ``(nodata, read_masked)``. ``nodata`` is the source's NoData, or the dtype
    fallback (``fallback_nodata``, heavy ``GDAL.getNoDataConstant``) when it declares
    none; the output declares it, the warp fills with it, and source pixels equal to it
    are NoData too (warped as the source NoData), instead of being moved by one and kept
    valid. ``read_masked`` says the source has its own (non-alpha) per-dataset mask band:
    a source NoData makes the warp ignore that mask, so each band is read masked and its
    masked pixels set to ``nodata`` before the warp (heavy reads the mask as an alpha
    band, ``WarpSource``). The masked pixels are set in place, so a masked read holds one
    source band of data plus its boolean mask (one byte per pixel) in memory at a time.

    ``None`` (the warp is left as it was) for a source that declares NoData and has no
    mask band (the warp carries its NoData), and for a source with an alpha band: the
    warped alpha band already marks the fill and the transparent pixels, and a declared
    NoData would shadow it (``NodataShadowWarning``), turning the transparent pixels
    valid and the source's real 0 pixels NoData. Heavy ``WarpSource.fillNoDataArgs``
    does the same.

    A band-level mix (some bands declare NoData, some do not) occurs only for VRT or
    in-memory sources: light treats it as declared (band 1's value), heavy marks the
    undeclared bands.
    """
    if has_alpha(ds):
        return None
    own_mask = has_own_mask(ds)
    declared = next((v for v in ds.nodatavals if v is not None), None)
    if declared is not None and not own_mask:
        return None
    nodata = fallback_nodata(ds.dtypes[0]) if declared is None else declared
    return nodata, own_mask


def gcp_georef(ds):
    """``(crs, gcps)`` of a tile georeferenced by ground control points, else ``None``.

    Such a tile has no ``ds.crs`` and an identity transform; its CRS is the GCP projection, and a warp
    takes the points in place of a transform.
    """
    if ds.crs is not None:
        return None
    gcps, crs = ds.gcps
    return (crs, gcps) if gcps and crs else None


def source_crs(ds):
    """The CRS a tile is georeferenced in: its own, else its GCP projection; ``None`` for a tile with neither."""
    georef = gcp_georef(ds)
    return ds.crs if georef is None else georef[0]


def _source_georef(ds, window=None) -> dict:
    """The ``reproject`` arguments that place ``ds`` (or a window of it) on the ground."""
    georef = gcp_georef(ds)
    if georef is not None:
        return {"gcps": georef[1], "src_crs": georef[0]}
    transform = ds.window_transform(window) if window is not None else ds.transform
    return {"src_transform": transform, "src_crs": ds.crs}


# The output grid of a warp, by the exact inputs that fix it. PROJ builds the CRS-to-CRS
# transformer again for every ``calculate_default_transform`` call (30 to 200 ms for a pair
# with a datum shift, such as BNG; under 1 ms for a pair it holds in memory), so a footprint
# warped again (a tile reprojected by several functions, or the same file by several tiles)
# does not pay it twice. The answer is a pure function of the key, so a hit is the grid a
# call would compute.
_GRID_CACHE_SIZE = 256
_GRID_CACHE: "OrderedDict" = OrderedDict()
_GRID_LOCK = threading.Lock()


def _cached_default_transform(src_crs, dst_crs, width, height, bounds):
    """``calculate_default_transform(src_crs, dst_crs, width, height, *bounds)``, remembered."""
    try:
        key = (src_crs, dst_crs, int(width), int(height), tuple(bounds))
        hash(key)
    except (
        TypeError
    ):  # an argument that cannot key the cache: compute, remember nothing
        return calculate_default_transform(src_crs, dst_crs, width, height, *bounds)
    with _GRID_LOCK:
        hit = _GRID_CACHE.get(key)
        if hit is not None:
            _GRID_CACHE.move_to_end(key)
            return hit
    grid = calculate_default_transform(src_crs, dst_crs, width, height, *bounds)
    with _GRID_LOCK:
        _GRID_CACHE[key] = grid
        while len(_GRID_CACHE) > _GRID_CACHE_SIZE:
            _GRID_CACHE.popitem(last=False)
    return grid


def _window_geometry(ds, window):
    """``(width, height, bounds)`` of ``ds``, or of ``window`` of it (None: the whole of ``ds``).

    A window's bounds are those a file holding just the window reports (``DatasetBase.bounds``):
    the corners of its transform. A window of a rotated transform is not supported
    (:func:`window_reprojectable`).
    """
    if window is None:
        return ds.width, ds.height, tuple(ds.bounds)
    left, bottom, right, top = rasterio.windows.bounds(window, ds.transform)
    return int(window.width), int(window.height), (left, bottom, right, top)


def _default_transform(ds, dst_crs, window=None):
    """``calculate_default_transform`` of ``ds`` (or of ``window`` of it) to ``dst_crs``, from its GCPs when it has them in place of a transform."""
    georef = gcp_georef(ds)
    if georef is not None:
        return calculate_default_transform(
            georef[0], dst_crs, ds.width, ds.height, gcps=georef[1]
        )
    width, height, bounds = _window_geometry(ds, window)
    return _cached_default_transform(ds.crs, dst_crs, width, height, bounds)


def full_extent(ds, window) -> bool:
    """True when ``window`` is None or covers all of ``ds``."""
    return window is None or (
        window.col_off == 0
        and window.row_off == 0
        and window.width == ds.width
        and window.height == ds.height
    )


def window_reprojectable(ds, window) -> bool:
    """True when :func:`reproject_to_crs` can warp ``window`` of ``ds`` as a file holding just that window.

    A whole-file window always can. A sub-window cannot when the source is georeferenced by
    GCPs (the points are the file's, not the window's) or by a rotated transform (a window's
    bounds are then not the corners of a box), or when it has its own mask band: GDAL leaves the
    pixels under the mask of a band it warps from the file unwritten, where a window read out
    of the file as an array carries them across, so the two differ under the mask.
    """
    if full_extent(ds, window):
        return True
    t = ds.transform
    return gcp_georef(ds) is None and t.b == 0 and t.d == 0 and not has_own_mask(ds)


def warped_own_mask(
    ds, *, dst_crs, transform, width: int, height: int, window=None
) -> np.ndarray:
    """The own mask band of ``ds`` warped like its pixels onto the given grid: ``(rows,
    cols)`` uint8, 255 valid and 0 invalid.

    The mask is warped as data (nearest), so it lands where the pixels do. A pixel no
    source pixel maps to stays valid, as the plain warp leaves it a valid zero; GDAL moves
    a valid 255 to 254 where it collides with the NoData 255 it fills with, so anything
    but 0 is valid. ``window`` limits the source to a window of ``ds`` (the pixels a
    caller warps from). Memory: the mask is held twice as uint8 per pixel (the source's,
    the warped one), on top of the warped pixels.
    """
    warped = np.full((height, width), 255, dtype="uint8")
    reproject(
        source=ds.read_masks(1, window=window),
        destination=warped,
        **_source_georef(ds, window),
        dst_transform=transform,
        dst_crs=dst_crs,
        resampling=resampling_enum("nearest"),
        src_nodata=None,
        dst_nodata=255,
    )
    return np.where(warped == 0, 0, 255).astype("uint8")


def warped_footprint(ds, *, dst_crs, transform, width: int, height: int) -> np.ndarray:
    """Where a warp of ``ds`` onto the given grid reaches a source pixel: ``(rows, cols)``
    uint8, 255 covered and 0 not.

    The output of a warp is a box on its grid; what the source does not reach (the corners of
    the box around a rotated footprint, the pad of a footprint snapped outward) is filled with
    zeros that read as data when the source declares no NoData to fill with. Memory: the source
    as one byte per pixel, on top of the output grid.
    """
    covered = np.zeros((height, width), dtype="uint8")
    reproject(
        source=np.ones((ds.height, ds.width), dtype="uint8"),
        destination=covered,
        **_source_georef(ds),
        dst_transform=transform,
        dst_crs=dst_crs,
        resampling=resampling_enum("nearest"),
        src_nodata=None,
        dst_nodata=None,
    )
    return np.where(covered != 0, 255, 0).astype("uint8")


def _same_value(a, b) -> bool:
    """True when two NoData values are the same (NaN equals NaN)."""
    return a == b or (a != a and b != b)


def _warp_bands(
    ds,
    profile,
    *,
    dst_crs,
    transform,
    resampling: str,
    mark_fill_nodata,
    window=None,
):
    """GTiff bytes of every band of ``ds`` warped into a new dataset of ``profile``;
    with ``mark_fill_nodata``, by the plan of :func:`_fill_plan`.

    A source's own mask band (``has_own_mask``) goes through the warp with its pixels: a
    warp ignores a mask band once a NoData is set, so where the source declares a NoData
    the masked pixels are set to it and the warp carries it, and without one the mask is
    warped to the output grid and written as its own mask band (:func:`warped_own_mask`),
    as the lazy read of a virtual tile with a target CRS does.

    ``window`` (a rasterio ``Window``, None for the whole of ``ds``) warps just that part of
    ``ds``, as a file holding only the window would be warped, without the window ever being
    copied out of ``ds`` whole: a band at a time is read from it. Memory: the whole-file warp
    streams the source through GDAL's block cache and holds no band; a window holds one band
    of the window (plus its mask, one byte per pixel, when the source has one).

    When every band takes the same source NoData, the bands go through one warp call: GDAL
    builds the coordinate transformer (a PROJ lookup, tens of milliseconds for a datum shift)
    once, where a call per band builds it once per band.
    """
    whole = full_extent(ds, window)
    window = None if whole else window
    plan = _fill_plan(ds) if mark_fill_nodata else None
    own_mask = has_own_mask(ds)
    nodata, read_masked = plan if plan else (None, False)
    band_nodata = [nodata] * ds.count  # each band's source NoData
    if plan:
        profile.update(nodata=nodata)
    elif own_mask and not has_alpha(ds):
        # As the fill plan: a source whose bands declare different values (a VRT or in-memory
        # source) is declared with the first one, and a band that declares none takes it. Each
        # band keeps its own value as its source NoData. (A source with an alpha band is left
        # to it, as the fill plan leaves it: its mask is warped and written below, and a
        # NoData set on the masked pixels would turn the masked alpha values opaque.)
        nodata = next((v for v in ds.nodatavals if v is not None), None)
        band_nodata = [v if v is not None else nodata for v in ds.nodatavals]
        read_masked = nodata is not None
        if read_masked:
            profile.update(nodata=nodata)
    mask = (
        warped_own_mask(
            ds,
            dst_crs=dst_crs,
            transform=transform,
            width=profile["width"],
            height=profile["height"],
            window=window,
        )
        if own_mask and not (plan or read_masked)
        else None
    )
    resamp = resampling_enum(resampling)
    indexes = list(range(1, ds.count + 1))
    one_call = (
        whole
        and not read_masked
        and all(_same_value(v, band_nodata[0]) for v in band_nodata)
    )
    with _comp.internal_mask_env(mask), MemoryFile() as mf:
        with mf.open(**profile) as dst:
            _comp.match_alpha(dst, ds.colorinterp)
            if one_call:
                reproject(
                    source=rasterio.band(ds, indexes),
                    destination=rasterio.band(dst, indexes),
                    **_source_georef(ds),
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=resamp,
                    src_nodata=band_nodata[0],
                    dst_nodata=nodata,
                )
            else:
                for i in indexes:
                    source = rasterio.band(ds, i) if whole else None
                    if read_masked:
                        band = ds.read(i, window=window, masked=True)
                        # in place, dtype kept
                        band.data[np.ma.getmaskarray(band)] = band_nodata[i - 1]
                        source = band.data
                    elif source is None:
                        source = ds.read(i, window=window)
                    reproject(
                        source=source,
                        destination=rasterio.band(dst, i),
                        **_source_georef(ds, window),
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=resamp,
                        src_nodata=band_nodata[i - 1],
                        dst_nodata=nodata,
                    )
            if mask is not None:
                dst.write_mask(mask)
        return mf.read()


def target_grid(ds, dst_crs) -> tuple:
    """The ``(width, height)`` a reprojection of ``ds`` to ``dst_crs`` makes, from the header alone."""
    _, width, height = _default_transform(ds, dst_crs)
    return width, height


def _identity(ds, window=None) -> bytes:
    """``ds`` (or ``window`` of it) re-encoded as it is: its alpha band and its own mask band come with it."""
    mask = ds.read_masks(1, window=window) if has_own_mask(ds) else None
    profile = ds.profile
    if window is not None:
        profile = dict(profile)
        profile.update(
            width=int(window.width),
            height=int(window.height),
            transform=ds.window_transform(window),
        )
    return _comp.write_gtiff(
        profile, ds.read(window=window), alpha_like=ds.colorinterp, mask=mask
    )


def reproject_to_crs(
    ds,
    target_crs_value,
    resampling: str = "nearest",
    *,
    mark_fill_nodata: bool = False,
    window=None,
) -> bytes:
    """Reproject an open dataset to a string-given target CRS; return GTiff bytes.

    Accepts any ``resolve_crs``-parseable value: int EPSG, int-castable string
    (``"4326"``), authority string (``"ESRI:54008"``), WKT, PROJ4, …  Unlike
    ``reproject_to_srid`` this never requires a positive EPSG code, so
    non-EPSG targets (ESRI codes, WKT) work correctly. ``mark_fill_nodata`` is
    as in ``reproject_to_srid``.

    ``window`` (a rasterio ``Window`` of ``ds``; None for all of it) reprojects just that
    part, the bytes a file holding only the window would give, without copying the window
    out of ``ds`` first (see :func:`_warp_bands`). A caller checks
    :func:`window_reprojectable` first.

    Identity short-circuit: when ``ds.crs == resolved_target_crs`` the pixels
    are returned verbatim (no resample, no re-encode) exactly like
    ``reproject_to_srid``.
    """
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    dst_crs = resolve_crs(target_crs_value)
    if ds.crs and ds.crs == dst_crs:
        # Identity: re-encode with ZSTD baseline (same as reproject_to_srid).
        return _identity(ds, None if full_extent(ds, window) else window)

    transform, width, height = _default_transform(ds, dst_crs, window)
    out_dtype = ds.dtypes[0]
    decoded_bytes = ds.count * width * height * np.dtype(out_dtype).itemsize
    profile = _comp.source_profile(ds.profile)
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    return _warp_bands(
        ds,
        profile,
        dst_crs=dst_crs,
        transform=transform,
        resampling=resampling,
        mark_fill_nodata=mark_fill_nodata,
        window=window,
    )


def reproject_to_srid(
    ds, target_srid: int, resampling: str = "nearest", *, mark_fill_nodata: bool = False
) -> bytes:
    """Reproject an open dataset to EPSG:<target_srid>; return GTiff bytes.

    ``mark_fill_nodata`` makes the pixels the warp fills outside the source
    footprint NoData rather than valid zeros, for a source that declares no NoData:
    the output declares the dtype fallback (``fallback_nodata``) and source pixels
    equal to it become NoData too (as in heavy). A source that declares NoData keeps
    it either way. A source's own mask band still masks its pixels, also when the
    source declares NoData (see ``_fill_plan``). Off by default, so other callers
    (``rst_transform`` and friends) are unchanged; BNG tessellation and the
    raster-to-grid reducers turn it on so the fill is not read as data.

    Identity short-circuit: when the source CRS's EPSG code equals
    ``target_srid`` the reprojection is a no-op (produces no new pixels), so the
    source GTiff bytes are returned VERBATIM — no resample, no re-encode. This
    keeps an identity transform in the reference/passthrough bucket and
    preserves the raw-bytes sort key that ``agg_core.merge_tiles`` relies on for
    heavy-parity overlap resolution.
    """
    from databricks.labs.gbx.core.crs import authority_srid_of

    target_srid = int(target_srid)
    src_epsg = authority_srid_of(ds.crs)
    if src_epsg is not None and src_epsg == target_srid:
        # Identity: emit the source bytes re-encoded with ZSTD baseline.
        return _identity(ds)

    dst_crs = f"EPSG:{target_srid}"
    transform, width, height = _default_transform(ds, dst_crs)
    out_dtype = ds.dtypes[0]
    decoded_bytes = ds.count * width * height * np.dtype(out_dtype).itemsize
    profile = _comp.source_profile(ds.profile)
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    return _warp_bands(
        ds,
        profile,
        dst_crs=dst_crs,
        transform=transform,
        resampling=resampling,
        mark_fill_nodata=mark_fill_nodata,
    )


# ---------------------------------------------------------------------------
# Align a tile to a reference grid
# ---------------------------------------------------------------------------


def align_to_tiles(tile_bytes: bytes, ref_bytes: bytes, dst_nodata=None) -> bytes:
    """Warp ``tile_bytes`` onto ``ref_bytes``'s grid (GTiff bytes); see :func:`align_datasets`.

    Returns ``None`` when either input is missing/empty.
    """
    if not tile_bytes or not ref_bytes:
        return None
    with MemoryFile(bytes(tile_bytes)) as src_mf, open_bytes(src_mf) as src:
        with MemoryFile(bytes(ref_bytes)) as ref_mf, open_bytes(ref_mf) as ref:
            return align_datasets(src, ref, dst_nodata)


def align_datasets(src, ref, dst_nodata=None) -> bytes:
    """Warp the open dataset ``src`` onto the grid of the open dataset ``ref`` (GTiff bytes).

    Mirrors ``gbx_rst_align_to``: the output has exactly the same CRS, width,
    height, and geotransform as the reference. Nearest-neighbour resampling is
    used so pixel values are not interpolated (``-r near`` in gdalwarp).

    This is the explicit fix for the alignment precondition of the combine
    family: align each tile to a common reference, then combine.

    When the reference grid extends beyond the tile's footprint, the uncovered
    destination pixels are filled with the destination NoData value. By default
    that is the tile's own NoData (so a NoData-less tile leaves them at GDAL's
    fill of 0). Pass ``dst_nodata`` to force a sentinel for those uncovered
    pixels — this is what a NoData-less tile needs so padding is distinguishable
    from real values; the sentinel is also written as the output's NoData tag.
    """
    dst_crs, dst_transform = ref.crs, ref.transform
    dst_width, dst_height = ref.width, ref.height
    src_nodata = src.nodata
    effective_dst_nodata = src_nodata if dst_nodata is None else dst_nodata
    out_dtype = src.dtypes[0]
    decoded_bytes = src.count * dst_width * dst_height * np.dtype(out_dtype).itemsize

    profile = _comp.source_profile(src.profile)
    profile.update(
        driver="GTiff",
        crs=dst_crs,
        transform=dst_transform,
        width=dst_width,
        height=dst_height,
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    if dst_nodata is not None:
        profile.update(nodata=dst_nodata)

    with MemoryFile() as out_mf:
        with out_mf.open(**profile) as dst:
            _comp.match_alpha(dst, src.colorinterp)
            for b in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, b),
                    destination=rasterio.band(dst, b),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    src_nodata=src_nodata,
                    dst_nodata=effective_dst_nodata,
                    resampling=Resampling.nearest,
                )
        return out_mf.read()
