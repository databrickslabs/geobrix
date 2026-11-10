"""Spark-free tile-returning edit ops: clip to geometry, change data type,
initialise NoData, threshold, stamp SRID, extract band. Each returns new
GTiff bytes."""

import numpy as np
import shapely
import shapely.geometry
import shapely.wkb
from rasterio.mask import mask as _rio_mask

from databricks.labs.gbx.pyrx.core import _nodata as _nd
from databricks.labs.gbx.pyrx.core import compression as _comp

# GDAL data-type name -> numpy dtype string.
_GDAL_TO_NP = {
    "Byte": "uint8",
    "Int8": "int8",
    "UInt16": "uint16",
    "Int16": "int16",
    "UInt32": "uint32",
    "Int32": "int32",
    "Float32": "float32",
    "Float64": "float64",
}


def clip_shape(ds_crs, geom, geom_crs=None):
    """The cutline of *geom* in the CRS *ds_crs* of the raster it clips (a shapely geometry, or the GeoJSON-like dict of
    its reprojection).

    If the cutline carries a positive SRID and the raster has a CRS, the cutline is reprojected from EPSG:srid to the
    raster CRS before masking. Otherwise (srid 0/unknown, or no raster CRS) the cutline is
    assumed to already be in the raster CRS and used as-is. Any failure to build the source CRS or transform falls back
    to using the cutline as-is.
    """
    mask_shape = geom
    srid = shapely.get_srid(geom)  # 0 when no SRID is set
    # Rule 1 source-CRS: embedded SRID wins; else the explicit geom_crs (int SRID
    # or CRS string, incl. ESRI/WKT); else None (assume aligned, never error).
    from databricks.labs.gbx.pyrx.core.crs import resolve_source_crs

    src_crs = resolve_source_crs(srid, crs=geom_crs)
    if src_crs is not None and ds_crs is not None:
        try:
            from rasterio.warp import transform_geom
            from shapely.geometry import mapping

            if src_crs != ds_crs:
                # transform_geom returns a GeoJSON-like dict; rasterio.mask
                # accepts GeoJSON geometries directly.
                mask_shape = transform_geom(src_crs, ds_crs, mapping(geom))
        except Exception:
            # Unknown EPSG / transform failure -> fall back to as-is: the cutline
            # is taken to be in the raster's CRS already.
            mask_shape = geom
    return mask_shape


# the pixels `holds_no_pixel_centre` rasterizes at a time (what it holds, not the size of the cutline's window), and the
# most it scans for one cutline (a fixed number, so every tile of a raster stops at the same point)
_CENTRE_STRIP_PIXELS = 1 << 20
_CENTRE_SCAN_PIXELS = 1 << 24


def holds_no_pixel_centre(shape, transform, width: int, height: int) -> bool:
    """True when the cutline *shape* (in the raster's CRS, a shapely geometry or its GeoJSON-like dict) holds the centre of
    no pixel of the grid *transform* lays out, though it reaches the ``width`` x ``height`` raster. A clip that keeps the
    pixels whose centre it holds would keep only NoData, so a clip keeps the pixels it touches instead (``all_touched``):
    the pixels it stands in. A cutline that holds a centre keeps the caller's rule, however small or thin it is.

    The centres are the grid's, also past the raster's edge: the tiles of one raster, clipped one at a time, then decide
    as the whole raster does (a tile the cutline only grazes keeps nothing, not the pixels it touches). It looks at the
    pixel of the cutline's representative point first (most cutlines hold its centre), then rasterizes the window the
    cutline's bounds cover, a block at a time (``_CENTRE_STRIP_PIXELS``), until a centre is found. A cutline whose window
    is over ``_CENTRE_SCAN_PIXELS`` and holds no centre in the whole blocks that fit under it keeps the caller's rule: the scan's cost is
    bounded, and the fallback is never taken for a cutline that may hold a centre.
    """
    import math

    from rasterio.features import geometry_mask

    geom = shapely.geometry.shape(shape) if isinstance(shape, dict) else shape
    if geom.is_empty:
        return False
    inverse = ~transform
    x0, y0, x1, y1 = geom.bounds
    # a bound on a pixel edge is snapped to it, so float noise cannot move the window (or the scan's blocks) by a pixel
    corners = [
        tuple(round(v) if abs(v - round(v)) < 1e-6 else v for v in inverse * (x, y))
        for x in (x0, x1)
        for y in (y0, y1)
    ]
    c0 = math.floor(min(c for c, _ in corners))
    c1 = math.ceil(max(c for c, _ in corners))
    r0 = math.floor(min(r for _, r in corners))
    r1 = math.ceil(max(r for _, r in corners))
    if min(c1, width) <= max(c0, 0) or min(r1, height) <= max(r0, 0):
        return False  # it misses the raster: the clip has nothing to keep either way
    p = geom.representative_point()
    col, row = (math.floor(v) for v in inverse * (p.x, p.y))
    if geom.contains(shapely.Point(*(transform * (col + 0.5, row + 0.5)))):
        return False
    c1, r1 = max(c1, c0 + 1), max(r1, r0 + 1)
    cols = min(c1 - c0, _CENTRE_STRIP_PIXELS)
    rows = max(1, _CENTRE_STRIP_PIXELS // cols)
    scanned = 0
    for r in range(r0, r1, rows):
        for c in range(c0, c1, cols):
            shape_rc = (min(rows, r1 - r), min(cols, c1 - c))
            scanned += shape_rc[0] * shape_rc[1]
            if scanned > _CENTRE_SCAN_PIXELS:
                return False
            held = geometry_mask(
                [geom],
                out_shape=shape_rc,
                transform=transform * transform.translation(c, r),
                invert=True,
            )
            if held.any():
                return False
    return True


def clip_fill(ds):
    """The NoData a clip of *ds* declares, and fills the pixels it leaves out with (the pixels outside the cutline, and the
    ones the source masks): the source's own NoData, else the dtype's fallback (``_nodata.fallback_nodata``). A source
    with an alpha band and no NoData gets none: the alpha band marks those pixels (written as 0, so alpha 0), and a NoData
    declared over it would shadow the alpha band and turn them valid.
    """
    if ds.nodata is not None:
        return ds.nodata
    if _nd.has_alpha(ds):
        return None
    return _nd.fallback_nodata(ds.dtypes[0])


def clip_to_geom(
    ds, geom, all_touched: bool = False, geom_crs=None, nodata=None, crop: bool = True
):
    """Clip a raster to a geometry; return GTiff bytes, or ``None`` on non-overlap.

    ``geom`` may be a shapely geometry (preferred — carries SRID via
    ``shapely.set_srid``) or raw WKB/EWKB ``bytes`` (back-compat). The cutline is put in the
    raster's CRS as :func:`clip_shape` says.

    If the (reprojected) cutline does NOT overlap the raster, ``rasterio.mask``
    raises ``ValueError: Input shapes do not overlap raster.``. Heavy GDAL Warp
    ``-cutline`` produces an empty/NoData result rather than crashing a whole
    distributed job, so we mirror that intent by returning ``None`` (a null tile)
    on non-overlap. Only that specific non-overlap condition is caught; unrelated
    errors propagate.

    ``nodata``, when given, fills the pixels outside the geometry and is declared
    on the output. Otherwise the fill is the source NoData, else the dtype's fallback NoData, declared on the output
    (:func:`clip_fill`): the pixels the clip leaves out are never valid zeros. A cutline that holds no pixel centre keeps
    the pixels it touches (:func:`holds_no_pixel_centre`).

    ``crop=False`` keeps the raster's own grid (the output is as large as the raster, the pixels outside the geometry
    filled): the clip of one window of a raster that is cropped as a whole (see :func:`clip_masked`).
    """
    if isinstance(geom, (bytes, bytearray)):
        geom = shapely.wkb.loads(bytes(geom))  # handles WKB and EWKB
    shape = clip_shape(ds.crs, geom, geom_crs)
    all_touched = bool(all_touched) or holds_no_pixel_centre(
        shape, ds.transform, ds.width, ds.height
    )
    return clip_masked(ds, shape, all_touched=all_touched, nodata=nodata, crop=crop)


def clip_masked(
    ds,
    mask_shape,
    all_touched: bool = False,
    nodata=None,
    crop: bool = True,
    transform=None,
):
    """GTiff bytes of *ds* masked by the cutline *mask_shape* (already in the CRS of *ds*; see :func:`clip_shape`), or
    ``None`` when a ``crop`` finds no overlap. See :func:`clip_to_geom`. ``transform``, when given, is the output's
    geotransform in place of the one the clip derives (a window of a clip that is cropped as a whole places it from
    the crop's transform, to the last bit).
    """
    if nodata is None:
        nodata = clip_fill(ds)
    try:
        out_image, out_transform = _rio_mask(
            ds, [mask_shape], crop=crop, all_touched=bool(all_touched), nodata=nodata
        )
    except ValueError as exc:
        # rasterio.mask raises ValueError("Input shapes do not overlap raster.")
        # when the cutline misses the raster entirely: that is an empty result,
        # a null tile; re-raise anything else.
        if "do not overlap" in str(exc).lower():
            return None
        raise
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        height=out_image.shape[1],
        width=out_image.shape[2],
        transform=out_transform if transform is None else transform,
    )
    if nodata is not None:
        profile.update(nodata=nodata)
    # The clip is a chip of the raster (tessellation covering chips): it keeps the colour
    # interpretation of every band, not only the alpha band.
    return _comp.write_gtiff(
        profile, out_image, alpha_like=ds.colorinterp, all_labels=True
    )


def gdal_type(new_type) -> str:
    """The key of ``_GDAL_TO_NP`` that ``new_type`` names, in any case (GDAL reads a data-type name case-
    insensitively); a ``ValueError`` that names it and lists the known ones when it names none.
    """
    by_lower = {k.lower(): k for k in _GDAL_TO_NP}
    key = by_lower.get(str(new_type).lower())
    if key is None:
        raise ValueError(
            f"rst_updatetype: unknown data type {new_type!r}; known: {', '.join(_GDAL_TO_NP)}"
        )
    return key


def numpy_type(new_type) -> str:
    """The numpy name of the data type ``new_type`` names (see :func:`gdal_type`)."""
    return str(np.dtype(_GDAL_TO_NP[gdal_type(new_type)]))


def update_type(ds, new_type: str) -> bytes:
    """Cast all bands to a new GDAL data type name (e.g. 'Int32', any case); return GTiff bytes.

    As ``gdal_translate -ot``: a floating-point value cast to an integer type is rounded to the nearest integer
    and a value outside the type's range is clamped to its end, never wrapped.
    """
    np_dtype = _GDAL_TO_NP[gdal_type(new_type)]
    data = ds.read()
    if np.issubdtype(np.dtype(np_dtype), np.integer):
        info = np.iinfo(np_dtype)
        if np.issubdtype(data.dtype, np.floating):
            # NaN has no integer: it becomes 0, as GDAL's
            data = np.floor(np.nan_to_num(data.astype("float64"), nan=0.0) + 0.5)
        elif data.dtype == np.uint64:
            # nothing below zero, and the type's top fits uint64: no widening needed (or possible: past int64)
            data = np.minimum(data, np.uint64(info.max))
        else:
            # widen first: under NumPy 2 a bound outside the source dtype (uint16 -> Int16's 32767..) is an error
            lo, hi = max(info.min, np.iinfo("int64").min), min(
                info.max, np.iinfo("int64").max
            )
            data = np.clip(data.astype("int64"), lo, hi)
        if np.issubdtype(data.dtype, np.floating):
            data = np.clip(data, info.min, info.max)
    data = data.astype(np_dtype)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", dtype=np_dtype)
    # Drop a nodata value that can't be represented in the new dtype.
    nd = profile.get("nodata")
    if nd is not None:
        if np.issubdtype(np.dtype(np_dtype), np.integer):
            info = np.iinfo(np_dtype)
            if not (info.min <= nd <= info.max):
                profile["nodata"] = None
    return _comp.write_gtiff(profile, data, alpha_like=ds.colorinterp)


def init_nodata(ds, default: float | None = None) -> bytes:
    """Ensure NoData is set on the raster; use the dtype's fallback if not already set.

    Semantics (shared with the virtual apply-at-open path in open_tile):
    - If NoData is already set, the existing value is preserved.
    - If NoData is not set, the value stamped is *default* when given, else the
      dtype's fallback no-data value (``_nodata.fallback_nodata``; the same
      value the heavyweight tier uses, e.g. 65535 for uint16).
    - If that value does NOT fit the band dtype (only possible for an explicit
      *default*), nodata is left unset rather than writing an invalid value,
      which would raise ``ValueError`` on integer dtypes.
    - A source with its own (per-dataset, non-alpha) mask band keeps it: its masked
      pixels take the NoData value, as in heavy ``rst_initnodata``. Without a NoData
      value to write (an out-of-range *default*) the pixels are written as read.
    """
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    if profile.get("nodata") is None:
        dtype_str = profile.get("dtype", ds.dtypes[0])
        value = _nd.fallback_nodata(dtype_str) if default is None else default
        if _nd.nodata_fits_dtype(value, dtype_str):
            profile["nodata"] = value
        # else: explicit default out of range for this dtype; leave nodata unset.
    if _nd.has_own_mask(ds) and profile.get("nodata") is not None:
        data = ds.read(masked=True).filled(profile["nodata"])
    else:
        data = ds.read()
    return _comp.write_gtiff(profile, data, alpha_like=ds.colorinterp)


_THRESHOLD_OPS = {
    ">": np.greater,
    "<": np.less,
    ">=": np.greater_equal,
    "<=": np.less_equal,
    "==": np.equal,
    "!=": np.not_equal,
}


def check_threshold_op(op) -> str:
    """The comparison operator of ``rst_threshold`` (``>`` for NULL); a ``ValueError`` that lists the known ones when
    it names none."""
    op = ">" if op is None else str(op)
    if op not in _THRESHOLD_OPS:
        known = ", ".join(sorted(_THRESHOLD_OPS))
        raise ValueError(f"rst_threshold: unknown operator {op!r}. Known: {known}")
    return op


def threshold(ds, op: str = ">", value: float = 0.0) -> bytes:
    """Keep pixels satisfying ``op value``; set others to NoData.

    Args:
        ds:    Open rasterio DatasetReader.
        op:    Comparison operator string: one of ">", "<", ">=",
               "<=", "==", "!=".  Defaults to ">".
        value: Threshold scalar.  Defaults to 0.0.

    Returns:
        GTiff bytes with the same dtype and band count; pixels that fail
        the comparison are replaced with the raster's NoData value (the
        dtype's fallback no-data value when not set, as the heavyweight tier).
    """
    op = check_threshold_op(op)
    value = 0.0 if value is None else float(value)
    fn = _THRESHOLD_OPS[op]
    data = ds.read()
    nd = ds.nodata if ds.nodata is not None else _nd.fallback_nodata(data.dtype.name)
    keep = fn(data, value)
    out = np.where(keep, data, nd).astype(data.dtype)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", nodata=nd)
    return _comp.write_gtiff(profile, out, alpha_like=ds.colorinterp)


def set_srid(ds, srid: int) -> bytes:
    """Stamp the CRS from a SRID integer WITHOUT reprojecting.

    Mirrors the heavyweight ``gbx_rst_setsrid`` (``gdal_edit.py -a_srs``):
    pixel values and the GeoTransform are unchanged; only the CRS metadata is
    rewritten. Use ``rst_transform`` for an actual reprojecting warp.

    SRID bound: ``srid >= 0`` — a negative SRID is rejected (the one set-time
    guard). ``0`` means "no CRS" and clears the CRS metadata. A positive code is
    classified via the authoritative EPSG/ESRI rule (so ESRI codes like 54008
    work); a code in neither registry raises here — writing CRS bytes is the
    apply moment.

    Args:
        ds:   Open rasterio DatasetReader.
        srid: Non-negative SRID (EPSG or ESRI code; ``0`` clears the CRS).

    Returns:
        GTiff bytes with the same pixels/transform but the resolved CRS
        (or no CRS when ``srid == 0``).
    """
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    srid = int(srid)
    if srid < 0:
        raise ValueError(f"rst_setsrid: SRID must be >= 0; got {srid}")
    crs = (
        None if srid == 0 else resolve_crs(srid)
    )  # raises for an invalid positive code
    profile = ds.profile.copy()
    profile.update(driver="GTiff", crs=crs)
    return _comp.write_gtiff(profile, ds.read(), alpha_like=ds.colorinterp)


def set_crs(ds, crs_value) -> bytes:
    """Stamp the CRS WITHOUT reprojecting.

    Accepts an int EPSG code, an int-castable string (``"4326"``), or a CRS
    string (``"ESRI:54008"``, WKT, PROJJSON, PROJ4; never a file name or
    URL).  Pixel values and the GeoTransform are unchanged; only the
    CRS metadata is rewritten.  Use ``rst_transformcrs`` for an actual
    reprojecting warp.

    Args:
        ds:        Open rasterio DatasetReader.
        crs_value: CRS descriptor — int EPSG, int-castable string, or CRS string.

    Returns:
        GTiff bytes with the same pixels/transform but the new CRS.
    """
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    crs = resolve_crs(crs_value)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", crs=crs)
    return _comp.write_gtiff(profile, ds.read(), alpha_like=ds.colorinterp)


def band(ds, band_index: int) -> bytes:
    """Extract a single 1-based band as a new single-band GTiff tile.

    Mirrors the heavyweight ``gbx_rst_band`` (``gdal_translate -b``): the
    extracted tile preserves the source CRS, GeoTransform, nodata, and dtype;
    only the band count is reduced to 1.

    Args:
        ds:         Open rasterio DatasetReader.
        band_index: 1-based band index in ``[1 .. ds.count]``.

    Returns:
        Single-band GTiff bytes.
    """
    band_index = int(band_index)
    n_bands = ds.count
    if not (1 <= band_index <= n_bands):
        raise ValueError(
            f"rst_band: band_index {band_index} out of range [1..{n_bands}]"
        )
    data = ds.read(band_index)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1)
    # nodata in the multi-band profile already applies per-band; keep as-is.
    return _comp.write_gtiff(profile, data[np.newaxis, :, :])
