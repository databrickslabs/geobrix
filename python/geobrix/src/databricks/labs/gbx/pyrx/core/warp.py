"""Spark-free reproject/warp helpers (rasterio.warp). Tile-returning: each
function returns new GTiff bytes."""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform, reproject

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


def warped_own_mask(ds, *, dst_crs, transform, width: int, height: int) -> np.ndarray:
    """The own mask band of ``ds`` warped like its pixels onto the given grid: ``(rows,
    cols)`` uint8, 255 valid and 0 invalid.

    The mask is warped as data (nearest), so it lands where the pixels do. A pixel no
    source pixel maps to stays valid, as the plain warp leaves it a valid zero; GDAL moves
    a valid 255 to 254 where it collides with the NoData 255 it fills with, so anything
    but 0 is valid. Memory: the mask is held twice as uint8 per pixel (the source's, the
    warped one), on top of the warped pixels.
    """
    warped = np.full((height, width), 255, dtype="uint8")
    reproject(
        source=ds.read_masks(1),
        destination=warped,
        src_transform=ds.transform,
        src_crs=ds.crs,
        dst_transform=transform,
        dst_crs=dst_crs,
        resampling=resampling_enum("nearest"),
        src_nodata=None,
        dst_nodata=255,
    )
    return np.where(warped == 0, 0, 255).astype("uint8")


def _warp_bands(ds, profile, *, dst_crs, transform, resampling: str, mark_fill_nodata):
    """GTiff bytes of every band of ``ds`` warped into a new dataset of ``profile``;
    with ``mark_fill_nodata``, by the plan of :func:`_fill_plan`.

    A source's own mask band (``has_own_mask``) goes through the warp with its pixels: a
    warp ignores a mask band once a NoData is set, so where the source declares a NoData
    the masked pixels are set to it and the warp carries it, and without one the mask is
    warped to the output grid and written as its own mask band (:func:`warped_own_mask`),
    as the lazy read of a virtual tile with a target CRS does.
    """
    plan = _fill_plan(ds) if mark_fill_nodata else None
    nodata, read_masked = plan if plan else (None, False)
    if plan:
        profile.update(nodata=nodata)
    own_mask = has_own_mask(ds)
    if own_mask and not plan:
        read_masked = ds.nodata is not None
        nodata = ds.nodata
    mask = (
        warped_own_mask(
            ds,
            dst_crs=dst_crs,
            transform=transform,
            width=profile["width"],
            height=profile["height"],
        )
        if own_mask and not (plan or read_masked)
        else None
    )
    resamp = resampling_enum(resampling)
    with _comp.internal_mask_env(mask), MemoryFile() as mf:
        with mf.open(**profile) as dst:
            _comp.match_alpha(dst, ds.colorinterp)
            for i in range(1, ds.count + 1):
                source = rasterio.band(ds, i)
                if read_masked:
                    band = ds.read(i, masked=True)
                    band.data[np.ma.getmaskarray(band)] = nodata  # in place, dtype kept
                    source = band.data
                reproject(
                    source=source,
                    destination=rasterio.band(dst, i),
                    src_transform=ds.transform,
                    src_crs=ds.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=resamp,
                    src_nodata=nodata,
                    dst_nodata=nodata,
                )
            if mask is not None:
                dst.write_mask(mask)
        return mf.read()


def _identity(ds) -> bytes:
    """``ds`` re-encoded as it is: its alpha band and its own mask band come with it."""
    mask = ds.read_masks(1) if has_own_mask(ds) else None
    return _comp.write_gtiff(
        ds.profile, ds.read(), alpha_like=ds.colorinterp, mask=mask
    )


def reproject_to_crs(
    ds, target_crs_value, resampling: str = "nearest", *, mark_fill_nodata: bool = False
) -> bytes:
    """Reproject an open dataset to a string-given target CRS; return GTiff bytes.

    Accepts any ``resolve_crs``-parseable value: int EPSG, int-castable string
    (``"4326"``), authority string (``"ESRI:54008"``), WKT, PROJ4, …  Unlike
    ``reproject_to_srid`` this never requires a positive EPSG code, so
    non-EPSG targets (ESRI codes, WKT) work correctly. ``mark_fill_nodata`` is
    as in ``reproject_to_srid``.

    Identity short-circuit: when ``ds.crs == resolved_target_crs`` the pixels
    are returned verbatim (no resample, no re-encode) exactly like
    ``reproject_to_srid``.
    """
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    dst_crs = resolve_crs(target_crs_value)
    if ds.crs and ds.crs == dst_crs:
        # Identity: re-encode with ZSTD baseline (same as reproject_to_srid).
        return _identity(ds)

    transform, width, height = calculate_default_transform(
        ds.crs, dst_crs, ds.width, ds.height, *ds.bounds
    )
    out_dtype = ds.dtypes[0]
    decoded_bytes = ds.count * width * height * np.dtype(out_dtype).itemsize
    profile = _comp.drop_ycbcr(ds.profile)
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
    transform, width, height = calculate_default_transform(
        ds.crs, dst_crs, ds.width, ds.height, *ds.bounds
    )
    out_dtype = ds.dtypes[0]
    decoded_bytes = ds.count * width * height * np.dtype(out_dtype).itemsize
    profile = _comp.drop_ycbcr(ds.profile)
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
