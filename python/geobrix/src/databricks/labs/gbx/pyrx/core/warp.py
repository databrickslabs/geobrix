"""Spark-free reproject/warp helpers (rasterio.warp). Tile-returning: each
function returns new GTiff bytes."""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform, reproject

from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core._util import resampling_enum


def reproject_to_crs(ds, target_crs_value, resampling: str = "nearest") -> bytes:
    """Reproject an open dataset to a string-given target CRS; return GTiff bytes.

    Accepts any ``resolve_crs``-parseable value: int EPSG, int-castable string
    (``"4326"``), authority string (``"ESRI:54008"``), WKT, PROJ4, …  Unlike
    ``reproject_to_srid`` this never requires a positive EPSG code, so
    non-EPSG targets (ESRI codes, WKT) work correctly.

    Identity short-circuit: when ``ds.crs == resolved_target_crs`` the pixels
    are returned verbatim (no resample, no re-encode) exactly like
    ``reproject_to_srid``.
    """
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    dst_crs = resolve_crs(target_crs_value)
    if ds.crs and ds.crs == dst_crs:
        # Identity: re-encode with ZSTD baseline (same as reproject_to_srid).
        data = ds.read()
        out_dtype = ds.dtypes[0]
        profile = ds.profile.copy()
        profile.update(driver="GTiff")
        profile.update(
            _comp.creation_opts(out_dtype, decoded_bytes=data.nbytes, compress="auto")
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data)
            return mf.read()

    transform, width, height = calculate_default_transform(
        ds.crs, dst_crs, ds.width, ds.height, *ds.bounds
    )
    out_dtype = ds.dtypes[0]
    decoded_bytes = ds.count * width * height * np.dtype(out_dtype).itemsize
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    resamp = resampling_enum(resampling)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for i in range(1, ds.count + 1):
                reproject(
                    source=rasterio.band(ds, i),
                    destination=rasterio.band(dst, i),
                    src_transform=ds.transform,
                    src_crs=ds.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=resamp,
                )
        return mf.read()


def reproject_to_srid(ds, target_srid: int, resampling: str = "nearest") -> bytes:
    """Reproject an open dataset to EPSG:<target_srid>; return GTiff bytes.

    Identity short-circuit: when the source CRS's EPSG code equals
    ``target_srid`` the reprojection is a no-op (produces no new pixels), so the
    source GTiff bytes are returned VERBATIM — no resample, no re-encode. This
    keeps an identity transform in the reference/passthrough bucket and
    preserves the raw-bytes sort key that ``agg_core.merge_tiles`` relies on for
    heavy-parity overlap resolution.
    """
    target_srid = int(target_srid)
    src_epsg = ds.crs.to_epsg() if ds.crs else None
    if src_epsg is not None and src_epsg == target_srid:
        # Identity: emit the source bytes re-encoded with ZSTD baseline.
        data = ds.read()
        out_dtype = ds.dtypes[0]
        profile = ds.profile.copy()
        profile.update(driver="GTiff")
        profile.update(
            _comp.creation_opts(out_dtype, decoded_bytes=data.nbytes, compress="auto")
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data)
            return mf.read()

    dst_crs = f"EPSG:{target_srid}"
    transform, width, height = calculate_default_transform(
        ds.crs, dst_crs, ds.width, ds.height, *ds.bounds
    )
    out_dtype = ds.dtypes[0]
    decoded_bytes = ds.count * width * height * np.dtype(out_dtype).itemsize
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    resamp = resampling_enum(resampling)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for i in range(1, ds.count + 1):
                reproject(
                    source=rasterio.band(ds, i),
                    destination=rasterio.band(dst, i),
                    src_transform=ds.transform,
                    src_crs=ds.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=resamp,
                )
        return mf.read()
