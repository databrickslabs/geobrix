"""Resilient STAC asset download: HTTP-error-aware fetch + read-validation + retry.

A faithful fetch (no transformation). Validity = the file OPENS and DECODES a window
(rejects throttled error bodies and truncated files a size check would accept).
Note: read-validate decodes band 1 over a ≤512px window; it is not a full-file decode
(header-only truncation is rejected; very large corrupt tail sections may pass).
Volume I/O is sequential-only, so we download to local disk, validate locally, then
publish with a sequential copy.
"""

import logging
import math
import os
import shutil
import tempfile
import time
from typing import Callable, Optional

import requests

from databricks.labs.gbx.pyrx.core import compression as _comp

_log = logging.getLogger(__name__)

_EXIST_FLOOR_BYTES = 1024  # validate=False idempotency floor (non-empty check)


def download_href(href: str, outpath: str, get: Callable = requests.get) -> str:
    """Stream an href to outpath. raise_for_status() so HTTP throttle/expiry (429/403)
    raises -> the caller's retry backs off instead of writing the error body as data."""
    resp = get(href, timeout=100, stream=True)
    resp.raise_for_status()
    with open(outpath, "wb") as fh:
        for chunk in resp.iter_content(1024 * 1024):
            if chunk:
                fh.write(chunk)
    return outpath


def read_validate(path: str) -> bool:
    """True iff the file opens AND decodes a window (a genuine readable raster).

    Note: this is a window decode (band 1, ≤512px), not a full-file decode.
    It rejects throttled bodies and header-only truncation but does not catch
    corruption confined to large data sections.
    """
    import rasterio
    from rasterio.windows import Window

    try:
        with rasterio.open(path) as ds:
            ds.read(1, window=Window(0, 0, min(512, ds.width), min(512, ds.height)))
        return True
    except Exception:
        return False


def _sanitize_filename(filename: str) -> str:
    """Prevent path traversal: strip directory components from the filename.

    Collapses any path separators so that the result is a plain filename
    safe to join with os.path.join(out_dir, ...).
    """
    return os.path.basename(os.path.normpath(filename))


def windowed_download(
    href: str, outpath: str, bbox, bbox_crs=None, max_mpp=None
) -> str:
    """Open href (rasterio /vsicurl for https; any path locally), window to bbox, and
    write a windowed GeoTIFF. The window is clipped to the dataset, so the output is
    correctly georeferenced. Raises ValueError if the bbox does not overlap the asset.

    max_mpp: maximum metres-per-pixel (in SOURCE-CRS units — for NAIP/Sentinel in UTM
        this is metres; for EPSG:4326 sources it is degrees). When set and coarser than
        the source pixel size, the read is DECIMATED so that the output pixel size is
        approximately max_mpp. Rasterio fetches from the nearest COG overview when
        available, bounding both network transfer and UDF memory. When max_mpp is None
        (or <= the native pixel size), the full-resolution window is read unchanged.

        Memory bound: a 4 km × 4 km NAIP AOI at 0.6 m native is ~44 M pixels per band.
        Setting max_mpp=6.0 reduces that to ~444 K pixels (~100× smaller array).

    GDAL_CACHEMAX is set to 128 (MB) via rasterio.Env to bound /vsicurl block cache
    even when decimation is active (defence-in-depth for Serverless 1 GB UDF cap).
    """
    import rasterio

    from databricks.labs.gbx.ds._window import window_for_bbox

    with rasterio.Env(GDAL_CACHEMAX=128):
        with rasterio.open(href) as src:
            win = window_for_bbox(src, bbox, bbox_crs)
            if win is None:
                raise ValueError(f"bbox {bbox} does not overlap the asset {href!r}")

            # Decimation: compute output shape from max_mpp vs native pixel size.
            native = abs(src.transform.a)  # source pixel size in source-CRS units
            if max_mpp is not None and max_mpp > native:
                factor = max(1, int(max_mpp / native))
                ow = max(1, math.ceil(win.width / factor))
                oh = max(1, math.ceil(win.height / factor))
                # rasterio reads from the nearest overview when present (COG-aware)
                data = src.read(window=win, out_shape=(src.count, oh, ow))
                # Scale the window transform to match the decimated pixel size.
                # Preserves exact bounds to sub-pixel: top-left corner is unchanged,
                # pixel size is scaled by (win.width / ow, win.height / oh).
                transform = src.window_transform(win) * rasterio.Affine.scale(
                    win.width / ow, win.height / oh
                )
            else:
                # Full-resolution windowed read (max_mpp=None or finer than native).
                ow = int(win.width)
                oh = int(win.height)
                data = src.read(window=win)
                transform = src.window_transform(win)

            out_dtype = src.dtypes[0]
            decoded_bytes = data.nbytes
            profile = src.profile.copy()
            profile.update(
                driver="GTiff",
                width=ow,
                height=oh,
                transform=transform,
            )
            profile.update(
                _comp.creation_opts(
                    out_dtype, decoded_bytes=decoded_bytes, compress="auto"
                )
            )
            with rasterio.open(outpath, "w", **profile) as dst:
                dst.write(data)
    return outpath


def fetch_validate_publish(
    href_fn: Callable[[], str],
    out_dir: str,
    filename: str,
    get: Callable = requests.get,
    max_tries: int = 5,
    sleep: Callable = time.sleep,
    validate: bool = True,
    bbox=None,
    bbox_crs=None,
    max_mpp=None,
) -> Optional[str]:
    """Download -> optionally read-validate -> publish to out_dir, with retries.

    href_fn() is called each attempt so the href is (re-)signed (signed URLs expire).

    bbox=(minx, miny, maxx, maxy): when provided, use a windowed read via rasterio
        (/vsicurl range-reads for https; any local path) clipped to the AOI.
        A successful windowed write replaces the validate step. bbox_crs declares the
        bbox CRS (None = same as dataset CRS). Raises inside the retry loop if the
        bbox does not overlap the asset (all attempts fail -> returns None).

    max_mpp: maximum pixel size (in source-CRS units) for windowed reads. When set and
        coarser than the source native pixel size, the read is DECIMATED so the output
        pixel size is approximately max_mpp. Bounds UDF memory on Serverless (1 GB cap)
        for high-resolution sources such as NAIP (0.6 m, UTM). Ignored when bbox is
        None (byte-faithful download path). See windowed_download for full semantics.

    validate=True (default):
        Publish only if rasterio can open and decode a window of the file.
        On validation failure (corrupt/throttled body), back off and retry.

    validate=False:
        Download bytes -> publish atomically without rasterio decode.
        Still raise_for_status() so HTTP throttle/expiry triggers a retry.
        is_out_file_valid reflects "downloaded + published OK", not decode success.

    Idempotency (I3): if the target Volume path already exists and passes the
    validity check (read_validate when validate=True, exists-above-size-floor when
    validate=False), return it immediately WITHOUT re-downloading.

    I5: filename is sanitized via os.path.basename to prevent path-traversal.
    """
    safe_filename = _sanitize_filename(filename)
    outpath = os.path.join(out_dir, safe_filename)
    os.makedirs(out_dir, exist_ok=True)

    # I3 — idempotency short-circuit
    if os.path.exists(outpath):
        if validate:
            if read_validate(outpath):
                return outpath
        else:
            if os.path.getsize(outpath) >= _EXIST_FLOOR_BYTES:
                return outpath

    _last_exc: Optional[BaseException] = None
    for attempt in range(max_tries):
        tmpd = tempfile.mkdtemp(prefix="gbx_stac_dl_")
        try:
            local = os.path.join(tmpd, safe_filename)
            if bbox is not None:
                # Windowed read decodes-on-read; a successful write IS the validation,
                # so publish directly (skip the separate read_validate window-decode).
                windowed_download(href_fn(), local, bbox, bbox_crs, max_mpp=max_mpp)
                shutil.copyfile(local, outpath)
                return outpath
            download_href(href_fn(), local, get=get)
            if validate:
                if read_validate(local):
                    shutil.copyfile(local, outpath)  # publish only validated files
                    return outpath
            else:
                # I1 — no-validate path: publish without rasterio decode
                shutil.copyfile(local, outpath)
                return outpath
        except Exception as exc:
            _last_exc = exc
        finally:
            shutil.rmtree(tmpd, ignore_errors=True)
        if attempt < max_tries - 1:
            sleep(min(60, 4 * (2**attempt)))

    _log.warning(
        "fetch_validate_publish: all %d attempts exhausted for %r; last error: %r",
        max_tries,
        filename,
        _last_exc,
    )
    try:
        if os.path.exists(outpath):
            os.remove(outpath)
    except OSError:
        pass
    return None
