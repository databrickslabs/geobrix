"""Exploratory COG file-preparation core (Spark-free).

Wraps the streaming ``cog_convert_file`` with output-naming, optional NetCDF
subdataset URIs, skip-if-exists idempotency, and per-call error isolation.
Callable directly from the driver (front-door B) and from a scalar UDF
(front-door A, defined in the throwaway experiment notebook). NON-WIRED: not
registered as a gbx_* function.
"""

from __future__ import annotations

import os
import resource
import shutil
import sys
import tempfile
from typing import Dict, List, Optional, Tuple


def cog_output_name(source_basename: str) -> str:
    """Full source basename + ``.cog`` (extension preserved): x.tiff -> x.tiff.cog."""
    return f"{source_basename}.cog"


def _subdataset_uri(path: str, subdataset: Optional[str]) -> str:
    """Build a NetCDF subdataset URI when a subdataset is named, else the bare path.

    NetCDF is the primary multi-subdataset case in geobrix. HDF/GRIB users pass a
    complete GDAL subdataset URI as ``path`` with ``subdataset=None``.
    """
    if subdataset is None or str(subdataset).strip() == "":
        return path
    return f'NETCDF:"{path}":{subdataset}'


def prepare_cog(
    path: str,
    out_dir: str,
    blocksize: int = 512,
    resampling: str = "AVERAGE",
    compression: str = "DEFLATE",
    subdataset: Optional[str] = None,
    skip_if_exists: bool = True,
    out_name: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    """Prepare ONE master COG from ``path`` into ``out_dir`` as ``<basename>.cog``.

    Returns ``(output_path, status)``:
      * ``("<out>/<name>.cog", "ok")``       — converted successfully
      * ``("<out>/<name>.cog", "skipped")``  — already existed, skip_if_exists=True
      * ``(None, "error:<reason>")``          — convert failed (does NOT raise)

    OOM is uncatchable and will kill the task rather than return "error:".

    ``path`` must be a native filesystem / FUSE path (e.g. ``/Volumes/...``);
    callers holding a scheme-qualified URI (``dbfs:/...``, ``file:/...``) must
    strip it first (see ``ds._listing.to_local_path``).

    ``out_name`` (optional) overrides the basename used for the ``.cog`` output.
    When provided, output is ``<out_dir>/<out_name>.cog``; when ``None``,
    derives from ``os.path.basename(path)`` (existing behavior). Useful for
    callers that stage to a temp path but want the original source name.
    """
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

    base = out_name if out_name is not None else os.path.basename(path)
    name = cog_output_name(base)
    out_path = os.path.join(out_dir, name)

    if skip_if_exists and os.path.exists(out_path):
        return out_path, "skipped"

    src = _subdataset_uri(path, subdataset)
    try:
        os.makedirs(out_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(suffix=".cog")
        os.close(fd)
        try:
            cog_convert_file(
                src, tmp,
                compression=compression,
                blocksize=blocksize,
                overview_resampling=resampling,
            )
            shutil.copyfile(tmp, out_path)  # bytes-only → FUSE-safe
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        return out_path, "ok"
    except Exception as exc:  # noqa: BLE001 — per-row isolation is the contract
        return None, f"error:{type(exc).__name__}: {exc}"[:300]


def _peak_rss_mib() -> float:
    """Process high-water RSS in MiB (darwin reports bytes, linux KiB)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def prepare_cog_measured(
    path: str,
    out_dir: str,
    blocksize: int = 512,
    resampling: str = "AVERAGE",
    compression: str = "DEFLATE",
    subdataset: Optional[str] = None,
    skip_if_exists: bool = True,
) -> Dict[str, object]:
    """prepare_cog + peak-RSS capture, returning a driver-collectable dict.

    Keys: output_path (str|None), peak_rss_mib (float), status (str). This is the
    exact per-row payload the scalar UDF returns; RSS is captured on the DRIVER
    side by collecting this value (worker markers are unreliable on Serverless).
    """
    out_path, status = prepare_cog(
        path, out_dir,
        blocksize=blocksize,
        resampling=resampling,
        compression=compression,
        subdataset=subdataset,
        skip_if_exists=skip_if_exists,
    )
    return {
        "output_path": out_path,
        "peak_rss_mib": _peak_rss_mib(),
        "status": status,
    }


DEFAULT_RASTER_EXTS = (".tif", ".tiff", ".cog", ".nc", ".h5", ".hdf")


def _has_ext(path: str, extensions) -> bool:
    if not extensions:
        return True
    return os.path.splitext(path)[1].lower() in {e.lower() for e in extensions}


def _resolve_sources(
    sources,
    recursive: bool = True,
    extensions=DEFAULT_RASTER_EXTS,
) -> List[Tuple[str, Optional[str]]]:
    """Normalize dir | file | iterable-of-both into a flat deduped [(path, error)].

    error=None for a resolved existing file; error="not-found" for a listed path
    that does not exist. A directory is listed (recursive by default) and
    extension-filtered; an explicitly-named file bypasses the filter. Scheme-
    qualified inputs (dbfs:/..., file:/...) are stripped via ds._listing.to_local_path.
    """
    from databricks.labs.gbx.ds._listing import to_local_path

    # Normalize to a list of items. A lone str/PathLike is one item.
    if isinstance(sources, (str, os.PathLike)):
        items = [sources]
    else:
        items = list(sources)

    out: List[Tuple[str, Optional[str]]] = []
    seen = set()

    def _add(path: str, err: Optional[str]) -> None:
        if path in seen:
            return
        seen.add(path)
        out.append((path, err))

    for item in items:
        local = to_local_path(str(item))
        if os.path.isfile(local):
            _add(local, None)  # explicit file — no extension filter
        elif os.path.isdir(local):
            if recursive:
                for root, _dirs, names in os.walk(local):
                    for name in sorted(names):
                        full = os.path.join(root, name)
                        if _has_ext(full, extensions):
                            _add(full, None)
            else:
                for name in sorted(os.listdir(local)):
                    full = os.path.join(local, name)
                    if os.path.isfile(full) and _has_ext(full, extensions):
                        _add(full, None)
        else:
            _add(local, "not-found")

    return out
