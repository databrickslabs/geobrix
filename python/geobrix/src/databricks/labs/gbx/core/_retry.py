"""Retry for the transient errors of UC Volume FUSE mounts.

Light by design: no PySpark, rasterio or DataSource registration, so the Zarr store
and the readers can share it. ``ds._listing`` re-exports ``_retry_transient`` for the
callers that already import it from there.
"""

from __future__ import annotations

import errno
import os
import time
from time import (
    monotonic as _monotonic,  # held apart: tests replace this module's `time` to skip the sleeps
)
from typing import Callable, Dict, Optional, TypeVar

_T = TypeVar("_T")

# ---------------------------------------------------------------------------
# Transient-retry helper for UC Volume FUSE eventual-consistency
# ---------------------------------------------------------------------------
# UC Volume FUSE mounts can raise FileNotFoundError or OSError transiently
# even when the file is healthy and readable — especially soon after a write
# (eventual-consistency propagation lag). Mosaic worked around this with up to
# ~10 retries. We use the same bound.
#
# Usage:
#   result = _retry_transient(lambda: open(path, "rb"))
#   size   = _retry_transient(lambda: os.stat(path).st_size)
#
# A path whose retries ran out on a missing file is recorded for MISSING_HOLD_S: the next retry loop for that path (a
# read retries in several layers: the open probe, the staging copy, the read) tries it once. A missing path then pays
# one budget, not one per layer (2026-11-12: 45 s per task for a virtual tile's missing /Volumes file).
MISSING_HOLD_S = 60.0
_MISSING: Dict[str, float] = (
    {}
)  # path -> _monotonic() when its retries ran out on a missing file


def _key(path) -> Optional[str]:
    return (
        os.path.normpath(os.fspath(path))
        if isinstance(path, (str, os.PathLike)) and os.fspath(path)
        else None
    )


def _is_missing(exc: OSError) -> bool:
    """A missing file: FileNotFoundError, ENOENT, or GDAL's "No such file or directory" (rasterio sets no errno)."""
    return (
        isinstance(exc, FileNotFoundError)
        or exc.errno == errno.ENOENT
        or "No such file or directory" in str(exc)
    )


def _recently_missing(key: Optional[str]) -> bool:
    at = _MISSING.get(key) if key else None
    return at is not None and _monotonic() - at < MISSING_HOLD_S


def _retry_transient(
    fn: Callable[[], _T],
    attempts: int = 10,
    backoff: float = 0.5,
    path: Optional[str] = None,
) -> _T:
    """Call *fn()*, retrying on FileNotFoundError / OSError up to *attempts* times.

    Each retry waits ``backoff * attempt`` seconds (linear backoff; the defaults wait up to 22.5 s in all).  After
    all attempts are exhausted the last exception is re-raised.

    Only retries :class:`FileNotFoundError` and :class:`OSError` (the FUSE
    transient-miss family).  Programming errors (TypeError, ValueError, …) are
    propagated immediately.

    A path that a retry loop found missing to its last attempt is tried once by the next loops for it, for
    ``MISSING_HOLD_S`` (see ``_MISSING``). The path is *path*, else the ``filename`` of the error (``open`` and
    ``os.stat`` set it; a GDAL open does not, so its caller passes *path*).

    Args:
        fn:       Zero-argument callable to call and return the result of.
        attempts: Maximum number of tries (default 10; matches Mosaic precedent).
        backoff:  Base sleep multiplier in seconds (default 0.5).
        path:     The file *fn* reads, when its errors do not name it.

    Returns:
        The return value of *fn()* on the first successful call.

    Raises:
        The last :class:`OSError` / :class:`FileNotFoundError` if all attempts fail.
    """
    last_exc: OSError | None = None
    key = _key(path)
    for attempt in range(1, attempts + 1):
        try:
            out = fn()
            if key:
                _MISSING.pop(key, None)
            return out
        except (FileNotFoundError, OSError) as exc:
            last_exc = exc
            key = key or _key(exc.filename)
            if _is_missing(exc) and _recently_missing(key):
                break  # another loop just waited its whole budget for this path
            if attempt < attempts:
                time.sleep(backoff * attempt)
    if key and last_exc is not None and _is_missing(last_exc):
        _MISSING[key] = _monotonic()
    raise last_exc  # type: ignore[misc]
