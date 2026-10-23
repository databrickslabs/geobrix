"""Retry for the transient errors of UC Volume FUSE mounts.

Light by design: no PySpark, rasterio or DataSource registration, so the Zarr store
and the readers can share it. ``ds._listing`` re-exports ``_retry_transient`` for the
callers that already import it from there.
"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

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


def _retry_transient(
    fn: Callable[[], _T], attempts: int = 10, backoff: float = 0.5
) -> _T:
    """Call *fn()*, retrying on FileNotFoundError / OSError up to *attempts* times.

    Each retry waits ``backoff * attempt`` seconds (linear backoff).  After all
    attempts are exhausted the last exception is re-raised.

    Only retries :class:`FileNotFoundError` and :class:`OSError` (the FUSE
    transient-miss family).  Programming errors (TypeError, ValueError, …) are
    propagated immediately.

    Args:
        fn:       Zero-argument callable to call and return the result of.
        attempts: Maximum number of tries (default 10; matches Mosaic precedent).
        backoff:  Base sleep multiplier in seconds (default 0.5 → max ~4.5 s total).

    Returns:
        The return value of *fn()* on the first successful call.

    Raises:
        The last :class:`OSError` / :class:`FileNotFoundError` if all attempts fail.
    """
    last_exc: OSError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (FileNotFoundError, OSError) as exc:
            last_exc = exc
            if attempt < attempts:
                time.sleep(backoff * attempt)
    raise last_exc  # type: ignore[misc]
