"""Recursive file listing with a regex filter (mirrors HadoopUtils.listAllHadoopFiles).

Local-filesystem only — fits FUSE-mounted UC Volumes (/Volumes/...). Returns
sorted absolute paths so partition ordering is deterministic.
"""

from __future__ import annotations

import os
import re
import time
from typing import Callable, List, TypeVar

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


# Schemes Hadoop already understands; leave their qualified form untouched.
_KNOWN_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://?")


def to_spark_uri(path: str) -> str:
    """Scheme-qualify a listed path to the form Hadoop's FileSystem produces on Databricks.

    ``binaryFile`` and the heavy ``gdal``/``gtiff_gdal`` reader both qualify paths
    via the Hadoop FileSystem, so a Volume file comes back as ``dbfs:/Volumes/...``.
    The light reader lists bare FUSE paths (``os.path.abspath`` -> ``/Volumes/...``),
    which then fail to join (0 rows) against a binaryFile/heavy ``path`` column.
    This mirrors the heavy ``HadoopUtils.cleanPath`` mapping but for the OUTPUT
    form (what ``listFiles`` returns), so the light ``source`` column matches.

    The bare path is still what we hand to rasterio for the actual read — only the
    emitted ``source`` column is qualified.

        /Volumes/...        -> dbfs:/Volumes/...   (UC Volumes; the xView case)
        /dbfs/...           -> dbfs:/...           (DBFS FUSE)
        dbfs:/...           -> unchanged
        file:/...           -> unchanged
        <scheme>://...      -> unchanged           (s3, abfss, gs, wasbs, http(s), ...)
        /<other local abs>  -> unchanged           (local dev/test paths not mangled)
        relative/no-slash   -> unchanged
    """
    if path.startswith("/Volumes/"):
        return f"dbfs:{path}"
    if path.startswith("/dbfs/"):
        return "dbfs:/" + path[len("/dbfs/") :]
    if path.startswith("dbfs:/") or path.startswith("file:/"):
        return path
    if _KNOWN_SCHEME.match(path):
        return path
    # Bare local absolute paths and relative paths are left as-is so local
    # dev/test reads (and their joins) are never mangled.
    return path


def to_local_path(path: str) -> str:
    """Strip a Spark/Hadoop scheme back to the bare FUSE path for a native open.

    Inverse of :func:`to_spark_uri`. Columns store the ``dbfs:``-qualified form
    (so light ``source`` columns join against binaryFile / heavy ``gdal``); but
    rasterio / pyogrio / ``os`` / GDAL open a *filesystem* path, and on Databricks
    a UC Volume is FUSE-mounted at ``/Volumes/...`` (no ``dbfs:`` scheme). So every
    light path-CONSUMPTION site strips the scheme right before the native open.

    ``to_local_path(to_spark_uri(p)) == p`` holds exactly for the ``/Volumes``
    case (the operational one). For ``/dbfs`` the round trip lands on the
    equivalent FUSE location but not the identical string, because ``dbfs:/x`` and
    ``/dbfs/x`` denote the same DBFS path (``dbfs:/x`` is FUSE-mounted at
    ``/dbfs/x``). Object-store / remote schemes are GDAL/rasterio-native and are
    left untouched — stripping them would break the open.

        dbfs:/Volumes/...   -> /Volumes/...   (UC Volumes FUSE; the xView case)
        dbfs:/foo           -> /foo           (DBFS FUSE)
        file:/tmp/x         -> /tmp/x         (local filesystem)
        <scheme>://...      -> unchanged      (s3, s3a, abfss, gs, wasbs, http(s), ...)
        /vsi.../...         -> unchanged      (GDAL virtual filesystem, incl. /vsimem/)
        /<other local abs>  -> unchanged      (local dev/test paths not mangled)
        relative/no-slash   -> unchanged
    """
    if path.startswith("dbfs:/"):
        return path[len("dbfs:") :]
    if path.startswith("file:/"):
        return path[len("file:") :]
    # Object-store / remote schemes (s3, abfss, gs, wasbs, http(s), ...) and GDAL
    # /vsi... virtual paths are read natively by GDAL/rasterio — leave them as-is.
    return path


def list_files(path: str, filter_regex: str = ".*") -> List[str]:
    """Return sorted absolute file paths under ``path`` whose full path matches ``filter_regex``."""
    pattern = re.compile(filter_regex)
    # Input may arrive scheme-qualified (a column stores dbfs:/Volumes/...); strip
    # back to the bare FUSE path before any os.* listing call resolves it.
    abspath = os.path.abspath(to_local_path(path))

    if os.path.isfile(abspath):
        candidates = [abspath] if pattern.match(abspath) else []
    else:
        candidates = []
        for root, _dirs, names in os.walk(abspath):
            for name in names:
                full = os.path.join(root, name)
                if pattern.match(full):
                    candidates.append(full)

    if not candidates:
        raise FileNotFoundError(
            f"No files under {path!r} matched filterRegex {filter_regex!r}"
        )
    return sorted(candidates)
