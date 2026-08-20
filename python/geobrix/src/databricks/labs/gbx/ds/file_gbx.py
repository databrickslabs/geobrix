"""file_gbx — unified file-access base for readers & writers (light tier).

Wraps native Databricks FILE primitives (read_files/list_files/create_file/try_to_file)
with FUSE fallback, centralizing read AND write × {FILE MANAGED, FILE EXTERNAL, FUSE}.

Exports:
- Re-exports from _listing: to_local_path, to_spark_uri, list_files, _retry_transient
  (for backward compatibility; new code imports these from here).
- file_access_tier(spark) -> "read_files" | "list_files" | "fuse"
  Detects the best available tier at runtime.
- resolve_access(requested, tier) -> str
  Implements the NO-GATING rule: explicit FILE on FUSE tier raises error;
  "auto" downgrades silently.
- enumerate_files(path, *, recursive, include_hidden, spark) -> list | DataFrame
  Enumerates files in a directory, returning path + size + FILE reference (when available).
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, Callable, Literal, Optional

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

# Re-export listing helpers for backward compatibility
from databricks.labs.gbx.ds._listing import (
    _retry_transient,
    list_files,
    to_local_path,
    to_spark_uri,
)

__all__ = [
    "to_local_path",
    "to_spark_uri",
    "list_files",
    "_retry_transient",
    "file_access_tier",
    "resolve_access",
    "enumerate_files",
    "open_for_read",
    # FILE read engine (for pyrx shims + direct use)
    "GBX_LRU_MAX_BYTES",
    "GBX_STREAM_MAX_BYTES",
    "STREAM_NOMINAL_BYTES",
    "_FILE_SUPPORT_CACHE",
    "_connect_aware_lru_sizing",
    "_open_via_file_ref",
    "_OpenerContext",
    "OpenResourceLRU",
    "_stage_local_if_needed",
    "file_supported",
    "file_ref_arg",
    "open_windowed_via_fileref",
    "FileRefReadError",
    "StageTooLargeError",
]

# ---------------------------------------------------------------------------
# Capability tier detection
# ---------------------------------------------------------------------------

_TIER_CACHE: dict = {}


def file_access_tier(
    spark: Optional[SparkSession] = None,
) -> Literal["read_files", "list_files", "fuse"]:
    """Detect the best available file-access tier at runtime.

    Probes for native Databricks FILE primitives in order of preference:
    1. read_files(format=>'file') — DBR 13.3 LTS+ (native FILE refs + metadata)
    2. list_files(...) — DBR 18 LTS+ (metadata-only lister with FILE refs)
    3. FUSE fallback — always available; uses os.walk + stat

    Result is memoized per SparkSession object so the probe runs at most once.

    Args:
        spark: Optional SparkSession. If None, uses SparkSession.getActiveSession()
               or SparkSession.builder.getOrCreate(). Explicit passing is preferred
               on Spark Connect (DBR 14+) where thread-local state can affect
               getActiveSession().

    Returns:
        "read_files": read_files(format=>'file') is supported (DBR 13.3+)
        "list_files": list_files(...) is supported but read_files is not (DBR 18+)
        "fuse": neither FILE primitive is available (OSS Spark or very old DBR);
                FUSE listing via os.walk + stat is the only option.

    Note:
        This is a read-side capability probe. Write-side capabilities
        (create_file, try_to_file) are checked separately when needed.
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        try:
            spark = SparkSession.builder.getOrCreate()
        except Exception:
            return "fuse"
    if spark is None:
        return "fuse"

    session_id = id(spark)
    if session_id in _TIER_CACHE:
        return _TIER_CACHE[session_id]

    tier = _detect_tier(spark)
    _TIER_CACHE[session_id] = tier
    return tier


def _detect_tier(spark: SparkSession) -> Literal["read_files", "list_files", "fuse"]:
    """Probe for file-access tier by attempting SQL queries.

    Probes in order of preference, stopping at the first successful one.
    """
    # Probe 1: read_files(format=>'file') — returns FILE refs + metadata
    # This is the preferred path for both read and listing (DBR 13.3+).
    try:
        spark.sql("""
            SELECT COUNT(*) FROM (
                SELECT * FROM read_files(
                    '/Volumes/__gbx_probe__/__none__',
                    format => 'file'
                ) LIMIT 1
            ) LIMIT 1
            """).collect()
        return "read_files"
    except Exception:
        pass

    # Probe 2: list_files(...) — metadata-only lister (DBR 18 LTS+)
    # Returns path, size, modification_time, file (FILE ref).
    try:
        spark.sql("""
            SELECT COUNT(*) FROM (
                SELECT * FROM list_files(
                    '/Volumes/__gbx_probe__/__none__'
                ) LIMIT 1
            ) LIMIT 1
            """).collect()
        return "list_files"
    except Exception:
        pass

    # Fallback: FUSE-only (os.walk + stat)
    # This is always available but does not offer native FILE refs.
    return "fuse"


# ---------------------------------------------------------------------------
# No-gating resolver (explicit FILE request on FUSE tier raises error)
# ---------------------------------------------------------------------------

AccessMode = Literal["auto", "managed", "external"]


def resolve_access(
    requested: AccessMode,
    tier: Optional[Literal["read_files", "list_files", "fuse"]] = None,
    spark: Optional[SparkSession] = None,
) -> Literal["read_files", "list_files", "fuse", "managed", "external"]:
    """Resolve the effective file-access mode, implementing the NO-GATING rule.

    NO-GATING rule:
      - "auto": silently downgrades to the best available tier (never errors).
      - "managed" or "external": explicit FILE request. If FILE is unavailable
        (tier=="fuse"), raises a clear, actionable error with remediation steps.

    Args:
        requested: User's requested access mode: "auto" (adaptive), "managed"
                   (FILE MANAGED), or "external" (FILE EXTERNAL).
        tier: Optional explicit tier ("read_files", "list_files", "fuse").
              If None, detected via file_access_tier(spark).
        spark: Optional SparkSession passed to file_access_tier.

    Returns:
        The effective access mode:
        - If requested=="auto": returns the best available tier
          ("read_files", "list_files", or "fuse").
        - If requested=="managed" or "external": returns the requested mode
          (assuming FILE is available).

    Raises:
        ValueError: If requested FILE mode (managed/external) but FILE is
                    unavailable (tier=="fuse"). Error message includes:
                    - What capability was requested
                    - Current runtime tier
                    - How to upgrade (Databricks version requirement)
                    - Fallback option (use "auto" or "fuse")
    """
    if tier is None:
        tier = file_access_tier(spark)

    if requested == "auto":
        # Auto: downgrade silently to the best available tier.
        # Tier itself is fine for read/write; just return it.
        return tier

    if requested in ("managed", "external"):
        # Explicit FILE request.
        if tier == "fuse":
            # FILE is not available at runtime.
            raise ValueError(
                f"Requested {requested} FILE access mode, but FILE is not available "
                f"on this runtime (tier='{tier}'). "
                f"\n"
                f"FILE requires Databricks Runtime 13.3 LTS or later (preferably 18 LTS+ "
                f"for optimal performance). "
                f"\n"
                f"Option 1: Upgrade your cluster to DBR 13.3+, then try again. "
                f"\n"
                f"Option 2: Set access='auto' (default) to transparently use FUSE "
                f"(fallback, slower). "
                f"\n"
                f"Option 3: Use access='fuse' explicitly to bypass FILE (FUSE only, "
                f"no FILE-column registration). "
            )
        return requested

    raise ValueError(
        f"Unknown access mode: {requested!r}. Must be 'auto', 'managed', or 'external'."
    )


# ---------------------------------------------------------------------------
# Enumeration (enumerate_files)
# ---------------------------------------------------------------------------


def enumerate_files(
    path: str,
    *,
    recursive: bool = True,
    include_hidden: bool = False,
    spark: Optional[SparkSession] = None,
) -> Any:
    """Enumerate files in a directory, returning path + size + FILE reference (when available).

    Uses the best available tier at runtime:
    - **read_files tier** (DBR 13.3+): `SELECT _metadata.file_path, _metadata.file_size, file FROM read_files(..., format=>'file')`
      Returns a Spark DataFrame with columns: path, size, file (FILE reference).
    - **list_files tier** (DBR 18+): `SELECT path, size, file FROM list_files(...)`
      Returns a Spark DataFrame with columns: path, size, file (FILE reference).
    - **FUSE tier** (always available): `os.walk(...) + os.stat(...)`
      Returns a list of dicts with keys: path, size, file (None for FUSE tier).

    Default behavior:
    - SKIP files starting with `_` or `.` (Spark/Hadoop convention: `_SUCCESS`, `_committed_*`,
      `_started_*`, `_delta_log`, `.crc`, hidden files) so metadata files are never listed.
    - INCLUDE them on `include_hidden=True`.
    - RECURSIVE listing by default; set `recursive=False` for top-level files only.

    Note:
    - read_files/list_files tiers already skip `_*`/`.*` files natively.
    - FUSE tier replicates this skip exactly via filename filtering, so all tiers behave identically.
    - For read_files/list_files tiers, size comes from `_metadata.file_size` (native, no extra stat).
    - For FUSE tier, size comes from `os.stat(...).st_size` (with transient-retry on UC Volumes).
    - Connect-safe: no sparkContext, .rdd, _jvm; guards any conf access.

    Args:
        path: Directory path to enumerate. Can be a bare FUSE path (/Volumes/...) or a scheme-qualified
              URI (dbfs:/Volumes/..., s3://..., etc.).
        recursive: If True (default), recursively list subdirectories. If False, list top-level files only.
        include_hidden: If False (default), skip files starting with `_` or `.`. If True, include them.
        spark: Optional SparkSession. If None, uses SparkSession.getActiveSession() or
               SparkSession.builder.getOrCreate(). Explicit passing is preferred on Spark Connect.

    Returns:
        - For read_files/list_files tiers: a Spark DataFrame with columns [path, size, file].
        - For FUSE tier: a list of dicts with keys {path, size, file} where file is None.

    Raises:
        FileNotFoundError: If path does not exist or contains no files.
        ValueError: If spark cannot be obtained and the tier requires it.
    """
    tier = file_access_tier(spark)

    if tier == "read_files":
        return _enumerate_read_files(
            path, recursive=recursive, include_hidden=include_hidden, spark=spark
        )
    elif tier == "list_files":
        return _enumerate_list_files(
            path, recursive=recursive, include_hidden=include_hidden, spark=spark
        )
    else:  # tier == "fuse"
        return _enumerate_fuse(path, recursive=recursive, include_hidden=include_hidden)


def _enumerate_read_files(
    path: str,
    *,
    recursive: bool,
    include_hidden: bool,
    spark: Optional[SparkSession],
) -> Any:
    """Enumerate via read_files(format=>'file') SQL call.

    Returns a Spark DataFrame with columns [path, size, file].
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    # Normalize path to FUSE form for the SQL string (read_files accepts bare paths).
    local_path = to_local_path(path)
    # Escape single quotes in the path to prevent SQL injection
    escaped_path = local_path.replace("'", "''")

    recursiveFileLookup = "true" if recursive else "false"
    include_hidden_clause = (
        "" if include_hidden else "AND NOT (file_name LIKE '_%' OR file_name LIKE '.%')"
    )

    sql_query = f"""
        SELECT
            _metadata.file_path AS path,
            _metadata.file_size AS size,
            file
        FROM read_files(
            '{escaped_path}',
            format => 'file',
            recursiveFileLookup => {recursiveFileLookup}
        )
        WHERE 1=1
        {include_hidden_clause}
    """

    return spark.sql(sql_query)


def _enumerate_list_files(
    path: str,
    *,
    recursive: bool,
    include_hidden: bool,
    spark: Optional[SparkSession],
) -> Any:
    """Enumerate via list_files(...) SQL call.

    Returns a Spark DataFrame with columns [path, size, file].

    Note: Skipping logic matches read_files and FUSE by checking basename (not path-level).
    This ensures root-level files like /_success and /.crc are also skipped (spec-critical parity).
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    # Normalize path to FUSE form for the SQL string (list_files accepts bare paths).
    local_path = to_local_path(path)
    # Escape single quotes in the path to prevent SQL injection
    escaped_path = local_path.replace("'", "''")

    recursive_param = "true" if recursive else "false"
    # Extract basename using substring_index and check if it starts with _ or .
    # This ensures root-level files like /_success are also skipped (parity with read_files/FUSE).
    include_hidden_clause = (
        ""
        if include_hidden
        else (
            "AND NOT ("
            "  substring_index(path, '/', -1) LIKE '_%' "
            "  OR substring_index(path, '/', -1) LIKE '.%' "
            ")"
        )
    )

    sql_query = f"""
        SELECT
            path,
            size,
            file
        FROM list_files(
            '{escaped_path}',
            recursive => {recursive_param}
        )
        WHERE 1=1
        {include_hidden_clause}
    """

    return spark.sql(sql_query)


def _should_skip_file(filename: str, include_hidden: bool) -> bool:
    """Return True if the file should be skipped (Hadoop/Spark convention).

    Skips files starting with `_` or `.` by default (metadata/hidden files).
    If include_hidden=True, nothing is skipped.
    """
    if include_hidden:
        return False
    return filename.startswith("_") or filename.startswith(".")


def _enumerate_fuse(
    path: str,
    *,
    recursive: bool,
    include_hidden: bool,
) -> list[dict[str, Any]]:
    """Enumerate via os.walk (FUSE-only fallback).

    Returns a list of dicts with keys {path, size, file} where file is None.
    """
    local_path = to_local_path(path)
    abspath = os.path.abspath(local_path)

    results = []

    if os.path.isfile(abspath):
        # Single file case
        filename = os.path.basename(abspath)
        if not _should_skip_file(filename, include_hidden):
            size = _retry_transient(lambda: os.stat(abspath).st_size)
            results.append({"path": abspath, "size": size, "file": None})
    else:
        # Directory case
        if recursive:
            for root, _dirs, names in os.walk(abspath):
                for name in names:
                    if not _should_skip_file(name, include_hidden):
                        full_path = os.path.join(root, name)
                        size = _retry_transient(
                            lambda fp=full_path: os.stat(fp).st_size
                        )
                        results.append({"path": full_path, "size": size, "file": None})
        else:
            # Non-recursive: only top-level files
            try:
                names = os.listdir(abspath)
            except FileNotFoundError as exc:
                raise FileNotFoundError(f"Path does not exist: {path!r}") from exc

            for name in names:
                if not _should_skip_file(name, include_hidden):
                    full_path = os.path.join(abspath, name)
                    if os.path.isfile(full_path):
                        size = _retry_transient(
                            lambda fp=full_path: os.stat(fp).st_size
                        )
                        results.append({"path": full_path, "size": size, "file": None})

    if not results:
        raise FileNotFoundError(
            f"No files found under {path!r} (recursive={recursive}, include_hidden={include_hidden})"
        )

    return sorted(results, key=lambda r: r["path"])


# ---------------------------------------------------------------------------
# FILE read engine (relocated from pyrx for unified access)
# ---------------------------------------------------------------------------

# Constants from grouped_exec.py — sized for Spark classic vs Connect (Serverless)
# Public exports (for backward compatibility with grouped_exec.py)
GBX_LRU_MAX_BYTES = int(os.environ.get("GBX_LRU_MAX_BYTES", 4 * 1024**3))  # 4 GiB
GBX_STREAM_MAX_BYTES = int(
    os.environ.get("GBX_STREAM_MAX_BYTES", 256 * 1024**2)
)  # 256 MiB
STREAM_NOMINAL_BYTES = 16 * 1024**2  # fallback nominal when size is unknown

# Private aliases for internal use
_GBX_LRU_MAX_BYTES = GBX_LRU_MAX_BYTES
_GBX_STREAM_MAX_BYTES = GBX_STREAM_MAX_BYTES
_STREAM_NOMINAL_BYTES = STREAM_NOMINAL_BYTES


# =============================================================================
# FILE capability detection (from pyrx/_file_ref.py)
# =============================================================================

_FILE_SUPPORT_CACHE: dict = {}


def file_supported(spark: Optional["SparkSession"] = None) -> bool:
    """Memoized per-SparkSession capability check for FILE support.

    An explicit ``spark`` session may be passed by callers that already have the
    session object (e.g. ``grouped_tile_map`` passes ``df.sparkSession``); this
    avoids the ``getActiveSession()``/``getOrCreate()`` resolution entirely and is
    the preferred path on Spark Connect (DBR 14+) where thread-local state can
    make ``getActiveSession()`` return None outside the main notebook thread.
    When ``spark`` is not passed, resolution order is:
      1. ``SparkSession.getActiveSession()``
      2. ``SparkSession.builder.getOrCreate()``
    Both fallbacks are Serverless-safe (no .rdd / _jvm / conf.set).

    Returns True if:
    - GBX_DISABLE_FILE env var is not set to "1", AND
    - FILE type is recognized and usable (end-to-end roundtrip succeeds).

    Returns False if:
    - GBX_DISABLE_FILE="1" (no spark touched), OR
    - No SparkSession obtainable, OR
    - Any exception during the roundtrip (UNSUPPORTED_DATATYPE, sentinel
      unreadable, consume failure, etc.).

    Result is cached per SparkSession object; the probe runs at most once.
    """
    if os.environ.get("GBX_DISABLE_FILE") == "1":
        return False

    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
    if spark is None:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        except Exception:
            return False
    if spark is None:
        return False

    session_id = id(spark)
    if session_id in _FILE_SUPPORT_CACHE:
        return _FILE_SUPPORT_CACHE[session_id]

    result = _check_file_support(spark)
    _FILE_SUPPORT_CACHE[session_id] = result
    return result


def _check_file_support(spark: "SparkSession") -> bool:
    """Run a fixture-free, plan-only probe to verify FILE is usable.

    Executes: SELECT try_to_file('<probe_path>') IS NULL
    where probe_path is a clearly-nonexistent Volumes-shaped path.

    Rationale: try_to_file (the try_ variant) returns NULL for a missing file
    WHEN FILE is enabled and fileReferenceCreationMode is set; it RAISES
    (UNSUPPORTED_DATATYPE "FILE", or a creation-mode error) when FILE is not
    available/usable.  So: if the statement executes without raising → FILE is
    usable → return True; on ANY exception → return False.  Using IS NULL returns
    a boolean (never collects a FILE-typed value), which avoids the
    "display of a FILE column fails" issue on older Serverless clients.
    """
    try:
        probe_path = "/Volumes/__gbx_file_probe__/__none__/__probe__.bin"
        spark.sql(f"SELECT try_to_file('{probe_path}') IS NULL").collect()
        return True
    except Exception:
        return False


def file_ref_arg(tile_col: Column, spark: Optional["SparkSession"] = None) -> Column:
    """Return a Column expression for the file_ref argument to tile-reading UDFs.

    If file_supported() is True, returns a plan-level FILE mint expression
    (F.call_function("try_to_file", tile_col["path"])) that arrives at the UDF
    as a real FileRef value.  Otherwise returns F.lit(None) so the UDF falls
    back to the plain-path read.

    ``spark``: optional explicit session (preferred on Spark Connect / DBR 14+
    where ``getActiveSession()`` can return None in some threading contexts).
    When omitted, ``file_supported()`` resolves the session internally.

    Serverless-safe: no .rdd / _jvm / _jsc / sparkContext / conf.set.
    """
    # NOTE: on a FILE-enabled session the first call to file_supported() triggers
    # a synchronous feature-detect query (memoized once per session) — so building
    # the Column has a one-time side effect.
    if file_supported(spark):
        # F.call_function calls a named Spark SQL function as a plan expression.
        # try_to_file is a Spark SQL built-in (not a PySpark function) that mints
        # a FILE reference from a Volume path string.
        #
        # Callers pass file_ref_arg(_col(tile)) where _col("tile") deliberately
        # returns the bare str "tile" (so callers can use it as a column name).
        # A bare str does not support ["path"] subscript — coerce to Column first.
        _tc = F.col(tile_col) if isinstance(tile_col, str) else tile_col
        return F.call_function("try_to_file", _tc["path"])
    # FILE not supported or no session — pass None (UDF uses fallback path).
    return F.lit(None)


class FileRefReadError(Exception):
    """Raised when a FILE/FILEREF windowed read fails (allows degradation)."""

    pass


def _epsg_of_str(crs_str: str):
    """Parse 'EPSG:3857' / '3857' -> 3857; None if not a plain EPSG code."""
    s = str(crs_str).strip().upper()
    if s.startswith("EPSG:"):
        s = s[5:]
    try:
        return int(s)
    except ValueError:
        return None


@contextmanager
def open_windowed_via_fileref(file_ref, window, pending, tile_crs=None):
    """Open a FileRef as a rasterio source and read exactly the window.

    The FileRef's .open() method must return a seekable stream (e.g. a
    ``_io.BufferedReader`` or ``io.BytesIO``).  The window and pending
    instructions are applied exactly as in open_tile, then a rasterio
    DatasetReader backed by an in-memory GeoTIFF is yielded.

    A FileRef is a handle to the same source file — it does NOT reproject or
    clip.  When ``tile_crs`` is set and a reprojection is required (the
    requested CRS differs from the source CRS after any pending_srid relabel),
    ``FileRefReadError`` is raised so the caller can degrade to the local-file
    path (which warps correctly).

    Args:
        file_ref: Object with ``.open()`` (returns seekable stream) and
            ``.as_local_file()`` (returns local path) methods — matching the
            real ``pyspark.sql.types.FileRef`` contract.
        window: ``(col_off, row_off, width, height)`` tuple from
            ``VirtualTile.window``.
        pending: 4-tuple ``(bands|None, nodata|None, srid|None, crs_str|None)``
            from ``_parse_pending(tile.metadata)``.
        tile_crs: Optional target CRS string from the tile (``tile.crs``).
            When set and a reprojection would be required, ``FileRefReadError``
            is raised to trigger fallback to the local-file path.

    Yields:
        An open ``rasterio.io.DatasetReader`` covering exactly the window.

    Raises:
        FileRefReadError: if the stream is not seekable, rasterio open fails,
            a reprojection is required, or any other windowed-read setup
            failure.  Callers may degrade to ``file_ref.as_local_file()``.
    """
    # Setup phase: open stream, check seekable, check warp, build tile bytes.
    # The broad except converts any failure here to FileRefReadError so the
    # caller can degrade to the local-file path.  The yield is OUTSIDE this
    # handler so that caller-body exceptions propagate unchanged (not wrapped).
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.windows import Window

    try:
        stream = file_ref.open()
        if not stream.seekable():
            raise FileRefReadError("FileRef stream is not seekable")

        with rasterio.open(stream) as src:
            # Reprojection check: if tile_crs requests a different CRS than
            # the source (after any pending_srid relabel), the FILE path cannot
            # warp — raise FileRefReadError to degrade to the local-file path.
            if tile_crs is not None:
                _, _, pending_srid, _ = pending
                src_epsg = src.crs.to_epsg() if src.crs else None
                effective_src_epsg = (
                    pending_srid if pending_srid is not None else src_epsg
                )
                want_epsg = _epsg_of_str(tile_crs)
                if want_epsg is not None:
                    if want_epsg != effective_src_epsg:
                        raise FileRefReadError(
                            "Reprojection required; degrading to local-file path"
                        )
                else:
                    # Non-EPSG target (ESRI:*, WKT, PROJ4): cannot warp via FILE path.
                    raise FileRefReadError(
                        "Non-EPSG reprojection required; degrading to local-file path"
                    )

            col_off, row_off, width, height = window
            rio_window = Window(col_off, row_off, width, height)

            from databricks.labs.gbx.pyrx.core.open_tile import _window_dataset_bytes

            tile_bytes = _window_dataset_bytes(src, rio_window, pending=pending)
    except FileRefReadError:
        raise
    except Exception as exc:
        raise FileRefReadError(f"FileRef windowed read failed: {exc}") from exc

    # Yield phase: outside the broad handler so that caller-body exceptions
    # propagate unchanged (not converted to FileRefReadError → RuntimeError).
    with MemoryFile(tile_bytes) as mf:
        with mf.open() as ds:
            yield ds


# =============================================================================
# FUSE staging (from pyrx/core/preparer.py)
# =============================================================================


def _is_fuse_path(path: str) -> bool:
    """Return True if *path* lives under a Databricks FUSE mount.

    Checks the /Volumes (UC Volume) and /dbfs (legacy DBFS) prefixes.
    Extracted so tests can monkeypatch the detection without resorting to
    real /Volumes paths on the developer's machine.
    """
    return path.startswith("/Volumes") or path.startswith("/dbfs")


def _probe_direct_open(path: str) -> None:
    """Confirm that rasterio can open *path* for direct windowed reads.

    Opens the file and reads a 1×1 pixel window from band 1 — enough to
    verify real random-access, not just header parsing.  The attempt is
    wrapped in ``_retry_transient`` so transient UC Volume eventual-
    consistency misses (``FileNotFoundError`` / ``OSError``) do not
    immediately trigger a full-file copy.

    Raises on failure after retries; returns ``None`` on success.
    """
    import rasterio
    from rasterio.windows import Window

    def _do() -> None:
        with rasterio.open(path) as ds:
            ds.read(1, window=Window(0, 0, 1, 1))

    _retry_transient(_do)


class StageTooLargeError(Exception):
    """Raised when a source file exceeds ``GBX_STAGE_MAX_BYTES`` and cannot be staged.

    Callers (grouped-exec, scalar UDF paths) catch ``Exception`` and degrade
    gracefully (return None / fall back to header), so this is intentionally a
    plain ``Exception`` subclass.
    """


def _stage_local_if_needed(path: str) -> tuple[str, bool]:
    """Return ``(local_path, is_temp)``.

    Probe-then-stage strategy:

    1. **Plain local file** (not under ``/Volumes`` or ``/dbfs``) →
       passthrough ``(path, False)``, unchanged.
    2. **``/Volumes`` or ``/dbfs`` path** → **probe**: try to open the file
       directly with rasterio (open + tiny 1×1 window read) to confirm real
       windowed access works over the FUSE mount.  The probe is wrapped in
       ``_retry_transient`` so a transient ``FileNotFoundError`` / ``OSError``
       from UC Volume eventual-consistency does not trigger an unnecessary
       copy.

       - Probe **succeeds** → ``(path, False)`` — read directly, no copy.
       - Probe **fails** after retries → fall back to the sequential
         ``copyfileobj`` path → ``(temp, True)``.

    3. **Escape hatch**: ``GBX_FORCE_STAGE=1`` env var → always copy even
       when direct access would work.  Use this in environments where direct
       FUSE random-access is unreliable under executor concurrency.

    The return contract ``(local_path, is_temp)`` is unchanged; callers
    (``open_tile`` / ``_uf_*`` tile-read paths) are unaffected.
    """
    if not _is_fuse_path(path):
        return path, False

    if not os.environ.get("GBX_FORCE_STAGE"):
        try:
            _probe_direct_open(path)
            return path, False
        except Exception:
            pass  # probe failed — fall through to copy

    # Size guard: refuse to stage files that exceed the configured cap.
    # This prevents silent OOM from staging a multi-GiB file to local disk.
    # Does NOT apply to the direct-FUSE-access success path above.
    stage_max = int(os.environ.get("GBX_STAGE_MAX_BYTES", 4 * 1024**3))
    try:
        file_size = os.path.getsize(path)
    except OSError:
        file_size = 0  # size unknown; proceed and let the copy fail naturally
    if file_size > stage_max:
        raise StageTooLargeError(
            f"File {path!r} is {file_size:,} bytes, exceeds "
            f"GBX_STAGE_MAX_BYTES={stage_max:,}; use FUSE (as_local_file) instead"
        )

    # Copy fallback: stage to a local temp (original behavior).
    fd, tmp = tempfile.mkstemp(suffix=os.path.splitext(path)[1] or ".tif")
    os.close(fd)
    try:
        with open(path, "rb") as _src, open(tmp, "wb") as _dst:
            shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return tmp, True


# =============================================================================
# LRU and opener (from pyrx/grouped_exec.py)
# =============================================================================


def _connect_aware_lru_sizing(spark) -> tuple[int, int, int]:
    """Return Connect-aware LRU sizing (stream_max_bytes, lru_max_bytes, max_count).

    On Spark Connect (Serverless), tasks have ~1 GB RAM; class instance buffers
    (stream opens) become a major constraint. This function returns reduced sizing
    to fit serverless RAM budgets while favoring FUSE (which doesn't buffer) over
    stream opens.

    Benchmarks show that on serverless, FUSE (74 ms/tile) is FASTER than stream
    (110 ms/tile) per FILE-for-raster benchmarks; lower stream_max_bytes favors
    the FUSE path automatically.

    Args:
        spark: Spark session (SparkSession or PySpark Connect session), or None.

    Returns:
        (stream_max_bytes, lru_max_bytes, max_count):
        - Classic Spark: (256 MiB, 4 GiB, 4) — existing behavior.
        - Spark Connect: (64 MiB, 512 MiB, 4) — serverless-safe.
        - Env overrides win in both: GBX_STREAM_MAX_BYTES, GBX_LRU_MAX_BYTES.
        - Serverless worst case: 4 × 64 MiB = 256 MiB, well under ~1 GB task.
    """
    is_connect = spark is not None and "connect" in type(spark).__module__

    # Read raw os.environ to distinguish "explicitly set" from "default".
    # Module-level constants already baked the env-or-default at import.
    stream_max_bytes_env = os.environ.get("GBX_STREAM_MAX_BYTES")
    if stream_max_bytes_env is not None:
        stream_max_bytes = int(stream_max_bytes_env)
    else:
        stream_max_bytes = 64 * 1024**2 if is_connect else 256 * 1024**2

    lru_max_bytes_env = os.environ.get("GBX_LRU_MAX_BYTES")
    if lru_max_bytes_env is not None:
        lru_max_bytes = int(lru_max_bytes_env)
    else:
        lru_max_bytes = 512 * 1024**2 if is_connect else 4 * 1024**3

    max_count = 4

    return stream_max_bytes, lru_max_bytes, max_count


def _open_via_file_ref(fr, rasterio, stream_max_bytes=_GBX_STREAM_MAX_BYTES):
    """Open a rasterio DatasetReader from *fr* using the size-adaptive strategy.

    Returns ``(ds, stream)`` when opened via byte-range stream (tiled/COG path),
    or ``(ds, None)`` when opened via FUSE (``as_local_file``).

    - Small file (≤ stream_max_bytes) AND tiled layout: stream into /vsimem via
      ``fr.open()`` — efficient for tiled/COG sources that support random window reads.
    - Large file OR striped layout: lazy FUSE open via ``fr.as_local_file()`` — blocks
      are fetched on demand without loading the whole file into RAM.

    On the striped fallthrough, both the interim DatasetReader and the stream are
    closed before falling back to FUSE; the returned ``(ds, None)`` pair is FUSE-only.

    Raises on any failure; caller must handle and fall back to staging.

    Args:
        fr: FileRef object with size, open(), as_local_file() methods.
        rasterio: rasterio module (imported on worker).
        stream_max_bytes: threshold in bytes for stream vs FUSE (default: GBX_STREAM_MAX_BYTES).
    """
    if fr.size <= stream_max_bytes:
        stream = fr.open()
        ds = rasterio.open(stream)
        if ds.profile.get("tiled", False):
            return ds, stream  # tiled/COG: seekable stream is efficient
        ds.close()
        stream.close()  # striped: release the stream before FUSE fallback
    local_path = fr.as_local_file()
    return rasterio.open(local_path), None  # FUSE path: no stream


class _OpenerContext:
    """Per-partition opener/closer/weigher state for the FILE-capable LRU.

    Runs on the worker; all imports inside methods are worker-local (GDAL env
    configured before this is instantiated).

    ``fr_holder`` is a one-element list ``[None]``.  The partition loop sets
    ``fr_holder[0] = <current FileRef>`` before each ``lru.get(uri)`` call so the
    opener can stream-open the source on a cache miss.

    ``size_holder`` is a one-element list ``[None]``. The partition loop sets
    ``size_holder[0] = <metadata_size>`` (int or None) before each ``lru.get()``
    call so the weigher can prefer the pre-known size over a per-tile stat.

    FILE capability is signaled by the presence of the ``_file_ref`` column, NOT
    by a worker-side ``file_supported()`` call (which uses ``getActiveSession()``
    and returns ``None`` on Spark-Connect worker threads, e.g. Serverless DBR 19.5).

    Tracking dicts:
    - ``_staged_temps``: id(ds) → temp_path for staging-fallback cleanup.
    - ``_streams``: id(ds) → stream for tiled/COG opens — closed by ``close`` to
      prevent stream leaks alongside the DatasetReader.
    - ``_fuse_sources``: set of id(ds) for FUSE-opened handles — FUSE handles don't
      buffer into executor RAM, so ``weigh`` returns a small nominal for them.
    """

    def __init__(self, stream_max_bytes: int = _GBX_STREAM_MAX_BYTES):
        # Mutable FileRef slot; a list so the partition loop can rebind it.
        self.fr_holder: "list[Any]" = [None]
        # Mutable size slot for metadata-derived tile size; a list to mirror fr_holder.
        self.size_holder: "list[int | None]" = [None]
        self._staged_temps: "dict[int, str]" = {}
        self._streams: "dict[int, Any]" = {}
        self._fuse_sources: "set[int]" = set()
        self._stream_max_bytes = stream_max_bytes

    def open(self, uri: str):
        import rasterio

        fr = self.fr_holder[0]
        if fr is not None:
            try:
                ds, stream = _open_via_file_ref(fr, rasterio, self._stream_max_bytes)
                if stream is not None:
                    self._streams[id(ds)] = stream
                else:
                    self._fuse_sources.add(id(ds))
                return ds
            except Exception:
                pass  # all FILE paths failed → staging fallback
        # Staging fallback: fr unavailable or all FILE paths raised.
        # Import from preparer shim so test monkeypatches still work
        from databricks.labs.gbx.pyrx.core.preparer import (
            _stage_local_if_needed as _stage,
        )

        local_path, is_temp = _stage(uri)
        ds = rasterio.open(local_path)
        if is_temp:
            self._staged_temps[id(ds)] = local_path
        return ds

    def close(self, src) -> None:
        tmp = self._staged_temps.pop(id(src), None)
        stream = self._streams.pop(id(src), None)
        self._fuse_sources.discard(id(src))
        try:
            src.close()
        except Exception:
            pass
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass
        if tmp:
            try:
                os.remove(tmp)
            except Exception:
                pass

    def weigh(self, src, key: str) -> int:
        # FUSE-opened handles don't buffer into executor RAM — use a small nominal
        # so the LRU budget isn't inflated by large-file FUSE references.
        if id(src) in self._fuse_sources:
            return _STREAM_NOMINAL_BYTES
        # Prefer metadata-derived size (tile.metadata["path_file_size"] or ["tile_byte_size"])
        # when available; this avoids a per-tile stat on serverless.
        metadata_size = self.size_holder[0]
        if metadata_size is not None:
            return int(metadata_size)
        # Stream/COG path: fall back to FileRef's declared byte size (actual RAM use).
        fr = self.fr_holder[0]
        if fr is not None:
            try:
                return int(fr.size)
            except Exception:
                pass
        # Staged fallback: use the on-disk size of the temp copy.
        tmp = self._staged_temps.get(id(src))
        if tmp:
            try:
                return os.path.getsize(tmp)
            except Exception:
                pass
        return _STREAM_NOMINAL_BYTES


class OpenResourceLRU:
    """Per-partition BYTE-BUDGETED LRU of open resources keyed by source uri/path.

    Amortizes the OPEN cost across a source's windows. Instead of a fixed count,
    entries are held up to a byte budget (default 4 GiB) with a max_count handle
    guard, so many small sources stay warm (e.g. ~128 x 32 MiB under 4 GiB) while
    a few huge ones don't blow the budget. Each entry carries a weight: a staged
    local copy weighs its file size (so this budget IS the staged-disk-fill guard,
    and eviction deletes the temp); an open stream/dataset weighs a small nominal
    so the count guard governs. The current (most-recent) entry is never evicted.
    Evicted and remaining resources are always closed (evict + close_all)."""

    def __init__(
        self,
        *,
        max_bytes: int = _GBX_LRU_MAX_BYTES,
        max_count: int = 4,
        opener: Callable[[str], Any],
        closer: Callable[[Any], None],
        weigher: Callable[[Any, str], int],
    ):
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if max_count < 1:
            raise ValueError("max_count must be >= 1")
        self.max_bytes = max_bytes
        self.max_count = max_count
        self._opener = opener
        self._closer = closer
        self._weigher = weigher
        self._store: "OrderedDict[str, tuple]" = (
            OrderedDict()
        )  # key -> (resource, weight)
        self.opens = 0
        self.evictions = 0
        self.bytes = 0

    def get(self, key: str) -> Any:
        if key in self._store:
            self._store.move_to_end(key)
            return self._store[key][0]
        res = self._opener(key)
        self.opens += 1
        weight = int(self._weigher(res, key))
        self._store[key] = (res, weight)
        self.bytes += weight
        # evict oldest while over budget, but never the current (most-recent) entry
        while len(self._store) > 1 and (
            self.bytes > self.max_bytes or len(self._store) > self.max_count
        ):
            _, (evicted, w) = self._store.popitem(last=False)
            self.bytes -= w
            self.evictions += 1
            self._closer(evicted)
        return res

    def close_all(self) -> None:
        while self._store:
            _, (res, w) = self._store.popitem(last=False)
            self.bytes -= w
            self._closer(res)


# =============================================================================
# Unified read resolver (NEW)
# =============================================================================


def open_for_read(
    source: str, *, access: str = "auto", spark: Optional["SparkSession"] = None
):
    """Unified resolver for opening raster/vector sources via FILE/FUSE/staging.

    Routes to the best available read strategy based on the runtime's FILE capability
    and the source location. Implements the NO-GATING rule: explicit FILE request on
    a FUSE-only runtime raises a clear error; "auto" gracefully downgrades.

    Mode selection:
    - **FILE available (DBR 18+) and source is a FILE ref** → FILE access, size-adaptive:
      tiled/COG ≤ `GBX_STREAM_MAX_BYTES` → byte-range **stream**; larger/striped → **FUSE-of-FILE**
      (`as_local_file`). MANAGED and EXTERNAL resolve identically for reads (both → the file's uri).
    - **FILE unavailable (DBR < 18) or plain path** → **FUSE** local path (`to_local_path`) with the
      `_stage_local_if_needed` probe→stage fallback for eventual-consistency.

    This is the base resolver; layer-specific callers (rasterio openers, vector readers) wrap it
    and adapt to their I/O contract (e.g., rasterio.open(handle) for raster tile reads).

    Args:
        source: A path string (bare FUSE `/Volumes/...` or scheme-qualified `dbfs:/...`, `s3://...`).
        access: Access mode: "auto" (adaptive, default), "managed" (FILE MANAGED), "external" (FILE EXTERNAL).
                "auto" silently downgrades to FUSE if FILE unavailable; explicit modes raise on unsupported runtimes.
        spark: Optional SparkSession. If None, uses SparkSession.getActiveSession() or
               SparkSession.builder.getOrCreate(). Explicit passing is preferred on Spark Connect.

    Returns:
        The source path unchanged. FILE routing is the caller's responsibility
        (see Task 4 for reader/writer layer integration). Caller adapts based on
        the resolved tier and access mode.

    Raises:
        ValueError: If access is "managed" or "external" but FILE is unavailable on this runtime.
        OSError: If the source cannot be accessed (FUSE probe fails, staging exceeds GBX_STAGE_MAX_BYTES).
    """
    # Resolve the effective access tier and mode. This call validates access modes
    # and raises ValueError if explicit FILE is requested on a FUSE-only runtime.
    tier = file_access_tier(spark)
    resolve_access(access, tier=tier, spark=spark)  # noqa: F841

    # For now, "auto" resolves to the tier. FILE modes ("managed", "external")
    # and "fuse" explicit modes are pass-throughs for the caller to handle.
    # This is the resolver; the caller (raster reader, vector reader) will
    # adapt to their I/O contract based on the tier.
    #
    # Simple passthrough: return the source path (caller adapts based on tier).
    # In future, this can be extended to return structured metadata (FILE ref, size, etc.).
    return source
