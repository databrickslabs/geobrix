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
from typing import Any, Literal, Optional

from pyspark.sql import SparkSession

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
