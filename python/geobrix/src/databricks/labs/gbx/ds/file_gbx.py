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

import fnmatch
import os
import re as _re
import shutil
import tempfile
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, Callable, Literal, Optional

from pyspark.sql import Column, DataFrame, SparkSession
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
    "resolve_local_path",
    "open_for_write",
    "ingest_files",
    # FILE read engine (for pyrx shims + direct use)
    "GBX_LRU_MAX_BYTES",
    "GBX_STREAM_MAX_BYTES",
    "STREAM_NOMINAL_BYTES",
    "_COG_DRIVER_MAX_BYTES",
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
    "_accept_basename",
    "list_local_files",
    "_fuse_direct_disabled",
    "_resolve_session_for_cap",
    "materialize_decision",
    "report_detected_cap",
    # FILE-column table helpers (moved from pyrx/file_table.py for DRY shared core)
    "_table_props",
    "_describe_cols",
    # Generic session-ful FILE-column table read
    "resolve_file_table",
    # Generic session-ful read
    "_classify_source",
    "gbx_file_read",
    # Generic session-ful write
    "gbx_file_write",
]

# ===========================================================================
# SESSION-DEPENDENT (driver / function-layer only — a SparkSession is required;
# NEVER call these from inside a session-less DataSource reader/writer):
#   file_access_tier, resolve_access, enumerate_files (FILE tiers),
#   file_supported, file_ref_arg, open_windowed_via_fileref,
#   open_for_write, ingest_files, gbx_file_read, gbx_file_write.
# SESSION-FREE (safe in a session-less DataSource reader/writer):
#   to_local_path, to_spark_uri, _accept_basename, list_local_files,
#   _enumerate_fuse, resolve_local_path (FUSE), _stage_local_if_needed.
# ===========================================================================

# ---------------------------------------------------------------------------
# Capability tier detection
# ---------------------------------------------------------------------------

_TIER_CACHE: dict = {}


def _is_routine_unavailable(exc: Exception) -> bool:
    """Return True if the exception indicates the SQL routine/function does not exist.

    Checks for error families that prove the SQL function itself is unavailable
    (cannot be resolved, not registered, syntax error in the function name, etc).

    Returns True for:
    - UNRESOLVED_ROUTINE, UNRESOLVABLE_ROUTINE
    - PARSE_SYNTAX_ERROR
    - UNSUPPORTED
    - Message patterns: "cannot resolve", "Undefined function", "does not exist" (when
      referring to the routine, not a path), "function ... not found"

    Returns False for all other errors, including PATH_NOT_FOUND, FileNotFoundError,
    IOError, etc. — these prove the function RAN (and the error is a data/runtime issue,
    not a function-unavailability issue).

    Args:
        exc: Exception to inspect.

    Returns:
        True if the exception indicates routine/function unavailability; False otherwise.
    """
    msg = str(exc).upper()

    # Check for error classes/codes that indicate routine unavailability
    routine_unavailable_keywords = (
        "UNRESOLVED_ROUTINE",
        "UNRESOLVABLE_ROUTINE",
        # TVF-specific error class emitted by local/OSS Spark when read_files /
        # list_files are not registered; must be recognised so _detect_tier falls
        # through to FUSE instead of incorrectly declaring "read_files" available.
        "UNRESOLVABLE_TABLE_VALUED_FUNCTION",
        "PARSE_SYNTAX_ERROR",
        "UNSUPPORTED",
        "CANNOT RESOLVE",
        "UNDEFINED FUNCTION",
        "FUNCTION",  # generic catch, combined with other patterns below
    )

    for keyword in routine_unavailable_keywords:
        if keyword in msg:
            # Additional checks to avoid false positives:
            # - "FUNCTION" alone is too broad, but combined with "NOT FOUND" or "DOES NOT EXIST" is specific
            if keyword == "FUNCTION":
                if "NOT FOUND" in msg or "DOES NOT EXIST" in msg:
                    return True
            else:
                return True

    # Final pattern check: "cannot resolve function X" or similar
    if "CANNOT RESOLVE" in msg or "UNDEFINED" in msg:
        return True

    return False


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

    Probes in order of preference:
    1. read_files(format=>'file') — returns FILE refs + metadata (DBR 13.3+)
    2. list_files(...) — metadata-only lister (DBR 18 LTS+)
    3. FUSE fallback (os.walk + stat) — always available

    Key insight: when probing a nonexistent path (like /Volumes/__gbx_probe__/__none__),
    an available function will raise PATH_NOT_FOUND (proving it EXISTS and RAN).
    Only if the function itself is unavailable (UNRESOLVED_ROUTINE, PARSE_SYNTAX_ERROR,
    etc.) do we fall through to the next probe. Any other error (including PATH_NOT_FOUND)
    means that tier IS available.
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
    except Exception as exc:
        # If the error proves the function ran (e.g., PATH_NOT_FOUND), return "read_files".
        # Only continue to next probe if the function itself is unavailable.
        if not _is_routine_unavailable(exc):
            return "read_files"

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
    except Exception as exc:
        # Same logic: if the error proves the function ran, return "list_files".
        if not _is_routine_unavailable(exc):
            return "list_files"

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
    extensions: Optional[tuple[str, ...]] = None,
    path_glob_filter: Optional[str] = None,
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

    Positive selection filter (independent of ``include_hidden``):
    - ``extensions``: case-insensitive suffix tuple, e.g. ``(".tif", ".nc")``.
      Sugar for ``path_glob_filter`` — compiled internally to ``["*.tif", "*.nc"]``.
    - ``path_glob_filter``: fnmatch-style glob applied to each file's basename, e.g.
      ``"*.tif"`` or ``"[!.]*"``.  Useful for patterns that LIKE cannot express.
    - Only one of ``extensions`` / ``path_glob_filter`` may be given; passing both raises
      ``ValueError``.
    - The positive filter is AND-ed with ``include_hidden``: setting
      ``include_hidden=True`` plus ``path_glob_filter="[!.]*"`` includes underscore-named
      files (``_data.tif``) but still excludes dot-named files (``.crc``, ``.DS_Store``).
    - Applied uniformly across all three tiers via :func:`_glob_to_sql_basename_predicate`
      (SQL tiers) and :func:`_fuse_matches_filter` (FUSE tier).

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
        extensions: Optional tuple of case-insensitive file extensions to include, e.g. ``(".tif", ".nc")``.
                    Mutually exclusive with ``path_glob_filter``.
        path_glob_filter: Optional fnmatch-style glob pattern applied to each file's basename, e.g.
                          ``"*.tif"`` or ``"[!.]*"``.  Mutually exclusive with ``extensions``.
        spark: Optional SparkSession. If None, uses SparkSession.getActiveSession() or
               SparkSession.builder.getOrCreate(). Explicit passing is preferred on Spark Connect.

    Returns:
        - For read_files/list_files tiers: a Spark DataFrame with columns [path, size, file].
        - For FUSE tier: a list of dicts with keys {path, size, file} where file is None.

    Raises:
        FileNotFoundError: If path does not exist or contains no files.
        ValueError: If both ``extensions`` and ``path_glob_filter`` are given, or if spark
                    cannot be obtained and the tier requires it.
    """
    # Validate and compile the positive filter (raises ValueError if both given).
    glob_patterns = _build_glob_filter(extensions, path_glob_filter)

    tier = file_access_tier(spark)

    if tier == "read_files":
        return _enumerate_read_files(
            path,
            recursive=recursive,
            include_hidden=include_hidden,
            glob_patterns=glob_patterns,
            spark=spark,
        )
    elif tier == "list_files":
        return _enumerate_list_files(
            path,
            recursive=recursive,
            include_hidden=include_hidden,
            glob_patterns=glob_patterns,
            spark=spark,
        )
    else:  # tier == "fuse"
        return _enumerate_fuse(
            path,
            recursive=recursive,
            include_hidden=include_hidden,
            glob_patterns=glob_patterns,
        )


def _enumerate_read_files(
    path: str,
    *,
    recursive: bool,
    include_hidden: bool,
    glob_patterns: Optional[list[str]],
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
    # Derive basename from the top-level path column for hidden-file and glob predicates.
    # Note: path comes back as dbfs:/Volumes/... form; substring_index extracts the correct
    # basename regardless of the scheme prefix.
    _basename_expr = "substring_index(path, '/', -1)"
    include_hidden_clause = (
        ""
        if include_hidden
        else (
            "AND NOT ("
            f"  startswith({_basename_expr}, '_') "
            f"  OR startswith({_basename_expr}, '.') "
            ")"
        )
    )
    filter_clause = (
        _glob_to_sql_basename_predicate(glob_patterns, _basename_expr)
        if glob_patterns
        else ""
    )

    sql_query = f"""
        SELECT
            regexp_replace(path, '^dbfs:', '') AS path,
            size,
            file
        FROM read_files(
            '{escaped_path}',
            format => 'file',
            recursiveFileLookup => {recursiveFileLookup}
        )
        WHERE 1=1
        {include_hidden_clause}
        {filter_clause}
    """

    return spark.sql(sql_query)


def _enumerate_list_files(
    path: str,
    *,
    recursive: bool,
    include_hidden: bool,
    glob_patterns: Optional[list[str]],
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
    # Extract basename using substring_index for hidden-file and glob predicates.
    # Use startswith() instead of LIKE '_%' — SQL LIKE treats _ as a single-char wildcard,
    # so LIKE '_%' matches EVERY non-empty filename and NOT(...) would exclude all rows.
    _basename_expr = "substring_index(path, '/', -1)"
    include_hidden_clause = (
        ""
        if include_hidden
        else (
            "AND NOT ("
            f"  startswith({_basename_expr}, '_') "
            f"  OR startswith({_basename_expr}, '.') "
            ")"
        )
    )
    filter_clause = (
        _glob_to_sql_basename_predicate(glob_patterns, _basename_expr)
        if glob_patterns
        else ""
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
        {filter_clause}
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


def _accept_basename(
    filename: str,
    *,
    include_hidden: bool,
    glob_patterns: Optional[list[str]],
) -> bool:
    """Single shared accept predicate for every session-free listing path.

    Consolidates the hidden-file skip (:func:`_should_skip_file`) and the positive
    glob filter (:func:`_fuse_matches_filter`).  Returns True iff *filename*
    survives the hidden-skip AND — when *glob_patterns* is given — matches at least
    one pattern (case-insensitive).  ``enumerate_files`` (FUSE tier) and
    ``list_local_files`` both route through this so their behavior is identical.
    """
    if _should_skip_file(filename, include_hidden):
        return False
    if glob_patterns is not None and not _fuse_matches_filter(filename, glob_patterns):
        return False
    return True


# ---------------------------------------------------------------------------
# Positive path filter helpers (Task 8b)
# ---------------------------------------------------------------------------


def _build_glob_filter(
    extensions: Optional[tuple[str, ...]],
    path_glob_filter: Optional[str],
) -> Optional[list[str]]:
    """Compile extensions or path_glob_filter into a list of lowercase glob patterns.

    Returns None when no filter is requested.  Raises ValueError if both params
    are given (they are mutually exclusive).

    - ``extensions`` sugar: ``(".tif", ".nc")`` → ``["*.tif", "*.nc"]``
    - ``path_glob_filter``: used as-is in a single-element list.
    """
    if extensions is not None and path_glob_filter is not None:
        raise ValueError(
            "extensions and path_glob_filter are mutually exclusive; "
            "provide at most one."
        )
    if extensions is not None:
        return [f"*{ext.lower()}" for ext in extensions]
    if path_glob_filter is not None:
        return [path_glob_filter]
    return None


def _fuse_matches_filter(basename: str, patterns: list[str]) -> bool:
    """Return True if *basename* matches any glob pattern (case-insensitive)."""
    lb = basename.lower()
    return any(fnmatch.fnmatch(lb, p.lower()) for p in patterns)


def _glob_to_sql_regex(pat: str) -> str:
    """Convert a glob pattern containing ``[...]`` / ``?`` to an anchored SQL RLIKE regex.

    Handles:
    - ``[!...]``  → ``[^...]``  (negated character class)
    - ``[...]``   → ``[...]``   (character class, kept as-is)
    - ``*``       → ``.*``
    - ``?``       → ``.``
    - Other chars → ``re.escape``d for literal match

    Returns a case-insensitive, anchored regex: ``(?i)^...$``.

    Note: the caller must escape the result for embedding in a SQL single-quoted
    literal (double backslashes via ``replace("\\", "\\\\")``).
    """
    parts = ["(?i)^"]
    i = 0
    n = len(pat)
    while i < n:
        c = pat[i]
        if c == "*":
            parts.append(".*")
            i += 1
        elif c == "?":
            parts.append(".")
            i += 1
        elif c == "[":
            end = pat.find("]", i + 1)
            if end == -1:
                # Malformed character class — treat [ as literal
                parts.append(_re.escape("["))
                i += 1
            else:
                inner = pat[i + 1 : end]
                if inner.startswith("!"):
                    parts.append("[^" + inner[1:] + "]")
                else:
                    parts.append("[" + inner + "]")
                i = end + 1
        else:
            parts.append(_re.escape(c))
            i += 1
    parts.append("$")
    return "".join(parts)


def _glob_to_sql_basename_predicate(patterns: list[str], basename_expr: str) -> str:
    """Convert a list of glob patterns to a SQL ``AND (...)`` clause for *basename_expr*.

    Strategy:
    - Patterns that contain ``[``, ``?``, ``_``, or ``%`` use *basename_expr*
      ``RLIKE '...'`` via :func:`_glob_to_sql_regex`.  This avoids the LIKE
      wildcard semantics of ``_`` (single char) and ``%`` (multi-char), which would
      diverge from ``fnmatch``'s literal treatment of those characters.
      Backslashes in the generated regex are doubled for correct SQL string-literal
      embedding.
    - All other patterns (typically the ``*.ext`` form produced by ``extensions``)
      use ``LOWER(basename_expr) LIKE '...'`` — no backslash escaping needed for
      these simple suffix patterns.

    Multiple patterns are OR-ed.  Returns an empty string when *patterns* is empty.

    The single-quote escape (``replace("'", "''")``) is applied to all embedded
    literals consistent with the project-wide SQL escaping convention.
    """
    if not patterns:
        return ""
    parts: list[str] = []
    for pat in patterns:
        if "[" in pat or "?" in pat or "_" in pat or "%" in pat:
            sql_regex = _glob_to_sql_regex(pat)
            # Escape single quotes first, then double backslashes for SQL string literal.
            escaped = sql_regex.replace("'", "''").replace("\\", "\\\\")
            parts.append(f"{basename_expr} RLIKE '{escaped}'")
        else:
            # Simple glob (only * wildcard): convert * → %, safe for LIKE.
            sql_pat = pat.replace("'", "''").replace("*", "%")
            parts.append(f"LOWER({basename_expr}) LIKE '{sql_pat.lower()}'")
    clause = " OR ".join(parts)
    return f"AND ({clause})"


def _enumerate_fuse(
    path: str,
    *,
    recursive: bool,
    include_hidden: bool,
    glob_patterns: Optional[list[str]] = None,
    need_size: bool = True,
) -> list[dict[str, Any]]:
    """Enumerate via os.scandir (FUSE-only fallback).

    Returns a list of dicts with keys {path, size, file} where file is None.

    When *need_size* is ``False`` (used by :func:`list_local_files`, which
    discards size), no ``stat`` syscall is issued for any individual file.
    Over a FUSE Volume mount with 10,000 files this eliminates ~165 s of
    per-file overhead, reducing listing to readdir-only time.

    When *need_size* is ``True`` (default, used by :func:`enumerate_files`),
    ``entry.stat().st_size`` is called per accepted file, wrapped in
    :func:`_retry_transient` for FUSE eventual-consistency tolerance.

    ``os.scandir`` is used in place of ``os.walk`` / ``os.listdir``: the
    ``DirEntry`` objects cache the file-type from the readdir call so
    ``entry.is_file()`` / ``entry.is_dir()`` require no extra syscall.
    This makes even the ``need_size=True`` path cheaper than the old
    ``os.stat``-per-name approach.

    *glob_patterns* (from :func:`_build_glob_filter`) is an optional positive
    selection filter applied after the ``include_hidden`` skip logic.  When
    present, only filenames that match at least one pattern are included
    (case-insensitive via :func:`_fuse_matches_filter`).
    """
    local_path = to_local_path(path)
    abspath = os.path.abspath(local_path)

    def _accept(filename: str) -> bool:
        return _accept_basename(
            filename, include_hidden=include_hidden, glob_patterns=glob_patterns
        )

    def _entry_size(entry: "os.DirEntry[str]") -> Optional[int]:
        """Return size for this entry, or None when need_size is False."""
        if not need_size:
            return None
        return _retry_transient(lambda e=entry: e.stat().st_size)

    results: list[dict[str, Any]] = []

    if os.path.isfile(abspath):
        # Single file — no DirEntry available, fall back to os.stat for size.
        filename = os.path.basename(abspath)
        if _accept(filename):
            if need_size:
                size: Optional[int] = _retry_transient(lambda: os.stat(abspath).st_size)
            else:
                size = None
            results.append({"path": abspath, "size": size, "file": None})
    else:
        # Directory — use scandir so file-type checks (is_file / is_dir) consume
        # no extra syscalls (type is readdir-cached on Linux and macOS).
        def _walk(dir_path: str) -> None:
            try:
                scan_entries = _retry_transient(lambda p=dir_path: list(os.scandir(p)))
            except FileNotFoundError as exc:
                raise FileNotFoundError(f"Path does not exist: {path!r}") from exc
            for entry in scan_entries:
                name = entry.name
                if entry.is_dir(follow_symlinks=False):
                    if not include_hidden and name.startswith((".", "_")):
                        # Prune hidden dirs so we never descend into writer
                        # in-flight containers, Spark markers, etc.
                        continue
                    if recursive:
                        _walk(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    if _accept(name):
                        results.append(
                            {
                                "path": entry.path,
                                "size": _entry_size(entry),
                                "file": None,
                            }
                        )

        _walk(abspath)

    if not results:
        raise FileNotFoundError(
            f"No files found under {path!r} "
            f"(recursive={recursive}, include_hidden={include_hidden}, "
            f"glob_patterns={glob_patterns!r})"
        )

    return sorted(results, key=lambda r: r["path"])


def list_local_files(
    path: str,
    *,
    recursive: bool = True,
    include_hidden: bool = False,
    extensions: Optional[tuple[str, ...]] = None,
    path_glob_filter: Optional[str] = None,
) -> list[str]:
    """Session-free: return sorted local (FUSE) file paths under *path*.

    The single enumeration routine every DataSource reader consumes — no
    SparkSession, no FILE-tier SQL, safe inside a session-less DataSource on
    Spark Connect.  Applies the same hidden-skip + positive-glob semantics as
    ``enumerate_files`` (FUSE tier) via :func:`_accept_basename`.

    Raises ``FileNotFoundError`` when no files match, ``ValueError`` if both
    ``extensions`` and ``path_glob_filter`` are given.
    """
    glob_patterns = _build_glob_filter(extensions, path_glob_filter)
    rows = _enumerate_fuse(
        path,
        recursive=recursive,
        include_hidden=include_hidden,
        glob_patterns=glob_patterns,
        need_size=False,  # caller discards size; skip all per-file stat syscalls
    )
    return [r["path"] for r in rows]


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

# Driver-side COG-conversion headroom ceiling (used by materialize_decision kind="cog_write").
# Sources up to this size are auto-routed to the driver-side path when they exceed the executor
# cap.  GDAL COG conversion has a FLAT ~2 GiB RSS profile regardless of source size (tested
# up to 10 GiB sources), so the limit is driven by Spark Connect channel-timeout risk
# (~1 GB/min throughput → 10 GiB ≈ 10 min, near the channel cancellation boundary) rather
# than RAM headroom.  Sources beyond this bound are refused with StageTooLargeError; split
# with sizeInMB= or use a classic cluster (no channel timeout).
# Override via GBX_COG_DRIVER_MAX_BYTES environment variable.
_COG_DRIVER_MAX_BYTES = int(
    os.environ.get("GBX_COG_DRIVER_MAX_BYTES", 10 * 1024**3)  # default: 10 GiB
)


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

    Note: Unlike _detect_tier's read_files/list_files probes, try_to_file
    returns NULL for a missing path rather than raising PATH_NOT_FOUND, so the
    probe strategy differs intentionally: bare except Exception catches all errors
    (because any exception means FILE is not available), whereas _detect_tier must
    distinguish between PATH_NOT_FOUND (function works) and UNRESOLVED_ROUTINE
    (function missing).
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
    When omitted, the session is resolved via ``_resolve_session_for_cap()``
    (same mechanism as ``file_supported()``).

    Serverless-safe: no .rdd / _jvm / _jsc / sparkContext / conf.set.

    Size-gated FUSE-direct (DEFAULT):
    For a whole-file virtual tile (``tile.window IS NULL``) on a FUSE-accessible
    path (``/Volumes/...``, ``/dbfs/...``) that is LARGER than the connect-aware
    stream cap, the ``try_to_file`` mint is skipped — the worker opens it lazily
    via FUSE, never materializing the whole file (essential under the ~1 GB
    Serverless per-task cap). SMALL whole-file tiles (<= cap) keep the FileRef
    and STREAM — bulk in-memory read is faster on Serverless (small-file
    FUSE-direct regressed the dir read ~1.6× in benchmarking). The cap is baked
    at plan-build: 64 MiB on Connect/Serverless, 256 MiB classic
    (``GBX_STREAM_MAX_BYTES`` overrides both). WINDOWED tiles
    (``tile.window IS NOT NULL``), remote paths (``s3://``, ``abfss://``,
    ``gs://``), and MANAGED-non-FUSE paths keep the FileRef for the governed
    byte-range stream. A NULL or missing ``path_file_size`` metadata value
    makes the size comparison NULL, which falls through to the try_to_file
    branch (safe stream fallback). FILE governance and lifecycle are unaffected.
    Set ``GBX_DISABLE_FUSE_DIRECT=1`` to force the byte-range stream for every
    read (kill-switch for regression isolation).
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
        path_col = _tc["path"]
        if not _fuse_direct_disabled():
            # Size-gated FUSE-direct (DEFAULT): a WHOLE-FILE virtual tile (window IS
            # NULL) on a FUSE-accessible path (/Volumes, /dbfs) that is LARGER than
            # the connect-aware stream cap skips the try_to_file mint AND the
            # byte-range stream — the worker opens it lazily via FUSE, never
            # materializing the whole file (essential under the ~1 GB Serverless
            # per-task cap). SMALL whole-file tiles (<= cap) keep the FileRef and
            # STREAM (bulk in-memory read + parse) — faster on Serverless, where a
            # small-file FUSE-direct regressed the dir read ~1.6x in benchmarking.
            # WINDOWED tiles (split/striped: window IS NOT NULL), remote paths
            # (s3://, abfss://, gs://), and MANAGED-non-FUSE paths keep the FileRef
            # for the governed byte-range stream. NULL here => the UDF receives
            # file_ref=None and takes open_tile's FUSE local-path branch. FILE
            # governance/lifecycle is unaffected — this only changes HOW a resolved
            # file is opened. Cap = 64 MiB Connect/Serverless, 256 MiB classic
            # (GBX_STREAM_MAX_BYTES override), from _connect_aware_lru_sizing, baked
            # at plan-build. The reader stamps path_file_size into tile metadata; a
            # missing/NULL size falls through to the stream branch. Set
            # GBX_DISABLE_FUSE_DIRECT=1 to force the stream for every read.
            # NOTE: cap source + threshold semantics shared with materialize_decision().
            _sess = spark if spark is not None else _resolve_session_for_cap()
            cap_bytes = _connect_aware_lru_sizing(_sess)[0]
            whole_file = _tc["window"].isNull()
            fuse_path = path_col.startswith("/Volumes") | path_col.startswith("/dbfs")
            large = _tc["metadata"]["path_file_size"].cast("long") > F.lit(cap_bytes)
            return F.when(whole_file & fuse_path & large, F.lit(None)).otherwise(
                F.call_function("try_to_file", path_col)
            )
        return F.call_function("try_to_file", path_col)
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

            # Resolve window=None → full extent, mirroring open_header() semantics.
            if window is None:
                col_off, row_off, width, height = 0, 0, src.width, src.height
            else:
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


def _fuse_direct_disabled() -> bool:
    """Return True when the scoped FUSE-direct fast path is force-disabled.

    Scoped FUSE-direct is the DEFAULT: a whole-file virtual tile resolved to a
    /Volumes (FUSE) path opens ~100x faster via rasterio.open than via the
    FileRef byte-range stream on Serverless (env v6: ~5 ms vs ~525 ms/open).
    Set ``GBX_DISABLE_FUSE_DIRECT=1`` (a kill-switch) to force the FileRef
    stream for every read — e.g. to isolate a regression or run in an
    environment where FUSE-direct misbehaves. Classic behavior is unchanged
    either way (no /Volumes FUSE mount → the predicate never fires).

    Worker-safe: reads os.environ only (no SparkSession).
    """
    return os.environ.get("GBX_DISABLE_FUSE_DIRECT") == "1"


def _resolve_session_for_cap() -> Optional["SparkSession"]:
    """Resolve the active SparkSession for baking the connect-aware stream cap.

    Mirrors the session-resolution in file_supported(): tries getActiveSession()
    first, falls back to getOrCreate(), returns None if both fail.  None is an
    acceptable result — _connect_aware_lru_sizing(None) returns the 256 MiB classic
    cap as a safe fallback (same as the non-Connect branch).

    Worker-safe: no .rdd / _jvm / conf.set.
    """
    sess = SparkSession.getActiveSession()
    if sess is not None:
        return sess
    try:
        return SparkSession.builder.getOrCreate()
    except Exception:
        return None


def materialize_decision(
    size_bytes: Optional[int],
    kind: str,
    spark: Optional["SparkSession"] = None,
    cap_bytes: Optional[int] = None,
) -> str:
    """Single connect-aware decision: is it safe to materialize this many bytes in RAM here?

    Returns:
      "stream" — size_bytes <= cap: safe to hold in memory (bulk read / full materialize).
      "fuse"   — size_bytes  > cap AND kind in {"read","write"}: too big for RAM; caller
                 must open lazily via FUSE (reads) or windowed-stream (writes).
      "driver" — size_bytes  > cap AND kind == "cog_write" AND size_bytes <=
                 _COG_DRIVER_MAX_BYTES: auto-route to the driver-side COG conversion path.
                 GDAL COG conversion has a flat ~2 GiB RSS profile regardless of source
                 size; the driver (not the executor) can safely run it.  Caller must defer
                 to CogGbxWriter's driver-side path (commit() → prepare_cogs), which is
                 invoked automatically — no user opt-in required.
      "error"  — size_bytes  > cap AND kind == "ingest": explicit materialize=True guard —
                 fail fast.  Prevents silent OOM when a user passes rst_fromfile(path,
                 materialize=True) with a multi-hundred-MiB source on Serverless.
               — size_bytes  > _COG_DRIVER_MAX_BYTES AND kind == "cog_write": source
                 exceeds driver headroom; split with sizeInMB= or use a classic cluster.

    kind is one of {"read","write","ingest","cog_write"}:
    - "read" / "write": over-cap → "fuse" (lazy open, no bytes in RAM).
    - "ingest": over-cap → "error" (explicit materialize guard — fail fast).
    - "cog_write": over-cap but <= _COG_DRIVER_MAX_BYTES → "driver" (auto driver-side COG);
                   over _COG_DRIVER_MAX_BYTES → "error" (beyond driver headroom).
    size_bytes is None → "stream" (safe default; unknown size, matches the read gate's
    NULL-size behavior), regardless of kind.

    Cap resolution:
    - When ``cap_bytes`` is provided, it is used DIRECTLY — no session resolution.
      This is the WORKER-SAFE path: write gates run this from inside a session-less
      DataSource ``write(iterator)`` on a Serverless worker, where inspecting a live
      session (``_connect_aware_lru_sizing``) would find none and wrongly fall back
      to the 256 MiB classic cap. The DRIVER captures the connect-aware cap once (in
      the writer's ``__init__`` / at the ``rst_fromfile`` Column-build point) and
      passes it here so the correct 64 MiB Serverless cap travels to the worker.
    - When ``cap_bytes`` is None, the cap is resolved as
      ``_connect_aware_lru_sizing(spark or _resolve_session_for_cap())[0]``
      (64 MiB Connect/Serverless, 256 MiB classic, GBX_STREAM_MAX_BYTES override).
    _COG_DRIVER_MAX_BYTES = 10 GiB default (GBX_COG_DRIVER_MAX_BYTES override).
    Connect-safe: reads os.environ + session type only; no .rdd/_jvm/conf.set.
    """
    if size_bytes is None:
        return "stream"
    if cap_bytes is not None:
        cap = cap_bytes
    else:
        _sess = spark if spark is not None else _resolve_session_for_cap()
        cap = _connect_aware_lru_sizing(_sess)[0]
    if size_bytes <= cap:
        return "stream"
    # Over cap: branch on kind.
    if kind == "ingest":
        return "error"
    if kind == "cog_write":
        if size_bytes <= _COG_DRIVER_MAX_BYTES:
            return "driver"
        return "error"
    return "fuse"


def report_detected_cap(spark: Optional["SparkSession"] = None) -> int:
    """Return the connect-aware stream cap (in bytes) that materialize_decision uses.

    For the Serverless validation to assert the cap is 64 MiB on env v6 (the
    linchpin — a mis-detect silently uses the 256 MiB classic cap → OOM).

    Returns:
        The stream cap in bytes:
        - 64 MiB (67,108,864) on Spark Connect / Serverless.
        - 256 MiB (268,435,456) on classic Spark (no Connect session).
        - ``GBX_STREAM_MAX_BYTES`` override if set (wins in both modes).

    Args:
        spark: Optional SparkSession. If None, resolved via
               ``_resolve_session_for_cap()`` (same as ``materialize_decision``).
    """
    _sess = spark if spark is not None else _resolve_session_for_cap()
    return _connect_aware_lru_sizing(_sess)[0]


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


def resolve_local_path(
    source: str, *, access: str = "auto", spark: Optional["SparkSession"] = None
) -> str:
    """Resolve *source* to a local filesystem path for sequential readers (e.g. pyogrio).

    Unlike ``open_for_read`` (which returns the source unchanged), this returns the
    actual local path suitable for passing to GDAL / pyogrio / ``shutil.copy``.
    Both FUSE and FILE-capable tiers return a FUSE-backed ``/Volumes/...`` path;
    on FILE-capable runtimes (DBR 18+) that path is served via the FILE API for
    efficient sequential I/O.

    Access validation follows the NO-GATING rule (same as ``open_for_read``):

    - ``"auto"``: silently uses the best available tier; never raises.
    - ``"managed"`` / ``"external"``: explicit FILE request; raises ``ValueError``
      if FILE is unavailable on this runtime (tier='fuse').

    Args:
        source: Source path (bare FUSE ``/Volumes/...`` or scheme-qualified).
        access: ``"auto"`` (default), ``"managed"``, or ``"external"``.
        spark: Optional SparkSession; used for tier detection.

    Returns:
        A local FUSE filesystem path suitable for sequential I/O.

    Raises:
        ValueError: If explicit FILE access is requested on a FUSE-only runtime.
    """
    tier = file_access_tier(spark)
    resolve_access(
        access, tier=tier, spark=spark
    )  # raises on explicit FILE + FUSE tier
    return to_local_path(source)


# =============================================================================
# Write committer: open_for_write + ingest_files
# =============================================================================

# Valid layout values — shared between open_for_write and ingest_files.
_VALID_LAYOUTS = frozenset({"order", "cluster", "plain"})


def _validate_layout(layout: str) -> None:
    """Raise ValueError for invalid layout values.

    Rejects any layout not in {"order", "cluster", "plain"}, including any attempt
    to pass a partition-by value or other unsupported strategy.
    """
    if layout not in _VALID_LAYOUTS:
        raise ValueError(
            f"layout must be one of {sorted(_VALID_LAYOUTS)!r}, got {layout!r}. "
            "partition_by and ZORDER are not supported. Use layout='order' (default), "
            "'cluster' (CLUSTER BY + OPTIMIZE), or 'plain' (no ordering)."
        )


def _fuse_select_expr(df_schema) -> str:
    """Build a SQL SELECT expression for flattening a tile-struct schema.

    If the schema has a top-level 'tile' struct, returns
    '`tile`.`f1` AS `f1`, `tile`.`f2` AS `f2`, …' (excluding 'path_mode')
    plus any other non-tile top-level columns (backtick-quoted).
    If the schema is already flat, returns '*'.

    Column names are backtick-escaped so that tile fields whose names collide
    with SQL reserved words (e.g. 'window', 'path') produce valid CTAS SQL.

    This is a pure string operation — no SparkContext required.
    """
    field_names = [f.name for f in df_schema.fields]
    if "tile" not in field_names:
        return "*"

    tile_fields = [
        f.name for f in df_schema["tile"].dataType.fields if f.name != "path_mode"
    ]
    other_fields = [name for name in field_names if name != "tile"]
    parts = [f"`tile`.`{name}` AS `{name}`" for name in tile_fields]
    parts += [f"`{name}`" for name in other_fields]
    return ", ".join(parts)


def open_for_write(
    spark: "SparkSession",
    df: "DataFrame",
    target: str,
    *,
    file_mode: str = "auto",
    filespace: Optional[str] = None,
    layout: str = "order",
    overwrite: bool = False,
    file_col: str = "tile_file",
) -> None:
    """Write df to a Delta table, routing via FILE MANAGED / EXTERNAL / FUSE.

    Mode selection (``file_mode`` parameter):

    - **"auto"** (default):
      - FILE-capable runtime + ``filespace`` provided → ``"managed"``
        (FILE MANAGED via ``create_file``).
      - FILE-capable runtime + no ``filespace`` → ``"external"``
        (FILE EXTERNAL via ``try_to_file``).
      - No-FILE runtime (fuse tier) → ``"fuse"`` (plain Delta write, no FILE column).
    - **"managed"** or **"external"**: explicit FILE mode.  Raises ``ValueError`` if
      FILE is not available on this runtime (tier='fuse'), or if ``"managed"`` is
      requested without a ``filespace``.
    - **"fuse"**: plain Delta write regardless of FILE capability.

    Fuse mode writes the DataFrame as a plain Delta table with tile columns as
    ordinary columns (``path`` → STRING, ``raster`` → BINARY).  No FILE column,
    no ``create_file`` / ``try_to_file``.  The path column is used to order writes
    according to the ``layout`` strategy.

    Fuse + ``layout="cluster"``: CLUSTER BY is only available on FILE-column tables,
    so the fuse path cannot honour it.  Instead, ORDER BY path is applied (same as
    ``layout="order"``) and a ``warnings.warn`` is emitted explaining the downgrade.

    Args:
        spark: SparkSession.
        df: DataFrame to write.  Must have a ``tile`` struct column or be pre-flattened.
        target: Fully-qualified Delta table name (e.g. ``"catalog.schema.table"``).
        file_mode: ``"auto"`` (default), ``"managed"``, ``"external"``, or ``"fuse"``.
        filespace: Required for managed mode — the filespace path (``/Volumes/…``).
        layout: Write strategy: ``"order"`` (ORDER BY path, default), ``"cluster"``
                (CLUSTER BY path + ORDER BY on FILE tables; ORDER BY + warning on fuse),
                or ``"plain"`` (no ordering).  Any other value raises ``ValueError``.
        overwrite: If ``True``, drop and recreate the target table before writing.
        file_col: Name of the FILE-typed column (managed/external modes only).

    Raises:
        ValueError: If ``layout`` is invalid, if explicit ``"managed"`` is requested
                    without a ``filespace``, or if ``"managed"`` / ``"external"`` is
                    requested on a fuse-only runtime.
    """
    import warnings

    from databricks.labs.gbx.pyrx.file_table import write_file_table

    # 1. Validate layout before any side effects.
    _validate_layout(layout)

    # 2. Early guard: explicit managed without filespace fails here, before any
    #    tier detection or side effect.  write_file_table enforces this too, but
    #    the wrapper should fail fast with its own actionable message.
    if file_mode == "managed" and not filespace:
        raise ValueError(
            "file_mode='managed' requires filespace=/Volumes/… to be provided. "
            "Pass filespace='/Volumes/<catalog>/<schema>/<volume>' or use "
            "file_mode='external' (no filespace needed) or file_mode='auto' "
            "(auto-selects based on whether filespace is given)."
        )

    # 3. Resolve effective write mode.
    # Gate on the WRITE-primitive probe (file_supported / create_file / try_to_file)
    # rather than the read-tier probe (file_access_tier / read_files).  If the two
    # probes diverge — read_files present but write primitives absent — an explicit
    # managed/external request would otherwise pass the read-tier gate, route to
    # write_file_table, and surface a raw downstream error instead of the promised
    # actionable ValueError.
    if file_mode == "auto":
        if file_supported(spark):
            effective_mode = "managed" if filespace else "external"
        else:
            effective_mode = "fuse"
    elif file_mode in ("managed", "external"):
        if not file_supported(spark):
            raise ValueError(
                f"Requested {file_mode} FILE access mode, but the FILE write-primitive "
                f"(create_file/try_to_file) is not available on this runtime. "
                f"\n"
                f"FILE MANAGED/EXTERNAL writes require Databricks Runtime 19+ with "
                f"FILE write-primitive support enabled. "
                f"\n"
                f"Option 1: Upgrade your cluster to DBR 19+ with FILE support, then try again. "
                f"\n"
                f"Option 2: Set file_mode='auto' (default) to transparently use FUSE "
                f"(fallback, no FILE column). "
                f"\n"
                f"Option 3: Use file_mode='fuse' explicitly to bypass FILE (FUSE only, "
                f"no FILE-column registration). "
            )
        effective_mode = file_mode
    elif file_mode == "fuse":
        effective_mode = "fuse"
    else:
        raise ValueError(
            f"file_mode must be 'auto', 'managed', 'external', or 'fuse'; got {file_mode!r}"
        )

    # 4. Emit layout notes.
    if layout == "cluster":
        if effective_mode != "fuse":
            # FILE path: CLUSTER BY is written to the DDL; durable clustering needs OPTIMIZE.
            warnings.warn(
                f"layout='cluster' writes CLUSTER BY (path) at table-creation time, but "
                f"durable clustering is only applied when you run: OPTIMIZE {target}",
                stacklevel=2,
            )
        else:
            # Fuse path: CLUSTER BY requires a FILE-column table, so it cannot be applied.
            # Fall back to ORDER BY path (same effect as layout='order') and warn.
            warnings.warn(
                "layout='cluster' is not supported in fuse mode (CLUSTER BY requires a "
                "FILE-column table). Writing with ORDER BY path instead. "
                "Use a FILE-capable runtime (DBR 13.3+) for durable clustering.",
                stacklevel=2,
            )

    # 5. Route to the appropriate writer.
    if effective_mode == "fuse":
        # Fuse path: plain Delta write via CTAS, no FILE column.
        # Uses SQL (via spark.sql) for the same mockability as write_file_table.
        view = f"_gbx_fuse_src_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(view)
        try:
            if overwrite:
                spark.sql(f"DROP TABLE IF EXISTS {target}")
            # Check if 'path' column exists before emitting ORDER BY path.
            # The flattened tile schema includes 'path'; if absent, layout cannot use ORDER BY.
            field_names = [f.name for f in df.schema.fields]
            tile_fields = []
            if "tile" in field_names:
                tile_fields = [f.name for f in df.schema["tile"].dataType.fields]
            has_path = "path" in field_names or "path" in tile_fields
            if layout in ("order", "cluster") and not has_path:
                raise ValueError(
                    f"layout='{layout}' requires a 'path' column for ordering, but the "
                    f"dataframe has no 'path' column. Schema fields: {field_names}. "
                    f"Ensure the dataframe includes a path column, or use layout='plain'."
                )
            order_clause = " ORDER BY path" if layout in ("order", "cluster") else ""
            select_expr = _fuse_select_expr(df.schema)
            spark.sql(
                f"CREATE TABLE {target} USING DELTA AS "
                f"SELECT {select_expr} FROM {view}{order_clause}"
            )
        finally:
            try:
                spark.sql(f"DROP VIEW IF EXISTS {view}")
            except Exception:
                pass
    else:
        # FILE path: delegate to write_file_table (handles create_file / try_to_file).
        write_file_table(
            spark,
            df,
            target,
            file_mode=effective_mode,
            filespace=filespace,
            layout=layout,
            overwrite=overwrite,
            file_col=file_col,
        )


def ingest_files(
    spark: "SparkSession",
    src: str,
    target: str,
    *,
    filespace: str,
    file_col: str = "tile_file",
    layout: str = "order",
    recursive: bool = True,
    overwrite: bool = False,
) -> None:
    """Ingest existing external files into a managed FILE-column Delta table.

    Reads files from ``src`` via ``read_files(src, format=>'file')`` (DBR 13.3+)
    and INSERTs them into a managed FILE-column Delta table at ``target``.

    The managed table is created on first call (``CREATE TABLE IF NOT EXISTS``).
    Schema: ``path STRING, <file_col> FILE MANAGED``.

    Requires a FILE-capable runtime (tier ``"read_files"`` or ``"list_files"``).
    Raises ``ValueError`` on a fuse-only runtime — use ``open_for_write(file_mode='fuse')``
    for a plain Delta write without FILE column registration.

    Args:
        spark: SparkSession.
        src: Source directory path (``/Volumes/…`` or other FUSE-accessible path).
             Single quotes in the path are escaped safely.
        target: Destination managed FILE-column Delta table name.
        filespace: Filespace path (``/Volumes/…``) for the managed table.
        file_col: Name of the FILE-typed column (default ``"tile_file"``).
        layout: Write strategy: ``"order"`` (ORDER BY path, default), ``"cluster"``,
                or ``"plain"``.
        recursive: If ``True`` (default), ingest files recursively under ``src``.
        overwrite: If ``True``, drop and recreate the managed table before ingesting.

    Raises:
        ValueError: If FILE support is unavailable (tier='fuse'), or ``layout`` is
                    invalid.
    """
    from databricks.labs.gbx.pyrx.file_table import build_create_sql

    # 1. Validate layout before any side effects.
    _validate_layout(layout)

    # 2. Require write-primitive capability; raise the actionable error if unavailable.
    # Gate on file_supported (create_file/try_to_file) rather than file_access_tier
    # (read_files/list_files) so the actionable-error contract holds when read_files
    # is present but the write primitives are not.
    if not file_supported(spark):
        raise ValueError(
            "ingest_files requires FILE write-primitive support (create_file/try_to_file), "
            "which is not available on this runtime. "
            "FILE writes require Databricks Runtime 19+ with FILE support enabled. "
            "Upgrade your cluster to DBR 19+ to use ingest_files, or use "
            "open_for_write(file_mode='fuse') for a plain Delta write without a FILE column."
        )

    # 3. Normalize and escape the source path (single quotes → doubled).
    local_src = to_local_path(src)
    escaped_src = local_src.replace("'", "''")
    recursive_param = "true" if recursive else "false"

    # 4. Build CREATE TABLE DDL for the managed FILE-column table.
    #    Schema: path STRING, <file_col> FILE MANAGED.
    create_ddl = build_create_sql(
        target,
        plain_cols=[("path", "string")],
        file_col=file_col,
        file_mode="managed",
        filespace=filespace,
        layout=layout,
    )

    if overwrite:
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        spark.sql(create_ddl)
    else:
        # Idempotent: only create if the table does not already exist.
        create_ifne = create_ddl.replace(
            "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1
        )
        spark.sql(create_ifne)

    # 5. INSERT from read_files into the managed table.
    #    file_col receives the FILE reference returned by read_files(format=>'file').
    order_clause = " ORDER BY path" if layout in ("order", "cluster") else ""
    insert_sql = (
        f"INSERT INTO {target} "
        f"SELECT _metadata.file_path AS path, file AS {file_col} "
        f"FROM read_files('{escaped_src}', format => 'file', "
        f"recursiveFileLookup => {recursive_param})"
        f"{order_clause}"
    )
    spark.sql(insert_sql)


# =============================================================================
# Generic format-agnostic read: gbx_file_read
# =============================================================================


# =============================================================================
# FILE-column table helpers (moved from pyrx/file_table.py — shared core)
# =============================================================================
# These helpers were formerly private to pyrx/file_table.py.  They are now
# canonical here so both the raster reader (pyrx/file_table.py) and future
# format-agnostic consumers (vector table read, gbx_file_read table mode) can
# share them without duplication.  pyrx/file_table.py imports them from here.


def _table_props(spark: SparkSession, table: str) -> dict:
    """Return TBLPROPERTIES for *table* as a plain dict (key → value)."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {r["key"]: r["value"] for r in rows}


def _describe_cols(spark: SparkSession, table: str):
    """Return ``(plain_col_names_set, file_col_name_or_None)`` for *table*.

    FILE-typed columns are excluded from the plain set and reported separately.
    Column discovery via ``DESCRIBE TABLE`` — **Serverless-GC-safe** (never
    touches ``spark.table(...).schema``, which Spark Connect refuses when a
    FILE column is present).

    Partition/metadata header rows (``col_name`` is ``None`` or starts with
    ``#``) are ignored.

    Matches FILE-type variants returned by different DBR versions:
    - bare ``"file"``
    - qualified ``"file managed"``, ``"file external"``, ``"file (managed)"``
    - reversed ``"managed file"``, ``"external file"``
    """
    desc = spark.sql(f"DESCRIBE TABLE {table}").collect()
    plain = set()
    file_col = None
    for r in desc:
        col_name = r["col_name"]
        data_type = (r["data_type"] or "").lower()
        if not col_name or col_name.startswith("#"):
            continue
        is_file_type = (
            data_type == "file"
            or data_type.startswith("file ")
            or data_type.endswith(" file")
        )
        if is_file_type:
            file_col = col_name
        else:
            plain.add(col_name)
    return plain, file_col


def resolve_file_table(
    spark: SparkSession,
    table: str,
    *,
    skip_ordering: bool = False,
) -> DataFrame:
    """Resolve a FILE-column Delta table to a DataFrame with source paths.

    Shared core for FILE-column table reads.  Handles FILE column detection,
    path resolution (MANAGED uri-stripping / EXTERNAL / plain), size discovery,
    and auto-ordering — so every format-specific reader (raster, vector) can
    delegate here rather than duplicating this logic.

    **Columns returned:**

    - ``source`` (STRING): resolved ``/Volumes/...`` path.  For MANAGED FILE
      tables on a FILE-capable runtime, the FILE column's ``.uri`` subfield is
      projected via SQL and the ``dbfs:`` scheme prefix is stripped.  For all
      other cases (EXTERNAL table, un-stamped table, or runtime without FILE
      support) the plain ``path`` column is used as-is.  If the table has no
      ``path`` column, ``source`` is NULL.
    - ``size`` (BIGINT or null): file size from the table where the ``size``
      column is present; null otherwise.
    - ``path_mode`` (STRING): ``"managed"`` or ``"external"`` — derived from
      the table's ``geobrix.file.write_strategy`` TBLPROPERTY.  Un-stamped
      tables default to ``"external"``.
    - All other non-FILE columns from the table (excluding ``path`` and
      ``size``, which are consumed into ``source`` and ``size``).

    **Serverless-GC safety:** column discovery uses ``DESCRIBE TABLE`` (not
    ``spark.table(...).schema``); the FILE column is only referenced in the
    SQL projection when ``file_supported(spark)`` is True *and* the table is
    MANAGED — on Serverless-GC (FILE currently absent) the plain-column branch
    is taken and the FILE column is never touched.  The returned DataFrame is
    lazy; no ``.collect()`` of FILE-typed values occurs.

    **Ordering:** unless ``skip_ordering=True``, the result is sorted by
    ``source`` ascending with NULLs last (``F.col("source").asc_nulls_last()``)
    so that tiles from the same source file land in the same partition, which
    amortizes per-source open costs (the T8 convention for table reads).

    Args:
        spark: Active SparkSession (driver-side; never called from a worker).
        table: Fully-qualified or unqualified Delta table name.
        skip_ordering: If True, suppress the auto-sort and return rows in table
            scan order.  Use when the table is already physically ordered (e.g.
            written with ``layout="order"`` or ``layout="cluster"``) or when
            the caller applies its own ordering downstream.

    Returns:
        A lazy Spark DataFrame with columns ``[source, size, path_mode,
        <passthrough plain cols>]``.
    """
    from databricks.labs.gbx.pyrx import file_props as _fp

    parsed = _fp.parse_props(_table_props(spark, table))
    file_mode = parsed["file_mode"] or "external"

    plain, file_col_name = _describe_cols(spark, table)

    # Determine whether to resolve path via the FILE column's .uri subfield.
    # FILE column is referenced only when file_supported(spark) is True — on
    # Serverless-GC today (FILE absent) we fall through to the plain branch.
    use_managed_uri = (
        file_mode == "managed" and file_col_name is not None and file_supported(spark)
    )

    # Passthrough columns: all plain columns except 'path' and 'size' (which
    # become the 'source' and 'size' output columns respectively).
    passthrough_cols = [c for c in sorted(plain) if c not in ("path", "size")]
    has_size = "size" in plain

    if use_managed_uri:
        # Managed + FILE-capable: project the FILE column to resolve .uri.
        # Exclude 'path' and 'size' from the SELECT (path comes from uri).
        select_cols = passthrough_cols + [file_col_name]
        if not select_cols:
            raise ValueError(f"no columns to project from {table!r}")
        base = spark.sql(f"SELECT {', '.join(select_cols)} FROM {table}")
        source_expr = F.expr(f"regexp_replace({file_col_name}.uri, '^dbfs:', '')")
        size_expr = F.lit(None).cast("bigint")
    else:
        # Plain branch: FILE column never referenced (Serverless-GC-safe).
        path_cols = ["path"] if "path" in plain else []
        sz_cols = ["size"] if has_size else []
        all_cols = path_cols + sz_cols + passthrough_cols
        if not all_cols:
            raise ValueError(f"no plain columns to project from {table!r}")
        base = spark.sql(f"SELECT {', '.join(all_cols)} FROM {table}")
        source_expr = F.col("path") if "path" in plain else F.lit(None).cast("string")
        size_expr = (
            F.col("size").cast("bigint") if has_size else F.lit(None).cast("bigint")
        )

    # Build output: source, size, path_mode + passthrough cols.
    out = base.withColumn("source", source_expr)
    out = out.withColumn("size", size_expr)
    out = out.withColumn("path_mode", F.lit(file_mode))

    # Drop the raw 'path' column (replaced by 'source') and the FILE col (if selected).
    cols_to_drop = []
    if "path" in plain and not use_managed_uri:
        # 'path' was selected in the plain branch; replaced by 'source'.
        cols_to_drop.append("path")
    if use_managed_uri and file_col_name is not None:
        # FILE col was selected in the managed branch; already consumed into 'source'.
        cols_to_drop.append(file_col_name)
    if cols_to_drop:
        out = out.drop(*cols_to_drop)

    # Auto-order by source (nulls last) unless caller opts out.
    if not skip_ordering:
        out = out.orderBy(F.col("source").asc_nulls_last())

    # Return canonical column order: source, size, path_mode, passthrough.
    return out.select("source", "size", "path_mode", *passthrough_cols)


def _classify_source(source: str) -> str:
    """Return "location" for a path/URI, "table" for a qualified table name."""
    if source.startswith("/") or _re.match(r"^[A-Za-z][A-Za-z0-9+.\-]*://?", source):
        return "location"
    return "table"


def gbx_file_read(
    spark: "SparkSession",
    source: str,
    *,
    source_type: str = "auto",
    recursive: bool = True,
    include_hidden: bool = False,
    access: str = "auto",
    extensions: Optional[tuple[str, ...]] = None,
    path_glob_filter: Optional[str] = None,
    skip_ordering: bool = False,
) -> "DataFrame":
    """Generic, format-agnostic, session-ful read → DataFrame of FILE/FUSE references.

    Returns a DataFrame with columns **``[path, size, file]``** — NEVER bytes/content:

    - ``path``: STRING — the ``/Volumes/...`` path.
    - ``size``: BIGINT — file size where available (null for table read-backs).
    - ``file``: a **MANAGED | EXTERNAL FILE** reference when the runtime supports
      FILE (``enumerate_files`` on a FILE-capable tier), else **null** (FUSE floor or
      table read-back where FILE column is not surfaced).

    Two source kinds:

    - **location** (path/Volume): composes :func:`enumerate_files`, which already
      returns ``[path, size, file]`` on FILE-capable tiers and a list of dicts
      ``{path, size, file: None}`` on the FUSE tier.  The FUSE list is normalized
      to a DataFrame via ``spark.createDataFrame``.
    - **table** (FILE-column Delta table): delegates to :func:`resolve_file_table`
      and projects ``source AS path`` (real resolved path), real ``size`` where the
      table has it, and ``file=null`` (no raw FILE ref is surfaced from a table
      read-back).  Rows are auto-ordered by path unless ``skip_ordering=True``.

    ``access`` gating (owner-confirmed, see task brief):

    - ``"auto"`` (default): never raises; returns FILE refs when capable, null ``file``
      on FUSE.
    - ``"external"``: requires a FILE-capable runtime (``read_files`` or ``list_files``
      tier); raises ``ValueError`` on a FUSE-only runtime.
    - ``"managed"``: valid **only** for a table source (MANAGED FILE-column table
      read-back).  Raises ``ValueError`` for any location/path source — a Volume path
      cannot yield a MANAGED reference; that is minted on write via
      :func:`gbx_file_write`.

    ``source_type="auto"`` classifies via :func:`_classify_source`.

    Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
    """
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    # Validate access mode.
    if access not in ("auto", "external", "managed"):
        raise ValueError(
            f"access must be 'auto', 'external', or 'managed'; got {access!r}"
        )

    # Classify source.
    st = _classify_source(source) if source_type == "auto" else source_type
    if st not in ("location", "table"):
        raise ValueError(
            f"source_type must be 'auto', 'location', or 'table'; got {source_type!r}"
        )

    # access="managed" is only valid for a managed FILE-column table source.
    # Raise immediately — no tier detection needed — for a location source.
    if access == "managed" and st == "location":
        raise ValueError(
            "access='managed' is only valid for a MANAGED FILE-column table source. "
            "A Volume path/directory cannot yield a MANAGED FILE reference — that is "
            "minted on write via gbx_file_write. "
            "Use access='external' for FILE EXTERNAL references to existing Volume "
            "files, access='auto' for graceful FILE/FUSE fallback, or point at a "
            "MANAGED FILE-column table."
        )

    # Detect tier for access gating.
    tier = file_access_tier(spark)
    file_capable = tier != "fuse"

    # access="external" requires FILE support.
    if access == "external" and not file_capable:
        raise ValueError(
            f"access='external' requires FILE support (Databricks Runtime 19+ with "
            f"read_files(format=>'file') or list_files), but this runtime only has FUSE "
            f"(tier={tier!r}). "
            f"Upgrade to DBR 19+ for FILE support, or use access='auto' for graceful "
            f"FILE/FUSE fallback."
        )

    # --- table source: delegate to resolve_file_table ---
    if st == "table":
        resolved = resolve_file_table(spark, source, skip_ordering=skip_ordering)
        # Project resolved DataFrame to the [path, size, file] contract:
        #   source → path  (resolved /Volumes/... path from the FILE column or
        #                   plain path column — real path, not a NULL placeholder)
        #   size           real BIGINT size where the table carries it; null otherwise
        #   file = NULL    no raw FILE ref is surfaced from a table read-back; the
        #                  FILE column is consumed internally by resolve_file_table
        return resolved.select(
            F.col("source").alias("path"),
            F.col("size"),
            F.lit(None).cast("string").alias("file"),
        )

    # --- location source: compose enumerate_files ---
    local_path = to_local_path(source)
    result = enumerate_files(
        local_path,
        recursive=recursive,
        include_hidden=include_hidden,
        extensions=extensions,
        path_glob_filter=path_glob_filter,
        spark=spark,
    )

    # enumerate_files returns a DataFrame on FILE tiers, a list of dicts on FUSE.
    if isinstance(result, list):
        # FUSE tier: normalize list of dicts {path, size, file: None} to a DataFrame.
        # 'file' is a nullable StringType holding None on FUSE. Note: FILE-tier
        # DataFrames carry the native FILE type in this column, so a cross-tier
        # union requires explicit schema coercion (the two are not union-compatible
        # as-is) — callers pick one tier's output, they do not concat across tiers.
        fuse_schema = StructType(
            [
                StructField("path", StringType(), nullable=True),
                StructField("size", LongType(), nullable=True),
                StructField("file", StringType(), nullable=True),
            ]
        )
        return spark.createDataFrame(result, schema=fuse_schema)

    # FILE tiers (read_files / list_files): enumerate_files already returns
    # a DataFrame with columns [path, size, file] — return it directly.
    return result


# =============================================================================
# Generic format-agnostic write: gbx_file_write
# =============================================================================


def gbx_file_write(
    df: "DataFrame",
    target: str,
    *,
    file_mode: str = "auto",
    filespace: Optional[str] = None,
    layout: str = "order",
    overwrite: bool = False,
    file_col: str = "tile_file",
    spark: Optional["SparkSession"] = None,
) -> None:
    """Generic, format-agnostic, session-ful write → table or FILE.

    Composes :func:`open_for_write` (MANAGED ``create_file`` / EXTERNAL
    ``try_to_file`` / FUSE plain Delta).  Resolves the session from
    ``df.sparkSession`` when *spark* is not given, so callers hold no session.

    Validates ``layout`` up front (fail before any side effect) and delegates
    everything else — including the ``managed``-without-``filespace`` guard and
    the no-gating actionable error — to ``open_for_write``.

    Connect-safe: ``df.sparkSession`` is a driver-only attribute.
    """
    _validate_layout(layout)
    if spark is None:
        spark = df.sparkSession
    open_for_write(
        spark,
        df,
        target,
        file_mode=file_mode,
        filespace=filespace,
        layout=layout,
        overwrite=overwrite,
        file_col=file_col,
    )
