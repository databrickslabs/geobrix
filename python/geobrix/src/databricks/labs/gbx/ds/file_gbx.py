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
"""

from __future__ import annotations

from typing import Literal, Optional

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
    tier: Literal["read_files", "list_files", "fuse"] = None,
    spark: Optional[SparkSession] = None,
) -> Literal["fuse", "managed", "external"]:
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
        return tier  # type: ignore[return-value]

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
