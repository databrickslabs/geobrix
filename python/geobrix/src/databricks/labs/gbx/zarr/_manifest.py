"""The per-store Delta manifest: schema, properties, snapshots and the CAS commit.

A store's whole state is one Delta table. Rows are the store's keys (groups, arrays,
shards, chunks) plus one ``commit`` row per commit. ``write_id`` is the
compare-and-swap token: a row a commit replaces or deletes is matched on the
``write_id`` it had in the writer's base snapshot, so one MERGE is one store
transaction and a stale expectation aborts all of it (``raise_error`` inside a MERGE
action; confirmed on OSS Delta 4.0.1, DBR 19 and Serverless environment 6).

Everything here goes through SQL and DataFrame calls that work under Spark Connect:
no ``spark.conf.set``, ``sparkContext``, ``_jvm`` or ``.rdd``. Errors from Spark and
Delta are classified by message text, because the exception class differs per runtime
(Py4J on classic, ``pyspark.errors.exceptions.connect`` on Serverless).
"""

from __future__ import annotations

import json
import math
import numbers
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.zarr.errors import (
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
    ZarrVersionUnavailable,
    ZarrWriteConflict,
)

__all__ = [
    "KINDS",
    "MANIFEST_COLUMNS",
    "StoreProps",
    "Snapshot",
    "ConcurrentCommit",
    "create_manifest",
    "read_props",
    "snapshot",
    "read_rows",
    "commit",
    "find_commit_version",
    "changed_since",
    "classify_commit_error",
    "parse_cas_failure",
    "check_tag",
    "retain_seconds",
    "retain_days",
]

KINDS = ("group", "array", "shard", "chunk", "commit", "lock")
STORE_KINDS = ("managed", "registered")
FILE_MODES = ("fuse", "external", "managed")  # resolved: never "auto"
FORMAT_VERSION = "1"

PROP_PREFIX = "geobrix.zarr."
HORIZON_PROP = PROP_PREFIX + "vacuum_horizon"  # written by a vacuum: UTC ISO timestamp

MANIFEST_COLUMNS = (
    "kind",
    "array",
    "key",
    "shard_key",
    "file",
    "offset",
    "length",
    "checksum",
    "origin",
    "identity",
    "write_id",
    "tag",
    "meta",
)
_COMMIT_COLUMNS = MANIFEST_COLUMNS + ("op", "expected_write_id")
_REQUIRED_COLUMNS = ("kind", "write_id", "op", "expected_write_id")
_OPS = ("upsert", "delete")

_INT_COLUMNS = ("offset", "length")
_COMMIT_SCHEMA = pa.schema(
    [(c, pa.int64() if c in _INT_COLUMNS else pa.string()) for c in _COMMIT_COLUMNS]
)
_KEYS_SCHEMA = pa.schema(
    [(c, pa.string()) for c in ("kind", "array", "key", "expected_write_id")]
)

_WRITE_ID_RE = re.compile(r"[0-9a-f]{32}")
_TAG_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+=-]{0,127}")
_RETAIN_RE = re.compile(
    r"\s*(\d+(?:\.\d+)?)\s*(second|minute|hour|day|week)s?\s*", re.IGNORECASE
)
_RETAIN_UNIT_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
}

_UNAVAILABLE_FIX = "pick a later version or tag the version you need with gbx_zarr_tag"
_ROW_TRACKING_FIX = "check that delta.enableRowTracking is true on the manifest table"

# One MERGE is one transaction. A matched row whose write_id is not the expected one
# falls through to the raise_error branch; an expectation of NULL ("must not exist")
# on an existing key does too, because ``t.write_id = NULL`` is never true.
_MERGE_SQL = """
MERGE INTO {table} AS t
USING {view} AS s
ON t.kind = s.kind AND t.array <=> s.array AND t.key <=> s.key AND t.tag IS NULL AND s.tag IS NULL
WHEN MATCHED AND s.op = 'delete' AND t.write_id = s.expected_write_id THEN DELETE
WHEN MATCHED AND s.op = 'upsert' AND t.write_id = s.expected_write_id THEN UPDATE SET
  shard_key = s.shard_key, file = s.file, offset = s.offset, length = s.length, checksum = s.checksum,
  origin = s.origin, identity = s.identity, write_id = s.write_id, meta = s.meta
WHEN MATCHED THEN UPDATE SET
  kind = raise_error(concat('GBX_ZARR_CAS ', s.kind, ' ', coalesce(s.array, ''), ' ', coalesce(s.key, '')))
WHEN NOT MATCHED AND s.op = 'upsert' AND s.expected_write_id IS NULL THEN INSERT
  (kind, array, key, shard_key, file, offset, length, checksum, origin, identity, write_id, tag, meta)
  VALUES (s.kind, s.array, s.key, s.shard_key, s.file, s.offset, s.length, s.checksum, s.origin,
          s.identity, s.write_id, NULL, s.meta)
WHEN NOT MATCHED THEN INSERT (kind, write_id)
  VALUES (raise_error(concat('GBX_ZARR_CAS ', s.kind, ' ', coalesce(s.array, ''), ' ', coalesce(s.key, ''))), s.write_id)
"""

_CHANGED_SQL = """
SELECT s.kind, s.array, s.key, s.expected_write_id, t.write_id AS write_id
FROM {view} AS s
LEFT JOIN {table} AS t
  ON t.kind = s.kind AND t.array <=> s.array AND t.key <=> s.key AND t.tag IS NULL
WHERE t.write_id IS DISTINCT FROM s.expected_write_id
"""


@dataclass(frozen=True)
class StoreProps:
    kind: str  # "managed" | "registered"
    root: str  # managed: data root; registered: base path
    overlay_root: str | None  # registered + writable only
    writable: bool
    file_mode: str  # "fuse" | "external" | "managed" (resolved, never "auto")
    retain: str  # e.g. "7 days"


@dataclass(frozen=True)
class Snapshot:
    table: str
    version: int
    tag: str | None = None  # set when read via version_as_of="tag:<name>"


class ConcurrentCommit(Exception):
    """Delta rejected a commit because a concurrent one touched the same files or rows.

    Retryable: re-read the store and commit again. When the retry finds a changed
    ``write_id`` it fails as a ``ZarrWriteConflict`` instead.
    """


# --- names, literals and small validators -----------------------------------------


def _split_name(name: str) -> list[str]:
    """Split ``catalog.schema.table`` into parts; a part may be backtick-quoted."""
    if not isinstance(name, str) or not name:
        raise ValueError("table name must be a non-empty string")
    parts: list[str] = []
    i, n = 0, len(name)
    while True:
        if i < n and name[i] == "`":
            j, buf = i + 1, []
            while True:
                if j >= n:
                    raise ValueError(f"unterminated backtick in table name {name!r}")
                if name[j] == "`":
                    if j + 1 < n and name[j + 1] == "`":
                        buf.append("`")
                        j += 2
                        continue
                    break
                buf.append(name[j])
                j += 1
            part, i = "".join(buf), j + 1
        else:
            j = i
            while j < n and name[j] != ".":
                if name[j] == "`":
                    raise ValueError(f"stray backtick in table name {name!r}")
                j += 1
            part, i = name[i:j], j
        if not part:
            raise ValueError(f"empty part in table name {name!r}")
        parts.append(part)
        if i == n:
            break
        if name[i] != ".":
            raise ValueError(f"expected '.' after a quoted part in {name!r}")
        i += 1
    if len(parts) > 3:
        raise ValueError(f"table name {name!r} has more than catalog.schema.table")
    return parts


def _q(name: str) -> str:
    """Quote a user-supplied table name: each dotted part in backticks. The only place."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in _split_name(name))


def _lit(value: str) -> str:
    """A Spark SQL string literal (backslash and single-quote escaped)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _check_write_id(write_id) -> str:
    if not isinstance(write_id, str) or not _WRITE_ID_RE.fullmatch(write_id):
        raise ValueError(
            f"write_id must be 32 lowercase hex characters (uuid4().hex), got {write_id!r}"
        )
    return write_id


def check_tag(name) -> str:
    """Validate a tag name: 1-128 characters of ``A-Za-z0-9._:/@+=-``, starting alphanumeric."""
    if not isinstance(name, str) or not _TAG_RE.fullmatch(name):
        raise ValueError(
            f"tag names are 1-128 characters of A-Z a-z 0-9 . _ : / @ + = -, "
            f"starting with a letter or digit; got {name!r}"
        )
    return name


def retain_seconds(retain: str) -> int:
    """Seconds in a retention string such as ``"7 days"``, ``"36 hours"``, ``"2 weeks"``."""
    m = _RETAIN_RE.fullmatch(retain) if isinstance(retain, str) else None
    if m is None:
        raise ValueError(
            f"retain must look like '7 days' (seconds, minutes, hours, days or weeks); "
            f"got {retain!r}"
        )
    return int(round(float(m.group(1)) * _RETAIN_UNIT_SECONDS[m.group(2).lower()]))


def retain_days(retain: str) -> int:
    """``retain`` rounded up to whole days, at least 1 (Delta's retention is in days)."""
    return max(1, math.ceil(retain_seconds(retain) / 86400))


def _check_props(props: StoreProps) -> None:
    if props.kind not in STORE_KINDS:
        raise ValueError(f"kind must be one of {STORE_KINDS}, got {props.kind!r}")
    if props.file_mode not in FILE_MODES:
        raise ValueError(
            f"file_mode must be one of {FILE_MODES} (resolved, never 'auto'), "
            f"got {props.file_mode!r}"
        )
    if props.overlay_root and not (props.kind == "registered" and props.writable):
        raise ValueError("overlay_root is only for a writable registered store")
    retain_seconds(props.retain)


# --- error text -------------------------------------------------------------------

# 'GBX_ZARR_CAS <kind> <array> <key> SQLSTATE: P0001'; array is empty for commit rows
# (two spaces) and key can be empty too. Anchored on a real kind so the MERGE text that
# some errors echo back (``concat('GBX_ZARR_CAS ', s.kind, ...``) never counts.
_CAS_RE = re.compile(r"GBX_ZARR_CAS (" + "|".join(KINDS) + r") (\S*) (\S*)(?=\s|$)")
_CONCURRENT_MARKS = (
    "DELTA_CONCURRENT_APPEND",
    "DELTA_CONCURRENT_DELETE",
    "DELTA_CONCURRENT_TRANSACTION",
    "ROW_LEVEL_CHANGES",
    "ConcurrentAppendException",
    "ConcurrentModificationException",
    "DELTA_METADATA_CHANGED",
    "MetadataChangedException",
)


def _error_text(exc: BaseException) -> str:
    """The message of ``exc`` and its causes, plus any Spark error condition."""
    parts, seen, e = [], set(), exc
    while e is not None and id(e) not in seen and len(parts) < 6:
        seen.add(id(e))
        try:
            parts.append(f"{type(e).__name__}: {e}")
        except Exception:
            parts.append(type(e).__name__)
        for getter in ("getCondition", "getErrorClass"):
            try:
                cond = getattr(e, getter)()
            except Exception:
                cond = None
            if cond:
                parts.append(str(cond))
                break
        e = e.__cause__ or e.__context__
    return "\n".join(parts)


def classify_commit_error(text: str) -> str | None:
    """``"cas"`` for a failed expectation, ``"concurrent"`` for a retryable Delta conflict.

    Decided by message text alone, whatever the exception class: classic raises Py4J or
    captured errors, Spark Connect raises ``pyspark.errors.exceptions.connect.*``, and
    Serverless appends Photon stack text. ``None`` means "not ours": re-raise unchanged.
    """
    if _CAS_RE.search(text):
        return "cas"
    if any(mark in text for mark in _CONCURRENT_MARKS):
        return "concurrent"
    return None


def parse_cas_failure(text: str) -> tuple[str, str, str] | None:
    """``(kind, array, key)`` of the first key a CAS abort names, or ``None``."""
    m = _CAS_RE.search(text)
    return (m.group(1), m.group(2), m.group(3)) if m else None


def _store_key(array, key) -> str:
    return "/".join(p for p in (array, key) if p)


# --- create / properties ----------------------------------------------------------


def create_manifest(spark, table: str, props: StoreProps) -> None:
    """Create the manifest table (fuse file mode) with the store's properties.

    Raises ``ZarrOwnershipError`` when the table already exists.
    """
    _check_props(props)
    days = retain_days(props.retain)
    log_days = max(30, days)
    writable = "true" if props.writable else "false"
    ddl = f"""
CREATE TABLE {_q(table)} (
  kind STRING NOT NULL, array STRING, key STRING, shard_key STRING,
  file STRING, offset BIGINT, length BIGINT, checksum STRING,
  origin STRING, identity STRING, write_id STRING NOT NULL, tag STRING, meta STRING)
USING DELTA
CLUSTER BY (array, shard_key)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableChangeDataFeed' = 'true',
  'delta.logRetentionDuration' = 'interval {log_days} days',
  'delta.deletedFileRetentionDuration' = 'interval {days} days',
  '{PROP_PREFIX}format' = '{FORMAT_VERSION}',
  '{PROP_PREFIX}kind' = {_lit(props.kind)},
  '{PROP_PREFIX}root' = {_lit(props.root)},
  '{PROP_PREFIX}overlay_root' = {_lit(props.overlay_root or "")},
  '{PROP_PREFIX}writable' = '{writable}',
  '{PROP_PREFIX}file_mode' = {_lit(props.file_mode)},
  '{PROP_PREFIX}retain' = {_lit(props.retain)})
"""
    try:
        spark.sql(ddl)
    except Exception as e:
        if "already exists" in _error_text(e).lower() or "ALREADY_EXISTS" in str(e):
            raise ZarrOwnershipError(
                f"{table} already exists",
                fix="choose a new table name, or read/write the existing store",
            ) from e
        raise


def _tblprops(spark, table: str) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {_q(table)}").collect()
    return {r["key"]: r["value"] for r in rows}


def read_props(spark, table: str) -> StoreProps:
    """The store properties recorded on the manifest table."""
    tp = _tblprops(spark, table)
    fmt = tp.get(PROP_PREFIX + "format")
    if fmt is None:
        raise ZarrOwnershipError(
            f"{table} is not a GeoBrix Zarr manifest",
            fix="pass the manifest table of a store made by gbx_zarr_write or gbx_zarr_register",
        )
    if fmt != FORMAT_VERSION:
        raise ZarrUnsupported(
            f"{table} uses manifest format {fmt}, this GeoBrix reads format {FORMAT_VERSION}",
            fix="upgrade GeoBrix to a release that reads this manifest format",
        )
    return StoreProps(
        kind=tp[PROP_PREFIX + "kind"],
        root=tp[PROP_PREFIX + "root"],
        overlay_root=tp.get(PROP_PREFIX + "overlay_root") or None,
        writable=tp[PROP_PREFIX + "writable"] == "true",
        file_mode=tp[PROP_PREFIX + "file_mode"],
        retain=tp[PROP_PREFIX + "retain"],
    )


# --- snapshots --------------------------------------------------------------------


def _latest_version(spark, table: str) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {_q(table)} LIMIT 1").select("version").first()
    return int(row[0])


def _parse_horizon(text: str) -> datetime:
    try:
        dt = datetime.fromisoformat(text.strip())
    except ValueError:
        raise ZarrError(
            f"table property {HORIZON_PROP} is not an ISO timestamp: {text!r}",
            fix=f"remove it with ALTER TABLE ... UNSET TBLPROPERTIES ('{HORIZON_PROP}') "
            "and run gbx_zarr_vacuum again",
        ) from None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(
        timezone.utc
    )


def _effective_horizon(props: dict[str, str]) -> datetime | None:
    """The instant before which older versions are not promised to be readable.

    The later of the vacuum horizon a GeoBrix vacuum recorded and ``now - retain``:
    Delta's own vacuum (predictive optimization on Unity Catalog) removes files that
    left the retain window whether or not a GeoBrix vacuum ever ran.
    """
    found = []
    if props.get(HORIZON_PROP):
        found.append(_parse_horizon(props[HORIZON_PROP]))
    retain = props.get(PROP_PREFIX + "retain")
    if retain:
        try:
            window = timedelta(seconds=retain_seconds(retain))
        except ValueError:
            raise ZarrError(
                f"table property {PROP_PREFIX}retain is not a duration: {retain!r}",
                fix=f"set it with ALTER TABLE ... SET TBLPROPERTIES "
                f"('{PROP_PREFIX}retain' = '7 days')",
            ) from None
        found.append(datetime.now(timezone.utc) - window)
    return max(found) if found else None


def _timestamp_micros(spark, value) -> int:
    """``value`` (a str or datetime) as microseconds since the epoch, parsed by Spark."""
    from pyspark.sql import functions as F

    col = (
        F.lit(value) if isinstance(value, datetime) else F.lit(value).cast("timestamp")
    )
    try:
        parsed = spark.range(1).select(F.unix_micros(col)).first()[0]
    except Exception as e:  # ANSI mode raises on an unparseable string
        raise ValueError(f"timestamp_as_of {value!r} is not a timestamp") from e
    if parsed is None:  # ANSI off: unparseable becomes NULL
        raise ValueError(f"timestamp_as_of {value!r} is not a timestamp")
    return int(parsed)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def snapshot(
    spark, table: str, *, version_as_of=None, timestamp_as_of=None
) -> Snapshot:
    """Pin a version of the manifest.

    With no argument: the latest version. ``version_as_of`` is an int, or
    ``"tag:<name>"`` (the latest version, read through the tag's rows; a tag that does
    not exist raises). ``timestamp_as_of`` (str or datetime) gives the latest version
    committed at or before it.

    A version raises ``ZarrVersionUnavailable`` when it is not in the table history, or
    when it is older than the version that was live at the horizon, the later of the
    ``geobrix.zarr.vacuum_horizon`` property and ``now - retain``: its files may be gone.
    The version live at the horizon, everything after it and every tag stay readable.
    Other versions on the table (OPTIMIZE, property changes) are not GeoBrix commits;
    ``find_commit_version`` and the commit rows identify those.
    """
    if version_as_of is not None and timestamp_as_of is not None:
        raise ValueError("pass version_as_of or timestamp_as_of, not both")
    tag, version = None, None
    if isinstance(version_as_of, str):
        if not version_as_of.startswith("tag:"):
            raise ValueError(
                f"version_as_of must be an int or 'tag:<name>', got {version_as_of!r}"
            )
        tag = check_tag(version_as_of[len("tag:") :])
    elif version_as_of is not None:
        if isinstance(version_as_of, bool) or not isinstance(
            version_as_of, numbers.Integral
        ):
            raise ValueError(
                f"version_as_of must be an int or 'tag:<name>', got {version_as_of!r}"
            )
        version = int(version_as_of)
    if timestamp_as_of is not None and not isinstance(timestamp_as_of, (str, datetime)):
        raise ValueError(
            f"timestamp_as_of must be a str or datetime, got {timestamp_as_of!r}"
        )

    from pyspark.sql import functions as F

    if version is None and timestamp_as_of is None:  # the latest, or a tag read there
        latest = _latest_version(spark, table)
        if tag is None:
            return Snapshot(table=table, version=latest)
        present = (
            spark.sql(f"SELECT tag FROM {_q(table)} VERSION AS OF {latest}")
            .where(F.col("tag") == F.lit(tag))
            .limit(1)
            .first()
        )
        if present is None:
            raise ZarrVersionUnavailable(
                f"tag {tag!r} does not exist in {table}",
                fix="pick an existing tag, or create it with gbx_zarr_tag",
            )
        return Snapshot(table=table, version=latest, tag=tag)

    horizon = _effective_horizon(_tblprops(spark, table))
    # (version, commit time in microseconds): read once, then decided here
    history = [
        (int(r["version"]), int(r["micros"]))
        for r in spark.sql(f"DESCRIBE HISTORY {_q(table)}")
        .select("version", F.unix_micros("timestamp").alias("micros"))
        .collect()
    ]
    if version is not None:
        wanted = f"version {version}"
        picked = version if any(v == version for v, _ in history) else None
    else:
        wanted = f"the version at {timestamp_as_of}"
        at = _timestamp_micros(spark, timestamp_as_of)
        picked = max((v for v, t in history if t <= at), default=None)
    if picked is None:
        raise ZarrVersionUnavailable(
            f"{wanted} of {table} does not exist or is no longer in its history",
            fix=_UNAVAILABLE_FIX,
        )
    if horizon is not None:
        limit = (horizon - _EPOCH) // timedelta(microseconds=1)
        oldest = max((v for v, t in history if t <= limit), default=None)
        if oldest is not None and picked < oldest:
            raise ZarrVersionUnavailable(
                f"version {picked} of {table} predates version {oldest}, the "
                f"one live at {horizon.isoformat()} (its vacuum horizon or retain "
                "window), so its files may have been removed",
                fix=_UNAVAILABLE_FIX,
            )
    return Snapshot(table=table, version=picked, tag=None)


# Longest list sent as an IN predicate. Longer ones become a semi-join against a small
# DataFrame of the values, which the cluster evaluates: either way only matching rows
# reach the driver.
_IN_LIST_LIMIT = 10_000


def _pruned(spark, df, column: str, values: list[str]):
    """``df`` narrowed to the rows whose ``column`` is one of ``values``.

    A short list is an IN predicate (which Delta can use to skip files); a long one is a
    left semi-join against the distinct values, so the filter runs on the cluster and not
    in a literal that the planner has to carry.
    """
    from pyspark.sql import functions as F

    if len(values) <= _IN_LIST_LIMIT:
        return df.where(F.col(column).isin(values))
    wanted = spark.createDataFrame(
        [(v,) for v in dict.fromkeys(values)], f"{column} STRING"
    )
    return df.join(wanted, on=column, how="left_semi")


def read_rows(
    spark, snap: Snapshot, *, kinds=None, arrays=None, shard_keys=None, keys=None
):
    """Manifest rows of a snapshot as an Arrow table.

    Live rows (``tag IS NULL``), or the tag's rows for a tag snapshot, optionally
    narrowed to the given kinds, arrays, shard keys and keys. ``keys`` is how a read
    prunes the chunk rows of an unsharded array, which have no shard key; an empty
    list matches nothing. A list of more than ``_IN_LIST_LIMIT`` values is applied as a
    semi-join, so a read of a huge selection still fetches only the rows it names.
    """
    from pyspark.sql import functions as F

    cond = (
        F.col("tag").isNull()
        if snap.tag is None
        else F.col("tag") == F.lit(check_tag(snap.tag))
    )
    if kinds is not None:
        kinds = [kinds] if isinstance(kinds, str) else list(kinds)
        bad = [k for k in kinds if k not in KINDS]
        if bad:
            raise ValueError(f"unknown kind(s) {bad}; expected some of {KINDS}")
        cond = cond & F.col("kind").isin(kinds)
    cols = ", ".join(MANIFEST_COLUMNS)
    df = spark.sql(
        f"SELECT {cols} FROM {_q(snap.table)} VERSION AS OF {int(snap.version)}"
    )
    for column, values in (("array", arrays), ("shard_key", shard_keys), ("key", keys)):
        if values is None:
            continue
        values = [values] if isinstance(values, str) else list(values)
        if len(values) <= _IN_LIST_LIMIT:
            cond = cond & F.col(column).isin(values)
        else:
            df = _pruned(spark, df, column, values)
    return df.where(cond).toArrow()


# --- commit -----------------------------------------------------------------------


def _spark_schema(arrow_schema: pa.Schema):
    from pyspark.sql import types as T

    kinds = {pa.string(): T.StringType(), pa.int64(): T.LongType()}
    return T.StructType(
        [T.StructField(f.name, kinds[f.type], True) for f in arrow_schema]
    )


def _typed(rows: pa.Table, schema: pa.Schema) -> pa.Table:
    """``rows`` laid out to ``schema``: columns reordered and cast, absent ones null."""
    arrays = [
        (
            rows.column(f.name).cast(f.type)
            if f.name in rows.column_names
            else pa.nulls(rows.num_rows, f.type)
        )
        for f in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def _first(column, mask):
    """First value of ``column`` where ``mask`` is true."""
    return column.filter(mask)[0].as_py()


def _commit_source(rows: pa.Table, write_id: str, meta: dict) -> pa.Table:
    """Validate ``rows`` and append the commit row; this is the MERGE source.

    Validation is columnar (``pyarrow.compute``): a commit can carry millions of rows.
    """
    missing = [c for c in _REQUIRED_COLUMNS if c not in rows.column_names]
    if missing:
        raise ValueError(f"rows are missing column(s) {missing}")
    unknown = [c for c in rows.column_names if c not in _COMMIT_COLUMNS]
    if unknown:
        raise ValueError(f"rows have unexpected column(s) {unknown}")
    typed = _typed(rows, _COMMIT_SCHEMA)
    if typed.num_rows:
        kind, op = typed.column("kind"), typed.column("op")
        bad_op = pc.invert(pc.is_in(op, value_set=pa.array(list(_OPS))))
        if pc.any(bad_op, min_count=0).as_py():
            raise ValueError(
                f"op must be 'upsert' or 'delete', got {_first(op, bad_op)!r}"
            )
        is_commit = pc.fill_null(pc.equal(kind, "commit"), False)
        if pc.any(is_commit, min_count=0).as_py():
            raise ValueError("rows must not hold kind 'commit': commit() adds that row")
        bad_kind = pc.invert(pc.is_in(kind, value_set=pa.array(list(KINDS))))
        if pc.any(bad_kind, min_count=0).as_py():
            raise ValueError(
                f"unknown kind {_first(kind, bad_kind)!r}; expected one of {KINDS}"
            )
        if pc.any(pc.is_valid(typed.column("tag")), min_count=0).as_py():
            raise ValueError("commit() writes live rows only: tag must be null")
        wid = typed.column("write_id")
        foreign = pc.and_(
            pc.equal(op, "upsert"),
            pc.invert(pc.fill_null(pc.equal(wid, write_id), False)),
        )
        if pc.any(foreign, min_count=0).as_py():
            raise ValueError(
                f"an upserted row's write_id must be the commit's write_id "
                f"({write_id}), got {_first(wid, foreign)!r}"
            )
        counts = typed.group_by(["kind", "array", "key"]).aggregate([("kind", "count")])
        crowded = pc.greater(counts.column("kind_count"), 1)
        if pc.any(crowded, min_count=0).as_py():
            ident = counts.filter(crowded).slice(0, 1).to_pylist()[0]
            raise ValueError(
                "duplicate row for (kind, array, key) = "
                f"{(ident['kind'], ident['array'], ident['key'])}"
            )
    commit_row = pa.Table.from_pylist(
        [
            {
                "kind": "commit",
                "key": write_id,
                "write_id": write_id,
                "meta": json.dumps(meta, sort_keys=True),
                "op": "upsert",
            }
        ],
        schema=pa.schema(
            [
                f
                for f in _COMMIT_SCHEMA
                if f.name in ("kind", "key", "write_id", "meta", "op")
            ]
        ),
    )
    return pa.concat_tables(
        [_typed(rows, _COMMIT_SCHEMA), _typed(commit_row, _COMMIT_SCHEMA)]
    )


def _with_view(spark, data: pa.Table, schema: pa.Schema, run):
    """Run ``run(view_name)`` over ``data`` exposed as a temp view; always drop it."""
    view = f"_gbx_zarr_src_{uuid.uuid4().hex}"
    # Spark's JVM Arrow iterator (ArrowConverters) ends at the first zero-row batch and
    # silently drops every row after it. That affects PySpark 4.0 and 4.2, classic and
    # Connect; concatenated tables carry such chunks whenever a part came out empty.
    data = pa.Table.from_batches(
        [b for b in data.to_batches() if b.num_rows], schema=data.schema
    )
    try:
        spark.createDataFrame(
            data, schema=_spark_schema(schema)
        ).createOrReplaceTempView(view)
        return run(view)
    finally:
        spark.catalog.dropTempView(view)


def find_commit_version(spark, table: str, write_id: str, *, since: int = 0):
    """Delta version that committed ``write_id``, or ``None``.

    Read from the commit row itself: a commit row is inserted once and never updated,
    so its row-tracking commit version (``_metadata.row_commit_version``) is the
    version of the commit that wrote it. That needs no change files, which Delta's
    own vacuum (predictive optimization on Unity Catalog) removes long before the log
    expires. Commits older than ``since`` are not reported.
    """
    _check_write_id(write_id)
    if isinstance(since, bool) or not isinstance(since, numbers.Integral) or since < 0:
        raise ValueError(f"since must be a non-negative int, got {since!r}")
    try:
        row = spark.sql(
            f"SELECT _metadata.row_commit_version AS v FROM {_q(table)} "
            f"WHERE kind = 'commit' AND key = '{write_id}' AND tag IS NULL"
        ).first()
    except Exception as e:
        # Row tracking is off exactly when Spark reports FIELD_NOT_FOUND for the field.
        # The name alone is not enough: the error for a missing table prints the
        # unresolved plan, which projects _metadata.row_commit_version too.
        text = _error_text(e)
        if "FIELD_NOT_FOUND" in text and "row_commit_version" in text:
            raise ZarrError(
                f"{table} does not track row commit versions, so the version a commit "
                "landed in cannot be read",
                fix=_ROW_TRACKING_FIX,
            ) from e
        raise
    if row is None or row[0] is None or int(row[0]) < since:
        return None
    return int(row[0])


def changed_since(spark, table: str, base: Snapshot, keys: pa.Table) -> pa.Table:
    """Rows of ``keys`` whose live ``write_id`` is not the one expected.

    ``keys`` has ``kind, array, key, expected_write_id`` (extra columns are ignored).
    The comparison is against the latest live rows, so it lists what changed since
    ``base``, the snapshot the expectations were read at; a tag snapshot is refused
    because tags never change. Returns ``kind, array, key, expected_write_id`` and the
    live ``write_id`` (null when the row is gone).
    """
    if base.tag is not None:
        raise ValueError(
            "changed_since compares live rows; base must not be a tag snapshot"
        )
    missing = [c for c in _KEYS_SCHEMA.names if c not in keys.column_names]
    if missing:
        raise ValueError(f"keys are missing column(s) {missing}")
    sql_table = _q(table)
    return _with_view(
        spark,
        _typed(keys, _KEYS_SCHEMA),
        _KEYS_SCHEMA,
        lambda view: spark.sql(
            _CHANGED_SQL.format(view=view, table=sql_table)
        ).toArrow(),
    )


def _write_conflict(
    spark, table: str, source: pa.Table, text: str
) -> ZarrWriteConflict:
    """The ``ZarrWriteConflict`` for a CAS abort, naming every conflicting key.

    The abort message carries only the first failing key. The full list comes from
    ``changed_since`` over this commit's own expectations; if that lookup fails the
    conflict is still raised, with just the key the abort named.
    """
    first = parse_cas_failure(text)
    try:
        base = Snapshot(table, _latest_version(spark, table))
        changed = changed_since(spark, table, base, source).to_pylist()
    except Exception:
        changed = []
    changed.sort(key=lambda r: (r["kind"], _store_key(r["array"], r["key"])))
    keys = [_store_key(r["array"], r["key"]) for r in changed]
    if not keys and first is not None:
        keys = [_store_key(first[1], first[2])]
    winner = None
    if first is not None:
        for r in changed:
            if (r["kind"], r["array"] or "", r["key"] or "") == first:
                winner = r["write_id"]
                break
    shown = ", ".join(keys[:5]) + (
        f" and {len(keys) - 5} more" if len(keys) > 5 else ""
    )
    return ZarrWriteConflict(
        f"{len(keys) or 'some'} key(s) of {table} changed since this write read "
        f"them{': ' + shown if keys else ''}.",
        fix="re-read the store at its latest version and write again",
        keys=tuple(keys),
        winner=winner,
        conflicts=tuple(changed),
    )


def commit(spark, table: str, rows: pa.Table, *, write_id: str, meta: dict) -> int:
    """Commit ``rows`` plus a commit row in one MERGE; returns the Delta version.

    ``rows`` holds the manifest columns plus ``op`` (``"upsert"`` | ``"delete"``) and
    ``expected_write_id`` (the row's ``write_id`` in the base snapshot; null means
    "must not exist"). A stale expectation aborts the whole MERGE and raises
    ``ZarrWriteConflict`` listing every key that changed; a Delta conflict with a
    concurrent commit raises the retryable ``ConcurrentCommit``. A ``write_id`` that
    already committed returns its existing version, so retries are idempotent, even
    when a twin of the same write lands between the check and the MERGE.
    """
    _check_write_id(write_id)
    source = _commit_source(rows, write_id, meta)
    existing = find_commit_version(spark, table, write_id)
    if existing is not None:
        return existing
    sql_table = _q(table)

    def merge(view: str) -> None:
        spark.sql(_MERGE_SQL.format(table=sql_table, view=view))

    try:
        _with_view(spark, source, _COMMIT_SCHEMA, merge)
    except Exception as e:
        text = _error_text(e)
        kind = classify_commit_error(text)
        if kind == "cas":
            try:
                landed = find_commit_version(spark, table, write_id)
            except Exception:
                # no answer: the user still gets the conflict, not the lookup's error
                landed = None
            if landed is not None:  # a twin of this write_id won the race
                return landed
            raise _write_conflict(spark, table, source, text) from e
        if kind == "concurrent":
            mark = next(m for m in _CONCURRENT_MARKS if m in text)
            raise ConcurrentCommit(
                f"Delta reported {mark} committing to {table}: a concurrent commit "
                "touched the same files or rows. Fix: read the store again and retry."
            ) from e
        raise
    version = find_commit_version(spark, table, write_id)
    if version is None:
        raise ZarrError(
            f"the MERGE into {table} finished but the commit row for {write_id} is "
            "not live",
            fix=_ROW_TRACKING_FIX,
        )
    return version
