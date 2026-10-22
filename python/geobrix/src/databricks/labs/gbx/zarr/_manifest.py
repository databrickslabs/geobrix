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
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pyarrow as pa

from databricks.labs.gbx.zarr.errors import (
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
    ZarrVersionUnavailable,
    ZarrWriteConflict,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column

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

_CAS_MARK = "GBX_ZARR_CAS"
# 'GBX_ZARR_CAS <kind> <array> <key> SQLSTATE: P0001'; array is empty for commit rows
# (two spaces) and key can be empty too.
_CAS_RE = re.compile(r"GBX_ZARR_CAS (\S+) (\S*) (\S*)(?=\s|$)")
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
    if _CAS_MARK in text:
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


def _earliest_version(spark, table: str) -> int:
    """Oldest commit still in the log; the change feed cannot start earlier."""
    from pyspark.sql import functions as F

    row = spark.sql(f"DESCRIBE HISTORY {_q(table)}").agg(F.min("version")).first()
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


def _timestamp_column(spark, value) -> "Column":
    from pyspark.sql import functions as F

    col = (
        F.lit(value) if isinstance(value, datetime) else F.lit(value).cast("timestamp")
    )
    try:
        parsed = spark.range(1).select(col).first()[0]
    except Exception as e:  # ANSI mode raises on an unparseable string
        raise ValueError(f"timestamp_as_of {value!r} is not a timestamp") from e
    if parsed is None:  # ANSI off: unparseable becomes NULL
        raise ValueError(f"timestamp_as_of {value!r} is not a timestamp")
    return col


def snapshot(
    spark, table: str, *, version_as_of=None, timestamp_as_of=None
) -> Snapshot:
    """Pin a version of the manifest.

    With no argument: the latest version. ``version_as_of`` is an int, or
    ``"tag:<name>"`` (the latest version, read through the tag's rows).
    ``timestamp_as_of`` (str or datetime) gives the latest version committed at or
    before it. A version that no longer exists, or whose commit predates the
    table's ``geobrix.zarr.vacuum_horizon`` (so its files may be gone), raises
    ``ZarrVersionUnavailable``. Tags and the latest version are always available.
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

    latest = _latest_version(spark, table)
    if tag is not None or (version is None and timestamp_as_of is None):
        return Snapshot(table=table, version=latest, tag=tag)

    from pyspark.sql import functions as F

    horizon_text = _tblprops(spark, table).get(HORIZON_PROP)
    older = F.lit(False)
    if horizon_text:
        older = F.col("timestamp") < F.lit(_parse_horizon(horizon_text))
    hist = spark.sql(f"DESCRIBE HISTORY {_q(table)}").select("version", "timestamp")
    if version is not None:
        wanted = f"version {version}"
        pick = hist.where(F.col("version") == version)
    else:
        wanted = f"the version at {timestamp_as_of}"
        pick = (
            hist.where(F.col("timestamp") <= _timestamp_column(spark, timestamp_as_of))
            .orderBy(F.col("version").desc())
            .limit(1)
        )
    row = pick.select("version", older.alias("older")).first()
    if row is None:
        raise ZarrVersionUnavailable(
            f"{wanted} of {table} does not exist or is no longer in its history",
            fix=_UNAVAILABLE_FIX,
        )
    if row["version"] < latest and row["older"]:
        raise ZarrVersionUnavailable(
            f"version {row['version']} of {table} was committed before the vacuum "
            f"horizon {horizon_text}, so its files may have been removed",
            fix=_UNAVAILABLE_FIX,
        )
    return Snapshot(table=table, version=int(row["version"]), tag=None)


def read_rows(spark, snap: Snapshot, *, kinds=None, arrays=None, shard_keys=None):
    """Manifest rows of a snapshot as an Arrow table.

    Live rows (``tag IS NULL``), or the tag's rows for a tag snapshot, optionally
    narrowed to the given kinds, arrays and shard keys.
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
    for column, values in (("array", arrays), ("shard_key", shard_keys)):
        if values is not None:
            values = [values] if isinstance(values, str) else list(values)
            cond = cond & F.col(column).isin(values)
    cols = ", ".join(MANIFEST_COLUMNS)
    df = spark.sql(
        f"SELECT {cols} FROM {_q(snap.table)} VERSION AS OF {int(snap.version)}"
    )
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


def _commit_source(rows: pa.Table, write_id: str, meta: dict) -> pa.Table:
    """Validate ``rows`` and append the commit row; this is the MERGE source."""
    missing = [c for c in _REQUIRED_COLUMNS if c not in rows.column_names]
    if missing:
        raise ValueError(f"rows are missing column(s) {missing}")
    unknown = [c for c in rows.column_names if c not in _COMMIT_COLUMNS]
    if unknown:
        raise ValueError(f"rows have unexpected column(s) {unknown}")
    seen = set()
    needed = ["kind", "array", "key", "op", "write_id", "tag"]
    for r in _typed(rows, _COMMIT_SCHEMA).select(needed).to_pylist():
        if r["op"] not in _OPS:
            raise ValueError(f"op must be 'upsert' or 'delete', got {r['op']!r}")
        if r["kind"] == "commit":
            raise ValueError("rows must not hold kind 'commit': commit() adds that row")
        if r["kind"] not in KINDS:
            raise ValueError(f"unknown kind {r['kind']!r}; expected one of {KINDS}")
        if r["tag"] is not None:
            raise ValueError("commit() writes live rows only: tag must be null")
        if r["op"] == "upsert" and r["write_id"] != write_id:
            raise ValueError(
                f"an upserted row's write_id must be the commit's write_id "
                f"({write_id}), got {r['write_id']!r}"
            )
        ident = (r["kind"], r["array"], r["key"])
        if ident in seen:
            raise ValueError(f"duplicate row for (kind, array, key) = {ident}")
        seen.add(ident)
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
    try:
        spark.createDataFrame(
            data, schema=_spark_schema(schema)
        ).createOrReplaceTempView(view)
        return run(view)
    finally:
        spark.catalog.dropTempView(view)


def find_commit_version(spark, table: str, write_id: str, *, since: int = 0):
    """Delta version that committed ``write_id``, or ``None``.

    Read from the change feed (``since`` is the first version to look at, clamped to
    the oldest commit still in the log).
    """
    _check_write_id(write_id)
    if isinstance(since, bool) or not isinstance(since, numbers.Integral) or since < 0:
        raise ValueError(f"since must be a non-negative int, got {since!r}")
    since = max(int(since), _earliest_version(spark, table) if since == 0 else 0)
    row = spark.sql(
        f"SELECT max(_commit_version) FROM table_changes({_lit(_q(table))}, {since}) "
        f"WHERE kind = 'commit' AND key = '{write_id}' AND _change_type = 'insert'"
    ).first()
    return None if row is None or row[0] is None else int(row[0])


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
    spark, table: str, base_version: int, source: pa.Table, text: str
) -> ZarrWriteConflict:
    """The ``ZarrWriteConflict`` for a CAS abort, naming every conflicting key.

    The abort message carries only the first failing key. The full list comes from
    ``changed_since`` over this commit's own expectations; if that lookup fails the
    conflict is still raised, with just the key the abort named.
    """
    first = parse_cas_failure(text)
    try:
        changed = changed_since(
            spark, table, Snapshot(table, base_version), source
        ).to_pylist()
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
    err = ZarrWriteConflict(
        f"{len(keys) or 'some'} key(s) of {table} changed since this write read "
        f"them{': ' + shown if keys else ''}.",
        fix="re-read the store at its latest version and write again",
        keys=tuple(keys),
        winner=winner,
    )
    err.conflicts = tuple(
        changed
    )  # rows: kind, array, key, expected_write_id, write_id
    return err


def commit(
    spark,
    table: str,
    rows: pa.Table,
    *,
    write_id: str,
    meta: dict,
    since: int | None = None,
) -> int:
    """Commit ``rows`` plus a commit row in one MERGE; returns the Delta version.

    ``rows`` holds the manifest columns plus ``op`` (``"upsert"`` | ``"delete"``) and
    ``expected_write_id`` (the row's ``write_id`` in the base snapshot; null means
    "must not exist"). A stale expectation aborts the whole MERGE and raises
    ``ZarrWriteConflict`` listing every key that changed; a Delta conflict with a
    concurrent commit raises the retryable ``ConcurrentCommit``. A ``write_id`` that
    already committed returns its existing version, so retries are idempotent;
    ``since`` bounds that lookup (the version the writer's base snapshot was read at).
    """
    _check_write_id(write_id)
    source = _commit_source(rows, write_id, meta)
    existing = find_commit_version(spark, table, write_id, since=since or 0)
    if existing is not None:
        return existing
    base_version = _latest_version(spark, table)
    sql_table = _q(table)

    def merge(view: str) -> None:
        spark.sql(_MERGE_SQL.format(table=sql_table, view=view))

    try:
        _with_view(spark, source, _COMMIT_SCHEMA, merge)
    except Exception as e:
        text = _error_text(e)
        kind = classify_commit_error(text)
        if kind == "cas":
            raise _write_conflict(spark, table, base_version, source, text) from e
        if kind == "concurrent":
            mark = next(m for m in _CONCURRENT_MARKS if m in text)
            raise ConcurrentCommit(
                f"Delta reported {mark} committing to {table}: a concurrent commit "
                "touched the same files or rows. Fix: read the store again and retry."
            ) from e
        raise
    version = find_commit_version(spark, table, write_id, since=base_version)
    if version is None:
        raise ZarrError(
            f"the MERGE into {table} finished but no commit row for {write_id} is in "
            f"the change feed from version {base_version}",
            fix="check that delta.enableChangeDataFeed is true on the manifest table",
        )
    return version
