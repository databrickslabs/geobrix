"""Maintenance of a Zarr store: optimize, vacuum, tags, history and restore.

**Optimize** folds a sharded array's chunk rows (what appends and updates leave behind)
into whole shards: for each shard with at least one chunk row it lays the shard out as the
reader sees it (the shard row's bytes with the chunk rows laid over them, tombstones
dropping their chunks), copies those bytes without decoding into a new pack, and commits
one ``shard`` row per shard in place of the chunk rows, all in one compare-and-swap commit.
A shard whose rows changed while it ran is skipped and reported, not failed.

**Vacuum** deletes the files GeoBrix wrote under the store's data root (a managed store's
root, a writable registration's overlay root) that no version still readable needs. A pack
file is kept when a live row or a tag row points at it, when it belongs to a version within
``retain`` (see ``_protected``), or when it is younger than the one-hour grace period that
protects a write still in flight. The base objects of a registered store and any root whose
owner marker is not this table's are never touched. A vacuum records the instant before
which versions are no longer promised (``geobrix.zarr.vacuum_horizon``, read by
``_manifest.snapshot``) before it deletes anything. It never reads the change data feed:
that is removed long before the table's log expires. A ``file_mode="managed"`` store hands
its files to the table: there the vacuum is Delta's.

**Tags** copy the live rows (or those of an older version) as rows with ``tag = name``.
They are current rows, so they outlive vacuum, Delta's own VACUUM and history expiry.

**History** is built from the commit rows. **Restore** commits the changes that make the
live rows equal an older version's.

Everything goes through SQL and DataFrame calls that work under Spark Connect.
"""

from __future__ import annotations

import json
import logging
import math
import os
import stat
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.core._arrow import to_spark_df
from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr._access import FuseRangeReader
from databricks.labs.gbx.zarr._pack import PackWriter
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr._store import ManifestStore
from databricks.labs.gbx.zarr.errors import (
    ZarrError,
    ZarrReadOnly,
    ZarrUnsupported,
    ZarrVersionUnavailable,
    ZarrWriteConflict,
)

__all__ = [
    "gbx_zarr_optimize",
    "gbx_zarr_vacuum",
    "gbx_zarr_tag",
    "gbx_zarr_untag",
    "gbx_zarr_history",
    "gbx_zarr_restore",
]

_LOG = logging.getLogger(__name__)

_MIB = 1 << 20
_DATA_KINDS = ["group", "array", "shard", "chunk"]  # the kinds a version is made of
_GRACE = timedelta(hours=1)  # a file younger than this may belong to a write in flight
_MIN_RETAIN_SECONDS = 3600
_ATTEMPTS = 5  # commit attempts of an optimize or a restore
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SEP = "\x1f"
_REPORT_DDL = "file STRING, action STRING, reason STRING"
_STREAM_WINDOW = 32 * _MIB  # bytes of one shard read at a time while it is copied
_CONTENT = (  # the columns that make a row the same row in two versions
    "shard_key file offset length checksum origin identity meta".split()
)


def _utcnow() -> datetime:
    """The vacuum's clock (tests replace it)."""
    return datetime.now(timezone.utc)


# --- shared -----------------------------------------------------------------------------


def _roots(spark, table: str, props) -> tuple[str | None, str | None]:
    """``(owned root, base root)`` of a store, the owner marker of the owned root verified.

    The owned root is where GeoBrix put its files: a managed store's data root, a writable
    registration's overlay root; ``None`` for a read-only registration, which owns none.
    """
    from databricks.labs.gbx.zarr._register import (
        check_owner_marker,
        qualified_table_name,
    )

    owner = qualified_table_name(spark, table)
    if props.kind == "registered":
        if not props.writable or not props.overlay_root:
            return None, props.root
        check_owner_marker(props.overlay_root, owner, "overlay")
        return props.overlay_root, props.root
    check_owner_marker(props.root, owner, "managed")
    return props.root, None


def _ids(table: pa.Table) -> pa.Array:
    return pc.binary_join_element_wise(
        table.column("kind"),
        table.column("array"),
        table.column("key"),
        _SEP,
        null_handling="replace",
        null_replacement="",
    )


def _rel(file: str, root: str) -> str:
    """A manifest ``file`` as a path relative to ``root``, in the form the listing uses."""
    path = os.path.normpath(file)
    if os.path.isabs(path):
        path = os.path.relpath(path, os.path.normpath(root))
    return path.replace(os.sep, "/")


def _report(spark, rows: list[tuple[str, str, str]]):
    table = pa.Table.from_pylist(
        [{"file": f, "action": a, "reason": r} for f, a, r in rows],
        schema=pa.schema([(n, pa.string()) for n in ("file", "action", "reason")]),
    )
    return to_spark_df(spark, table, _REPORT_DDL)


def commit_versions(spark, table: str) -> pa.Table:
    """The live commit rows: ``write_id``, the Delta ``version`` that committed it and its
    ``mode``. Read from the rows themselves (row tracking), never from the change feed.
    """
    try:
        return spark.sql(
            f"SELECT key AS write_id, _metadata.row_commit_version AS version, "
            f"get_json_object(meta, '$.mode') AS mode FROM {M._q(table)} "
            "WHERE kind = 'commit' AND tag IS NULL"
        ).toArrow()
    except Exception as e:
        text = M._error_text(e)
        if "FIELD_NOT_FOUND" in text and "row_commit_version" in text:
            raise ZarrError(
                f"{table} does not track row commit versions",
                fix=M._ROW_TRACKING_FIX,
            ) from e
        raise


# --- optimize ---------------------------------------------------------------------------


def _array_names(variables, docs) -> list[str]:
    """The sharded arrays to fold: all of them, or the named ones."""
    sharded = sorted(n for n, m in docs.arrays.items() if m.shard is not None)
    if variables is None:
        return sharded
    names = [variables] if isinstance(variables, str) else list(variables)
    unknown = [n for n in names if n not in docs.arrays and n not in docs.unsupported]
    if unknown:
        raise ValueError(
            f"{unknown} are not arrays of this store (it has {sorted(docs.arrays)}). "
            "Fix: name arrays of the store."
        )
    return [n for n in names if n in sharded]  # an unsharded array has nothing to fold


def _stream(shard, reader, window: int = _STREAM_WINDOW):
    """The bytes of the virtual ``shard`` in order, a window of regions read at a time."""

    def flush(batch):
        regions = [p for p in batch if isinstance(p, Region)]
        fetched = iter(reader.read(regions) if regions else ())
        for part in batch:
            yield next(fetched) if isinstance(part, Region) else part

    batch, held = [], 0
    for part in shard.pieces(0, shard.size):
        batch.append(part)
        if isinstance(part, Region):
            held += part.length
        if held >= window:
            yield from flush(batch)
            batch, held = [], 0
    yield from flush(batch)


def _crc32c_extend(data, value: int) -> int:
    from numcodecs import CRC32C

    return CRC32C.checksum(bytes(data), value) & 0xFFFFFFFF


def _tombstone(array, key, identity, write_id, expected) -> dict:
    """The row that keeps a base shard hidden once nothing is left of it (Ruling Z49)."""
    return {
        "kind": "shard",
        "array": array,
        "key": key,
        "shard_key": key,
        "length": 0,
        "origin": "gbx",
        "identity": identity,
        "write_id": write_id,
        "op": "upsert",
        "expected_write_id": expected,
    }


def _fold_array(name, meta, groups, rows, reader, root, write_id, cap, ledger) -> dict:
    """Commit rows per ``(array, shard_key)`` for the shards of one array.

    ``groups`` maps a shard key to ``(shard row or None, [chunk rows])``; ``rows`` is every
    row of the array, which the ``ManifestStore`` reads the shards from.
    """
    store = ManifestStore({}, rows, reader, {name: meta})
    out: dict[tuple, list[dict]] = {}
    pack = None

    def close_pack():
        nonlocal pack
        if pack is not None:
            pack.close()
            pack = None

    try:
        for key in sorted(groups):
            old, chunks = groups[key]
            identity = old["identity"] if old is not None else None
            shard = (
                store._virtual_shard(f"{name}/{key}")
                if f"{name}/{key}" in store._shards
                else None
            )
            held = shard is not None and any(
                r is not None for r in shard.chunks.values()
            )
            plan = out.setdefault((name, key), [])
            if held:
                if pack is not None and pack.size and pack.size + shard.size > cap:
                    close_pack()
                if pack is None:
                    pack = PackWriter(
                        os.path.join(root, "data", name, write_id), reader_root=root
                    )
                offset, crc, copied = None, 0, 0
                for part in _stream(shard, reader):
                    at, n = pack.add(part)
                    offset = at if offset is None else offset
                    crc = _crc32c_extend(part, crc)
                    copied += n
                if copied != shard.size:
                    raise ZarrError(
                        f"folding {name}/{key} copied {copied} bytes of {shard.size}",
                        fix="run gbx_zarr_optimize again; if it repeats, the pack files "
                        "of this shard are damaged (gbx_zarr_sync for a registered store)",
                    )
                plan.append(
                    {
                        "kind": "shard",
                        "array": name,
                        "key": key,
                        "shard_key": key,
                        "file": pack.path,
                        "offset": offset,
                        "length": shard.size,
                        "checksum": f"{crc:08x}",
                        "origin": "gbx",
                        "identity": identity,
                        "write_id": write_id,
                        "op": "upsert",
                        "expected_write_id": old["write_id"] if old else None,
                    }
                )
            elif old is not None and old["file"] is None:
                pass  # already a tombstone: it keeps hiding what it hides
            elif old is not None and identity is not None:
                # nothing is left of the shard, but a base object stands behind it
                plan.append(_tombstone(name, key, identity, write_id, old["write_id"]))
            elif old is not None:
                plan.append(
                    {
                        "kind": "shard",
                        "array": name,
                        "key": key,
                        "write_id": write_id,
                        "op": "delete",
                        "expected_write_id": old["write_id"],
                    }
                )
            for c in chunks:
                plan.append(
                    {
                        "kind": "chunk",
                        "array": name,
                        "key": c["key"],
                        "write_id": write_id,
                        "op": "delete",
                        "expected_write_id": c["write_id"],
                    }
                )
                ledger[("chunk", name, c["key"])] = (name, key)
            ledger[("shard", name, key)] = (name, key)
        close_pack()
    except BaseException:
        if pack is not None:
            pack.abort()
        raise
    finally:
        store.close()
    return out


def _optimize(
    spark,
    table: str,
    *,
    variables=None,
    target_file_mb: int = 128,
    shards: set | None = None,
) -> int:
    """``gbx_zarr_optimize``; ``shards`` (a set of ``(array, shard_key)``) narrows it to the
    shards a write has just touched."""
    from databricks.labs.gbx.zarr import _write

    if (
        isinstance(target_file_mb, bool)
        or not isinstance(target_file_mb, int)
        or target_file_mb < 1
    ):
        raise ValueError(
            f"target_file_mb must be a whole number of MB, at least 1; got "
            f"{target_file_mb!r}. Fix: pass target_file_mb=128."
        )
    props = M.read_props(spark, table)
    if props.file_mode != "fuse":
        raise ZarrUnsupported(
            f"{table} keeps its files in file_mode {props.file_mode!r}, which GeoBrix does "
            "not write yet",
            fix="optimize a store with file_mode 'fuse'",
        )
    root, base = _roots(spark, table, props)
    if root is None:
        raise ZarrReadOnly(
            f"{table} is a read-only registration of {props.root}, so it has nowhere to "
            "put the folded shards",
            fix="register the store with writable=True and an overlay_root",
        )
    snap = M.snapshot(spark, table)
    docs = _write._docs_at(spark, snap)
    names = _array_names(variables, docs)
    wanted = None if shards is None else {s for s in shards if s[0] in names}
    if not names or wanted == set():
        return snap.version
    chunk_rows = M.read_rows(
        spark,
        snap,
        kinds="chunk",
        arrays=names,
        shard_keys=None if wanted is None else sorted({k for _, k in wanted}),
    ).to_pylist()
    by_shard: dict[tuple, list[dict]] = {}
    for r in chunk_rows:
        if r["shard_key"] is not None and (
            wanted is None or (r["array"], r["shard_key"]) in wanted
        ):
            by_shard.setdefault((r["array"], r["shard_key"]), []).append(r)
    if not by_shard:
        return snap.version
    shard_rows = {
        (r["array"], r["key"]): r
        for r in M.read_rows(
            spark,
            snap,
            kinds="shard",
            arrays=sorted({a for a, _ in by_shard}),
            shard_keys=sorted({k for _, k in by_shard}),
        ).to_pylist()
    }

    write_id = uuid.uuid4().hex
    reader = FuseRangeReader(root, base_root=base)
    plan: dict[tuple, list[dict]] = {}
    ledger: dict[tuple, tuple] = {}  # (kind, array, key) of a row -> its shard
    cap = target_file_mb * _MIB
    for name in sorted({a for a, _ in by_shard}):
        groups = {
            k: (shard_rows.get((a, k)), rs)
            for (a, k), rs in by_shard.items()
            if a == name
        }
        rows = [r for _, rs in groups.values() for r in rs] + [
            old for old, _ in groups.values() if old is not None
        ]
        plan.update(
            _fold_array(
                name,
                docs.arrays[name],
                groups,
                rows,
                reader,
                root,
                write_id,
                cap,
                ledger,
            )
        )

    sharded = {n: m.shard is not None for n, m in docs.arrays.items()}
    skipped: list[tuple] = []
    for _ in range(_ATTEMPTS):
        rows = [r for rs in plan.values() for r in rs]
        if not rows:
            break
        meta = {
            "mode": "optimize",
            "write_id": write_id,
            "variables": sorted({a for a, _ in plan}),
            "region": {},
            "user": _write._current_user(spark),
            "shards": len(plan),
            "folded": sum(1 for r in rows if r["kind"] == "chunk"),
            "skipped": len(skipped),
        }
        try:
            version = M.commit(
                spark,
                table,
                pa.Table.from_pylist(rows, schema=M._COMMIT_SCHEMA),
                write_id=write_id,
                meta=meta,
                sharded=sharded,
            )
        except M.ConcurrentCommit:
            continue  # the same rows, once more: a stale one now fails as a conflict
        except ZarrWriteConflict as e:
            lost = {ledger.get((c["kind"], c["array"], c["key"])) for c in e.conflicts}
            if not e.conflicts or None in lost:
                raise
            for shard in lost:
                plan.pop(shard, None)
                skipped.append(shard)
            continue
        _warn_skipped(table, skipped)
        return version
    if plan:
        raise ZarrWriteConflict(
            f"gave up folding {table} after {_ATTEMPTS} attempts",
            fix="retry once the other writers are done",
        )
    _warn_skipped(table, skipped)
    return M.snapshot(spark, table).version


def _warn_skipped(table: str, skipped: list) -> None:
    if skipped:
        _LOG.warning(
            "gbx_zarr_optimize %s skipped %d shard(s) that changed while it ran "
            "(run it again to fold them): %s",
            table,
            len(skipped),
            ", ".join(f"{a}/{k}" for a, k in sorted(skipped)[:5]),
        )


def gbx_zarr_optimize(
    spark, table: str, *, variables=None, target_file_mb: int = 128
) -> int:
    """Fold the chunk rows of a store's sharded arrays into whole shards; returns the
    store version.

    Appends and updates add one manifest row per inner chunk they change, which keeps a
    write cheap but spreads a shard over many small pack files. This copies, without
    decoding, each shard that has chunk rows into a new pack file as a standard Zarr v3
    shard (the inner chunks plus their index), with one ``shard`` row for it in place of
    the chunk rows. Chunks that an all-fill write deleted (tombstones) are folded away.
    Reads of the folded shards are served from one object; it is worth running after many
    small appends, especially on FUSE.

    ``variables`` limits it to the named arrays (an unsharded array has nothing to fold);
    ``target_file_mb`` caps a pack file's size (a shard larger than that gets its own).
    The commit is compare-and-swap: a shard whose rows changed while it ran is skipped,
    logged as a warning, and folded next time. In a registered store the folded shard goes
    to the overlay and keeps the identity of the base shard it shadows. Returns the
    version the fold committed, or the current version when there was nothing to fold.
    """
    return _optimize(spark, table, variables=variables, target_file_mb=target_file_mb)


# --- vacuum -----------------------------------------------------------------------------


def _stamped_horizon(tp: dict) -> datetime | None:
    text = tp.get(M.HORIZON_PROP)
    return M._parse_horizon(text) if text else None


def _list_data(root: str) -> list[tuple[str, float]]:
    """Every regular file under ``<root>/data`` as ``(path relative to root, mtime)``.
    Links and anything else that is not a plain file are left alone."""
    top = os.path.join(root, "data")

    def walk():
        out = []
        for folder, _, names in os.walk(top, followlinks=False):
            for name in names:
                path = os.path.join(folder, name)
                st = os.lstat(path)
                if stat.S_ISREG(st.st_mode):
                    out.append(
                        (os.path.relpath(path, root).replace(os.sep, "/"), st.st_mtime)
                    )
        return sorted(out)

    return _retry_transient(walk) if os.path.isdir(top) else []


def _files_at(spark, table: str, version: int, root: str) -> set[str]:
    """The pack files the live rows of ``version`` point at."""
    try:
        rows = spark.sql(
            f"SELECT DISTINCT file FROM {M._q(table)} VERSION AS OF {int(version)} "
            "WHERE tag IS NULL AND kind IN ('shard', 'chunk') AND file IS NOT NULL "
            "AND coalesce(origin, 'gbx') = 'gbx'"
        ).toArrow()
    except Exception as e:
        raise ZarrError(
            f"version {version} of {table} cannot be read, so the files it needs cannot "
            f"be told ({type(e).__name__})",
            fix="vacuum with a retain no longer than the store's own (geobrix.zarr.retain)",
        ) from e
    return {_rel(f, root) for f in rows.column("file").to_pylist()}


def _protected(spark, table: str, root: str, horizon: datetime):
    """What a vacuum may not delete, as ``(live, tagged, window files, commit versions,
    oldest)``.

    A version is still readable if it is at or after ``oldest``, the version that was live
    at ``horizon`` (``None``: the table is younger than that, so every version is). The
    files of those versions are: the files of ``oldest`` itself; every file a later write
    put in its own directory (``data/<array>/<write_id>/``: written, so referenced by the
    version that committed it); and the files a later restore pointed the rows back at.
    Anything else was dereferenced before ``oldest`` and no readable version needs it.
    """
    from pyspark.sql import functions as F

    rows = spark.sql(
        f"SELECT DISTINCT file, tag IS NOT NULL AS tagged FROM {M._q(table)} "
        "WHERE kind IN ('shard', 'chunk') AND file IS NOT NULL "
        "AND coalesce(origin, 'gbx') = 'gbx'"
    ).toArrow()
    live, tagged = set(), set()
    for f, is_tag in zip(
        rows.column("file").to_pylist(), rows.column("tagged").to_pylist()
    ):
        (tagged if is_tag else live).add(_rel(f, root))
    tagged -= live

    history = [
        (int(r["version"]), int(r["micros"]))
        for r in spark.sql(f"DESCRIBE HISTORY {M._q(table)}")
        .select("version", F.unix_micros("timestamp").alias("micros"))
        .collect()
    ]
    limit = (horizon - _EPOCH) // timedelta(microseconds=1)
    oldest = max((v for v, t in history if t <= limit), default=None)
    commits = commit_versions(spark, table)
    versions = {  # a commit whose version is unknown (None) is taken to be recent
        w: (None if v is None else int(v), m)
        for w, v, m in zip(
            commits.column("write_id").to_pylist(),
            commits.column("version").to_pylist(),
            commits.column("mode").to_pylist(),
        )
    }
    window = set()
    if oldest is not None:
        window |= _files_at(spark, table, oldest, root)
        for _, (v, mode) in versions.items():
            if mode == "restore" and v is not None and v > oldest:
                window |= _files_at(spark, table, v, root)
    return live, tagged, window, versions, oldest


def _set_properties(
    spark, table: str, tp: dict, seconds: int, horizon: datetime, stamp: bool = True
) -> None:
    """One ``ALTER TABLE`` for a vacuum: Delta's retention properties, set to the retain the
    vacuum used, and with ``stamp`` the horizon (it only ever moves forward)."""
    days = max(1, math.ceil(seconds / 86400))
    want = {
        "delta.deletedFileRetentionDuration": f"interval {days} days",
        "delta.logRetentionDuration": f"interval {max(30, days)} days",
    }
    held = _stamped_horizon(tp)
    if stamp and (held is None or horizon > held):
        want[M.HORIZON_PROP] = horizon.isoformat()
    todo = {k: v for k, v in want.items() if tp.get(k) != v}
    if todo:
        sets = ", ".join(f"{M._lit(k)} = {M._lit(v)}" for k, v in todo.items())
        spark.sql(f"ALTER TABLE {M._q(table)} SET TBLPROPERTIES ({sets})")


def _delta_vacuum(spark, table: str, tp: dict, seconds: int, horizon, dry_run: bool):
    """A ``file_mode="managed"`` store: its files belong to the table, so the vacuum is
    Delta's. The retention property is set first and a plain ``VACUUM`` follows (Ruling Z10:
    GeoBrix never sets ``retentionDurationCheck``, so there is no ``RETAIN`` clause)."""
    sql_table = M._q(table)
    if not dry_run:
        _set_properties(spark, table, tp, seconds, horizon)
    listing = spark.sql(f"VACUUM {sql_table} DRY RUN").collect()
    if not dry_run:
        spark.sql(f"VACUUM {sql_table}").collect()
    action = "would_delete" if dry_run else "deleted"
    return _report(spark, [(str(r[0]), action, "delta") for r in listing])


def gbx_zarr_vacuum(
    spark,
    table: str,
    *,
    retain: str | None = None,
    dry_run: bool = False,
    force: bool = False,
):
    """Delete the files of a store that no readable version needs.

    Returns a DataFrame with one row per file under the store's data root: ``file`` (relative
    to the root), ``action`` (``"deleted"``, ``"would_delete"`` or ``"kept"``) and ``reason``:

    - kept: ``live`` (a live row points at it), ``tagged`` (a tag row does), ``retained``
      (a version within ``retain`` does, or the write that made it is that recent), or
      ``recent`` (younger than the one-hour grace that protects a write in flight);
    - deleted: ``superseded`` (only versions older than ``retain`` pointed at it) or
      ``uncommitted`` (its write never committed: a failed, retried or losing write).

    ``retain`` is how long old versions stay readable (default: the store's own, ``"7 days"``
    unless set at creation; one given longer than the store's has no further effect, since
    the manifest's history is kept for that long). Under one hour it is refused unless
    ``force=True``. ``dry_run=True`` lists what would go and changes nothing.

    A run records ``geobrix.zarr.vacuum_horizon`` on the table first, so versions older than
    ``retain`` can no longer be read (``ZarrVersionUnavailable``) before their files go; sets
    Delta's retention properties to the retain it used; and never deletes a commit row, so a
    replayed ``write_id`` is still recognised. Base objects of a registered store, and a
    root whose owner marker is not this table's (``ZarrOwnershipError``), are never touched.
    Old versions you want to keep indefinitely are tagged (``gbx_zarr_tag``). In
    ``file_mode="managed"`` the table owns the files and this runs Delta's ``VACUUM``.
    """
    if retain is not None:
        seconds = M.retain_seconds(retain)
    props = M.read_props(spark, table)
    if retain is None:
        seconds = M.property_retain_seconds(props.retain)
    if seconds < _MIN_RETAIN_SECONDS and not force:
        raise ValueError(
            f"retain of {retain or props.retain!r} is under the 1 hour floor, which "
            "protects writes still in flight. Fix: pass retain='1 hour' or more, or "
            "force=True if no write is running."
        )
    now = _utcnow()
    tp = M._tblprops(spark, table)
    horizon = max(
        d
        for d in (
            now - timedelta(seconds=seconds),
            now - timedelta(seconds=M.property_retain_seconds(props.retain)),
            _stamped_horizon(tp),
        )
        if d is not None
    )
    if props.file_mode == "managed":
        return _delta_vacuum(spark, table, tp, seconds, horizon, dry_run)

    root, _ = _roots(spark, table, props)
    if root is None:
        return _report(spark, [])
    files = _list_data(root)
    live, tagged, window, versions, oldest = _protected(spark, table, root, horizon)

    decided: list[tuple[str, str, str]] = []
    doomed: list[str] = []
    grace = now - _GRACE
    stale = now - timedelta(seconds=seconds)
    for rel, mtime in files:
        written = datetime.fromtimestamp(mtime, timezone.utc)
        write_id = rel.rsplit("/", 2)[-2] if rel.count("/") >= 2 else ""
        committed = versions.get(write_id)
        if rel in live:
            decided.append((rel, "kept", "live"))
        elif rel in tagged:
            decided.append((rel, "kept", "tagged"))
        elif rel in window or (
            committed is not None
            and (oldest is None or committed[0] is None or committed[0] >= oldest)
        ):
            decided.append((rel, "kept", "retained"))
        elif written >= grace or (committed is None and written >= stale):
            decided.append((rel, "kept", "recent"))
        else:
            doomed.append(rel)
            reason = "superseded" if committed is not None else "uncommitted"
            decided.append((rel, "would_delete", reason))
    if dry_run:
        return _report(spark, decided)
    # Delta's retention follows the retain used; versions older than the horizon stop being
    # readable before any file goes (and only if one does)
    _set_properties(spark, table, tp, seconds, horizon, stamp=bool(doomed))
    failed: dict[str, str] = {}
    for rel in doomed:
        try:
            os.remove(os.path.join(root, rel))
        except FileNotFoundError:
            pass
        except OSError as e:
            failed[rel] = f"{type(e).__name__}: {e.strerror or e}"
    _prune_empty(root, doomed)
    if failed:
        _LOG.warning(
            "gbx_zarr_vacuum %s could not delete %d file(s), e.g. %s",
            table,
            len(failed),
            next(iter(failed.items())),
        )

    def final(file, action, reason):
        if action != "would_delete":
            return file, action, reason
        if file in failed:
            return file, "kept", f"could not delete ({failed[file]})"
        return file, "deleted", reason

    return _report(spark, [final(*d) for d in decided])


def _prune_empty(root: str, deleted: list[str]) -> None:
    """Remove the directories a vacuum emptied, up to (not including) ``<root>/data``."""
    top = os.path.join(root, "data")
    for folder in sorted(
        {os.path.dirname(os.path.join(root, r)) for r in deleted}, key=len, reverse=True
    ):
        while folder != top and folder.startswith(top):
            try:
                os.rmdir(folder)
            except OSError:
                break
            folder = os.path.dirname(folder)


# --- tags -------------------------------------------------------------------------------

_COLUMNS = ", ".join(M.MANIFEST_COLUMNS)


def _commit_source_sql(write_id: str, meta: dict) -> str:
    """One ``commit`` row as a SELECT in the manifest's column order."""
    wid = M._lit(write_id)
    return (
        f"SELECT 'commit' AS kind, CAST(NULL AS STRING) AS array, {wid} AS key, "
        "CAST(NULL AS STRING) AS shard_key, CAST(NULL AS STRING) AS file, "
        "CAST(NULL AS BIGINT) AS offset, CAST(NULL AS BIGINT) AS length, "
        "CAST(NULL AS STRING) AS checksum, CAST(NULL AS STRING) AS origin, "
        f"CAST(NULL AS STRING) AS identity, {wid} AS write_id, "
        f"CAST(NULL AS STRING) AS tag, {M._lit(json.dumps(meta, sort_keys=True))} AS meta"
    )


def _tag_exists(spark, table: str, name: str) -> bool:
    return (
        spark.sql(
            f"SELECT 1 FROM {M._q(table)} WHERE tag = {M._lit(name)} LIMIT 1"
        ).first()
        is not None
    )


def gbx_zarr_tag(spark, table: str, name: str, *, version_as_of=None) -> None:
    """Name a version of the store so it outlives vacuum.

    Copies the live rows (or those of ``version_as_of``, an int, or ``"tag:<name>"`` to
    copy another tag) into the manifest as rows with ``tag = name``, in one MERGE. Read it
    with ``version_as_of="tag:<name>"``; vacuum, and Delta's own VACUUM and history expiry,
    never remove what a tag points at. A tag costs one manifest row per shard and chunk of
    the version. A name that is already a tag raises ``ValueError``; remove it with
    ``gbx_zarr_untag``.
    """
    M.check_tag(name)
    M.read_props(spark, table)
    if _tag_exists(spark, table, name):
        raise ValueError(
            f"tag {name!r} already exists in {table}. Fix: pick another name, or remove "
            "it first with gbx_zarr_untag."
        )
    snap = M.snapshot(spark, table, version_as_of=version_as_of)
    source = "tag IS NULL" if snap.tag is None else f"tag = {M._lit(snap.tag)}"
    write_id = uuid.uuid4().hex
    meta = {
        "mode": "tag",
        "write_id": write_id,
        "tag": name,
        "from": snap.version if snap.tag is None else f"tag:{snap.tag}",
    }
    sql_table = M._q(table)
    spark.sql(f"""
MERGE INTO {sql_table} AS t
USING (
  SELECT kind, array, key, shard_key, file, offset, length, checksum, origin, identity,
         write_id, {M._lit(name)} AS tag, meta
  FROM {sql_table} VERSION AS OF {int(snap.version)}
  WHERE {source} AND kind IN ('group', 'array', 'shard', 'chunk')
  UNION ALL
  {_commit_source_sql(write_id, meta)}
) AS s
ON t.kind = s.kind AND t.array <=> s.array AND t.key <=> s.key AND t.tag <=> s.tag
WHEN NOT MATCHED THEN INSERT ({_COLUMNS})
  VALUES ({", ".join("s." + c for c in M.MANIFEST_COLUMNS)})
""")


def gbx_zarr_untag(spark, table: str, name: str) -> None:
    """Remove a tag: delete the rows where ``tag = name``. Its files become collectable by
    the next vacuum unless a live row or another tag still points at them. A tag that does
    not exist raises ``ZarrVersionUnavailable``."""
    M.check_tag(name)
    M.read_props(spark, table)
    if not _tag_exists(spark, table, name):
        raise ZarrVersionUnavailable(
            f"tag {name!r} does not exist in {table}",
            fix="pass an existing tag name; gbx_zarr_history shows the versions",
        )
    write_id = uuid.uuid4().hex
    meta = {"mode": "untag", "write_id": write_id, "tag": name}
    sql_table = M._q(table)
    spark.sql(f"""
MERGE INTO {sql_table} AS t
USING (
  SELECT kind, array, key, tag FROM {sql_table} WHERE tag = {M._lit(name)}
  UNION ALL
  SELECT 'commit', CAST(NULL AS STRING), {M._lit(write_id)}, CAST(NULL AS STRING)
) AS s
ON t.kind = s.kind AND t.array <=> s.array AND t.key <=> s.key AND t.tag <=> s.tag
WHEN MATCHED AND s.kind <> 'commit' THEN DELETE
WHEN NOT MATCHED AND s.kind = 'commit' THEN INSERT (kind, key, write_id, meta)
  VALUES ('commit', {M._lit(write_id)}, {M._lit(write_id)}, {M._lit(json.dumps(meta, sort_keys=True))})
""")


# --- history ----------------------------------------------------------------------------


def gbx_zarr_history(spark, table: str):
    """One row per commit of the store, oldest first.

    Columns: ``version`` (the store version, the Delta version), ``timestamp``, ``user``,
    ``mode`` (``create``, ``append``, ``update``, ``overwrite``, ``optimize``, ``tag``,
    ``untag``, ``restore``, ``register``, ``sync``, ``publish``), ``write_id``, ``variables``
    (array of the arrays it wrote) and ``region`` (per dimension, the ``[start, stop)`` it
    wrote). Built from the commit rows and the Delta history; versions that are not GeoBrix
    commits (a Delta ``OPTIMIZE``, a property change) are not listed.
    """
    from pyspark.sql import functions as F

    M.read_props(spark, table)
    sql_table = M._q(table)
    commits = spark.sql(
        f"SELECT _metadata.row_commit_version AS version, key AS write_id, meta "
        f"FROM {sql_table} WHERE kind = 'commit' AND tag IS NULL"
    )
    delta = spark.sql(f"DESCRIBE HISTORY {sql_table}").select(
        "version", "timestamp", "userName"
    )
    joined = commits.join(delta, on="version", how="left")
    return joined.select(
        F.col("version"),
        F.col("timestamp"),
        F.coalesce(F.col("userName"), F.get_json_object("meta", "$.user")).alias(
            "user"
        ),
        F.get_json_object("meta", "$.mode").alias("mode"),
        F.col("write_id"),
        F.from_json(F.get_json_object("meta", "$.variables"), "array<string>").alias(
            "variables"
        ),
        F.from_json(
            F.get_json_object("meta", "$.region"), "map<string,array<bigint>>"
        ).alias("region"),
    ).orderBy("version")


# --- restore ----------------------------------------------------------------------------


def _differs(a: pa.Table, b: pa.Table) -> np.ndarray:
    """Per row, whether the content columns of ``a`` and ``b`` (same length) differ."""
    out = np.zeros(a.num_rows, dtype=bool)
    for c in _CONTENT:
        x, y = a.column(c), b.column(c)
        moved = pc.or_(
            pc.xor(pc.is_null(x), pc.is_null(y)),
            pc.fill_null(pc.not_equal(x, y), False),
        )
        out |= np.asarray(moved.to_numpy(zero_copy_only=False), dtype=bool)
    return out


def _upserts(src: pa.Table, write_id: str, expected: pa.Array) -> pa.Table:
    n = src.num_rows
    cols = {c: src.column(c) for c in M.MANIFEST_COLUMNS}
    cols["write_id"] = pa.array([write_id] * n, pa.string())
    cols["tag"] = pa.nulls(n, pa.string())
    cols["op"] = pa.array(["upsert"] * n, pa.string())
    cols["expected_write_id"] = expected
    return pa.table(cols)


def _deletes(src: pa.Table, write_id: str) -> pa.Table:
    n = src.num_rows
    return pa.table(
        {
            "kind": src.column("kind"),
            "array": src.column("array"),
            "key": src.column("key"),
            "write_id": pa.array([write_id] * n, pa.string()),
            "op": pa.array(["delete"] * n, pa.string()),
            "expected_write_id": src.column("write_id"),
        }
    )


def _restore_rows(
    live: pa.Table, want: pa.Table, write_id: str
) -> tuple[pa.Table, pa.Table]:
    """``(commit rows, the upserted rows)`` that turn the rows ``live`` into ``want``.

    Rows are the same row when their (kind, array, key) and content match; a restored row
    takes the restore's ``write_id`` (a commit's upserts must), so equality is by content.
    """
    live_ids, want_ids = _ids(live), _ids(want)
    in_live = np.asarray(
        pc.fill_null(pc.index_in(want_ids, value_set=live_ids), -1), dtype=np.int64
    )
    in_want = np.asarray(
        pc.fill_null(pc.index_in(live_ids, value_set=want_ids), -1), dtype=np.int64
    )
    new = want.take(pa.array(np.flatnonzero(in_live < 0)))
    both = np.flatnonzero(in_live >= 0)
    theirs, ours = want.take(pa.array(both)), live.take(pa.array(in_live[both]))
    moved = np.flatnonzero(_differs(theirs, ours)) if both.size else both
    gone = live.take(pa.array(np.flatnonzero(in_want < 0)))
    upserts = pa.concat_tables(
        [
            _typed(_upserts(new, write_id, pa.nulls(new.num_rows, pa.string()))),
            _typed(
                _upserts(
                    theirs.take(pa.array(moved)),
                    write_id,
                    ours.take(pa.array(moved)).column("write_id"),
                )
            ),
        ]
    )
    return pa.concat_tables([upserts, _typed(_deletes(gone, write_id))]), upserts


def _typed(rows: pa.Table) -> pa.Table:
    return M._typed(rows, M._COMMIT_SCHEMA)


def _require_files(upserts: pa.Table, root: str | None, table: str, version) -> None:
    """Every file the rows being brought back point at must still exist."""
    if root is None or not upserts.num_rows:
        return
    mine = upserts.filter(
        pc.and_(
            pc.is_valid(upserts.column("file")),
            pc.fill_null(pc.not_equal(upserts.column("origin"), "external"), True),
        )
    )
    files = sorted(set(mine.column("file").to_pylist()))
    gone = [
        f
        for f in files
        if not os.path.exists(f if os.path.isabs(f) else os.path.join(root, f))
    ]
    if gone:
        raise ZarrVersionUnavailable(
            f"version {version} of {table} points at {len(gone)} file(s) that no longer "
            f"exist (e.g. {gone[0]})",
            fix="the files were vacuumed; restore a later version or a tag",
        )


def gbx_zarr_restore(
    spark, table: str, *, version_as_of=None, timestamp_as_of=None
) -> int:
    """Make the live rows equal an older version's, as a new commit; returns the version.

    Pass ``version_as_of`` (an int, or ``"tag:<name>"``) or ``timestamp_as_of``. The commit
    upserts the rows the version had and the live rows lack or have changed, and deletes the
    live rows it did not have, under compare-and-swap; nothing is copied and nothing is
    removed, so older versions stay readable and the restore can itself be undone. A chunk
    that was hidden by a tombstone since comes back when the version predates the tombstone,
    and stays hidden when it does not. It refuses a version vacuum has already removed, or
    whose files are gone (``ZarrVersionUnavailable``: restore a later version or a tag). A
    version already equal to the live rows commits nothing and returns the current version.
    """
    if version_as_of is None and timestamp_as_of is None:
        raise ValueError(
            "pass version_as_of or timestamp_as_of. Fix: give the version, tag or time "
            "to restore."
        )
    props = M.read_props(spark, table)
    target = M.snapshot(
        spark, table, version_as_of=version_as_of, timestamp_as_of=timestamp_as_of
    )
    root = props.overlay_root if props.kind == "registered" else props.root
    label = target.version if target.tag is None else f"tag:{target.tag}"
    want = M.read_rows(spark, target, kinds=_DATA_KINDS)
    write_id = uuid.uuid4().hex
    meta = {"mode": "restore", "from": label, "write_id": write_id}
    for _ in range(_ATTEMPTS):
        latest = M.snapshot(spark, table)
        live = M.read_rows(spark, latest, kinds=_DATA_KINDS)
        rows, upserts = _restore_rows(live, want, write_id)
        if rows.num_rows == 0:
            return latest.version
        _require_files(upserts, root, table, label)
        try:
            return M.commit(spark, table, rows, write_id=write_id, meta=meta)
        except M.ConcurrentCommit:
            continue  # re-read the live rows and work out the changes again
    raise ZarrWriteConflict(
        f"gave up restoring {table} after {_ATTEMPTS} attempts",
        fix="retry once the other writers are done",
    )
