"""Drift, ``gbx_zarr_sync`` and publish: a registered store whose files other writers change.

GeoBrix cannot make another program's writes to a store transactional. What it can do is
notice them. Every external object a registered table points at carries the identity it had
when it was listed (``"<mtime_ms>:<size>"``), and a read or a read-modify-write compares
the file against it first (``_access.check_identity``): a file that changed outside GeoBrix
raises ``ZarrStoreDrift`` instead of returning bytes the manifest never described.

``gbx_zarr_sync`` records those changes as one table version. It lists the store again and
diffs the listing against the live rows:

- an object whose identity changed is ``changed``, one the table does not know ``new``, one
  that is gone ``deleted``;
- a key GeoBrix has written over keeps its overlay. When the base moved too it is ``both``:
  an unsharded chunk row remembers the identity of the base object it replaced, and the
  chunk rows inside a shard are compared through the shard's own row;
- metadata documents are compared as the raw JSON, so a change to attributes alone is seen.
  An array's extent becomes the larger of the base's and the overlay's. A change to an
  array's dtype, chunk grid or codecs cannot be followed and raises ``ZarrUnsupported``.

``publish`` is the opposite direction, and explicit: it writes the overlay into the base
and points the table at the result. It refuses when the base moved since the last sync.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.core._arrow import to_spark_df
from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr._access import (
    FuseRangeReader,
    check_identity,
    identities_match,
)
from databricks.labs.gbx.zarr._filemode import FileLayout
from databricks.labs.gbx.zarr._meta import doc_bytes, same_layout, with_shape
from databricks.labs.gbx.zarr._register import (
    _chunk_rows,
    _hidden_path,
    _layout_of,
    _layout_of_unsupported,
    _metadata_rows,
    _say,
    _scan,
    _with_hidden,
    check_owner_marker,
    identity_of,
    qualified_table_name,
    read_base_metadata,
)
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr._store import ManifestStore
from databricks.labs.gbx.zarr.errors import (
    ZarrReadOnly,
    ZarrStoreDrift,
    ZarrUnsupported,
    ZarrWriteConflict,
)

__all__ = ["check_identity", "gbx_zarr_sync", "publish"]

_LOG = logging.getLogger(__name__)

_LIVE_KINDS = ["group", "array", "shard", "chunk"]
_DOC_KINDS = ("group", "array")
_SEP = "\x1f"
_WORKERS = 8  # base objects written at once
_LOCK_KEY = "publish"
# A publish that died leaves its lock row. Nothing clears it by itself: a caller who knows
# no publish is running passes ``break_lock=True``, which works on a lock at least this old
# (a younger one may belong to a publish that is still writing).
_LOCK_BREAK_AFTER_SECONDS = 3600
_CONSOLIDATED = "consolidated_metadata"

# Tests set this to run code while a publish holds its lock.
_AFTER_LOCK = None

# What a publish records an object's identity from: the stat a reader's compare sees (the
# mount's), taken again until a Volume has settled the object's time (see ``_settled``).
# Tests replace the three.
_stat = os.stat
_sleep = time.sleep
_clock = time.monotonic
_SETTLE_SECONDS = 2.0  # the longest a publish waits, in all, for the times to settle
_SETTLE_STEP = 0.1  # between two looks at the same objects
_VOLUMES = "/Volumes/"

_REPORT_DDL = "change STRING, array STRING, key STRING"
_SYNC_AGAIN = "run gbx_zarr_sync again"


# --- the diff ---------------------------------------------------------------------


class _Diff:
    """What a listing says about the live rows: a report line and commit rows per change."""

    def __init__(self, write_id: str):
        self.write_id = write_id
        self.report: list[tuple[str, str | None, str]] = []
        self.rows: list[dict] = []

    def __bool__(self) -> bool:
        return bool(self.report)

    def upsert(self, change: str, row: dict, expected: str | None, **over) -> None:
        self.report.append((change, row["array"], row["key"]))
        self.rows.append(
            {
                **row,
                **over,
                "write_id": self.write_id,
                "op": "upsert",
                "expected_write_id": expected,
            }
        )

    def delete(self, change: str, live: dict) -> None:
        self.report.append((change, live["array"], live["key"]))
        self.rows.append(
            {
                "kind": live["kind"],
                "array": live["array"],
                "key": live["key"],
                "write_id": self.write_id,
                "op": "delete",
                "expected_write_id": live["write_id"],
            }
        )

    def counts(self) -> dict:
        out = {"changed": 0, "new": 0, "deleted": 0, "both": 0}
        for change, _, _ in self.report:
            out[change] += 1
        return out


def _ids(table: pa.Table) -> pa.Array:
    return pc.binary_join_element_wise(
        table.column("kind"), table.column("array"), table.column("key"), _SEP
    )


def _rows(table: pa.Table, idx) -> list[dict]:
    return table.take(pa.array(np.asarray(idx, dtype=np.int64))).to_pylist()


def _positions(ids: pa.Array, other: pa.Array) -> np.ndarray:
    """For each of ``ids`` the index of the same id in ``other``, -1 where there is none."""
    found = pc.index_in(ids, value_set=other)
    return np.asarray(pc.fill_null(found, -1))


def _diff_data(diff: _Diff, live: pa.Table, new: pa.Table) -> None:
    """Shard and chunk rows: ``live`` (the table now) against ``new`` (the listing).

    The comparison is columnar; only the rows that changed become Python objects.
    """
    gbx = pc.fill_null(pc.equal(live.column("origin"), "gbx"), False)
    # shards that hold chunk rows GeoBrix wrote: a base change to one keeps the overlay
    held = live.filter(
        pc.and_(
            gbx,
            pc.and_(
                pc.equal(live.column("kind"), "chunk"),
                pc.is_valid(live.column("shard_key")),
            ),
        )
    )
    overlaid = set(
        zip(held.column("array").to_pylist(), held.column("shard_key").to_pylist())
    )
    live_ids, new_ids = _ids(live), _ids(new)
    in_live = _positions(new_ids, live_ids)
    in_new = _positions(live_ids, new_ids)
    is_gbx = np.asarray(gbx)

    def by(change: str, row: dict) -> str:
        shard = row["kind"] == "shard" and (row["array"], row["key"]) in overlaid
        return "both" if shard else change

    for n in _rows(new, np.flatnonzero(in_live < 0)):
        diff.upsert(by("new", n), n, None)

    both = np.flatnonzero(in_live >= 0)
    if both.size:
        theirs = new.take(pa.array(both))
        ours = live.take(pa.array(in_live[both]))
        moved = np.asarray(
            pc.fill_null(
                pc.not_equal(theirs.column("identity"), ours.column("identity")), True
            )
        )
        # strings that differ can still be one file, seen at the resolution in force
        for i in np.flatnonzero(moved):
            a, b = theirs.column("identity")[int(i)], ours.column("identity")[int(i)]
            if a.is_valid and b.is_valid and identities_match(a.as_py(), b.as_py()):
                moved[i] = False
        for n, old, overlay in zip(
            theirs.filter(pa.array(moved)).to_pylist(),
            ours.filter(pa.array(moved)).to_pylist(),
            is_gbx[in_live[both]][moved],
        ):
            if overlay:  # an unsharded chunk GeoBrix wrote over a base object
                diff.upsert("both", old, old["write_id"], identity=n["identity"])
            else:
                diff.upsert(by("changed", n), n, old["write_id"])

    # a base object that is gone: the row of one the table points at, or the identity an
    # overlay chunk remembered for the object it replaced
    identity_known = np.asarray(pc.is_valid(live.column("identity")))
    for old in _rows(live, np.flatnonzero((in_new < 0) & (~is_gbx | identity_known))):
        if old["origin"] == "gbx":
            diff.upsert("both", old, old["write_id"], identity=None)
        else:
            diff.delete(by("deleted", old), old)


def _store_key(array: str | None, key: str) -> str:
    return f"{array}/{key}" if array else key


def _diff_documents(diff: _Diff, live: list[dict], new: list[dict], arrays) -> None:
    """Metadata documents, compared as the raw text the store holds.

    A document GeoBrix wrote (``origin="gbx"``) is an array whose extent it grew, or a
    group or array it made. Where the store has the document too, it wins, except that an
    array keeps the larger extent of the two. A document only GeoBrix has stays.
    """
    old = {(r["kind"], r["array"], r["key"]): r for r in live}
    seen = set()
    for doc in new:
        ident = (doc["kind"], doc["array"], doc["key"])
        seen.add(ident)
        ours = old.get(ident)
        if ours is None:
            diff.upsert("new", doc, None)
        elif ours["origin"] != "gbx":
            if ours["meta"] != doc["meta"]:
                diff.upsert("changed", doc, ours["write_id"])
        else:
            text, grown = _with_our_extent(ours, doc, arrays)
            if json.loads(ours["meta"]) != json.loads(text):
                over = {"meta": text, "origin": "gbx" if grown else "external"}
                diff.upsert("both", doc, ours["write_id"], **over)
    for ident, ours in old.items():
        if ident not in seen and ours["origin"] != "gbx":
            diff.delete("deleted", ours)


def _with_our_extent(ours: dict, doc: dict, arrays) -> tuple[str, bool]:
    """The store's document ``doc`` with the larger of its extent and our ``ours``, and
    whether ours is the larger. Only an array GeoBrix can decode has an extent to keep, and
    only its ``zarr.json`` or ``.zarray`` carries one; any other document is the store's.
    """
    meta = arrays.get(doc["array"]) if doc["kind"] == "array" else None
    if meta is None or doc["key"] not in ("zarr.json", ".zarray"):
        return doc["meta"], False
    shape = json.loads(ours["meta"]).get("shape")
    if not isinstance(shape, list) or len(shape) != len(meta.shape):
        return doc["meta"], False
    larger = tuple(max(a, b) for a, b in zip(shape, meta.shape))
    if larger == tuple(
        meta.shape
    ):  # the store's extent wins: its own text stays as it is
        return doc["meta"], False
    return doc_bytes(with_shape(meta, larger))[doc["key"]].decode(), True


def _table_arrays(live_docs: list[dict]) -> dict:
    """The arrays GeoBrix can decode, from the documents the table holds."""
    from databricks.labs.gbx.zarr._read import parse_documents

    held = {
        _store_key(r["array"], r["key"]): r["meta"].encode()
        for r in live_docs
        if r["meta"] is not None
    }
    return parse_documents(held)[0]


def _check_layouts(live_docs: list[dict], arrays) -> None:
    """Refuse a store whose array changed dtype, chunk grid or codecs under the table."""
    before = _table_arrays(live_docs)
    for name, now in sorted(arrays.items()):
        was = before.get(name)
        if was is not None and not same_layout(was, now):
            raise ZarrUnsupported(
                f"array {name!r} changed its dtype, chunk grid or codecs outside GeoBrix, "
                "so the table's chunks no longer describe it.",
                fix="re-register the store",
            )


def _inspect(spark, base: str):
    """``(objects, metas, arrays, unsupported)`` of the store at ``base``, as register reads it."""
    _say(f"listing the objects of {base}")
    visible = _scan(spark, base)
    metas, arrays, unsupported = read_base_metadata(base, listing=visible)
    hidden = [a for a in (*arrays, *unsupported) if _hidden_path(a)]
    return _with_hidden(spark, base, visible, hidden), metas, arrays, unsupported


def _compare(spark, base: str, live: pa.Table, write_id: str) -> tuple[_Diff, dict]:
    """The diff of the store at ``base`` against the live rows ``live``, and its arrays."""
    objects, metas, arrays, unsupported = _inspect(spark, base)
    layouts = {a: _layout_of(m) for a, m in arrays.items()}
    layouts.update({a: _layout_of_unsupported(a, metas) for a in unsupported})
    listed, _ = _chunk_rows(objects, layouts, write_id)
    docs = _metadata_rows(metas, set(arrays) | set(unsupported), write_id)
    kinds = live.column("kind")
    is_doc = pc.is_in(kinds, value_set=pa.array(list(_DOC_KINDS)))
    live_docs = live.filter(is_doc).to_pylist()
    _check_layouts(live_docs, arrays)
    diff = _Diff(write_id)
    _diff_data(diff, live.filter(pc.invert(is_doc)), listed)
    _diff_documents(diff, live_docs, docs.to_pylist(), arrays)
    return diff, arrays


def _report(spark, report) -> "DataFrame":  # noqa: F821
    rows = sorted(report, key=lambda r: (r[1] or "", r[2], r[0]))
    table = pa.Table.from_pylist(
        [{"change": c, "array": a, "key": k} for c, a, k in rows],
        schema=pa.schema([(n, pa.string()) for n in ("change", "array", "key")]),
    )
    return to_spark_df(spark, table, _REPORT_DDL)


# --- locks ------------------------------------------------------------------------


def _locks(spark, snap) -> list[dict]:
    return M.read_rows(spark, snap, kinds="lock").to_pylist()


def _lock_age(lock: dict) -> float | None:
    """Seconds since the lock was taken; ``None`` for a lock row nobody can date."""
    try:
        return time.time() - float(json.loads(lock["meta"])["started"])
    except (TypeError, ValueError, KeyError):
        return None


def _ago(seconds: float) -> str:
    if seconds < 120:
        return f"{seconds:.0f} seconds"
    if seconds < 7200:
        return f"{seconds / 60:.0f} minutes"
    return f"{seconds / 3600:.0f} hours"


def _breakable(lock: dict) -> bool:
    age = _lock_age(lock)
    return age is None or age >= _LOCK_BREAK_AFTER_SECONDS


def _locked(table: str, lock: dict, break_lock: bool = False) -> ZarrWriteConflict:
    """Why ``table`` cannot be synced or published now, and exactly how to clear a dead lock."""
    age = _lock_age(lock)
    when = "" if age is None else f", taken {_ago(age)} ago"
    hours = _LOCK_BREAK_AFTER_SECONDS // 3600
    if break_lock and not _breakable(lock):
        why = (
            f"The publish lock of {table}{when} is too young to break: the publish may "
            "still be writing."
        )
    else:
        why = f"{table} has a publish lock{when}: a publish is running, or was killed."
    return ZarrWriteConflict(
        why,
        fix="wait for the publish to finish. If it was killed and none is running, clear "
        f"the lock with gbx_zarr_sync({table!r}, break_lock=True), then publish again "
        f'with gbx_zarr_export({table!r}, method="publish"); a lock can be broken once '
        f"it is at least {hours} hour old",
        keys=(_LOCK_KEY,),
        winner=lock.get("write_id"),
    )


def _release_row_of(lock: dict, write_id: str) -> dict:
    return {
        "kind": "lock",
        "key": _LOCK_KEY,
        "write_id": write_id,
        "op": "delete",
        "expected_write_id": lock["write_id"],
    }


def _break_lock(
    spark, table: str, lock: dict, break_lock: bool, files: FileLayout | None = None
) -> None:
    """Clear a lock a sync found, or refuse: only on request, and only an old one."""
    if not (break_lock and _breakable(lock)):
        raise _locked(table, lock, break_lock)
    wid = uuid.uuid4().hex
    try:
        M.commit(
            spark,
            table,
            pa.Table.from_pylist([_release_row_of(lock, wid)]),
            write_id=wid,
            meta={"mode": "publish-lock-broken"},
            files=files,
        )
    except (ZarrWriteConflict, M.ConcurrentCommit):
        raise _locked(table, lock) from None  # it changed under us: look again
    _LOG.warning("broke the publish lock of %s (taken %s ago)", table, _lock_age(lock))


def _take_lock(
    spark, table: str, break_lock: bool = False, files: FileLayout | None = None
) -> str:
    """Insert the publish lock row; returns its ``write_id``.

    The insert is a compare-and-swap against "no lock", so of two publishes one gets the
    row and the other ``ZarrWriteConflict``. A lock that is there is replaced only with
    ``break_lock=True`` and only when it is old enough (``_LOCK_BREAK_AFTER_SECONDS``).
    """
    held = _locks(spark, M.snapshot(spark, table, files=files))
    expected = None
    if held:
        if not (break_lock and _breakable(held[0])):
            raise _locked(table, held[0], break_lock)
        expected = held[0]["write_id"]
        _LOG.warning(
            "breaking the publish lock of %s (taken %s ago)", table, _lock_age(held[0])
        )
    lock = uuid.uuid4().hex
    row = {
        "kind": "lock",
        "key": _LOCK_KEY,
        "write_id": lock,
        "op": "upsert",
        "expected_write_id": expected,
        "meta": json.dumps({"started": time.time()}),
    }
    try:
        M.commit(
            spark,
            table,
            pa.Table.from_pylist([row]),
            write_id=lock,
            meta={"mode": "publish-lock"},
            files=files,
        )
    except (ZarrWriteConflict, M.ConcurrentCommit):
        now = _locks(spark, M.snapshot(spark, table, files=files))
        raise _locked(table, now[0] if now else {}) from None
    return lock


def _release_row(lock: str, write_id: str) -> dict:
    return {
        "kind": "lock",
        "key": _LOCK_KEY,
        "write_id": write_id,
        "op": "delete",
        "expected_write_id": lock,
    }


def _release(spark, table: str, lock: str, files: FileLayout | None = None) -> None:
    """Drop the lock row on a publish that stopped short; never hides the error that did."""
    wid = uuid.uuid4().hex
    try:
        M.commit(
            spark,
            table,
            pa.Table.from_pylist([_release_row(lock, wid)]),
            write_id=wid,
            meta={"mode": "publish-release"},
            files=files,
        )
    except Exception:
        _LOG.warning(
            "could not release the publish lock of %s; clear it with break_lock=True "
            "once it is %d hour old",
            table,
            _LOCK_BREAK_AFTER_SECONDS // 3600,
            exc_info=True,
        )


# --- sync -------------------------------------------------------------------------


def _registered(spark, table: str, what: str) -> M.StoreProps:
    props = M.read_props(spark, table)
    if props.kind != "registered":
        raise ZarrUnsupported(
            f"{what} applies to a store registered in place, and {table} holds its own data",
            fix="pass the table of a store made by gbx_zarr_register",
        )
    return props


def _commit_diff(
    spark,
    table: str,
    diff: _Diff,
    arrays: dict,
    mode: str,
    files: FileLayout | None = None,
) -> int:
    rows = pa.Table.from_pylist(diff.rows, schema=M._COMMIT_SCHEMA)
    meta = {"mode": mode, **diff.counts()}
    sharded = {name: m.shard is not None for name, m in arrays.items()}
    try:
        return M.commit(
            spark,
            table,
            rows,
            write_id=diff.write_id,
            meta=meta,
            sharded=sharded,
            files=files,
        )
    except (ZarrWriteConflict, M.ConcurrentCommit):
        raise ZarrWriteConflict(
            f"{table} changed while it was being synced",
            fix=_SYNC_AGAIN,
        ) from None


def gbx_zarr_sync(
    spark, table: str, *, break_lock: bool = False
) -> "DataFrame":  # noqa: F821
    """Bring the manifest ``table`` of a registered store up to date with its files.

    Lists the store again and records what other writers changed as one table version.
    Returns a DataFrame with one row per change: ``change`` (``"changed"``, ``"new"``,
    ``"deleted"`` or ``"both"``), ``array`` and ``key`` (a metadata document is
    ``zarr.json``, ``.zarray``, ``.zattrs`` or ``.zgroup`` of its array or group).

    - ``changed``: the object (or document) has a new identity, or new contents.
    - ``new`` and ``deleted``: the store gained or lost an object.
    - ``both``: the store changed an object GeoBrix has written over. The overlay is kept and
      still wins on read; the base's change is recorded.

    An array's extent becomes the larger of the store's and the overlay's. A store whose
    array changed its dtype, chunk grid or codecs raises ``ZarrUnsupported`` (register it
    again). Nothing is committed when nothing changed.

    A sync refuses while a publish holds its lock on the table (``ZarrWriteConflict``). A
    publish that was killed leaves the lock behind; once it is at least an hour old,
    ``break_lock=True`` clears it and the sync goes on. A younger lock is never broken.
    """
    props = _registered(spark, table, "gbx_zarr_sync")
    files = FileLayout.of(props)
    held = _locks(spark, M.snapshot(spark, table, files=files))
    if held:
        _break_lock(spark, table, held[0], break_lock, files)
    snap = M.snapshot(spark, table, files=files)
    live = M.read_rows(spark, snap, kinds=_LIVE_KINDS)
    diff, arrays = _compare(spark, props.root, live, uuid.uuid4().hex)
    if diff:
        version = _commit_diff(spark, table, diff, arrays, "sync", files)
        _say(f"synced {table} at version {version}: {diff.counts()}")
    else:
        _say(f"{table} is up to date")
    return _report(spark, diff.report)


# --- publish ----------------------------------------------------------------------


def _identity_of(path: str) -> str:
    st = _stat(path)
    return identity_of(st.st_mtime_ns // 1_000_000, st.st_size)


def _settled(path: str, identity: str) -> bool:
    """Whether ``identity`` is the one a reader will see. A Unity Catalog Volume reports whole
    seconds only (the time the object reached storage), but just after a rename it still shows
    the local time, with a fraction: so a fractional time under ``/Volumes`` is not yet the
    one the mount will keep (measured: it then moves up by 0.3 to 1 s)."""
    if not os.path.abspath(path).startswith(_VOLUMES):
        return True
    return int(identity.partition(":")[0]) % 1000 == 0


def _settle(paths) -> dict[str, str]:
    """The identity of each of ``paths``, taken once the mount has settled their times.

    A rename returns before a Volume has settled the object's modification time, so the
    identity read right after it (``_atomic_write``'s) is not the one every later read
    compares with: recorded as it is, it makes every read of the published object raise
    ``ZarrStoreDrift``. The objects are looked at again, together, until a look changes
    nothing and no Volume object still shows a fraction of a second; that takes the mount a
    second at most, and all the objects of a publish wait at once. The wait is bounded by
    ``_SETTLE_SECONDS``: past it the latest identities are returned, with a warning, and
    ``gbx_zarr_sync`` puts any that moved right.
    """
    paths = sorted(set(paths))
    if not paths:
        return {}
    deadline = _clock() + _SETTLE_SECONDS
    seen = {p: _retry_transient(lambda p=p: _identity_of(p)) for p in paths}
    while _clock() < deadline:
        _sleep(_SETTLE_STEP)
        now = {p: _retry_transient(lambda p=p: _identity_of(p)) for p in paths}
        stable, seen = now == seen, now
        if stable and all(_settled(p, i) for p, i in seen.items()):
            return seen
    loose = [p for p, i in seen.items() if not _settled(p, i)]
    if loose:
        _LOG.warning(
            "%d object(s) had not settled their modification time after %.0f s (e.g. "
            "%s); if a read reports them as changed, run gbx_zarr_sync",
            len(loose),
            _SETTLE_SECONDS,
            loose[0],
        )
    return seen


def _restamp(rows: list[dict], base: str) -> None:
    """Give every object row a publish wrote the identity the mount settled on."""
    held = [r for r in rows if r.get("identity") and r.get("file")]
    settled = _settle(os.path.join(base, r["file"]) for r in held)
    for r in held:
        r["identity"] = settled[os.path.join(base, r["file"])]


def _atomic_write(path: str, parts, write_id: str) -> str:
    """Write ``parts`` (byte strings) to ``path`` through a temp name and a rename, and
    return the identity of the result as the rename left it. On a Volume that is not final:
    a caller that records it settles it first (``_restamp``)."""
    folder = os.path.dirname(path)
    os.makedirs(folder, exist_ok=True)
    temp = os.path.join(folder, f".{os.path.basename(path)}.{write_id}.tmp")
    try:
        with open(temp, "wb") as f:
            for part in parts:
                f.write(part)
        os.replace(temp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temp)
        raise
    return _identity_of(path)


def _object_row(kind, array, key, path, base, identity, write_id, expected) -> dict:
    """The row of a base object this publish wrote: external, with its new identity."""
    return {
        "kind": kind,
        "array": array,
        "key": key,
        "shard_key": key if kind == "shard" else None,
        "file": os.path.relpath(path, base).replace(os.sep, "/"),
        "offset": 0,
        "length": int(identity.rpartition(":")[2]),
        "origin": "external",
        "identity": identity,
        "write_id": write_id,
        "op": "upsert",
        "expected_write_id": expected,
    }


def _delete_of(row: dict, write_id: str) -> dict:
    return {
        "kind": row["kind"],
        "array": row["array"],
        "key": row["key"],
        "write_id": write_id,
        "op": "delete",
        "expected_write_id": row["write_id"],
    }


def _copy_chunk(reader, base: str, row: dict, write_id: str) -> dict:
    """An unsharded overlay chunk's bytes into the base object at its canonical key."""
    path = os.path.join(base, row["array"], row["key"])
    region = Region(row["file"], row["offset"], row["length"], "gbx")
    identity = _atomic_write(path, reader.read([region]), write_id)
    return _object_row(
        "chunk",
        row["array"],
        row["key"],
        path,
        base,
        identity,
        write_id,
        row["write_id"],
    )


def _rebuild_shard(store, reader, base: str, array: str, key: str, write_id: str):
    """The shard ``key`` of ``array`` as the table now reads it, whole, into the base."""
    path = os.path.join(base, array, key)
    shard = store._virtual_shard(f"{array}/{key}")

    def parts():
        for part in shard.pieces(0, shard.size):
            yield reader.read([part])[0] if isinstance(part, Region) else part

    return path, _atomic_write(path, parts(), write_id)


def _publish_shards(pool, reader, base, meta, keys, inner, shard_rows, write_id):
    """Rows for the shards of one sharded array that hold overlay chunks: the rebuilt shard
    as an external row, and the overlay chunk rows (tombstones too) gone. A shard left with
    only tombstones is removed."""
    rows = []
    for k in keys:
        rows += inner[k]
        if k in shard_rows:
            rows.append(shard_rows[k])
    store = ManifestStore({}, rows, reader, {meta.name: meta})
    out = []
    try:
        jobs = {}
        for array, key in keys:
            if f"{array}/{key}" in store._shards:
                jobs[array, key] = pool.submit(
                    _rebuild_shard, store, reader, base, array, key, write_id
                )
            else:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(os.path.join(base, array, key))
        for array, key in keys:
            old = shard_rows.get((array, key))
            if (array, key) in jobs:
                path, identity = jobs[array, key].result()
                expected = None if old is None else old["write_id"]
                out.append(
                    _object_row(
                        "shard", array, key, path, base, identity, write_id, expected
                    )
                )
            elif old is not None:
                out.append(_delete_of(old, write_id))
            out += [_delete_of(c, write_id) for c in inner[array, key]]
    finally:
        store.close()
    return out


def _reconsolidate(base: str, groups: list[dict], write_id: str) -> list[dict]:
    """Rows for the consolidated copies of the metadata, rebuilt after a publish.

    A copy (``.zmetadata`` in Zarr v2, the ``consolidated_metadata`` block of a group's
    ``zarr.json`` in v3) is a snapshot of the per-node documents, so it is stale once
    documents were written. Each group that had one gets it again from ``zarr`` itself, read
    from the documents now in the store; a store that had none is left without.
    """
    import warnings

    import zarr

    out = []
    for row in groups:
        had = row["key"] == ".zmetadata" or (
            row["key"] == "zarr.json"
            and row["meta"] is not None
            and _CONSOLIDATED in json.loads(row["meta"])
        )
        if not had:
            continue
        folder = os.path.join(base, *(row["array"].split("/") if row["array"] else ()))
        with (
            warnings.catch_warnings()
        ):  # zarr warns that v3 consolidation is not in the spec
            warnings.simplefilter("ignore")
            zarr.consolidate_metadata(folder)
        with open(os.path.join(folder, row["key"]), encoding="utf-8") as f:
            text = f.read()
        out.append(
            {
                **{k: row[k] for k in ("kind", "array", "key")},
                "origin": "external",
                "meta": text,
                "write_id": write_id,
                "op": "upsert",
                "expected_write_id": row["write_id"],
            }
        )
    return out


def publish(spark, table: str, *, break_lock: bool = False) -> int:
    """Write the overlay of a registered, writable store into the store itself.

    This is what ``gbx_zarr_export(method="publish")`` runs. In order:

    1. a lock row is inserted (a second publish raises ``ZarrWriteConflict``);
    2. the store is listed again; if anything in it changed since the last
       ``gbx_zarr_sync`` the publish stops with ``ZarrStoreDrift``;
    3. each overlay chunk's bytes go to the store's own key, one object at a time, to a temp
       name and then renamed; a sharded array rebuilds each affected shard from the shard
       the table reads (every inner chunk) and writes it whole;
    4. the metadata documents GeoBrix wrote go last, and the consolidated copies of the
       metadata (which would be stale) are rebuilt from them;
    5. one commit turns the overlay rows into external rows with the new identities and
       releases the lock.

    Older versions of the table that point at an object this overwrote raise
    ``ZarrStoreDrift`` when read. A publish that stops after step 3 leaves the store ahead
    of the table; ``gbx_zarr_sync`` then reports those objects. Returns the version of the
    publishing commit, or the current version when there was nothing to publish.

    A publish killed before it finished leaves its lock row, and the next sync or publish
    refuses it. Once the lock is at least an hour old, ``break_lock=True`` replaces it; a
    younger lock is never broken, since its publish may still be writing.
    """
    props = M.read_props(spark, table)
    if props.kind != "registered":
        raise ZarrReadOnly(
            f"{table} holds its own data, so there is no store to publish into",
            fix="publish applies to a store registered in place: gbx_zarr_register(..., "
            "writable=True, overlay_root=...)",
        )
    if not props.writable or not props.overlay_root:
        raise ZarrReadOnly(
            f"{table} is a read-only registration of {props.root}, so it has no overlay "
            "to publish",
            fix="register the store with writable=True and an overlay_root",
        )
    check_owner_marker(
        props.overlay_root, qualified_table_name(spark, table), "overlay"
    )
    files = FileLayout.of(props)
    snap = M.snapshot(spark, table, files=files)
    ours = M.read_rows(spark, snap, kinds=_LIVE_KINDS)
    if not pc.any(pc.equal(ours.column("origin"), "gbx")).as_py():
        return snap.version
    lock = _take_lock(spark, table, break_lock, files)
    try:
        if _AFTER_LOCK is not None:
            _AFTER_LOCK()
        return _publish(spark, table, props, lock)
    except BaseException:
        _release(spark, table, lock, files)
        raise


def _publish(spark, table: str, props, lock: str) -> int:
    base, write_id = props.root, uuid.uuid4().hex
    files = FileLayout.of(props)
    live = M.read_rows(spark, M.snapshot(spark, table, files=files), kinds=_LIVE_KINDS)
    diff, _ = _compare(spark, base, live, write_id)
    if diff:
        shown = ", ".join(
            f"{c} {_store_key(a, k)}" for c, a, k in sorted(diff.report)[:3]
        )
        raise ZarrStoreDrift(
            f"{len(diff.report)} object(s) of {base} changed since the last sync "
            f"({shown}{' ...' if len(diff.report) > 3 else ''}), so publishing could "
            "overwrite them.",
            fix=f"run gbx_zarr_sync({table!r}), then publish again",
        )
    kind = live.column("kind")
    overlay = live.filter(pc.fill_null(pc.equal(live.column("origin"), "gbx"), False))
    docs = overlay.filter(
        pc.is_in(overlay.column("kind"), value_set=pa.array(list(_DOC_KINDS)))
    ).to_pylist()
    arrays = _table_arrays(
        live.filter(pc.is_in(kind, value_set=pa.array(list(_DOC_KINDS)))).to_pylist()
    )
    chunks = overlay.filter(pc.equal(overlay.column("kind"), "chunk")).to_pylist()
    shard_tombstones = overlay.filter(
        pc.and_(
            pc.equal(overlay.column("kind"), "shard"),
            pc.is_null(overlay.column("file")),
        )
    ).to_pylist()
    flat = [c for c in chunks if c["shard_key"] is None]
    inner: dict[tuple, list[dict]] = {}
    for c in chunks:
        if c["shard_key"] is not None:
            inner.setdefault((c["array"], c["shard_key"]), []).append(c)
    # a whole shard of the overlay (gbx_zarr_optimize folded it) is published like one
    # that has overlay chunks over it
    for r in overlay.filter(
        pc.and_(
            pc.equal(overlay.column("kind"), "shard"),
            pc.is_valid(overlay.column("file")),
        )
    ).to_pylist():
        inner.setdefault((r["array"], r["key"]), [])
    wanted = pa.array([_SEP.join(k) for k in inner], pa.string())
    shards = live.filter(pc.equal(kind, "shard"))
    shards = shards.filter(
        pc.is_in(
            pc.binary_join_element_wise(
                shards.column("array"), shards.column("key"), _SEP
            ),
            value_set=wanted,
        )
    )
    shard_rows = {(r["array"], r["key"]): r for r in shards.to_pylist()}
    reader = FuseRangeReader(props.overlay_root, base_root=base)
    out: list[dict] = []

    # data: one object per unsharded chunk, one per affected shard
    copies = [c for c in flat if c["file"] is not None]
    for c in flat:
        if c["file"] is None:  # a chunk deleted from an unsharded array
            with contextlib.suppress(FileNotFoundError):
                os.unlink(os.path.join(base, c["array"], c["key"]))
            out.append(_delete_of(c, write_id))
    for (
        r
    ) in (
        shard_tombstones
    ):  # a shard hidden for good; one with chunks over it is rebuilt
        if (r["array"], r["key"]) not in inner:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(os.path.join(base, r["array"], r["key"]))
            out.append(_delete_of(r, write_id))
    by_array: dict[str, list[tuple]] = {}
    for array, key in sorted(inner):
        by_array.setdefault(array, []).append((array, key))
    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        out += pool.map(lambda c: _copy_chunk(reader, base, c, write_id), copies)
        for array, keys in by_array.items():
            out += _publish_shards(
                pool, reader, base, arrays[array], keys, inner, shard_rows, write_id
            )

    # metadata last
    for d in docs:
        folder = os.path.join(base, *(d["array"].split("/") if d["array"] else ()))
        _atomic_write(os.path.join(folder, d["key"]), [d["meta"].encode()], write_id)
        out.append(
            {
                **{k: d[k] for k in ("kind", "array", "key", "meta")},
                "origin": "external",
                "write_id": write_id,
                "op": "upsert",
                "expected_write_id": d["write_id"],
            }
        )
    if docs:
        groups = live.filter(pc.equal(kind, "group")).to_pylist()
        out += _reconsolidate(base, groups, write_id)

    _restamp(
        out, base
    )  # after every write: the first ones have had the longest to settle
    out.append(_release_row(lock, write_id))
    meta = {
        "mode": "publish",
        "chunks": len(copies),
        "shards": len(inner),
        "documents": len(docs),
    }
    sharded = {name: m.shard is not None for name, m in arrays.items()}
    return M.commit(
        spark,
        table,
        pa.Table.from_pylist(out, schema=M._COMMIT_SCHEMA),
        write_id=write_id,
        meta=meta,
        sharded=sharded,
        files=files,
    )
