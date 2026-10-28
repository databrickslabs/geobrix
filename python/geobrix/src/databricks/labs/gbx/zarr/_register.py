"""``gbx_zarr_register``: put a manifest over an existing Zarr store, in place.

Registering writes no data and changes nothing in the store. It reads the store's
metadata documents and lists its objects once, then records one row per object (a
``shard`` row for a sharded array, a ``chunk`` row for any other) with the object's
identity, plus the metadata documents themselves. Reads of the table then see exactly
those objects, pinned to a version; writes (``writable=True``) go to an overlay root the
table owns and never touch the base.

The listing is a Spark ``binaryFile`` scan that selects only ``path``, ``length`` and
``modificationTime``: the file contents are never read, and the work is spread over the
cluster rather than walked on the driver over FUSE. Spark's file sources skip names that
start with ``.`` or ``_``, so an array whose path has such a name gets a scan of its own,
rooted at its directory. Every object's identity (``"<floor epoch ms>:<size>"``) therefore
comes from the same place, Hadoop's file status. Deriving the rows from the metadata alone
would be cheaper but cannot be complete: a sparse store holds only some of its keys, and
an object's identity needs its size and modification time.

The metadata is read on the driver with plain file reads, not through ``zarr``'s
``LocalStore``, which turns a missing file into "no such key": on a FUSE mount a healthy
file can miss for a moment, and that must not drop an array. A document the Spark listing
(Zarr v3) or the consolidated metadata (Zarr v2) names must exist, so a miss is retried
and then an error; anything else is probed a few times, except a Zarr v3 document the
listing does not show, which it proves absent and is read once. The per-node documents are
the truth; consolidated metadata is compared against them and a disagreement is reported.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr._filemode import resolve as resolve_file_mode
from databricks.labs.gbx.zarr._keys import KeyEncoding
from databricks.labs.gbx.zarr._manifest import (
    FORMAT_VERSION,
    StoreProps,
    _q,
    _split_name,
    commit,
    create_manifest,
    find_commit_version,
    retain_seconds,
)
from databricks.labs.gbx.zarr._meta import (
    NODE_DOCS,
    ArrayMeta,
    isolate_unreadable_groups,
    parse_v2,
    parse_v3,
)
from databricks.labs.gbx.zarr.errors import (
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
)

__all__ = [
    "OWNER_MARKER",
    "check_owner_marker",
    "gbx_zarr_register",
    "read_base_metadata",
    "list_objects",
    "write_owner_marker",
    "qualified_table_name",
    "identity_of",
    "same_identity",
]

_LOG = logging.getLogger(__name__)

OWNER_MARKER = "_gbx_store.json"
_MARKER_KINDS = ("overlay", "managed")

# Names a store keeps its metadata under; none of them is a chunk or a child array.
_DOC_NAMES = ("zarr.json", ".zgroup", ".zarray", ".zattrs", ".zmetadata")
_ZMETADATA = ".zmetadata"

_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")
_GLOB_CHARS = re.compile(r"([\\\[\]{}*?])")
_NUM = r"(?:0|[1-9][0-9]*)"  # a canonical index: no sign, no leading zeros
_PLAIN_NAME = re.compile(r"[a-z0-9_]+")

# Attempts for a document that may or may not exist (a node's probe, a ``.zattrs``: a FUSE
# mount can miss a healthy file once, and losing ``.zattrs`` loses the dimension names and
# the scale factor), and for one the consolidated metadata names.
_PROBE_ATTEMPTS = 3
_HINTED_ATTEMPTS = 6
_WORKERS = 16

_ROW_SCHEMA = pa.schema(
    [
        ("kind", pa.string()),
        ("array", pa.string()),
        ("key", pa.string()),
        ("shard_key", pa.string()),
        ("file", pa.string()),
        ("offset", pa.int64()),
        ("length", pa.int64()),
        ("origin", pa.string()),
        ("identity", pa.string()),
        ("write_id", pa.string()),
        ("op", pa.string()),
        ("expected_write_id", pa.string()),  # NULL: the key must not exist yet
        ("meta", pa.string()),
    ]
)
_OBJECT_SCHEMA = pa.schema(
    [("relpath", pa.string()), ("length", pa.int64()), ("mtime_ms", pa.int64())]
)


def _say(message: str) -> None:
    """Progress for a registration, which can take minutes on a large store."""
    print(f"[register] {message}", flush=True)


def _examples(names, limit: int = 3) -> str:
    names = list(names)
    shown = ", ".join(names[:limit])
    return shown + (f" and {len(names) - limit} more" if len(names) > limit else "")


def _table_exists(spark, table: str) -> bool:
    return bool(spark.catalog.tableExists(table))


# --- identity ---------------------------------------------------------------------


def identity_of(mtime_ms: int, size: int) -> str:
    """The identity of an object: its modification time (whole milliseconds since the
    epoch, UTC, rounded down) and size in bytes, as ``"<mtime_ms>:<size>"``."""
    return f"{int(mtime_ms)}:{int(size)}"


def _parse_identity(identity: str) -> tuple[int, int]:
    m = re.fullmatch(r"(-?\d+):(\d+)", identity) if isinstance(identity, str) else None
    if m is None:
        raise ValueError(
            f"an identity looks like '<mtime_ms>:<size>' (for example "
            f"'1792741018982:3'), got {identity!r}"
        )
    return int(m.group(1)), int(m.group(2))


def same_identity(a: str, b: str, *, mtime_resolution_ms: int = 1) -> bool:
    """Whether two identities name the same object.

    The size must match exactly. The modification times must fall in the same
    ``mtime_resolution_ms`` bucket, so ``1`` compares to the millisecond and ``1000``
    to the second (for a file system that reports whole seconds). The check cannot
    tell apart two versions of a file written within one bucket with the same size.
    """
    if (
        isinstance(mtime_resolution_ms, bool)
        or not isinstance(mtime_resolution_ms, int)
        or mtime_resolution_ms < 1
    ):
        raise ValueError(
            f"mtime_resolution_ms must be a positive integer, got {mtime_resolution_ms!r}"
        )
    (ta, sa), (tb, sb) = _parse_identity(a), _parse_identity(b)
    return sa == sb and ta // mtime_resolution_ms == tb // mtime_resolution_ms


# --- paths ------------------------------------------------------------------------


def _local_root(path: str) -> str:
    """The bare local path of a store or overlay root: no ``dbfs:``/``file:``, normalized.

    A Volume is ``/Volumes/...``, which Spark also spells ``dbfs:/Volumes/...`` (or
    ``dbfs:///Volumes/...``) and the DBFS mount ``/dbfs/Volumes/...``: all of those are the
    same path. Any other DBFS path is refused.
    """
    from databricks.labs.gbx.ds._listing import to_local_path, to_spark_uri

    if not isinstance(path, str) or not path.strip():
        raise ValueError("path must be a non-empty string")
    text = path.strip()
    local = to_local_path(to_spark_uri(text))  # /dbfs/Volumes/x -> /Volumes/x
    if text == "/dbfs" or text.startswith(("/dbfs/", "dbfs:")):
        volume = os.path.normpath(local)  # dbfs:///Volumes/x -> /Volumes/x
        if volume != "/Volumes" and not volume.startswith("/Volumes/"):
            raise ZarrUnsupported(
                f"{path!r} is a DBFS path; GeoBrix registers stores in place from Unity "
                "Catalog Volumes.",
                fix="pass the store's /Volumes/<catalog>/<schema>/<volume>/... path",
            )
    if _SCHEME.match(local):
        raise ZarrUnsupported(
            f"{path!r} is not a path GeoBrix can read in place: only paths on a Unity "
            "Catalog Volume (or any mounted file system) are supported.",
            fix="pass the store's /Volumes/<catalog>/<schema>/<volume>/... path",
        )
    return os.path.normpath(os.path.abspath(local))


def _within(path: str, root: str) -> bool:
    return os.path.commonpath([path, root]) == root


def _check_disjoint(base: str, overlay: str) -> None:
    """The overlay and the base must not contain one another (the base is never written)."""
    real_base, real_overlay = os.path.realpath(base), os.path.realpath(overlay)
    if _within(real_overlay, real_base) or _within(real_base, real_overlay):
        raise ZarrOwnershipError(
            f"overlay_root {overlay} and the store {base} overlap, so writes to the "
            "overlay would land in the store's own files.",
            fix="choose an overlay_root outside the store and not containing it",
        )


# --- owner markers ----------------------------------------------------------------


def qualified_table_name(spark, table: str) -> str:
    """``catalog.schema.table`` in lower case, as the owner marker records it.

    A name with fewer than three parts takes the session's current catalog and schema,
    so ``t`` under two schemas is two owners, and ``T`` and ``t`` are one.
    """
    parts = _split_name(table)
    if len(parts) < 3:
        row = spark.sql("SELECT current_catalog(), current_schema()").first()
        parts = [row[0], row[1]][: 3 - len(parts)] + parts
    return ".".join(
        p if _PLAIN_NAME.fullmatch(p) else "`" + p.replace("`", "``") + "`"
        for p in (q.lower() for q in parts)
    )


def _marker_path(root: str) -> str:
    return os.path.join(root, OWNER_MARKER)


def _owner_error(marker: str, owner: dict, table: str) -> ZarrOwnershipError:
    return ZarrOwnershipError(
        f"{os.path.dirname(marker)} is already owned by table "
        f"{owner.get('table')!r} ({owner.get('kind')}), not by {table!r}.",
        fix=f"choose a different root for {table}, or drop {owner.get('table')} and "
        f"remove {marker} if that store is gone",
    )


class _MarkerUnreadable(ZarrOwnershipError):
    """The marker is there but reading it failed (an I/O error, not bytes that are no
    marker): ``__cause__`` is the ``OSError``. What the marker says is still unknown."""


class _MarkerGarbled(ZarrOwnershipError):
    """The marker was read, but its bytes are not an owner marker (not a JSON object)."""


def _check_owner(marker: str, table: str, kind: str) -> None:
    problem = f"{marker} exists but is not a readable GeoBrix owner marker."
    fix = f"remove {marker} if no table owns this root, or use another root"
    try:
        with open(marker, "rb") as f:
            owner = json.loads(f.read())
        if not isinstance(owner, dict):
            raise ValueError("not an object")
    except OSError as e:
        raise _MarkerUnreadable(problem, fix=fix) from e
    except ValueError:
        raise _MarkerGarbled(problem, fix=fix) from None
    if owner.get("table") != table or owner.get("kind") != kind:
        raise _owner_error(marker, owner, table)


def _read_back(marker: str, table: str, kind: str) -> None:
    """``_check_owner`` with a failed read raised as its ``OSError``, so that
    ``_retry_transient`` retries a transient FUSE miss instead of reporting it."""
    try:
        _check_owner(marker, table, kind)
    except _MarkerUnreadable as e:
        raise e.__cause__ from None


def check_owner_marker(root: str, table: str, kind: str) -> None:
    """Refuse to write under ``root`` unless its owner marker names ``table`` as ``kind``.

    A writer calls this before it puts files under a managed data root or an overlay root:
    the root may have been claimed, or its marker removed, since the table was made. A
    transient miss is retried; a marker that is gone, garbled or names another table
    raises ``ZarrOwnershipError``.
    """
    marker = _marker_path(_local_root(root))
    try:
        _retry_transient(lambda: _read_back(marker, table, kind))
    except FileNotFoundError:
        raise ZarrOwnershipError(
            f"{os.path.dirname(marker)} has no owner marker ({OWNER_MARKER}), so it cannot "
            f"be shown to belong to {table}.",
            fix=f"restore {marker} if {table} still owns this root; otherwise register "
            "or create the store again",
        ) from None
    except OSError as e:
        raise ZarrError(
            f"cannot read the owner marker {marker} ({e}).",
            fix="check that the Volume is mounted and readable, then write again",
        ) from e


def _precheck_owner(root: str, table: str, kind: str) -> None:
    """Refuse a root another table owns, without claiming anything."""
    marker = _marker_path(root)
    if os.path.exists(marker):
        _check_owner(marker, table, kind)


def _remove_marker(marker: str) -> None:
    try:
        os.remove(marker)
    except OSError:
        pass


def _remove_marker_if_ours(marker: str, written: bytes) -> bool:
    """Remove ``marker`` only if it holds exactly ``written``, the bytes this call wrote
    (all of the marker, or the part that landed before a failed write); ``True`` when it
    was removed. A marker with any other content (another table's, or a longer one that
    merely starts the same) is never touched."""
    try:
        held = _retry_transient(lambda: _read_file(marker), attempts=_PROBE_ATTEMPTS)
    except OSError:
        return False
    if held != written:
        return False
    _remove_marker(marker)
    return True


def _claim_marker(root: str, table: str, kind: str) -> bool:
    """Claim ``root`` for ``table``; ``True`` when this call created the marker.

    The marker is created exclusively, written with ``os.write`` until every byte is out
    (the count is kept), then read back under ``_retry_transient``, so a FUSE mount that
    misses the new file for a moment does not fail the claim. Only a read-back that parses
    and names another table leaves the marker alone (it is theirs); on any other failure
    the marker is removed if it holds exactly the bytes this call wrote.
    """
    root = _local_root(root)
    marker = _marker_path(root)
    body = json.dumps(
        {"table": table, "kind": kind, "format": FORMAT_VERSION}, sort_keys=True
    ).encode()
    cannot = f"GeoBrix cannot claim {root} with an owner marker"
    fix = "check that the root is on a Volume you can write to, then try again"
    try:
        os.makedirs(root, exist_ok=True)
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        _check_owner(marker, table, kind)
        return False
    except OSError as e:
        raise ZarrError(f"{cannot} ({e}).", fix=fix) from e
    written = 0
    try:
        try:
            view = memoryview(body)
            while written < len(body):
                n = os.write(fd, view[written:])
                if not n:
                    raise OSError(errno.EIO, "the owner marker write made no progress")
                written += n
        finally:
            os.close(fd)
        # read it back: this call must own it
        _retry_transient(lambda: _read_back(marker, table, kind))
    except _MarkerGarbled:
        if _remove_marker_if_ours(marker, body[:written]):
            raise ZarrError(
                f"{cannot}: the marker it wrote did not read back whole.", fix=fix
            ) from None
        raise
    except ZarrOwnershipError:
        raise  # the marker parsed and names another table: theirs, not ours to remove
    except OSError as e:
        _remove_marker_if_ours(marker, body[:written])
        raise ZarrError(f"{cannot} ({e}).", fix=fix) from e
    except BaseException:
        _remove_marker_if_ours(marker, body[:written])
        raise
    return True


def _release_marker(root: str, table: str, kind: str) -> None:
    """Remove a marker this call created, if it still names this table."""
    marker = _marker_path(root)
    try:
        _check_owner(marker, table, kind)
    except ZarrOwnershipError:
        return
    _remove_marker(marker)


def write_owner_marker(root: str, table: str, kind: str) -> None:
    """Claim ``root`` for ``table`` with the owner marker ``_gbx_store.json``.

    ``table`` is the owner as ``qualified_table_name`` gives it. ``kind`` is
    ``"overlay"`` (a registered store's overlay root) or ``"managed"`` (a managed
    store's data root). Claiming a root the table already owns does nothing; a root owned
    by another table, or by the same table as another kind, raises
    ``ZarrOwnershipError``. The marker is created exclusively and read back, so two
    tables racing for one root cannot both win.
    """
    if kind not in _MARKER_KINDS:
        raise ValueError(f"kind must be one of {_MARKER_KINDS}, got {kind!r}")
    _claim_marker(root, table, kind)


# --- metadata ---------------------------------------------------------------------


def _read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _list_dirs(path: str) -> list[str]:
    with os.scandir(path) as it:
        return sorted(e.name for e in it if e.is_dir())


def _json(raw: bytes, key: str) -> dict:
    try:
        doc = json.loads(raw)
    except ValueError as e:
        raise ZarrUnsupported(
            f"the metadata document {key} is not valid JSON ({e}).",
            fix="repair or rewrite the store's metadata, then register it again",
        ) from None
    if not isinstance(doc, dict):
        raise ZarrUnsupported(
            f"the metadata document {key} is not a JSON object.",
            fix="repair or rewrite the store's metadata, then register it again",
        )
    return doc


def _root_array(root: str) -> ZarrUnsupported:
    return ZarrUnsupported(
        f"{root} is a single Zarr array, not a group of arrays.",
        fix="register a store whose root is a group, such as an xarray Dataset",
    )


class _Reader:
    """Reads a store's metadata documents, telling a missing file from a transient miss.

    ``named`` holds store keys the Spark listing says exist: reading one retries hard and
    then fails. ``hinted`` holds keys the consolidated metadata names, which may be stale:
    those are retried less and, when still missing, reported as absent. ``complete`` says
    ``named`` is the Spark listing of every visible document of the store (Zarr v3 with a
    listing): a visible key it does not name is absent, so it is read once. Anything else
    that is neither named nor hinted may be a healthy file a FUSE mount missed once, and is
    read ``_PROBE_ATTEMPTS`` times: that includes every name Spark's listing skips (it
    starts with ``.`` or ``_``), and a store with no listing.
    """

    def __init__(self, root: str, named=(), hinted=(), complete: bool = False):
        self.root = root
        self.named = set(named)
        self.hinted = set(hinted)
        self.complete = complete

    def _attempts(self, key: str) -> int:
        """Reads of a document that is neither named nor hinted."""
        return 1 if self.complete and not _hidden_path(key) else _PROBE_ATTEMPTS

    def _read(self, key: str, attempts: int) -> bytes | None:
        path = os.path.join(self.root, key)

        def attempt() -> bytes | None:
            try:
                return _read_file(path)  # a miss raises and is retried
            except (IsADirectoryError, NotADirectoryError):
                return None
            except PermissionError as e:  # waiting will not help
                raise ZarrError(
                    f"cannot read {key} of {self.root}: permission denied.",
                    fix="grant read access to the store, then register it again",
                ) from e

        try:
            return _retry_transient(attempt, attempts=attempts)
        except FileNotFoundError:
            return None

    def doc(self, key: str) -> bytes | None:
        if key in self.named:
            raw = self._read(key, 10)
            if raw is None:
                raise ZarrError(
                    f"{key} is listed in {self.root} but could not be read.",
                    fix="check that the Volume is mounted and readable, then register "
                    "the store again",
                )
            return raw
        if key in self.hinted:
            return self._read(key, _HINTED_ATTEMPTS)
        return self._read(key, self._attempts(key))

    def probe(self, path: str, names) -> tuple[str, bytes] | None:
        """The first of the node documents ``names`` that exists at ``path``."""
        keys = [f"{path}/{n}" for n in names]
        stale = False  # a hinted document was missing: the hint is out of date
        for n, key in zip(names, keys):
            if key in self.named:
                return n, self.doc(key)
            if key in self.hinted:
                raw = self.doc(key)
                if raw is not None:
                    return n, raw
                stale = True
        # a hinted key was tried above, with more attempts than a probe makes
        rest = [(n, key) for n, key in zip(names, keys) if key not in self.hinted]

        def attempt() -> tuple[str, bytes]:
            for n, key in rest:
                raw = self._read(key, 1)
                if raw is not None:
                    return n, raw
            raise FileNotFoundError(path)

        if not rest:
            return None
        # after a stale hint the other names are read once: the hinted read already waited
        # out a transient miss, and waiting again would only add to it
        tries = 1 if stale else max(self._attempts(key) for _, key in rest)
        try:
            return _retry_transient(attempt, attempts=tries)
        except FileNotFoundError:
            return None

    def dirs(self, group: str) -> list[str]:
        path = os.path.join(self.root, group) if group else self.root
        try:
            return _retry_transient(lambda: _list_dirs(path), attempts=_PROBE_ATTEMPTS)
        except OSError as e:
            raise ZarrError(
                f"cannot list {path} ({e}).",
                fix="check that the Volume is mounted and readable, then register the "
                "store again",
            ) from e


def _root_info(root: str):
    """The format of the store at ``root`` (2 or 3) and its root documents.

    Raises for anything that is not a group-rooted Zarr store, before any listing.
    """
    if not os.path.isdir(root):
        raise ZarrError(
            f"{root} is not a directory.",
            fix="pass the path of a Zarr store directory on a Volume",
        )
    reader = _Reader(root)
    names = ("zarr.json", ".zgroup", _ZMETADATA, ".zarray")
    seen: dict[str, bytes | None] = {}

    def attempt() -> dict[str, bytes | None]:
        seen.update({n: reader._read(n, 1) for n in names})
        # a .zmetadata without a root document is a miss on the .zgroup until proven
        # otherwise, so it is retried like having found nothing at all
        if all(seen[n] is None for n in ("zarr.json", ".zgroup", ".zarray")):
            raise FileNotFoundError(root)
        return dict(seen)

    try:
        found = _retry_transient(attempt, attempts=_PROBE_ATTEMPTS)
    except FileNotFoundError:
        if seen.get(_ZMETADATA) is not None:
            raise ZarrError(
                f"{root} is not a Zarr store: it has a .zmetadata but no .zgroup.",
                fix="pass the root of a Zarr v2 or v3 store (the directory holding "
                "zarr.json or .zgroup)",
            ) from None
        raise ZarrError(
            f"{root} is not a Zarr store: it has no zarr.json, .zgroup or .zmetadata.",
            fix="pass the root of a Zarr v2 or v3 store (the directory holding zarr.json "
            "or .zgroup)",
        ) from None
    if found[".zgroup"] is not None and found[_ZMETADATA] is None:
        # the attempt that found .zgroup may have missed a healthy .zmetadata once: when
        # the directory shows one, it is read again (a listing is one call; a blind retry
        # would cost every store without consolidated metadata the full backoff)
        try:
            listed = _ZMETADATA in os.listdir(root)
        except OSError:
            listed = False
        if listed:
            found[_ZMETADATA] = reader._read(_ZMETADATA, _HINTED_ATTEMPTS)
    if found["zarr.json"] is not None:
        doc = _json(found["zarr.json"], "zarr.json")
        if doc.get("zarr_format") != 3:
            raise ZarrUnsupported(
                f"zarr.json of {root} has zarr_format {doc.get('zarr_format')!r}.",
                fix="register a Zarr v3 store with a zarr_format 3 zarr.json",
            )
        if doc.get("node_type") == "array":
            raise _root_array(root)
        return 3, found
    if found[".zgroup"] is None:
        raise _root_array(root)  # .zarray without .zgroup
    return 2, found


def _add_array(path, parse, arrays, unsupported, tolerant=False) -> None:
    try:
        arrays[path] = parse()
    except ZarrUnsupported as e:
        unsupported[path] = e.message
    except (KeyError, TypeError, ValueError, IndexError) as e:
        text = f"the metadata of array {path!r} is malformed ({type(e).__name__}: {e})."
        if tolerant:
            unsupported[path] = text
            return
        raise ZarrUnsupported(
            text,
            fix="repair or rewrite the array's metadata, then register the store again",
        ) from None


def _nodes_by_parent(keys) -> dict[str, set[str]]:
    """Child node names of each group, from the store keys of node documents."""
    out: dict[str, set[str]] = {}
    for key in keys:
        node, _, doc = key.rpartition("/")
        if doc in NODE_DOCS and node:
            parent, _, name = node.rpartition("/")
            out.setdefault(parent, set()).add(name)
    return out


def _v3_node(key: str, raw: bytes, tolerant: bool):
    """``(node, None)`` for a readable Zarr v3 node document, else ``(None, problem)``.

    A document that is not JSON is a node nobody can read, as a v2 ``.zarray`` that is not
    JSON is. A ``node_type`` that is neither group nor array is the same for a reader, and
    a structure registration refuses.
    """
    try:
        node = _json(raw, key)
    except ZarrUnsupported as problem:
        return None, problem
    kind = node.get("node_type")
    if kind not in ("group", "array"):
        problem = ZarrUnsupported(
            f"{key} has node_type {kind!r}, expected 'group' or 'array'.",
            fix="repair or rewrite the store's metadata, then register it again",
        )
        if not tolerant:
            raise problem
        return None, problem
    return node, None


def _walk(fmt, reader, pool, tolerant=False):
    names = ("zarr.json",) if fmt == 3 else (".zarray", ".zgroup")
    by_parent = _nodes_by_parent(reader.named | reader.hinted)
    metas: dict[str, bytes] = {}
    arrays: dict[str, ArrayMeta] = {}
    unsupported: dict[str, str] = {}

    def look(task):
        path = task
        hit = reader.probe(path, names)
        zattrs = None
        if hit is not None and fmt == 2:
            zattrs = reader.doc(f"{path}/.zattrs")
        return path, hit, zattrs

    queue = [""]
    while queue:
        group = queue.pop(0)
        # the directory listing, and any child a listing or the consolidated metadata names
        children = set(reader.dirs(group)) | by_parent.get(group, set())
        paths = [
            f"{group}/{c}" if group else c for c in sorted(children - set(_DOC_NAMES))
        ]
        for path, hit, zattrs in pool.map(look, paths):
            if hit is None:  # a stray file, or a directory that holds no node
                continue
            doc, raw = hit
            key = f"{path}/{doc}"
            if fmt == 3:
                node, problem = _v3_node(key, raw, tolerant)
                if problem is not None:
                    # Walk past a node nobody can read. Its document does not say what it
                    # is, so it is an array nobody can decode, named only when asked for,
                    # unless nodes turn up under it: then it is a group, and
                    # isolate_unreadable_groups moves what it holds into ``unsupported``.
                    metas[key] = raw
                    unsupported[path] = problem.message
                    queue.append(path)
                    continue
                metas[key] = raw
                kind = node["node_type"]
                if kind == "group":
                    queue.append(path)
                else:
                    _add_array(
                        path,
                        lambda: parse_v3(path, node),
                        arrays,
                        unsupported,
                        tolerant,
                    )
                continue
            metas[key] = raw
            if zattrs is not None:
                metas[f"{path}/.zattrs"] = zattrs
            if doc == ".zgroup":
                queue.append(path)
                continue
            _add_array(
                path,
                lambda: parse_v2(
                    path,
                    _json(raw, key),
                    _json(zattrs, f"{path}/.zattrs") if zattrs is not None else {},
                ),
                arrays,
                unsupported,
                tolerant,
            )
    isolate_unreadable_groups(metas, arrays, unsupported)
    return metas, arrays, unsupported


def _loads(raw: bytes):
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def _consolidated_v2(raw: bytes):
    """``(documents, problem)`` of a ``.zmetadata``: the parsed documents or why not."""
    try:
        doc = json.loads(raw)
    except ValueError:
        return None, "it is not valid JSON"
    inner = doc.get("metadata") if isinstance(doc, dict) else None
    if not isinstance(inner, dict) or doc.get("zarr_consolidated_format") != 1:
        return None, "it is not Zarr v2 consolidated metadata (format 1)"
    return inner, None


def _disagreements_v2(metas, inner) -> list[str]:
    actual = {
        k: _loads(v)
        for k, v in metas.items()
        if k.rpartition("/")[2] in (".zgroup", ".zarray", ".zattrs")
    }
    out = [
        f"{k} differs"
        for k in sorted(inner.keys() & actual.keys())
        if inner[k] != actual[k]
    ]
    out += [f"{k} is only in .zmetadata" for k in sorted(inner.keys() - actual.keys())]
    out += [f"{k} is not in .zmetadata" for k in sorted(actual.keys() - inner.keys())]
    return out


def _disagreements_v3(metas, root_doc) -> list[str]:
    block = root_doc.get("consolidated_metadata")
    if block is None:
        return []
    inner = block.get("metadata") if isinstance(block, dict) else None
    if not isinstance(inner, dict):
        return ["zarr.json has a consolidated_metadata block that is not readable"]
    actual = {
        k.rpartition("/")[0]: _loads(v)
        for k, v in metas.items()
        if k.endswith("/zarr.json")
    }
    out = []
    for path in sorted(inner.keys() & actual.keys()):
        a, b = inner[path], actual[path]
        if (
            isinstance(a, dict)
            and isinstance(b, dict)
            and a.get("node_type") == "group"
        ):
            same = (
                a.get("attributes") == b.get("attributes")
                and b.get("node_type") == "group"
            )
        else:
            same = a == b
        if not same:
            out.append(f"{path}/zarr.json differs")
    out += [
        f"{p}/zarr.json is only in the consolidated metadata"
        for p in sorted(inner.keys() - actual.keys())
    ]
    out += [
        f"{p}/zarr.json is not in the consolidated metadata"
        for p in sorted(actual.keys() - inner.keys())
    ]
    return out


def read_base_metadata(
    path: str, *, listing: pa.Table | None = None, tolerant: bool = False
) -> tuple[dict[str, bytes], dict[str, ArrayMeta], dict[str, str]]:
    """The metadata documents of the store at ``path``, its arrays, and the ones it cannot decode.

    Returns ``(metas, arrays, unsupported)``. ``metas`` maps each store key
    (``zarr.json``, ``t2m/zarr.json``, ``t2m/.zarray``) to the document's bytes, for
    every group and array. ``arrays`` maps the path of each array GeoBrix can decode to
    its ``ArrayMeta``. ``unsupported`` maps the path of each array it cannot (for
    example a string coordinate) to the reason; its documents are in ``metas`` all the
    same.

    Zarr v3 stores contribute every ``zarr.json``; Zarr v2 stores ``.zgroup``,
    ``.zarray`` and ``.zattrs``, plus ``.zmetadata`` when present. The documents of each
    node are the truth: consolidated metadata (``.zmetadata``, or the
    ``consolidated_metadata`` of the root ``zarr.json``) is kept as it is and compared
    with them, and a disagreement is logged as a warning.

    ``listing`` is an optional Arrow table with a ``relpath`` column, the Spark listing
    of the store. Every ``zarr.json`` it holds is a document that must exist, so a
    transient miss on one is retried rather than dropping the array.

    A node whose document is not JSON is listed in ``unsupported`` with the reason, in
    either Zarr version, and the walk goes on: one corrupt array does not stop its
    neighbours. If nodes lie under it, it is a group and not an array: it is left out of
    ``unsupported`` and the arrays under it are in it instead, for that cause
    (``isolate_unreadable_groups``).

    ``tolerant`` is for a reader: a node whose document parses as JSON but is malformed (a
    missing key, a ``node_type`` that is neither group nor array) is listed in
    ``unsupported`` too, instead of failing the whole walk. Registering leaves it off, and
    refuses such a store: a structure that cannot be read at all should not be registered.
    """
    root = _local_root(path)
    fmt, found = _root_info(root)
    named: set[str] = set()
    hinted: set[str] = set()
    inner, problem = None, None
    if fmt == 3 and listing is not None:
        rel = listing["relpath"]
        keep = pc.or_(pc.equal(rel, "zarr.json"), pc.ends_with(rel, "/zarr.json"))
        named = set(rel.filter(keep).to_pylist())
    if fmt == 2 and found[_ZMETADATA] is not None:
        inner, problem = _consolidated_v2(found[_ZMETADATA])
        hinted = set(inner or ())
    reader = _Reader(root, named, hinted, complete=fmt == 3 and listing is not None)

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        metas, arrays, unsupported = _walk(fmt, reader, pool, tolerant)
    if fmt == 3:
        metas["zarr.json"] = found["zarr.json"]
        problems = _disagreements_v3(metas, _json(found["zarr.json"], "zarr.json"))
        which = "consolidated_metadata of zarr.json"
    else:
        metas[".zgroup"] = found[".zgroup"]
        zattrs = reader.doc(".zattrs")
        if zattrs is not None:
            metas[".zattrs"] = zattrs
        if found[_ZMETADATA] is not None:
            metas[_ZMETADATA] = found[_ZMETADATA]
            problems = (
                [f".zmetadata is not used: {problem}"]
                if problem
                else _disagreements_v2(metas, inner)
            )
        else:
            problems = []
        which = ".zmetadata"
    if problems:
        _LOG.warning(
            "%s: the %s disagrees with the store's own documents, which are used (%s)",
            root,
            which,
            _examples(problems),
        )
    return dict(sorted(metas.items())), arrays, unsupported


# --- layouts ----------------------------------------------------------------------


@dataclass(frozen=True)
class _Layout:
    """What decides which objects are an array's chunks: nothing about its dtype."""

    encoding: KeyEncoding
    ndim: int
    sharded: bool


def _layout_of(meta: ArrayMeta) -> _Layout:
    return _Layout(meta.key_encoding, len(meta.shape), meta.shard is not None)


def _layout_of_unsupported(path: str, metas: dict[str, bytes]) -> _Layout | None:
    """The layout of an array GeoBrix cannot decode, if its grid, key encoding and shard
    spec parse. They do not depend on the dtype, so the array is parsed again as bytes.
    """
    try:
        if f"{path}/zarr.json" in metas:
            doc = json.loads(metas[f"{path}/zarr.json"])
            probe = parse_v3(path, {**doc, "data_type": "uint8", "fill_value": 0})
        else:
            zarray = json.loads(metas[f"{path}/.zarray"])
            zattrs = json.loads(metas.get(f"{path}/.zattrs", b"{}"))
            probe = parse_v2(
                path, {**zarray, "dtype": "|u1", "order": "C", "fill_value": 0}, zattrs
            )
    except (ZarrUnsupported, KeyError, TypeError, ValueError, IndexError):
        return None
    return _layout_of(probe)


def _metadata_rows(metas: dict[str, bytes], array_names, write_id: str) -> pa.Table:
    """One row per metadata document: ``array`` is its node's path, ``key`` the file name."""
    recs = []
    for store_key in sorted(metas):
        prefix, _, name = store_key.rpartition("/")
        try:
            text = metas[store_key].decode("utf-8")
        except UnicodeDecodeError:
            raise ZarrUnsupported(
                f"the metadata document {store_key} is not UTF-8 text.",
                fix="rewrite the store's metadata as UTF-8 JSON, then register it again",
            ) from None
        recs.append(
            {
                "kind": "array" if prefix in array_names else "group",
                "array": prefix or None,
                "key": name,
                "origin": "external",
                "write_id": write_id,
                "op": "upsert",
                "meta": text,
            }
        )
    return pa.Table.from_pylist(recs, schema=_ROW_SCHEMA)


# --- listing ----------------------------------------------------------------------


def _hidden_path(name: str) -> bool:
    """Spark's file sources skip every path with a component that starts with ``.`` or ``_``."""
    return any(part.startswith((".", "_")) for part in name.split("/"))


def _scan(spark, base: str, roots=None) -> pa.Table:
    """Every object Spark's file listing sees under ``roots`` (default ``base``).

    Content is never read. ``relpath`` is relative to ``base``, which must contain every root.
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx.ds._listing import to_local_path, to_spark_uri

    # load() treats a path as a glob
    uris = [_GLOB_CHARS.sub(r"\\\1", to_spark_uri(r)) for r in (roots or [base])]
    listed = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(uris if len(uris) > 1 else uris[0])
        .select(
            F.col("path"),
            F.col("length"),
            F.unix_millis("modificationTime").alias("mtime_ms"),
        )
        .toArrow()
    )
    local = pa.array(
        [to_local_path(p) for p in listed["path"].to_pylist()], pa.string()
    )
    prefix = base.rstrip("/") + "/"
    inside = np.asarray(pc.starts_with(local, prefix))
    if not inside.all():
        outside = local.filter(pa.array(~inside))[0].as_py()
        raise ZarrError(
            f"the listing of {base} returned {outside!r}, which is not under it.",
            fix="register the store by its /Volumes/... path, without symbolic links",
        )
    return pa.table(
        {
            "relpath": pc.utf8_slice_codeunits(local, len(prefix)),
            "length": listed["length"].cast(pa.int64()),
            "mtime_ms": listed["mtime_ms"].cast(pa.int64()),
        },
        schema=_OBJECT_SCHEMA,
    )


def _with_hidden(spark, base: str, visible: pa.Table, hidden_prefixes) -> pa.Table:
    """``visible`` plus the objects of the arrays Spark's listing skips, sorted by path.

    Each such array's directory is scanned as a root of its own: Spark only skips hidden
    names *below* a root, so its objects are listed, with the same identity source.
    """
    objects = visible
    if hidden_prefixes:
        extra = _scan(
            spark, base, [os.path.join(base, p) for p in sorted(hidden_prefixes)]
        )
        fresh = pc.invert(pc.is_in(extra["relpath"], value_set=objects["relpath"]))
        objects = pa.concat_tables([objects, extra.filter(fresh)])
    return objects.sort_by("relpath")


def list_objects(spark, path: str, *, hidden_prefixes=None) -> pa.Table:
    """The objects under the store at ``path``: ``relpath``, ``length``, ``mtime_ms``.

    ``relpath`` is relative to the store root and sorted; ``mtime_ms`` is the
    modification time in whole milliseconds (the floor), which together with ``length``
    makes an object's identity (``identity_of``). Spark's listing skips names starting
    with ``.`` or ``_``, so an array whose path has such a component is listed by a scan
    of its own. ``hidden_prefixes`` names those arrays; ``None`` (the default) finds them
    from the store's metadata, so a listing is never missing an array.
    """
    root = _local_root(path)
    visible = _scan(spark, root)
    if hidden_prefixes is None:
        _, arrays, unsupported = read_base_metadata(root, listing=visible)
        hidden_prefixes = [a for a in (*arrays, *unsupported) if _hidden_path(a)]
    return _with_hidden(spark, root, visible, hidden_prefixes)


# --- rows -------------------------------------------------------------------------


def _key_pattern(layout: _Layout) -> str:
    """A regular expression for the canonical chunk (or shard) keys of an array."""
    enc = layout.encoding
    if layout.ndim == 0:
        return "^" + re.escape(enc.prefix or "0") + "$"
    head = re.escape(enc.prefix + enc.separator) if enc.prefix else ""
    return "^" + head + re.escape(enc.separator).join([_NUM] * layout.ndim) + "$"


def _chunk_rows(objects: pa.Table, layouts, write_id: str) -> tuple[pa.Table, dict]:
    """Manifest rows for the objects of the arrays in ``layouts``, and what was left out.

    A sharded array's objects become ``shard`` rows (``shard_key`` is the key); any
    other array's become ``chunk`` rows (``shard_key`` is NULL). Only canonical chunk
    keys count, so a stray file inside an array's directory is skipped, as is a chunk
    key whose object has zero bytes: an empty file is not a chunk, and a ``length=0``
    row would make the whole store unreadable (the chunk reads as the fill value).
    An array whose layout is ``None`` (its grid or key encoding cannot be read) gets no
    rows, and its objects are counted as ``no_layout``, not as strays. Returns
    ``(rows, counts)`` with the number of rows and, for each thing left out (``empty``
    chunk objects, ``stray`` objects), how many and a few names.
    """
    n = objects.num_rows
    relpath, length = objects.column("relpath"), objects.column("length")
    size = np.asarray(length)
    claimed = np.zeros(n, dtype=bool)  # has a chunk key of an array (empty or not)
    unlaid = np.zeros(n, dtype=bool)  # under an array whose layout cannot be read
    parts = []
    for name in sorted(layouts):
        layout = layouts[name]
        prefix = name + "/"
        under = np.asarray(pc.starts_with(relpath, prefix))
        if layout is None:
            unlaid |= under
            continue
        if not under.any():
            continue
        idx = np.flatnonzero(under)
        sub = objects.take(pa.array(idx))
        key = pc.utf8_slice_codeunits(sub["relpath"], len(prefix))
        is_chunk = np.asarray(pc.match_substring_regex(key, _key_pattern(layout)))
        claimed[idx[is_chunk]] = True
        keep = is_chunk & (size[idx] > 0)
        if not keep.any():
            continue
        kept = sub.filter(pa.array(keep))
        key = pc.utf8_slice_codeunits(kept["relpath"], len(prefix))
        count = kept.num_rows
        parts.append(
            pa.Table.from_arrays(
                [
                    pa.repeat("shard" if layout.sharded else "chunk", count),
                    pa.repeat(name, count),
                    key,
                    key if layout.sharded else pa.nulls(count, pa.string()),
                    kept["relpath"],
                    pa.array(np.zeros(count, dtype=np.int64)),
                    kept["length"],
                    pa.repeat("external", count),
                    # the same text as identity_of
                    pc.binary_join_element_wise(
                        pc.cast(kept["mtime_ms"], pa.string()),
                        pc.cast(kept["length"], pa.string()),
                        ":",
                    ),
                    pa.repeat(write_id, count),
                    pa.repeat("upsert", count),
                    pa.nulls(count, pa.string()),
                    pa.nulls(count, pa.string()),
                ],
                schema=_ROW_SCHEMA,
            )
        )
    is_doc = np.zeros(n, dtype=bool)
    for doc in _DOC_NAMES:
        is_doc |= np.asarray(pc.equal(relpath, doc)) | np.asarray(
            pc.ends_with(relpath, "/" + doc)
        )
    empty = claimed & (size == 0)  # chunk keys with nothing behind them
    unlaid &= ~is_doc
    stray = ~claimed & ~is_doc & ~unlaid

    def named(mask) -> list[str]:
        return relpath.filter(pa.array(mask)).slice(0, 3).to_pylist()

    rows = pa.concat_tables(parts) if parts else _ROW_SCHEMA.empty_table()
    return rows, {
        "objects": rows.num_rows,
        "no_layout": int(unlaid.sum()),
        "empty": int(empty.sum()),
        "empty_examples": named(empty),
        "stray": int(stray.sum()),
        "stray_examples": named(stray),
    }


# --- register ---------------------------------------------------------------------


def _label_unlaid(unsupported: dict[str, str], layouts) -> dict[str, str]:
    """``unsupported`` with the arrays that also have no readable layout labelled as such."""
    return {
        a: (
            reason
            if layouts.get(a) is not None
            else f"{reason} (no layout: its objects are not registered)"
        )
        for a, reason in unsupported.items()
    }


def _discard(spark, table: str, write_id: str) -> bool:
    """Drop a manifest this call created but never committed to; ``True`` when dropped.

    ``False`` when the commit landed, and when that cannot be told (the lookup failed): the
    table is then kept, and so must be the root's owner marker, since a committed store
    that loses its marker can be claimed by another table.
    """
    try:
        if find_commit_version(spark, table, write_id) is None:
            spark.sql(f"DROP TABLE IF EXISTS {_q(table)}")
            return True
    except Exception:
        _LOG.warning(
            "could not remove the partly registered table %s; drop it before "
            "registering again",
            table,
            exc_info=True,
        )
    return False


def gbx_zarr_register(
    spark,
    path: str,
    table: str,
    *,
    writable: bool = False,
    overlay_root: str | None = None,
    file_mode: str = "auto",
    retain: str = "7 days",
) -> int:
    """Register the Zarr store at ``path`` as the manifest table ``table``, in place.

    Nothing is copied and nothing in the store is written: the table's rows point at the
    store's own objects, each recorded with its identity so that a change made outside
    GeoBrix is detected on read (``gbx_zarr_sync`` brings the table up to date). Many
    tables can register one store; each is its own branch with its own history.

    The table is read-only unless ``writable=True``, which needs an ``overlay_root``:
    a directory (on a Volume) outside the store that this table owns and GeoBrix writes
    to, recorded by an owner marker so no other table can claim it. ``file_mode`` is
    ``"auto"`` or ``"fuse"`` (path strings relative to the store root); ``"external"``
    keeps FILE EXTERNAL references and needs a workspace with the File type preview,
    which is not yet available. ``"managed"`` is never allowed for a registered store: it
    would copy the base. ``retain`` is how long superseded versions stay readable.

    An array GeoBrix cannot decode (a string coordinate, say) is registered like any
    other, objects and documents, and listed under ``unsupported`` in the commit's
    metadata; reading it raises, and reading the rest works.

    Returns the table's first store version, the one that holds the registered rows.
    """
    if writable and not overlay_root:
        raise ValueError(
            "writable=True needs an overlay_root, the directory this table writes to. "
            "Fix: pass overlay_root='/Volumes/<catalog>/<schema>/<volume>/<dir>' "
            "(outside the store), or register read-only."
        )
    if overlay_root and not writable:
        raise ValueError(
            "overlay_root is only used by a writable registration. Fix: pass "
            "writable=True with it, or drop overlay_root."
        )
    retain_seconds(retain)
    _q(table)
    mode = resolve_file_mode(
        spark, file_mode, has_filespace=bool(overlay_root), registered=True
    )
    base = _local_root(path)
    overlay = _local_root(overlay_root) if overlay_root else None
    if overlay:
        _check_disjoint(base, overlay)
    if _table_exists(spark, table):
        raise ZarrOwnershipError(
            f"{table} already exists",
            fix="choose a new table name, or read or write the existing store",
        )
    owner = qualified_table_name(spark, table)
    if overlay:
        _precheck_owner(overlay, owner, "overlay")
    _root_info(base)  # not a store: refuse before the (possibly long) listing

    started = time.monotonic()
    _say(f"listing the objects of {base}")
    visible = _scan(spark, base)
    _say(f"reading the metadata of {base}")
    metas, arrays, unsupported = read_base_metadata(base, listing=visible)
    names = set(arrays) | set(unsupported)
    hidden = [a for a in names if _hidden_path(a)]
    _say(
        f"{len(arrays)} array(s) found"
        + (f"; {len(hidden)} need a scan of their own" if hidden else "")
    )
    objects = _with_hidden(spark, base, visible, hidden)
    _say(f"listed {objects.num_rows} object(s) in {time.monotonic() - started:.1f}s")

    write_id = uuid.uuid4().hex
    layouts = {a: _layout_of(m) for a, m in arrays.items()}
    layouts.update({a: _layout_of_unsupported(a, metas) for a in unsupported})
    chunk_rows, counts = _chunk_rows(objects, layouts, write_id)
    if counts["empty"]:
        _LOG.warning(
            "%s: %d zero-byte object(s) were not registered, so their chunks read as "
            "the fill value (e.g. %s)",
            base,
            counts["empty"],
            _examples(counts["empty_examples"]),
        )
    if counts["stray"]:
        _LOG.warning(
            "%s: %d object(s) that are not chunks of any array were left out (e.g. %s)",
            base,
            counts["stray"],
            _examples(counts["stray_examples"]),
        )
    unsupported = _label_unlaid(unsupported, layouts)
    if unsupported:
        _LOG.warning(
            "%s: %d array(s) use features GeoBrix cannot decode and are registered "
            "without being readable: %s",
            base,
            len(unsupported),
            _examples(f"{a} ({r})" for a, r in sorted(unsupported.items())),
        )
        _say(
            f"{len(unsupported)} array(s) cannot be decoded: {_examples(sorted(unsupported))}"
        )
    rows = pa.concat_tables([_metadata_rows(metas, names, write_id), chunk_rows])
    commit_meta = {"mode": "register", "objects": counts["objects"]}
    if counts["empty"] or counts["stray"]:
        commit_meta["skipped"] = {
            "empty": counts["empty"],
            "stray": counts["stray"],
        }
    if unsupported:
        commit_meta["unsupported"] = dict(sorted(unsupported.items()))

    claimed = made = False
    try:
        if overlay:
            claimed = _claim_marker(overlay, owner, "overlay")
        create_manifest(
            spark,
            table,
            StoreProps(
                kind="registered",
                root=base,
                overlay_root=overlay,
                writable=writable,
                file_mode=mode,
                retain=retain,
            ),
        )
        made = True
        _say(f"committing {rows.num_rows} row(s) to {table}")
        version = commit(spark, table, rows, write_id=write_id, meta=commit_meta)
    except BaseException:
        # a table that may hold a commit (the lookup could not tell) keeps its marker
        dropped = _discard(spark, table, write_id) if made else True
        if claimed and dropped:
            _release_marker(overlay, owner, "overlay")
        raise
    groups = len({k.rpartition("/")[0] for k in metas} - names)
    _say(
        f"registered {table} at version {version}: {counts['objects']} object(s), "
        f"{len(arrays) + len(unsupported)} array(s), {groups} group(s) in "
        f"{time.monotonic() - started:.1f}s"
    )
    return version
