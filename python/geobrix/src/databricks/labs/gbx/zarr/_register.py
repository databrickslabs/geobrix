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
start with ``.`` or ``_``, so an array whose path has such a name is listed on the
driver instead (``ds._listing.list_files``, which guards FUSE's transient misses). The
metadata is read on the driver with ``zarr``'s ``LocalStore``; deriving the rows from
the metadata alone would be cheaper but cannot be complete, since a sparse store holds
only some of its keys and an object's identity needs its size and modification time.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr._filemode import resolve as resolve_file_mode
from databricks.labs.gbx.zarr._manifest import (
    FORMAT_VERSION,
    StoreProps,
    _q,
    commit,
    create_manifest,
    find_commit_version,
    retain_seconds,
)
from databricks.labs.gbx.zarr._meta import ArrayMeta, parse_v2, parse_v3
from databricks.labs.gbx.zarr.errors import (
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
)

__all__ = [
    "OWNER_MARKER",
    "gbx_zarr_register",
    "read_base_metadata",
    "list_objects",
    "write_owner_marker",
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
    shown = ", ".join(names[:limit])
    return shown + (f" and {len(names) - limit} more" if len(names) > limit else "")


# --- paths ------------------------------------------------------------------------


def _local_root(path: str) -> str:
    """The bare local path of a store or overlay root: no ``dbfs:``/``file:``, normalized."""
    from databricks.labs.gbx.ds._listing import to_local_path

    if not isinstance(path, str) or not path.strip():
        raise ValueError("path must be a non-empty string")
    local = to_local_path(path.strip())
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


def _owner_error(marker: str, owner: dict, table: str) -> ZarrOwnershipError:
    return ZarrOwnershipError(
        f"{os.path.dirname(marker)} is already owned by table "
        f"{owner.get('table')!r} ({owner.get('kind')}), not by {table!r}.",
        fix=f"choose a different root for {table}, or drop {owner.get('table')} and "
        f"remove {marker} if that store is gone",
    )


def write_owner_marker(root: str, table: str, kind: str) -> None:
    """Claim ``root`` for ``table`` with the owner marker ``_gbx_store.json``.

    ``kind`` is ``"overlay"`` (a registered store's overlay root) or ``"managed"`` (a
    managed store's data root). Claiming a root the table already owns does nothing;
    a root owned by another table, or by the same table as another kind, raises
    ``ZarrOwnershipError``. The marker is created exclusively, so two tables racing for
    one root cannot both win.
    """
    if kind not in _MARKER_KINDS:
        raise ValueError(f"kind must be one of {_MARKER_KINDS}, got {kind!r}")
    root = _local_root(root)
    marker = os.path.join(root, OWNER_MARKER)
    body = json.dumps(
        {"table": table, "kind": kind, "format": FORMAT_VERSION}, sort_keys=True
    ).encode()
    os.makedirs(root, exist_ok=True)
    try:
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        _check_owner(marker, table, kind)
        return
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(body)
    except BaseException:
        try:
            os.remove(marker)
        except OSError:
            pass
        raise


def _check_owner(marker: str, table: str, kind: str) -> None:
    try:
        with open(marker, "rb") as f:
            owner = json.loads(f.read())
        if not isinstance(owner, dict):
            raise ValueError("not an object")
    except (OSError, ValueError):
        raise ZarrOwnershipError(
            f"{marker} exists but is not a readable GeoBrix owner marker.",
            fix=f"remove {marker} if no table owns this root, or use another root",
        ) from None
    if owner.get("table") != table or owner.get("kind") != kind:
        raise _owner_error(marker, owner, table)


# --- metadata ---------------------------------------------------------------------


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


def read_base_metadata(path: str) -> tuple[dict[str, bytes], dict[str, ArrayMeta]]:
    """The metadata documents of the store at ``path``, and its arrays.

    Returns ``(metas, arrays)``: ``metas`` maps each store key (``zarr.json``,
    ``t2m/zarr.json``, ``t2m/.zarray``) to the document's bytes; ``arrays`` maps each
    array's path to its ``ArrayMeta``. Zarr v3 stores contribute every ``zarr.json``.
    Zarr v2 stores contribute ``.zgroup``, ``.zarray`` and ``.zattrs``, and ``.zmetadata``
    when present: consolidated metadata is preferred, so the groups and arrays are taken
    from it without walking the directories (the same choice zarr-python and xarray make).
    A directory without a metadata document is not a node and is skipped.
    """
    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync
    from zarr.storage import LocalStore

    root = _local_root(path)
    if not os.path.isdir(root):
        raise ZarrError(
            f"{root} is not a directory.",
            fix="pass the path of a Zarr store directory on a Volume",
        )
    store = LocalStore(root, read_only=True)
    proto = default_buffer_prototype()

    def get(key: str) -> bytes | None:
        def read() -> bytes | None:
            buf = sync(store.get(key, proto))
            return None if buf is None else buf.to_bytes()

        return _retry_transient(read, attempts=3)

    def children(prefix: str) -> list[str]:
        async def names() -> list[str]:
            return sorted([n async for n in store.list_dir(prefix)])

        return _retry_transient(lambda: sync(names()), attempts=3)

    def root_documents() -> dict[str, bytes | None]:
        found = {n: get(n) for n in ("zarr.json", ".zgroup", _ZMETADATA, ".zarray")}
        if all(v is None for v in found.values()):
            raise FileNotFoundError(root)
        return found

    try:
        found = _retry_transient(root_documents, attempts=3)
    except FileNotFoundError:
        raise ZarrError(
            f"{root} is not a Zarr store: it has no zarr.json, .zgroup or .zmetadata.",
            fix="pass the root of a Zarr v2 or v3 store (the directory holding zarr.json "
            "or .zgroup)",
        ) from None

    if found["zarr.json"] is not None:
        return _walk_v3(root, found["zarr.json"], get, children)
    if found[".zarray"] is not None and found[".zgroup"] is None:
        raise _root_array(root)
    if found[_ZMETADATA] is not None:
        return _read_consolidated_v2(root, found[_ZMETADATA])
    return _walk_v2(root, found[".zgroup"], get, children)


def _walk_v3(root, root_doc, get, children):
    doc = _json(root_doc, "zarr.json")
    if doc.get("zarr_format") != 3:
        raise ZarrUnsupported(
            f"zarr.json of {root} has zarr_format {doc.get('zarr_format')!r}.",
            fix="register a Zarr v3 store with a zarr_format 3 zarr.json",
        )
    if doc.get("node_type") == "array":
        raise _root_array(root)
    metas: dict[str, bytes] = {"zarr.json": root_doc}
    arrays: dict[str, ArrayMeta] = {}
    queue = [""]
    while queue:
        group = queue.pop(0)
        for name in children(group):
            if name in _DOC_NAMES:
                continue
            path = f"{group}/{name}" if group else name
            raw = get(f"{path}/zarr.json")
            if raw is None:  # a stray file, or a directory that holds no node
                continue
            node = _json(raw, f"{path}/zarr.json")
            kind = node.get("node_type")
            if kind == "group":
                metas[f"{path}/zarr.json"] = raw
                queue.append(path)
            elif kind == "array":
                metas[f"{path}/zarr.json"] = raw
                arrays[path] = parse_v3(path, node)
            else:
                raise ZarrUnsupported(
                    f"{path}/zarr.json has node_type {kind!r}, expected 'group' or 'array'.",
                    fix="repair or rewrite the store's metadata, then register it again",
                )
    return metas, arrays


def _walk_v2(root, root_group, get, children):
    if root_group is None:
        raise ZarrError(
            f"{root} is not a Zarr store: it has no zarr.json, .zgroup or .zmetadata.",
            fix="pass the root of a Zarr v2 or v3 store",
        )
    metas: dict[str, bytes] = {".zgroup": root_group}
    attrs = get(".zattrs")
    if attrs is not None:
        metas[".zattrs"] = attrs
    arrays: dict[str, ArrayMeta] = {}
    queue = [""]
    while queue:
        group = queue.pop(0)
        for name in children(group):
            if name in _DOC_NAMES:
                continue
            path = f"{group}/{name}" if group else name
            zarray = get(f"{path}/.zarray")
            zgroup = None if zarray is not None else get(f"{path}/.zgroup")
            if zarray is None and zgroup is None:  # a stray file, or no node
                continue
            zattrs = get(f"{path}/.zattrs")
            if zattrs is not None:
                metas[f"{path}/.zattrs"] = zattrs
            if zarray is not None:
                metas[f"{path}/.zarray"] = zarray
                arrays[path] = parse_v2(
                    path,
                    _json(zarray, f"{path}/.zarray"),
                    _json(zattrs, f"{path}/.zattrs") if zattrs is not None else {},
                )
            else:
                metas[f"{path}/.zgroup"] = zgroup
                queue.append(path)
    return metas, arrays


def _read_consolidated_v2(root, raw):
    doc = _json(raw, _ZMETADATA)
    inner = doc.get("metadata")
    if doc.get("zarr_consolidated_format") != 1 or not isinstance(inner, dict):
        raise ZarrUnsupported(
            f"{_ZMETADATA} of {root} is not Zarr v2 consolidated metadata (format 1).",
            fix=f"remove {_ZMETADATA} so the store's own documents are read, or "
            "consolidate it again",
        )
    metas = {_ZMETADATA: raw}
    for key in sorted(inner):
        metas[key] = json.dumps(inner[key]).encode()
    arrays: dict[str, ArrayMeta] = {}
    for key in sorted(inner):
        prefix, _, name = key.rpartition("/")
        if name != ".zarray":
            continue
        if not prefix:
            raise _root_array(root)
        zattrs = inner.get(f"{prefix}/.zattrs") or {}
        arrays[prefix] = parse_v2(prefix, inner[key], zattrs)
    return metas, arrays


def _metadata_rows(metas: dict[str, bytes], arrays, write_id: str) -> pa.Table:
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
                "kind": "array" if prefix in arrays else "group",
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


def _scan(spark, root: str) -> pa.Table:
    """Every object Spark's file listing sees under ``root``, without reading any content."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.ds._listing import to_local_path, to_spark_uri

    uri = _GLOB_CHARS.sub(r"\\\1", to_spark_uri(root))  # load() treats a path as a glob
    listed = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(uri)
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
    prefix = root.rstrip("/") + "/"
    inside = np.asarray(pc.starts_with(local, prefix))
    if not inside.all():
        outside = local.filter(pa.array(~inside))[0].as_py()
        raise ZarrError(
            f"the listing of {root} returned {outside!r}, which is not under it.",
            fix="register the store by a path without symbolic links",
        )
    return pa.table(
        {
            "relpath": pc.utf8_slice_codeunits(local, len(prefix)),
            "length": listed["length"].cast(pa.int64()),
            "mtime_ms": listed["mtime_ms"].cast(pa.int64()),
        },
        schema=_OBJECT_SCHEMA,
    )


def _list_on_driver(root: str, prefixes) -> pa.Table:
    """Objects under ``prefixes`` (including hidden names), listed and stat-ed on the driver."""
    from databricks.labs.gbx.ds._listing import list_files

    paths = [
        p
        for prefix in prefixes
        for p in list_files(os.path.join(root, prefix), raise_on_empty=False)
    ]

    def stat(p: str) -> tuple[int, int]:
        st = _retry_transient(lambda: os.stat(p))
        return st.st_size, st.st_mtime_ns // 1_000_000

    with ThreadPoolExecutor(max_workers=min(16, max(1, len(paths)))) as pool:
        stats = list(pool.map(stat, paths))
    skip = len(root.rstrip("/")) + 1
    return pa.table(
        {
            "relpath": [p[skip:] for p in paths],
            "length": [s[0] for s in stats],
            "mtime_ms": [s[1] for s in stats],
        },
        schema=_OBJECT_SCHEMA,
    )


def list_objects(spark, path: str, *, hidden_prefixes=()) -> pa.Table:
    """The objects under the store at ``path``: ``relpath``, ``length``, ``mtime_ms``.

    ``relpath`` is relative to the store root and sorted; ``mtime_ms`` is the
    modification time in whole milliseconds (the floor), which together with ``length``
    makes an object's identity (``"<mtime_ms>:<length>"``). Spark's listing skips names
    starting with ``.`` or ``_``, including hidden metadata files; ``hidden_prefixes``
    names array paths that have such a component, whose objects are listed on the
    driver instead.
    """
    root = _local_root(path)
    objects = _scan(spark, root)
    if hidden_prefixes:
        extra = _list_on_driver(root, sorted(hidden_prefixes))
        fresh = pc.invert(pc.is_in(extra["relpath"], value_set=objects["relpath"]))
        objects = pa.concat_tables([objects, extra.filter(fresh)])
    return objects.sort_by("relpath")


# --- rows -------------------------------------------------------------------------


def _key_pattern(meta: ArrayMeta) -> str:
    """A regular expression for the canonical chunk (or shard) keys of ``meta``'s array."""
    enc, ndim = meta.key_encoding, len(meta.shape)
    if ndim == 0:
        return "^" + re.escape(enc.prefix or "0") + "$"
    head = re.escape(enc.prefix + enc.separator) if enc.prefix else ""
    return "^" + head + re.escape(enc.separator).join([_NUM] * ndim) + "$"


def _chunk_rows(objects: pa.Table, arrays, write_id: str) -> tuple[pa.Table, dict]:
    """Manifest rows for the objects of ``arrays``, and what was left out.

    A sharded array's objects become ``shard`` rows (``shard_key`` is the key); any
    other array's become ``chunk`` rows (``shard_key`` is NULL). Only canonical chunk
    keys count, so a stray file inside an array's directory is skipped, as is a chunk
    key whose object has zero bytes: an empty file is not a chunk, and a ``length=0``
    row would make the whole store unreadable (the chunk reads as the fill value).
    Returns ``(rows, counts)`` with the number of rows and, for each thing left out
    (``empty`` chunk objects, ``stray`` objects), how many and a few names.
    """
    n = objects.num_rows
    relpath, length = objects.column("relpath"), objects.column("length")
    size = np.asarray(length)
    claimed = np.zeros(n, dtype=bool)  # has a chunk key of an array (empty or not)
    parts = []
    for name in sorted(arrays):
        meta = arrays[name]
        prefix = name + "/"
        under = np.asarray(pc.starts_with(relpath, prefix))
        if not under.any():
            continue
        idx = np.flatnonzero(under)
        sub = objects.take(pa.array(idx))
        key = pc.utf8_slice_codeunits(sub["relpath"], len(prefix))
        is_chunk = np.asarray(pc.match_substring_regex(key, _key_pattern(meta)))
        claimed[idx[is_chunk]] = True
        keep = is_chunk & (size[idx] > 0)
        if not keep.any():
            continue
        kept = sub.filter(pa.array(keep))
        key = pc.utf8_slice_codeunits(kept["relpath"], len(prefix))
        count = kept.num_rows
        sharded = meta.shard is not None
        parts.append(
            pa.Table.from_arrays(
                [
                    pa.repeat("shard" if sharded else "chunk", count),
                    pa.repeat(name, count),
                    key,
                    key if sharded else pa.nulls(count, pa.string()),
                    kept["relpath"],
                    pa.array(np.zeros(count, dtype=np.int64)),
                    kept["length"],
                    pa.repeat("external", count),
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
    stray = ~claimed & ~is_doc

    def named(mask) -> list[str]:
        return relpath.filter(pa.array(mask)).slice(0, 3).to_pylist()

    rows = pa.concat_tables(parts) if parts else _ROW_SCHEMA.empty_table()
    return rows, {
        "objects": rows.num_rows,
        "empty": int(empty.sum()),
        "empty_examples": named(empty),
        "stray": int(stray.sum()),
        "stray_examples": named(stray),
    }


# --- register ---------------------------------------------------------------------


def _discard(spark, table: str, write_id: str) -> None:
    """Drop a manifest this registration created but never committed to."""
    try:
        if find_commit_version(spark, table, write_id) is None:
            spark.sql(f"DROP TABLE IF EXISTS {_q(table)}")
    except Exception:
        _LOG.warning(
            "could not remove the partly registered table %s; drop it before "
            "registering again",
            table,
            exc_info=True,
        )


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
    ``"auto"`` or ``"fuse"`` (path strings relative to the store root); ``retain`` is how
    long superseded versions stay readable.

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
    mode = resolve_file_mode(spark, file_mode, has_filespace=False)
    base = _local_root(path)
    overlay = _local_root(overlay_root) if overlay_root else None
    if overlay:
        _check_disjoint(base, overlay)
    if spark.catalog.tableExists(table):
        raise ZarrOwnershipError(
            f"{table} already exists",
            fix="choose a new table name, or read or write the existing store",
        )

    started = time.monotonic()
    _say(f"reading the metadata of {base}")
    metas, arrays = read_base_metadata(base)
    groups = len({k.rpartition("/")[0] for k in metas} - set(arrays))
    _say(f"{len(arrays)} array(s) found; listing the store's objects")
    listed = time.monotonic()
    objects = list_objects(
        spark, base, hidden_prefixes=[a for a in arrays if _hidden_path(a)]
    )
    _say(f"listed {objects.num_rows} object(s) in {time.monotonic() - listed:.1f}s")

    write_id = uuid.uuid4().hex
    chunk_rows, counts = _chunk_rows(objects, arrays, write_id)
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
    rows = pa.concat_tables([_metadata_rows(metas, arrays, write_id), chunk_rows])
    commit_meta = {"mode": "register", "objects": counts["objects"]}
    if counts["empty"] or counts["stray"]:
        commit_meta["skipped"] = {
            "empty": counts["empty"],
            "stray": counts["stray"],
        }

    if overlay:
        write_owner_marker(overlay, table, "overlay")
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
    _say(f"committing {rows.num_rows} row(s) to {table}")
    try:
        version = commit(spark, table, rows, write_id=write_id, meta=commit_meta)
    except BaseException:
        _discard(spark, table, write_id)
        raise
    _say(
        f"registered {table} at version {version}: {counts['objects']} object(s), "
        f"{len(arrays)} array(s), {groups} group(s) in {time.monotonic() - started:.1f}s"
    )
    return version
