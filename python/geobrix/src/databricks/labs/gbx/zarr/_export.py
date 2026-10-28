"""``gbx_zarr_export``: a store version as a plain Zarr copy, a reference file or a publish.

**copy** writes the version at canonical keys, in the store's own format, so any Zarr reader
opens it with no GeoBrix and no Delta. Each object is one shard (or one chunk of an
unsharded array). A shard whose rows are one standard shard is copied byte for byte; a
shard with chunk rows laid over it (appends and updates not yet folded by
``gbx_zarr_optimize``), or made of chunk rows alone, is assembled from them without
decoding. A tombstone (a chunk or shard that was deleted) writes no object, so readers see
the fill value. The work is distributed with ``mapInArrow``, one batch of shards per task,
by bytes. The metadata documents are written last, the root group's document the very last,
and the consolidated metadata is made again from the documents it is exported with (the
store's own consolidated copy, which a later write leaves stale, is never copied).

The copy is written in place: into ``to`` when it is an empty directory, into a new
directory when it does not exist. If anything fails the objects already written are removed
(a directory made for the copy is removed with them), and the version is checked before any
byte is written (``ZarrVersionUnavailable``). It does not stage in a sibling directory and
rename it: renaming a directory is not reliable on a Volume mount. A plain reader only
opens a store once its root document exists, which is written after every object.

**reference** writes one kerchunk (version 1) JSON file: the metadata documents as strings
and one ``[absolute_path, offset, length]`` per shard or chunk, pointing into the pack files
and base objects in place, so nothing is copied. A reference names one range per key, so a
sharded array must hold each shard as one region (``gbx_zarr_optimize`` folds the rest). It
pins the version, but vacuum can delete the pack files it points at: tag the version
(``gbx_zarr_tag``) to keep them. The reference is JSON only (Parquet references do not
support Zarr v3, measured), at about 100 bytes per object: a million shards make a file of
about 100 MB.

**publish** is ``gbx_zarr_sync.publish``.
"""

from __future__ import annotations

import contextlib
import heapq
import json
import math
import os
import shutil
import uuid
import warnings
from dataclasses import dataclass
from typing import Literal, overload

import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.ds._listing import to_local_path
from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr._access import (
    FUSE,
    FuseRangeReader,
    _within,
    resolve_access,
)
from databricks.labs.gbx.zarr._maint import _STREAM_WINDOW, _require_files, _stream
from databricks.labs.gbx.zarr._meta import xarray_fill_form
from databricks.labs.gbx.zarr._read import (
    _open_table,
    _work_frame,
    check_access,
    classify_source,
)
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr._store import ManifestStore
from databricks.labs.gbx.zarr._sync import _atomic_write, publish
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrStoreDrift, ZarrUnsupported

__all__ = ["gbx_zarr_export"]

_METHODS = ("copy", "reference", "publish")
_TASK_BYTES = 512 << 20  # bytes of shards one task copies (parallelism, not memory)
_MAX_TASKS = 512  # the same ceiling as the sampler's stage
_COLUMNS = [
    "kind",
    "key",
    "shard_key",
    "file",
    "offset",
    "length",
    "origin",
    "identity",
]
_REPORT = pa.schema(
    [("array", pa.string()), ("key", pa.string()), ("nbytes", pa.int64())]
)
_REPORT_DDL = "array STRING, key STRING, nbytes BIGINT"
_OPTIMIZE_FIX = 'run gbx_zarr_optimize first, or use method="copy"'

# a test hook: runs on the driver after the objects are written, before the documents
_AFTER_OBJECTS = None


# --- arguments --------------------------------------------------------------------------


def _check_arguments(source, to, method, version_as_of, access) -> None:
    if method not in _METHODS:
        raise ValueError(
            f"method must be one of {_METHODS}, got {method!r}. "
            "Fix: pass 'copy', 'reference' or 'publish'."
        )
    if classify_source(source) != "table":
        raise ValueError(
            f"source must be the manifest table of a store, not the path {source!r}. "
            "Fix: register the store with gbx_zarr_register and export its table."
        )
    check_access(access)
    if method != "reference" and access != "auto":
        raise ValueError(
            f"access applies to method='reference', which writes the location of each "
            f"file; method={method!r} does not take it. Fix: leave access out."
        )
    if method == "publish":
        if to is not None or version_as_of is not None:
            raise ValueError(
                "method='publish' writes the live overlay into the base store, so it "
                "takes no target and no version_as_of. "
                "Fix: call gbx_zarr_export(spark, table, method='publish')."
            )
        return
    if not isinstance(to, str) or not to.strip():
        raise ValueError(
            f"method={method!r} needs a target: a directory for 'copy', a .json path "
            "for 'reference'. Fix: pass to=<path>."
        )
    if method == "reference":
        if not to.lower().endswith(".json"):
            raise ValueError(
                f"a reference file is JSON (kerchunk version 1), so to must end in "
                f".json, got {to!r}; Parquet references do not support Zarr v3. "
                "Fix: pass a path ending in .json."
            )


def _check_copy_target(path: str, to: str) -> None:
    if not os.path.exists(path):
        return
    if not os.path.isdir(path):
        raise ValueError(
            f"{to} is not a directory. "
            "Fix: pass a directory that does not exist yet, or an empty one."
        )
    if os.listdir(path):
        raise ValueError(
            f"{to} is not empty, and a copy must not mix into other files. "
            "Fix: pass a directory that does not exist yet, or an empty one."
        )


def _check_reference_target(path: str, to: str) -> None:
    if os.path.exists(path):
        raise ValueError(
            f"{to} already exists. Fix: pass a path that does not exist, or remove it."
        )


# --- the objects of a version -----------------------------------------------------------


def _is_dead(row: dict) -> bool:
    """A tombstone: a deleted chunk or shard has no file and no bytes."""
    return row["file"] is None and not row["length"]


def _units(rows: pa.Table) -> list[dict]:
    """The objects a version has, one dict each: ``array``, ``key`` (the store key under the
    array), ``how`` (``"copy"``: its one row is the whole object; ``"assemble"``: a shard
    made of a shard row and chunk rows), ``rows`` and ``size`` (bytes it reads).

    A unit that is deleted (only tombstones, or nothing left in the shard) is not listed.
    """
    groups: dict[tuple, dict] = {}
    for array, row in zip(
        rows.column("array").to_pylist(), rows.select(_COLUMNS).to_pylist()
    ):
        shard_key = row["shard_key"]
        unit = row["key"] if row["kind"] == "shard" or shard_key is None else shard_key
        g = groups.setdefault((array, unit), {"shard": None, "chunks": []})
        if row["kind"] == "shard":
            g["shard"] = row
        else:
            g["chunks"].append(row)
    out = []
    for (array, key), g in sorted(groups.items()):
        shard, chunks = g["shard"], g["chunks"]
        sharded = shard is not None or any(c["shard_key"] is not None for c in chunks)
        if not sharded:  # one chunk, one object
            how, held = "copy", [c for c in chunks if not _is_dead(c)][-1:]
        elif not chunks:  # one standard shard, or a deleted one
            how, held = "copy", [shard] if not _is_dead(shard) else []
        else:  # a shard row and chunk rows over it, or chunk rows alone
            how = "assemble"
            alive = any(not _is_dead(r) for r in [shard, *chunks] if r is not None)
            # the tombstones go along: they hide what lies beneath them
            held = ([shard] if shard is not None else []) + chunks if alive else []
        if not held:
            continue
        out.append(
            {
                "array": array,
                "key": key,
                "how": how,
                "rows": held,
                "size": sum(r["length"] or 0 for r in held),
            }
        )
    return out


@dataclass
class _Batch:
    """The units one task copies; ``_work_frame`` ships it as one row."""

    units: list

    def to_json(self) -> str:
        return json.dumps(self.units)


def _batches(units: list[dict]) -> list[_Batch]:
    """``units`` in as many batches as the bytes call for (at most ``_MAX_TASKS``), the
    biggest units first into the lightest batch, so every task has about the same to copy.
    """
    total = sum(u["size"] for u in units)
    n = max(1, min(len(units), _MAX_TASKS, math.ceil(total / _TASK_BYTES)))
    heap = [(0, i) for i in range(n)]
    bins: list[list] = [[] for _ in range(n)]
    for u in sorted(units, key=lambda u: -u["size"]):
        load, i = heapq.heappop(heap)
        bins[i].append(u)
        heapq.heappush(heap, (load + u["size"], i))
    return [_Batch(b) for b in bins if b]


# --- the copy's tasks -------------------------------------------------------------------


@dataclass
class _CopyConfig:
    """What a task needs besides its batch; travels in the ``mapInArrow`` closure."""

    root: str
    base_root: str | None
    to: str
    arrays: dict  # array -> ArrayMeta, for the arrays with shards to assemble


def _object_path(to: str, array: str, key: str) -> str:
    parts = [p for p in (*array.split("/"), *key.split("/")) if p]
    if ".." in parts or "." in parts:
        raise ZarrStoreDrift(
            f"the manifest key {array}/{key} leaves the target directory",
            fix="run gbx_zarr_sync if the store changed outside GeoBrix; otherwise "
            "correct the row",
        )
    return os.path.join(to, *parts)


def _region_of(row: dict) -> Region:
    origin = row["origin"] or "gbx"
    identity = row["identity"] if origin == "external" else None
    return Region(row["file"], int(row["offset"]), int(row["length"]), origin, identity)


def _windows(reader, row: dict):
    """The bytes of a row's region, ``_STREAM_WINDOW`` at a time."""
    region = _region_of(row)
    at, end = region.offset, region.offset + region.length
    while at < end:
        n = min(_STREAM_WINDOW, end - at)
        yield reader.read([region.part(at, n)])[0]
        at += n


def _write_object(path: str, make_parts) -> int:
    """The parts of ``make_parts()`` as the file ``path``; the bytes written. The whole
    object is retried on the I/O errors a Volume mount raises now and then."""

    def write() -> int:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        n = 0
        with open(path, "wb") as f:
            for part in make_parts():
                f.write(part)
                n += len(part)
        return n

    return _retry_transient(write, attempts=3, backoff=0.2)


def _copy_units(cfg: _CopyConfig, reader, units: list[dict]) -> list[dict]:
    stores: dict[str, ManifestStore] = {}
    try:
        for array in sorted({u["array"] for u in units if u["how"] == "assemble"}):
            rows = [
                {**r, "array": array}
                for u in units
                if u["how"] == "assemble" and u["array"] == array
                for r in u["rows"]
            ]
            stores[array] = ManifestStore({}, rows, reader, {array: cfg.arrays[array]})
        out = []
        for u in units:
            array, key = u["array"], u["key"]
            target = _object_path(cfg.to, array, key)
            if u["how"] == "copy":
                row = u["rows"][0]
                n = _write_object(target, lambda row=row: _windows(reader, row))
            else:
                store, name = stores[array], f"{array}/{key}"
                shard = store._virtual_shard(name) if name in store._shards else None
                if shard is None or not any(r for r in shard.chunks.values()):
                    n = 0  # nothing is left of the shard: no object
                else:
                    n = _write_object(target, lambda s=shard: _stream(s, reader))
            out.append({"array": array, "key": key, "nbytes": n})
        return out
    finally:
        for store in stores.values():
            store.close()


def _copy_task(cfg: _CopyConfig):
    """The function ``mapInArrow`` runs: batches of units in, one report row per object."""

    def run(batches):
        reader = FuseRangeReader(cfg.root, base_root=cfg.base_root)
        for batch in batches:
            for text in batch.column("unit").to_pylist():
                report = _copy_units(cfg, reader, json.loads(text))
                yield pa.RecordBatch.from_pylist(report, schema=_REPORT)

    return run


# --- the copy's driver ------------------------------------------------------------------


def _sizes(root: str) -> dict[str, int]:
    out = {}
    for folder, _, names in os.walk(root):
        for name in names:
            path = os.path.join(folder, name)
            out[os.path.relpath(path, root).replace(os.sep, "/")] = os.path.getsize(
                path
            )
    return out


def _verify(to_path: str, units: list[dict], report: pa.Table) -> None:
    """The deliverable, not the tasks' say-so: every object the plan names was reported once,
    a whole shard has the bytes its row has, and the file system holds what was reported (a
    Volume listing can lag the writes by a moment, so a mismatch is looked at again)."""
    planned = {(u["array"], u["key"]): u for u in units}
    rows = report.to_pylist()
    got = {(r["array"], r["key"]): r["nbytes"] for r in rows}
    if len(rows) != len(planned) or set(got) != set(planned):
        raise ZarrError(
            f"the export tasks reported {len(rows)} objects for {len(planned)} planned",
            fix="export again to an empty target",
        )
    for k, u in planned.items():
        if u["how"] == "copy" and got[k] != u["rows"][0]["length"]:
            raise ZarrError(
                f"the copy of {k[0]}/{k[1]} wrote {got[k]} bytes of "
                f"{u['rows'][0]['length']}",
                fix="export again to an empty target",
            )

    def look() -> None:
        sizes = _sizes(to_path)
        bad = [
            f"{a}/{k}"
            for (a, k), n in got.items()
            if sizes.get(f"{a}/{k}", 0) != n or ((a + "/" + k) in sizes) != (n > 0)
        ]
        if bad:
            raise FileNotFoundError(
                f"{len(bad)} object(s) differ on disk, e.g. {bad[0]}"
            )

    try:
        _retry_transient(look, attempts=5, backoff=0.5)
    except OSError as e:
        raise ZarrError(
            f"the copy in {to_path} is not what the tasks wrote ({e})",
            fix="export again to an empty target",
        ) from e


def _discard(path: str, remove_dir: bool) -> None:
    """Take back what a failed copy wrote: the directory it made, or the contents of the
    empty directory it was given. Best effort: the error being raised is the one to read.
    """
    if remove_dir:
        shutil.rmtree(path, ignore_errors=True)
        return
    with contextlib.suppress(OSError):
        names = os.listdir(path)
        for name in names:
            entry = os.path.join(path, name)
            if os.path.isdir(entry) and not os.path.islink(entry):
                shutil.rmtree(entry, ignore_errors=True)
            else:
                with contextlib.suppress(OSError):
                    os.unlink(entry)


# --- the documents ----------------------------------------------------------------------


def _fill_form(raw: bytes) -> bytes:
    """A ``zarr.json`` with its float ``_FillValue`` attribute in xarray's form
    (``_meta.xarray_fill_form``). Documents GeoBrix wrote already are; this is for the ones
    an older write left, and for a registered store's own."""
    try:
        doc = json.loads(raw)
    except ValueError:
        return raw
    fixed = xarray_fill_form(doc)
    return raw if fixed is doc else json.dumps(fixed).encode()


def _consolidated(docs: dict[str, bytes]) -> dict[str, bytes]:
    """The root's consolidated metadata, made by ``zarr`` itself from ``docs``: the root
    ``zarr.json`` with a ``consolidated_metadata`` block (v3), or ``.zmetadata`` (v2). Empty
    when the store has no root group document."""
    import zarr

    from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

    if ".zgroup" in docs:
        fmt, name = 2, ".zmetadata"
    elif "zarr.json" in docs and (
        json.loads(docs["zarr.json"]).get("node_type") == "group"
    ):
        fmt, name = 3, "zarr.json"
    else:
        return {}
    scratch = new_local_temp_dir("gbx_zarr_export_")
    try:
        for key, raw in docs.items():
            path = os.path.join(scratch, *key.split("/"))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(raw)
        with (
            warnings.catch_warnings()
        ):  # zarr warns that v3 consolidation is not in the spec
            warnings.simplefilter("ignore")
            zarr.consolidate_metadata(scratch, zarr_format=fmt)
        with open(os.path.join(scratch, name), "rb") as f:
            return {name: f.read()}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def _documents(metas: dict[str, bytes]) -> dict[str, bytes]:
    """The metadata documents an export carries: the store's per-node documents (without any
    consolidated copy; ``store_documents`` already dropped them), float ``_FillValue``
    attributes in xarray's form, and the consolidated metadata made again from those."""
    docs = {
        key: _fill_form(raw) if key.rpartition("/")[2] == "zarr.json" else raw
        for key, raw in metas.items()
    }
    docs.update(_consolidated(docs))
    return docs


def _write_documents(to_path: str, docs: dict[str, bytes], write_id: str) -> None:
    """Every document, each through a temp name and a rename. The root's go last, and the
    document that makes the root a group the very last: a reader that finds it finds the
    objects."""

    def order(key: str):
        root = "/" not in key
        return (root, root and key in ("zarr.json", ".zgroup"), key)

    for key in sorted(docs, key=order):
        _atomic_write(os.path.join(to_path, *key.split("/")), [docs[key]], write_id)


# --- the two methods --------------------------------------------------------------------


def _copy(spark, src, units, docs, to_path: str, write_id: str) -> None:
    existed = os.path.isdir(to_path)
    os.makedirs(to_path, exist_ok=True)
    try:
        if units:
            needed = {u["array"] for u in units if u["how"] == "assemble"}
            cfg = _CopyConfig(
                root=to_local_path(src.root),
                base_root=to_local_path(src.base_root) if src.base_root else None,
                to=to_path,
                arrays={a: src.arrays[a] for a in needed},
            )
            frame = _work_frame(spark, _batches(units)).mapInArrow(
                _copy_task(cfg), _REPORT_DDL
            )
            _verify(to_path, units, frame.toArrow())
        if _AFTER_OBJECTS is not None:
            _AFTER_OBJECTS()
        _write_documents(to_path, docs, write_id)
    except BaseException:
        _discard(to_path, remove_dir=not existed)
        raise


def _absolute(src, row: dict) -> str:
    """The bare path of the file a row names, which must lie under the store's roots."""
    external = (row["origin"] or "gbx") == "external" and src.base_root
    roots = [
        os.path.normpath(os.path.abspath(to_local_path(r)))
        for r in (src.root, src.base_root)
        if r
    ]
    base = os.path.normpath(
        os.path.abspath(to_local_path(src.base_root if external else src.root))
    )
    file = row["file"]
    path = os.path.normpath(file if os.path.isabs(file) else os.path.join(base, file))
    if not any(_within(path, r) for r in roots):
        raise ZarrStoreDrift(
            f"manifest file {file!r} resolves outside the store root {' or '.join(roots)}",
            fix="correct the row or run gbx_zarr_sync; a manifest may only point at "
            "files under the store root or, for a registered store, its base root",
        )
    return path


def _reference(
    table: str, src, units, docs, to_path: str, write_id: str, reach=FUSE
) -> None:
    folded = [u for u in units if u["how"] == "assemble"]
    if folded:
        shown = ", ".join(f"{u['array']}/{u['key']}" for u in folded[:3])
        raise ZarrUnsupported(
            f"{len(folded)} shard(s) of {table} are not one contiguous region: chunk rows "
            f"lie over the shard, or stand in for it (e.g. {shown}), and a reference names "
            "one range per key",
            fix=_OPTIMIZE_FIX,
        )
    refs: dict[str, object] = {key: raw.decode() for key, raw in docs.items()}
    for u in units:
        row = u["rows"][0]
        where = _absolute(src, row)
        refs[f"{u['array']}/{u['key']}"] = [
            reach.url_of(where) if reach.is_direct else where,
            int(row["offset"]),
            int(row["length"]),
        ]
    payload = json.dumps({"version": 1, "refs": refs}, separators=(",", ":"))
    _atomic_write(to_path, [payload.encode()], write_id)


@overload
def gbx_zarr_export(
    spark,
    source: str,
    to: str,
    *,
    method: Literal["copy", "reference"] = "copy",
    version_as_of=None,
    access: str = "auto",
) -> str: ...


@overload
def gbx_zarr_export(
    spark,
    source: str,
    to: None = None,
    *,
    method: Literal["publish"],
    version_as_of: None = None,
    access: str = "auto",
) -> int: ...


def gbx_zarr_export(
    spark,
    source: str,
    to: str | None = None,
    *,
    method: str = "copy",
    version_as_of=None,
    access: str = "auto",
) -> str | int:
    """Export a version of the store whose manifest table is ``source``.

    Returns the path ``to`` (a ``str``) for ``method="copy"`` and ``"reference"``, and the
    version of the base store's new commit (an ``int``, as ``gbx_zarr_write`` and the other
    maintenance calls return a version) for ``"publish"``, which has no ``to``.

    ``method="copy"`` writes a plain Zarr store (the store's own format, canonical keys,
    consolidated metadata) into the directory ``to``, which must not exist or be empty
    (``ValueError``). Any Zarr tool opens it. Shards that are already standard shards are
    copied as bytes; chunk rows are assembled into shards without decoding; deleted chunks
    and shards are left out, so readers see the fill value. Float ``_FillValue`` attributes
    of Zarr v3 arrays are written in the form xarray reads. If the copy fails nothing is
    left in ``to``.

    ``method="reference"`` writes the kerchunk (version 1) JSON file ``to`` (a ``.json``
    path): the metadata as strings and each shard or chunk as ``[absolute_path, offset,
    length]`` into the store's own files, so nothing is copied; open it with fsspec's
    ``reference`` file system (``xarray.open_zarr("reference://", storage_options={"fo":
    to})``). It is JSON only, about 100 bytes per object (a million shards: about 100 MB).
    Every shard must be one region: after chunk rows were written over it, run
    ``gbx_zarr_optimize`` first (``ZarrUnsupported`` otherwise). The paths are the bare
    ``/Volumes`` paths for ``access="auto"`` and ``"fuse"``, so a reference reads the same on
    every compute. ``access="direct"`` writes the cloud URL of each file instead (``s3://``,
    ``abfss://``, ``gs://``), from the Volume's location in Unity Catalog, for readers
    outside Databricks that bring their own credentials; where Unity Catalog will not vend
    credentials for the Volume (Serverless, no permission, a path that is not on a Volume) it
    raises ``ZarrAccessUnavailable``, as in ``gbx_zarr_read``. The
    reference points at the pack files of the pinned version, and vacuum can delete them:
    tag the version with ``gbx_zarr_tag`` to keep them.

    Direct access sends short-lived (at most one hour), Volume-scoped credentials to the
    executors. Under memory pressure Spark may write task data, those credentials included,
    to the executors' local disk: turn on local-disk encryption on clusters that use direct
    access. ``access="fuse"`` sends no credentials.

    ``method="publish"`` runs ``gbx_zarr_sync.publish`` for a writable registered store and
    returns the version it committed; it takes neither ``to`` nor ``version_as_of``.

    ``version_as_of`` (an int or ``"tag:<name>"``) pins the version exported (default: the
    latest). A version that vacuum removed, or whose files are gone, raises
    ``ZarrVersionUnavailable`` before anything is written.
    """
    _check_arguments(source, to, method, version_as_of, access)
    if method == "publish":
        return publish(spark, source)
    path = to_local_path(to)
    if method == "copy":
        _check_copy_target(path, to)
    else:
        _check_reference_target(path, to)
    props = M.read_props(spark, source)
    if props.file_mode != "fuse":
        raise ZarrUnsupported(
            f"{source} keeps its files in file_mode {props.file_mode!r}, which GeoBrix "
            "does not read yet",
            fix="export a store with file_mode 'fuse'",
        )
    src = _open_table(spark, source, version_as_of, None)
    reach = FUSE
    if method == "reference" and access == "direct":
        # the cloud location of each file comes from Unity Catalog (nothing is read here)
        reach = resolve_access(
            spark,
            src.root,
            access="direct",
            operation="READ_VOLUME",
            also=[src.base_root] if src.base_root else [],
        )
    label = src.snap.version if src.snap.tag is None else f"tag:{src.snap.tag}"
    rows = M.read_rows(spark, src.snap, kinds=["shard", "chunk"])
    known = pa.array(sorted({*src.arrays, *src.unsupported}), pa.string())
    rows = rows.filter(pc.is_in(rows.column("array"), value_set=known))
    _require_files(
        rows.select(["file", "origin", "identity"]), props, source, label, verb="export"
    )
    units = _units(rows)
    docs = _documents(src.metas)
    write_id = uuid.uuid4().hex  # the temp-name token of the files this export writes
    if method == "reference":
        _reference(source, src, units, docs, path, write_id, reach)
        return to
    blocked = sorted(
        {u["array"] for u in units if u["how"] == "assemble"} - set(src.arrays)
    )
    if blocked:
        raise ZarrUnsupported(
            f"array {blocked[0]!r} cannot be copied: {src.unsupported[blocked[0]]}, and "
            "its shards are made of chunk rows that would have to be assembled",
            fix="convert the array to a numeric dtype, or export a version where its "
            "shards are whole",
        )
    _copy(spark, src, units, docs, path, write_id)
    return to
