"""The files of a plain Zarr store: what ``gbx_zarr_write(transactional=False)`` puts on disk.

A transactional write keeps its encoded chunks in pack files and points at them from a
manifest. A plain write has no manifest, so the tasks write the objects of the Zarr store
themselves: one file per chunk of an unsharded array, one file per shard of a sharded one
(its inner chunks back to back, then the index). The store's metadata documents come
last, after every one of those objects, so a write that stops part way leaves objects but
no ``zarr.json`` or ``.zgroup``: not a store that any reader opens.

``PlainSink`` is what a task writes through, in place of a pack. Everything else here is
the driver's: refusing a path that already holds a store, reading the documents of one to
append to, writing the documents, and taking back a failed create.

A Unity Catalog Volume is a FUSE mount: no seeks and no appends. Every object is written
whole and in order (a shard is read back from a local spool of its encoded chunks, one chunk
at a time), and an
object that replaces an earlier one goes through a temp name and a rename.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import tempfile
import uuid

import numpy as np

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr._keys import inner_index, shard_of
from databricks.labs.gbx.zarr._meta import ArrayMeta
from databricks.labs.gbx.zarr._shard import (
    MISSING,
    decode_index,
    encode_index,
    index_nbytes,
)
from databricks.labs.gbx.zarr.errors import ZarrError

__all__ = [
    "PlainSink",
    "check_absent",
    "check_no_leftovers",
    "discard",
    "object_path",
    "open_plain_array",
    "read_plain_docs",
    "read_shard_chunks",
    "shard_parts",
    "write_documents",
]

_DOCUMENTS = ("zarr.json", ".zgroup", ".zarray")


def object_path(root: str, array: str, key: str) -> str:
    """The file of the object ``key`` of ``array`` under ``root``."""
    parts = [p for p in (*array.split("/"), *key.split("/")) if p]
    if ".." in parts or "." in parts:
        raise ZarrError(
            f"the key {array}/{key} leaves the store directory",
            fix="use array names and chunk keys without '.' or '..' parts",
        )
    return os.path.join(root, *parts)


# --- shards -----------------------------------------------------------------------------


def shard_index(
    meta: ArrayMeta, lengths: dict[int, int]
) -> tuple[bytes, dict[int, int]]:
    """``(index, offsets)`` of a shard whose inner chunks (inner index to encoded length)
    are laid back to back in index order: the shard index bytes, and where each chunk starts
    in the shard object. A chunk the shard lacks is ``(MISSING, MISSING)`` in the index.
    """
    spec = meta.shard
    n = int(np.prod(spec.cps))
    size = index_nbytes(n, spec.index_codecs)
    at = size if spec.index_location == "start" else 0
    entries = np.full((n, 2), MISSING, dtype=np.uint64)
    offsets: dict[int, int] = {}
    for i in sorted(lengths):
        entries[i] = (at, lengths[i])
        offsets[i] = at
        at += lengths[i]
    return encode_index(entries, spec.index_codecs), offsets


def shard_parts(meta: ArrayMeta, chunks: dict[int, bytes]) -> list[bytes]:
    """The bytes of one shard object as ordered parts: ``chunks`` (inner index to encoded
    chunk) in index order, and the shard index, at the end or the start as the array's
    codecs say. A chunk the shard lacks is ``(MISSING, MISSING)`` in the index."""
    index, _ = shard_index(meta, {i: len(d) for i, d in chunks.items()})
    body = [chunks[i] for i in sorted(chunks)]
    return [index, *body] if meta.shard.index_location == "start" else [*body, index]


class _ShardParts:
    """A shard object's parts, read one inner chunk at a time from where the chunks wait
    (a spool on local disk, or the shard file this write replaces), so a task holds a chunk
    and the index, not the shard. Iterable more than once, since the write is retried.
    """

    def __init__(self, meta: ArrayMeta, sources: dict[int, tuple]):
        self.meta, self.sources = meta, sources  # idx -> (file, offset, length)
        self.index, _ = shard_index(meta, {i: s[2] for i, s in sources.items()})

    def __iter__(self):
        start = self.meta.shard.index_location == "start"
        if start:
            yield self.index
        for i in sorted(self.sources):
            f, off, n = self.sources[i]
            f.seek(off)
            yield f.read(n)
        if not start:
            yield self.index


def read_shard_index(f, meta: ArrayMeta, size: int, source: str) -> dict[int, tuple]:
    """``{inner index: (offset, length)}`` of the chunks a shard file ``f`` (``size`` bytes)
    holds, from its index alone."""
    spec = meta.shard
    n = int(np.prod(spec.cps))
    nbytes = index_nbytes(n, spec.index_codecs)
    f.seek(0 if spec.index_location == "start" else size - nbytes)
    entries = decode_index(f.read(nbytes), n, spec.index_codecs, source=source)
    return {
        i: (int(off), int(length))
        for i, (off, length) in enumerate(entries.tolist())
        if off != MISSING
    }


def read_shard_chunks(path: str, meta: ArrayMeta) -> dict[int, bytes]:
    """The encoded inner chunks a shard file holds, by inner index."""
    spec = meta.shard
    n = int(np.prod(spec.cps))
    size = index_nbytes(n, spec.index_codecs)

    def read() -> bytes:
        with open(path, "rb") as f:
            return f.read()

    buf = _retry_transient(read, attempts=3, backoff=0.2)
    raw = buf[:size] if spec.index_location == "start" else buf[len(buf) - size :]
    entries = decode_index(raw, n, spec.index_codecs, source=path)
    return {
        i: buf[int(off) : int(off) + int(nbytes)]
        for i, (off, nbytes) in enumerate(entries.tolist())
        if off != MISSING
    }


# --- the sink ---------------------------------------------------------------------------


def _put(path: str, parts, *, replace: bool) -> int:
    """``parts`` as the file ``path``, whole and in order; the bytes written. A file that
    replaces another is written under a temp name and renamed, so a reader sees the old
    one or the new one. Retried on the I/O errors a Volume mount raises now and then."""

    def write() -> int:
        folder = os.path.dirname(path)
        os.makedirs(folder, exist_ok=True)
        target = path
        if replace:
            target = os.path.join(
                folder, f".{os.path.basename(path)}.{uuid.uuid4().hex}.tmp"
            )
        n = 0
        try:
            with open(target, "wb") as f:
                for part in parts:
                    f.write(part)
                    n += len(part)
            if replace:
                os.replace(target, path)
        except BaseException:
            if replace:
                with contextlib.suppress(OSError):
                    os.unlink(target)
            raise
        return n

    return _retry_transient(write, attempts=3, backoff=0.2)


class _Held:
    """The inner chunks of one shard a task has encoded so far, kept on local disk (a spool)
    and not in memory: ``entries`` is inner index to ``(offset in the spool, length, empty)``.
    """

    def __init__(self, meta: ArrayMeta):
        self.meta = meta
        self.spool = tempfile.TemporaryFile()
        self.entries: dict[int, tuple[int, int, bool]] = {}

    def add(self, idx: int, data: bytes, empty: bool) -> None:
        self.spool.seek(0, os.SEEK_END)
        self.entries[idx] = (self.spool.tell(), len(data), empty)
        self.spool.write(data)

    def close(self) -> None:
        self.spool.close()


class PlainSink:
    """The objects one task writes into a plain store.

    ``add`` takes one encoded inner chunk. An unsharded array's chunk is written at once.
    A sharded array's chunks wait on local disk until ``flush``, which writes each shard whole
    from them, one chunk at a time: the caller flushes when it holds every chunk of the
    shards it has begun (an assemble group's), or, with ``stream``, the sink writes a shard
    as soon as a chunk of another shard of the same array arrives (chunks come shard by
    shard) and the caller flushes once at the end.

    ``merge`` is for an append, which may write into a unit an earlier write began: a shard
    file that exists keeps the inner chunks this write does not rewrite, and a chunk of
    fill is written (it replaces what was there). Without ``merge`` a chunk that is all
    fill is left out, as zarr does: an absent object reads as the fill value.
    """

    def __init__(self, root: str, *, merge: bool = False, stream: bool = False):
        self.root = root
        self.merge = merge
        self.stream = stream
        self.bytes = 0
        self.chunks = 0
        self._pending: dict[tuple[str, str], tuple[ArrayMeta, _Held]] = {}

    def add(self, meta: ArrayMeta, chunk, key: str, data: bytes, empty: bool) -> int:
        """Take one encoded chunk; the bytes it will occupy (0 for fill that is left out)."""
        if meta.shard is None:
            path = object_path(self.root, meta.name, key)
            if empty and not os.path.exists(path):
                return 0
            n = _put(path, [data], replace=self.merge)
            self.bytes += n
            self.chunks += 1
            return n
        shard_key = meta.key_encoding.chunk_key(shard_of(tuple(chunk), meta.shard.cps))
        if self.stream:
            done = [k for k in self._pending if k[0] == meta.name and k[1] != shard_key]
            if done:
                self._write({k: self._pending.pop(k) for k in done})
        held = self._pending.setdefault((meta.name, shard_key), (meta, _Held(meta)))[1]
        held.add(inner_index(tuple(chunk), meta.shard.cps), data, empty)
        return len(data)

    def flush(self) -> None:
        """Write the shards that wait."""
        pending, self._pending = self._pending, {}
        self._write(pending)

    def _write(self, pending: dict) -> None:
        for (name, shard_key), (meta, held) in sorted(pending.items()):
            try:
                self._write_shard(name, shard_key, meta, held)
            finally:
                held.close()

    def _write_shard(self, name: str, shard_key: str, meta: ArrayMeta, held) -> None:
        path = object_path(self.root, name, shard_key)
        old, old_index = None, {}
        try:
            if self.merge and os.path.exists(path):
                old = open(path, "rb")
                old_index = read_shard_index(old, meta, os.path.getsize(path), path)
            sources = {i: (old, off, n) for i, (off, n) in old_index.items()}
            fresh = {
                i: (held.spool, off, n)
                for i, (off, n, empty) in held.entries.items()
                if old is not None or not empty
            }
            sources.update(fresh)
            if not sources:
                return
            self.bytes += _put(
                path, _ShardParts(meta, sources), replace=old is not None
            )
            self.chunks += len(sources) if old is None else len(fresh)
        finally:
            if old is not None:
                old.close()

    def abort(self) -> None:
        pending, self._pending = self._pending, {}
        for _, held in pending.values():
            held.close()


# --- the store as it is -----------------------------------------------------------------


def check_absent(root: str) -> None:
    """Refuse a ``root`` that already holds a Zarr store: a ``create`` never writes over
    one."""
    for name in _DOCUMENTS:
        if os.path.exists(os.path.join(root, name)):
            raise ZarrError(
                f"{root} already holds a Zarr store ({name} exists); a create never "
                "writes over one",
                fix="choose a new path, add to the store with mode='append', or remove "
                "the store first",
            )


def check_no_leftovers(root: str, arrays) -> None:
    """Refuse a ``root`` that holds objects where this create would put its arrays: what a
    crashed write left (it has no documents, so it is not a store). Mixing the old and the
    new objects could make a store read wrong."""
    found = [
        a
        for a in sorted(arrays)
        if os.path.isdir(os.path.join(root, *a.split("/")))
        and os.listdir(os.path.join(root, *a.split("/")))
    ]
    if found:
        raise ZarrError(
            f"{root} holds objects under {', '.join(found[:3])}"
            f"{' ...' if len(found) > 3 else ''}, left by an earlier write that stopped "
            "before it finished (they are not a store)",
            fix=f"remove those directories from {root}, or choose a new path",
        )


def read_plain_docs(root: str):
    """The documents of the plain store at ``root`` as a ``_write._Docs`` (its arrays and
    the documents, with no consolidated copy), for an append."""
    from databricks.labs.gbx.zarr._read import parse_documents, store_documents
    from databricks.labs.gbx.zarr._register import read_base_metadata
    from databricks.labs.gbx.zarr._write import _Docs

    if not os.path.isdir(root) or not any(
        os.path.exists(os.path.join(root, n)) for n in _DOCUMENTS
    ):
        raise ZarrError(
            f"{root} holds no Zarr store to append to",
            fix="create it first with mode='create', or point at the store's path",
        )
    metas, _, _ = read_base_metadata(root, tolerant=True)
    metas = store_documents(metas)
    arrays, unsupported = parse_documents(metas)
    ids = {}
    for key, raw in metas.items():
        prefix, _, name = key.rpartition("/")
        if name == ".zgroup":
            is_group = True
        elif name == "zarr.json":
            try:
                is_group = json.loads(raw).get("node_type") == "group"
            except ValueError:
                is_group = False
        else:
            is_group = False
        if is_group:
            ids[("group", prefix or None, name)] = "plain"
    return _Docs(metas, arrays, unsupported, ids)


def open_plain_array(root: str, meta: ArrayMeta):
    """zarr-python's read-only view of the array ``meta`` in the store at ``root`` (its
    stored values: nothing is decoded)."""
    import zarr

    return zarr.open_array(
        store=os.path.join(root, *meta.name.split("/")),
        mode="r",
        zarr_format=meta.zarr_format,
    )


# --- the documents ----------------------------------------------------------------------


def write_documents(root: str, metas: dict[str, bytes], write_id: str) -> None:
    """The store's metadata documents, consolidated metadata made again from them, each
    through a temp name and a rename. The arrays' go first, the root's after them, and the
    document that makes the root a group last: a reader that finds it finds the objects.
    This is the one step that makes a store of the files a plain write put down."""
    from databricks.labs.gbx.zarr._export import _documents, _write_documents

    _write_documents(root, _documents(metas), write_id)


def discard(root: str, arrays, *, remove_root: bool) -> None:
    """Take back what a failed create wrote: the directories of its ``arrays`` (objects and
    any document), and ``root`` itself when the create made it and it is now empty. Only
    the create's own array directories are touched; best effort, since the error being
    raised is the one to read."""
    for name in sorted(set(arrays), key=len, reverse=True):
        shutil.rmtree(os.path.join(root, *name.split("/")), ignore_errors=True)
        parent = os.path.dirname(os.path.join(root, *name.split("/")))
        while parent != root and parent.startswith(root + os.sep):
            try:
                os.rmdir(parent)  # only when now empty
            except OSError:
                break
            parent = os.path.dirname(parent)
    for name in _DOCUMENTS:
        with contextlib.suppress(OSError):
            os.unlink(os.path.join(root, name))
    if remove_root:
        with contextlib.suppress(OSError):
            os.rmdir(root)
