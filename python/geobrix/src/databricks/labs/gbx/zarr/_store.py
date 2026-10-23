"""``ManifestStore``: a read-only ``zarr`` store served from manifest rows.

The store holds no data. Its keys are the metadata documents it is given plus the
objects the manifest rows describe, and every read is a byte range of a pack file:

- an unsharded array's chunk key is served from that chunk's row;
- a sharded array's shard key is served as a ``VirtualShard`` built from the shard's
  row (a full standard shard, whose own index says where each inner chunk is) and the
  chunk rows with that shard key, which replace single inner chunks without touching
  the shard. With neither kind of row the key does not exist.

A row with no file and no bytes is a tombstone: the chunk it names is deleted. In a
sharded array it hides the inner chunk the shard row holds; in an unsharded one the key
is simply absent; a shard left with only tombstones does not exist. A row that is
neither a tombstone nor a complete region is a manifest that does not match the store
(``ZarrStoreDrift``), as is a row whose ``shard_key`` is not the shard its key lies in.

zarr reads this through its normal sharding codec: it asks for the index with a
suffix request and then for single chunks with range requests, and only those bytes
are fetched.
"""

from __future__ import annotations

import asyncio
import math
import os
import threading
import weakref
from collections import OrderedDict
from collections.abc import AsyncIterator, Iterable
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)
from zarr.core.buffer import Buffer, BufferPrototype

from databricks.labs.gbx.zarr._access import RangeReader
from databricks.labs.gbx.zarr._keys import inner_index, shard_of
from databricks.labs.gbx.zarr._meta import ArrayMeta
from databricks.labs.gbx.zarr._shard import (
    MISSING,
    Region,
    VirtualShard,
    decode_index,
    index_nbytes,
)
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrReadOnly, ZarrStoreDrift

__all__ = ["ManifestStore"]

_READ_ONLY_FIX = (
    "write through gbx_zarr_write; a ManifestStore only reads what the manifest holds"
)


_ROWS_FIX = "run gbx_zarr_sync to rebuild the manifest rows from the store"


def _is_tombstone(row: dict) -> bool:
    """A deleted chunk: no file, and no bytes (length 0 or null)."""
    return row.get("file") is None and not row.get("length")


def _region(row: dict, label: str) -> Region:
    """The region a live row points at; anything less than a whole region is drift."""
    file, offset, length = row.get("file"), row.get("offset"), row.get("length")
    problem = None
    if file is None:
        problem = "has bytes but no file"
    elif not isinstance(file, str) or not file or "\0" in file:
        problem = f"has {file!r} for a file, which is not a usable path"
    elif offset is None:
        problem = "has a file but no offset"
    elif length is None:
        problem = "has a file but no length"
    elif length == 0:
        problem = "has a file but a length of 0 (a deleted chunk has no file)"
    elif offset < 0 or length < 0:
        problem = f"has a negative offset or length ({offset}, {length})"
    if problem:
        raise ZarrStoreDrift(
            f"the manifest {row.get('kind')} row {label} {problem}", fix=_ROWS_FIX
        )
    return Region(file, int(offset), int(length), row.get("origin") or "gbx")


def _span(size: int, byte_range: ByteRequest | None) -> tuple[int, int]:
    """``[start, stop)`` of ``byte_range`` within an object of ``size`` bytes.

    Follows zarr's own stores: a range past the end is cut at the end, and one that
    starts at or past it is empty.
    """
    if byte_range is None:
        start, stop = 0, size
    elif isinstance(byte_range, RangeByteRequest):
        start, stop = byte_range.start, byte_range.end
    elif isinstance(byte_range, OffsetByteRequest):
        start, stop = byte_range.offset, size
    elif isinstance(byte_range, SuffixByteRequest):
        start, stop = max(0, size - byte_range.suffix), size
    else:
        raise ValueError(f"Unexpected byte_range, got {byte_range}.")
    stop = min(stop, size)
    return (start, stop) if start < stop else (stop, stop)


class _ShardRows:
    """The rows behind one shard key: its shard row and its inner-chunk overrides."""

    __slots__ = ("shard", "chunks")

    def __init__(self):
        self.shard: Region | None = None
        self.chunks: dict[int, Region | None] = (
            {}
        )  # inner index -> region; None: deleted

    def exists(self) -> bool:
        return self.shard is not None or any(
            r is not None for r in self.chunks.values()
        )


class ManifestStore(Store):
    """Serves ``metas`` and the objects ``rows`` describe, reading bytes through ``reader``.

    ``metas`` maps a store key (``zarr.json``, ``t2m/zarr.json``, ``t2m/.zarray``) to its
    bytes. ``rows`` are live manifest rows of kinds ``shard`` and ``chunk`` (dicts with
    the manifest columns); rows of an array not in ``arrays`` are ignored. ``arrays``
    maps an array name to its ``ArrayMeta``.
    """

    supports_writes = False
    supports_deletes = False
    supports_listing = True

    _SHARD_CACHE_SIZE = 256  # virtual shards kept per store (least recently used out)

    def __init__(
        self,
        metas: dict[str, bytes],
        rows: list[dict],
        reader: RangeReader,
        arrays: dict[str, ArrayMeta],
    ):
        super().__init__(read_only=True)
        self._metas = dict(metas)
        self._reader = reader
        self._arrays = dict(arrays)
        self._chunks: dict[str, Region] = {}  # unsharded chunk key -> region
        self._shards: dict[str, tuple[ArrayMeta, _ShardRows]] = {}  # shard key -> rows
        for row in rows:
            self._add(row)
        self._shards = {k: v for k, v in self._shards.items() if v[1].exists()}
        self._keys = sorted({*self._metas, *self._chunks, *self._shards})
        self._start_runtime()

    def _start_runtime(self) -> None:
        """The parts that cannot be pickled: the read pool, its cleanup, the shard cache."""
        self._virtual: OrderedDict[str, VirtualShard] = OrderedDict()
        self._lock = threading.Lock()
        self._closed = False
        self._workers = min(32, (os.cpu_count() or 1) * 4)
        self._pool_threads: set[int] = set()
        threads = self._pool_threads
        self._pool = ThreadPoolExecutor(
            max_workers=self._workers,
            thread_name_prefix="gbx-zarr-read",
            initializer=lambda: threads.add(threading.get_ident()),
        )
        self._finalizer = weakref.finalize(self, self._pool.shutdown, wait=False)

    def __getstate__(self) -> dict:
        drop = (
            "_pool",
            "_finalizer",
            "_lock",
            "_virtual",
            "_workers",
            "_pool_threads",
        )
        return {k: v for k, v in self.__dict__.items() if k not in drop}

    def __setstate__(self, state: dict) -> None:
        closed = state.get("_closed", False)
        self.__dict__.update(state)
        self._start_runtime()
        if closed:  # a closed store stays closed: no reads, no live pool
            self._closed = True
            self._finalizer()

    def _add(self, row: dict) -> None:
        name, kind = row.get("array"), row.get("kind")
        meta = self._arrays.get(name)
        if meta is None or kind not in ("shard", "chunk"):
            return
        label = f"{name}/{row['key']}"
        region = None if _is_tombstone(row) else _region(row, label)
        if meta.shard is None:
            if kind == "chunk":
                if region is None:
                    self._chunks.pop(label, None)
                else:
                    self._chunks[label] = region
            return
        if kind == "shard":
            shard_key = row["key"]
        else:
            try:
                coords = meta.key_encoding.parse(row["key"], len(meta.shape))
            except ValueError as e:
                raise ZarrStoreDrift(
                    f"the manifest key {row['key']!r} of array {name!r} is not a chunk "
                    f"key: {e}",
                    fix=_ROWS_FIX,
                ) from None
            shard_key = meta.key_encoding.chunk_key(shard_of(coords, meta.shard.cps))
        claimed = row.get("shard_key")
        if claimed is not None and claimed != shard_key:
            raise ZarrStoreDrift(
                f"the manifest {kind} row {label} says its shard is {claimed!r}, but "
                f"its key lies in shard {shard_key!r}",
                fix=_ROWS_FIX,
            )
        rows = self._rows_of(name, meta, shard_key)
        if kind == "shard":
            rows.shard = region
        else:
            rows.chunks[inner_index(coords, meta.shard.cps)] = region

    def _rows_of(self, name: str, meta: ArrayMeta, shard_key: str) -> _ShardRows:
        key = f"{name}/{shard_key}"
        if key not in self._shards:
            self._shards[key] = (meta, _ShardRows())
        return self._shards[key][1]

    # --- reading ------------------------------------------------------------------

    def _virtual_shard(self, key: str) -> VirtualShard:
        """The shard object at ``key``; cached (least recently used out)."""
        with self._lock:
            shard = self._virtual.get(key)
            if shard is not None:
                self._virtual.move_to_end(key)
                return shard
        shard = self._build_shard(key)
        with self._lock:
            self._virtual[key] = shard
            self._virtual.move_to_end(key)
            while len(self._virtual) > self._SHARD_CACHE_SIZE:
                self._virtual.popitem(last=False)
        return shard

    def _build_shard(self, key: str) -> VirtualShard:
        """Read the shard row's index, if it has one, and lay the chunk rows over it."""
        meta, rows = self._shards[key]
        spec = meta.shard
        n = math.prod(spec.cps)
        chunks: dict[int, Region | None] = {}
        if rows.shard is not None:
            whole = rows.shard
            size = index_nbytes(n, spec.index_codecs)
            if whole.length < size:
                raise ZarrStoreDrift(
                    f"the shard {key} of {whole.file} has {whole.length} bytes, "
                    f"fewer than its {size}-byte index",
                    fix="run gbx_zarr_sync if the store changed outside GeoBrix; "
                    "otherwise the file is corrupt",
                )
            at = whole.offset + (
                whole.length - size if spec.index_location == "end" else 0
            )
            (raw,) = self._reader.read([Region(whole.file, at, size, whole.origin)])
            entries = decode_index(raw, n, spec.index_codecs, source=whole.file)
            held = np.flatnonzero(
                (entries[:, 0] != MISSING) | (entries[:, 1] != MISSING)
            )
            for i in held.tolist():
                offset, nbytes = int(entries[i, 0]), int(entries[i, 1])
                if offset + nbytes > whole.length:
                    raise ZarrStoreDrift(
                        f"chunk {i} of the shard {key} in {whole.file} lies outside the shard",
                        fix="run gbx_zarr_sync if the store changed outside GeoBrix; "
                        "otherwise the file is corrupt",
                    )
                chunks[i] = Region(
                    whole.file, whole.offset + offset, nbytes, whole.origin
                )
        chunks.update(rows.chunks)  # an override, or None: the chunk is deleted
        return VirtualShard(n, chunks, spec.index_codecs, spec.index_location)

    def ensure_outside_pool(self, what: str) -> None:
        """Raise unless the caller is outside this store's read pool.

        ``read_box`` and ``chunk_regions`` queue work on the pool and wait for it. Called
        from a pool thread they would wait on themselves, which deadlocks as soon as the
        pool is busy, so they are refused there.
        """
        if threading.get_ident() in self._pool_threads:
            raise ZarrError(
                f"{what} was called from one of the store's own threads, where it "
                "would wait on the pool it runs in",
                fix="call it from your own thread or task, not from inside a store read",
            )

    def chunk_regions(
        self, name: str, coords: list[tuple[int, ...]]
    ) -> list[Region | None]:
        """Where each (inner) chunk of array ``name`` lives, in the order of ``coords``.

        ``None`` marks a chunk the store does not hold (never written, or deleted): it
        reads as the fill value. For a sharded array this reads the index of every shard
        the chunks lie in, in parallel; for an unsharded one it only looks rows up. A
        reader that fetches chunk bytes itself, not through zarr's store protocol, starts
        here.
        """
        self.ensure_outside_pool("chunk_regions")
        meta = self._arrays[name]
        if meta.shard is None:
            return [
                self._chunks.get(f"{name}/{meta.key_encoding.chunk_key(c)}")
                for c in coords
            ]
        cps = meta.shard.cps
        shard_keys = [meta.key_encoding.chunk_key(shard_of(c, cps)) for c in coords]
        wanted = [k for k in dict.fromkeys(shard_keys) if f"{name}/{k}" in self._shards]
        if len(wanted) > 1:
            built = self._executor().map(
                lambda k: self._virtual_shard(f"{name}/{k}"), wanted
            )
        else:
            built = (self._virtual_shard(f"{name}/{k}") for k in wanted)
        shards = dict(zip(wanted, built))
        return [
            None if k not in shards else shards[k].chunks.get(inner_index(c, cps))
            for c, k in zip(coords, shard_keys)
        ]

    def _fetch(self, key: str, byte_range: ByteRequest | None) -> bytes | None:
        """The bytes of a chunk or shard key (blocking); ``None`` when it does not exist."""
        if key in self._chunks:
            region = self._chunks[key]
            start, stop = _span(region.length, byte_range)
            if start == stop:
                return b""
            return self._reader.read(
                [
                    Region(
                        region.file, region.offset + start, stop - start, region.origin
                    )
                ]
            )[0]
        if key in self._shards:
            shard = self._virtual_shard(key)
            start, stop = _span(shard.size, byte_range)
            parts = shard.pieces(start, stop)
            fetched = iter(
                self._reader.read([p for p in parts if isinstance(p, Region)])
            )
            return b"".join(
                next(fetched) if isinstance(p, Region) else p for p in parts
            )
        return None

    def _size(self, key: str) -> int | None:
        if key in self._chunks:
            return self._chunks[key].length
        if key in self._shards:
            return self._virtual_shard(key).size
        return None

    def _executor(self) -> ThreadPoolExecutor:
        if self._closed:
            raise ZarrError(
                "this ManifestStore is closed",
                fix="open a new store (read the table again) instead of reusing a closed one",
            )
        return self._pool

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        if key in self._metas:
            data = self._metas[key]
            start, stop = _span(len(data), byte_range)
            return prototype.buffer.from_bytes(data[start:stop])
        if key not in self._chunks and key not in self._shards:
            return None
        data = await asyncio.get_running_loop().run_in_executor(
            self._executor(), self._fetch, key, byte_range
        )
        return prototype.buffer.from_bytes(data)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        return list(
            await asyncio.gather(
                *(self.get(key, prototype, rng) for key, rng in key_ranges)
            )
        )

    async def getsize(self, key: str) -> int:
        if key in self._metas:
            return len(self._metas[key])
        if key not in self._chunks and key not in self._shards:
            raise FileNotFoundError(key)
        return await asyncio.get_running_loop().run_in_executor(
            self._executor(), self._size, key
        )

    async def exists(self, key: str) -> bool:
        return key in self._metas or key in self._chunks or key in self._shards

    # --- listing ------------------------------------------------------------------

    async def list(self) -> AsyncIterator[str]:
        for key in self._keys:
            yield key

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        for key in self._keys:
            if key.startswith(prefix):
                yield key

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        prefix = prefix.rstrip("/")
        head = f"{prefix}/" if prefix else ""
        seen: set[str] = set()
        for key in self._keys:
            if key.startswith(head) and key != prefix:
                name = key[len(head) :].split("/")[0]
                if name not in seen:
                    seen.add(name)
                    yield name

    # --- read-only surface --------------------------------------------------------

    async def set(self, key: str, value: Buffer) -> None:
        raise ZarrReadOnly(
            f"cannot write {key!r}: this store is read-only", fix=_READ_ONLY_FIX
        )

    async def delete(self, key: str) -> None:
        raise ZarrReadOnly(
            f"cannot delete {key!r}: this store is read-only", fix=_READ_ONLY_FIX
        )

    def with_read_only(self, read_only: bool = False) -> ManifestStore:
        if not read_only:
            raise ZarrReadOnly(
                "a ManifestStore cannot be made writable", fix=_READ_ONLY_FIX
            )
        return self

    async def delete_dir(self, prefix: str) -> None:
        raise ZarrReadOnly(
            f"cannot delete {prefix!r}: this store is read-only", fix=_READ_ONLY_FIX
        )

    async def clear(self) -> None:
        raise ZarrReadOnly("cannot clear: this store is read-only", fix=_READ_ONLY_FIX)

    def close(self) -> None:
        super().close()
        self._closed = True
        self._finalizer()  # shuts the pool down now, once

    def __eq__(self, value: object) -> bool:
        return self is value

    def __hash__(self) -> int:
        return id(self)

    def __repr__(self) -> str:
        return f"ManifestStore({len(self._arrays)} arrays, {len(self._keys)} keys)"
