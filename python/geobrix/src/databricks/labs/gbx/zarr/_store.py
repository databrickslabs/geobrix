"""``ManifestStore``: a read-only ``zarr`` store served from manifest rows.

The store holds no data. Its keys are the metadata documents it is given plus the
objects the manifest rows describe, and every read is a byte range of a pack file:

- an unsharded array's chunk key is served from that chunk's row;
- a sharded array's shard key is served as a ``VirtualShard`` built from the shard's
  row (a full standard shard, whose own index says where each inner chunk is) and the
  chunk rows with that shard key, which replace single inner chunks without touching
  the shard. With neither kind of row the key does not exist.

zarr reads this through its normal sharding codec: it asks for the index with a
suffix request and then for single chunks with range requests, and only those bytes
are fetched.
"""

from __future__ import annotations

import asyncio
import math
import os
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
from databricks.labs.gbx.zarr.errors import ZarrReadOnly, ZarrStoreDrift

__all__ = ["ManifestStore"]

_READ_ONLY_FIX = (
    "write through gbx_zarr_write; a ManifestStore only reads what the manifest holds"
)


def _region(row: dict) -> Region:
    return Region(
        row["file"], int(row["offset"]), int(row["length"]), row.get("origin") or "gbx"
    )


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

    __slots__ = ("shard_row", "chunk_rows")

    def __init__(self):
        self.shard_row: dict | None = None
        self.chunk_rows: dict[int, dict] = {}  # inner index -> row


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
        self._pool: ThreadPoolExecutor | None = None
        self._virtual: dict[str, VirtualShard] = {}
        self._chunks: dict[str, Region] = {}  # unsharded chunk key -> region
        self._shards: dict[str, tuple[ArrayMeta, _ShardRows]] = {}  # shard key -> rows
        for row in rows:
            self._add(row)
        self._keys = sorted({*self._metas, *self._chunks, *self._shards})

    def _add(self, row: dict) -> None:
        name, kind = row.get("array"), row.get("kind")
        meta = self._arrays.get(name)
        if meta is None or kind not in ("shard", "chunk"):
            return
        if meta.shard is None:
            if kind == "chunk":
                self._chunks[f"{name}/{row['key']}"] = _region(row)
            return
        if kind == "shard":
            shard_key = row.get("shard_key") or row["key"]
            self._rows_of(name, meta, shard_key).shard_row = row
            return
        try:
            coords = meta.key_encoding.parse(row["key"], len(meta.shape))
        except ValueError as e:
            raise ZarrStoreDrift(
                f"the manifest key {row['key']!r} of array {name!r} is not a chunk key: {e}",
                fix="run gbx_zarr_sync to rebuild the manifest rows from the store",
            ) from None
        shard_key = meta.key_encoding.chunk_key(shard_of(coords, meta.shard.cps))
        self._rows_of(name, meta, shard_key).chunk_rows[
            inner_index(coords, meta.shard.cps)
        ] = row

    def _rows_of(self, name: str, meta: ArrayMeta, shard_key: str) -> _ShardRows:
        key = f"{name}/{shard_key}"
        if key not in self._shards:
            self._shards[key] = (meta, _ShardRows())
        return self._shards[key][1]

    # --- reading ------------------------------------------------------------------

    def _virtual_shard(self, key: str) -> VirtualShard:
        """The shard object at ``key``; reads the shard row's index once, then caches."""
        if key in self._virtual:
            return self._virtual[key]
        meta, rows = self._shards[key]
        spec = meta.shard
        n = math.prod(spec.cps)
        chunks: dict[int, Region] = {}
        if rows.shard_row is not None:
            whole = _region(rows.shard_row)
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
        for i, row in rows.chunk_rows.items():
            chunks[i] = _region(row)
        shard = VirtualShard(n, chunks, spec.index_codecs, spec.index_location)
        self._virtual[key] = shard
        return shard

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
        if self._pool is None:
            self._pool = ThreadPoolExecutor(
                max_workers=min(32, (os.cpu_count() or 1) * 4),
                thread_name_prefix="gbx-zarr-read",
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

    def close(self) -> None:
        super().close()
        if self._pool is not None:
            self._pool.shutdown(wait=False)
            self._pool = None

    def __eq__(self, value: object) -> bool:
        return self is value

    def __hash__(self) -> int:
        return id(self)

    def __repr__(self) -> str:
        return f"ManifestStore({len(self._arrays)} arrays, {len(self._keys)} keys)"
