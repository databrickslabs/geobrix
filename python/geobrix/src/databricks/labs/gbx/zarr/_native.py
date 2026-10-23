"""Native box reads of a manifest array: batch the byte ranges, decode in a pool.

zarr-python reads a store one key at a time: a box over a sharded array becomes one range
request per inner chunk, and each request opens its file again. On a Unity Catalog Volume
(FUSE) an open costs tens of milliseconds, so that is where the time goes (performance
gate G1). This engine works from the other end. It asks the store where every chunk of the
box lives, groups the chunks by pack file, reads each file once (adjacent ranges in one
read), and decodes the chunks on the store's thread pool, writing each into its place in
the result. The codecs are the numcodecs ones the arrays use, so the bytes decoded are the
bytes zarr would decode.

Decoded: the Zarr v3 chain ``bytes`` followed by any of ``zstd``, ``gzip``, ``blosc``,
``crc32c``, and Zarr v2 ``compressor`` and ``filters`` (C order). An array with any other
codec is refused at open (``ZarrUnsupported``) and reads through zarr's own pipeline. The
result is in native byte order whatever the array's.

Memory: the result is the box at its decoded size. Beyond it, reads are submitted through
a window of about two groups per worker, and no more than ``_INFLIGHT_BYTES`` of compressed
bytes are read but not yet decoded at any time; a read's bytes are released as soon as
their decodes have run, so a box of any size costs the result plus that window, not the
stored size of the box. The compressed bytes of a single read group (``_IO_GROUP_BYTES``,
or one oversized chunk) are held whole, and the file reader copies each once more while it
splits a read into chunks.

A box is read from the caller's own thread, never from the store's pool (the call queues
work there and waits for it).
"""

from __future__ import annotations

import itertools
import queue
from collections import deque

import numpy as np

from databricks.labs.gbx.zarr._meta import ArrayMeta
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrStoreDrift, ZarrUnsupported

__all__ = ["make_decoder", "read_box_native"]

_BYTES_TO_BYTES = ("zstd", "gzip", "blosc", "crc32c")
_IO_GROUP_BYTES = 32 << 20  # split one file's reads into tasks of about this size
_INFLIGHT_BYTES = 64 << 20  # compressed bytes read (or being read) but not yet decoded
_FALLBACK_FIX = "read it with pipeline='default'"


def _numcodecs_for(name: str, meta: ArrayMeta):
    import numcodecs

    if name == "zstd":
        return numcodecs.Zstd()
    if name == "gzip":
        return numcodecs.GZip()
    if name == "blosc":
        return numcodecs.Blosc()
    crc32c = getattr(numcodecs, "CRC32C", None)  # needs a crc32c implementation
    if crc32c is None:
        raise _unsupported(meta, "this numcodecs has no crc32c codec")
    return crc32c()


def _unsupported(meta: ArrayMeta, why: str) -> ZarrUnsupported:
    return ZarrUnsupported(
        f"the native engine cannot decode array {meta.name!r}: {why}", fix=_FALLBACK_FIX
    )


def _v3_steps(meta: ArrayMeta) -> list:
    """The bytes-to-bytes codecs of a v3 array in decoding order."""
    codecs = list(meta.shard.inner_codecs if meta.shard else meta.doc.get("codecs", ()))
    names = [c.get("name") for c in codecs]
    if not names or names[0] != "bytes" or names.count("bytes") != 1:
        raise _unsupported(meta, f"its codecs {names} do not start with 'bytes'")
    for n in names[1:]:
        if n not in _BYTES_TO_BYTES:
            raise _unsupported(meta, f"it has the codec {n!r}")
    return [_numcodecs_for(n, meta) for n in reversed(names[1:])]


def _v2_steps(meta: ArrayMeta) -> list:
    """The compressor and filters of a v2 array in decoding order."""
    import numcodecs

    zarray = meta.doc[".zarray"]
    configs = []
    if zarray.get("compressor"):
        configs.append(zarray["compressor"])
    configs += list(reversed(zarray.get("filters") or ()))
    steps = []
    for cfg in configs:
        try:
            steps.append(numcodecs.get_codec(dict(cfg)))
        except (KeyError, ValueError, TypeError) as e:
            raise _unsupported(meta, f"its codec {cfg.get('id')!r} is unknown ({e})")
    return steps


def make_decoder(meta: ArrayMeta):
    """A function from the stored bytes of one chunk to its values, shaped like a chunk.

    Raises ``ZarrUnsupported`` for an array whose codecs this engine does not decode.
    """
    steps = _v3_steps(meta) if meta.zarr_format == 3 else _v2_steps(meta)
    dtype = np.dtype(meta.dtype)
    chunks = tuple(meta.chunks)
    want = int(np.prod(chunks, dtype=np.int64))

    def decode(data: bytes) -> np.ndarray:
        buf = data
        for step in steps:
            buf = step.decode(buf)
        if isinstance(buf, np.ndarray) and buf.dtype == dtype:
            values = buf.reshape(-1)
        else:
            values = np.frombuffer(buf, dtype=dtype)
        if values.size != want:
            raise ValueError(f"{values.size} values, expected {want}")
        return values.reshape(chunks)

    return decode


def _fill(meta: ArrayMeta):
    return 0 if meta.fill_value is None else meta.fill_value


def native_dtype(meta: ArrayMeta) -> np.dtype:
    """The dtype a read returns: the array's, in native byte order."""
    return np.dtype(meta.dtype).newbyteorder("=")


def _chunk_ranges(meta: ArrayMeta, box) -> list[range]:
    return [range(a // n, (b - 1) // n + 1) for (a, b), n in zip(box, meta.chunks)]


def _io_groups(items: list[tuple[tuple, Region]]) -> list[list[tuple[tuple, Region]]]:
    """Split the (coords, region) pairs into reads of one file each, in offset order."""
    by_file: dict[tuple[str, str], list] = {}
    for item in items:
        by_file.setdefault((item[1].file, item[1].origin), []).append(item)
    groups = []
    for members in by_file.values():
        members.sort(key=lambda it: it[1].offset)
        group, size = [], 0
        for item in members:
            if group and size + item[1].length > _IO_GROUP_BYTES:
                groups.append(group)
                group, size = [], 0
            group.append(item)
            size += item[1].length
        groups.append(group)
    return groups


def read_box_native(store, meta: ArrayMeta, box, decode) -> np.ndarray:
    """The box (``(start, stop)`` per axis, inside the array) of ``meta`` in ``store``.

    ``decode`` is ``make_decoder(meta)``. Chunks the store does not hold read as the
    fill value. Errors from the reader (``ZarrStoreDrift``) propagate. Call it from a
    thread of your own, not from the store's pool.
    """
    store.ensure_outside_pool("read_box")
    pool = store._executor()  # raises for a closed store
    reader = store._reader
    out = np.full(tuple(b - a for a, b in box), _fill(meta), dtype=native_dtype(meta))
    if not out.size:
        return out
    coords = list(itertools.product(*_chunk_ranges(meta, box)))
    regions = store.chunk_regions(meta.name, coords)
    held = [(c, r) for c, r in zip(coords, regions) if r is not None]

    def place(coord: tuple, region: Region, data: bytes) -> None:
        try:
            values = decode(data)
        except MemoryError:
            raise  # not a sign of a corrupt file
        except Exception as e:  # numcodecs raises RuntimeError, ValueError, ...
            raise ZarrStoreDrift(
                f"chunk {coord} of array {meta.name!r} in {region.file} could not be "
                f"decoded ({type(e).__name__}: {e})",
                fix="run gbx_zarr_sync if the store changed outside GeoBrix; "
                "otherwise the file is corrupt",
            ) from None
        src, dst = [], []
        for c, n, (a, b) in zip(coord, meta.chunks, box):
            origin = c * n
            lo, hi = max(a, origin), min(b, origin + n)
            src.append(slice(lo - origin, hi - origin))
            dst.append(slice(lo - a, hi - a))
        out[tuple(dst)] = values[tuple(src)]  # converts to native byte order

    def fetch(group: list) -> tuple[list, list[bytes]]:
        return group, reader.read([r for _, r in group])

    # Reads go through a window: at most two groups per worker in flight, and no more
    # than _INFLIGHT_BYTES of compressed bytes read but not yet decoded. A finished read
    # is taken off the queue at once and its bytes live only in its decode tasks.
    window = 2 * store._workers
    waiting = deque(_io_groups(held))
    finished: queue.SimpleQueue = queue.SimpleQueue()
    pending: dict = {}  # future -> ("read" | "decode", compressed bytes)
    reading = inflight = 0

    def submit(fn, kind: str, nbytes: int, *args) -> None:
        future = pool.submit(fn, *args)
        pending[future] = (kind, nbytes)
        future.add_done_callback(finished.put)

    def start_reads() -> None:
        nonlocal reading, inflight
        while waiting and reading < window:
            nbytes = sum(r.length for _, r in waiting[0])
            if inflight and inflight + nbytes > _INFLIGHT_BYTES:
                break
            submit(fetch, "read", nbytes, waiting.popleft())
            reading += 1
            inflight += nbytes

    start_reads()
    try:
        while pending:
            done = finished.get()
            kind, nbytes = pending.pop(done)
            if kind == "read":
                reading -= 1
                group, datas = done.result()
                for (coord, region), data in zip(group, datas):
                    submit(place, "decode", region.length, coord, region, data)
                del group, datas
            else:
                done.result()
                inflight -= nbytes
            done = None  # drop the future, and the bytes it holds, before the next wait
            start_reads()
    except BaseException:
        for future in pending:
            future.cancel()
        raise
    return out
