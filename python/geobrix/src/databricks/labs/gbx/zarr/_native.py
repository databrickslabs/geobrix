"""Native box reads of a manifest array: batch the byte ranges, decode in a pool.

zarr-python reads a store one key at a time: a box over a sharded array becomes one range
request per inner chunk, and each request opens its file again. On a Unity Catalog Volume
(FUSE) an open costs tens of milliseconds, so that is where the time goes (measured in a
benchmark). This engine works from the other end. It asks the store where every chunk of the
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
stored size of the box. A read group holds at most ``_group_bytes()`` compressed bytes
(an eighth of the budget, at most ``_IO_GROUP_BYTES``; one oversized chunk is its own
group), so the budget never limits the reads in flight to fewer than about eight. The file
reader copies each group once more while it splits it into chunks.

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
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrStoreDrift, ZarrUnsupported

__all__ = ["make_decoder", "read_box_native", "read_points_native"]

_BYTES_TO_BYTES = ("zstd", "gzip", "blosc", "crc32c")
_IO_GROUP_BYTES = 32 << 20  # split one file's reads into tasks of about this size
_INFLIGHT_BYTES = 64 << 20  # compressed bytes read (or being read) but not yet decoded
_MIN_READS_IN_FLIGHT = 8  # the budget is cut into at least this many read groups
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


def _group_bytes() -> int:
    """The most compressed bytes one read group holds.

    A group is one read call: on a Volume it pays a latency and runs at the rate of one
    stream, so a box is read fastest with many groups in flight. The budget bounds memory
    by bytes, so a group is a fraction of it: at least ``_MIN_READS_IN_FLIGHT`` groups fit
    in the budget at once, however large the files are.
    """
    return max(1, min(_IO_GROUP_BYTES, _INFLIGHT_BYTES // _MIN_READS_IN_FLIGHT))


def _io_groups(items: list[tuple[tuple, Region]]) -> list[list[tuple[tuple, Region]]]:
    """Split the (coords, region) pairs into reads of one file each, in offset order."""
    limit = _group_bytes()
    by_file: dict[tuple[str, str], list] = {}
    for item in items:
        by_file.setdefault((item[1].file, item[1].origin), []).append(item)
    groups = []
    for members in by_file.values():
        members.sort(key=lambda it: it[1].offset)
        group, size = [], 0
        for item in members:
            if group and size + item[1].length > limit:
                groups.append(group)
                group, size = [], 0
            group.append(item)
            size += item[1].length
        groups.append(group)
    return groups


def _decoded(meta: ArrayMeta, decode, coord: tuple, region: Region, data: bytes):
    """The values of one chunk, or ``ZarrStoreDrift`` when its bytes do not decode."""
    try:
        return decode(data)
    except MemoryError:
        raise  # not a sign of a corrupt file
    except Exception as e:  # numcodecs raises RuntimeError, ValueError, ...
        raise ZarrStoreDrift(
            f"chunk {coord} of array {meta.name!r} in {region.file} could not be "
            f"decoded ({type(e).__name__}: {e})",
            fix="run gbx_zarr_sync if the store changed outside GeoBrix; "
            "otherwise the file is corrupt",
        ) from None


def read_box_native(store, meta: ArrayMeta, box, decode) -> np.ndarray:
    """The box (``(start, stop)`` per axis, inside the array) of ``meta`` in ``store``.

    ``decode`` is ``make_decoder(meta)``. Chunks the store does not hold read as the
    fill value. Errors from the reader (``ZarrStoreDrift``) propagate. Call it from a
    thread of your own, not from the store's pool.
    """
    store.ensure_outside_pool("read_box")
    store._executor()  # raises for a closed store, empty box or not
    out = np.full(tuple(b - a for a, b in box), _fill(meta), dtype=native_dtype(meta))
    if not out.size:
        return out

    def place(coord: tuple, region: Region, data: bytes) -> None:
        values = _decoded(meta, decode, coord, region, data)
        src, dst = [], []
        for c, n, (a, b) in zip(coord, meta.chunks, box):
            origin = c * n
            lo, hi = max(a, origin), min(b, origin + n)
            src.append(slice(lo - origin, hi - origin))
            dst.append(slice(lo - a, hi - a))
        out[tuple(dst)] = values[tuple(src)]  # converts to native byte order

    _read_chunks(store, meta, list(itertools.product(*_chunk_ranges(meta, box))), place)
    return out


def read_points_native(store, meta: ArrayMeta, coords, decode) -> np.ndarray:
    """The values at the cells ``coords`` (one integer array per axis, all one length) of
    ``meta`` in ``store``, as a 1-D array in that order.

    Only the chunks that hold a requested cell are read and decoded, each once, however many
    cells it holds; a cell of a chunk the store does not hold reads as the fill value. Errors
    from the reader (``ZarrStoreDrift``) propagate. Call it from a thread of your own, not
    from the store's pool.
    """
    store.ensure_outside_pool("read_points")
    store._executor()  # raises for a closed store
    coords = [np.asarray(c, dtype=np.int64).reshape(-1) for c in coords]
    out = np.full(coords[0].shape, _fill(meta), dtype=native_dtype(meta))
    if not out.size:
        return out
    grid = tuple(-(-n // c) for n, c in zip(meta.shape, meta.chunks))
    which = np.ravel_multi_index(
        tuple(c // n for c, n in zip(coords, meta.chunks)), grid
    )
    order = np.argsort(which, kind="stable")
    ids, starts = np.unique(which[order], return_index=True)
    ends = np.append(starts[1:], order.size)
    cells = {
        int(i): order[a:b] for i, a, b in zip(ids, starts, ends, strict=True)
    }  # chunk id -> the requested cells it holds

    def place(coord: tuple, region: Region, data: bytes) -> None:
        values = _decoded(meta, decode, coord, region, data)
        at = cells[int(np.ravel_multi_index(coord, grid))]
        out[at] = values[
            tuple(c[at] - k * n for c, k, n in zip(coords, coord, meta.chunks))
        ]

    wanted = [tuple(int(k) for k in np.unravel_index(i, grid)) for i in cells]
    _read_chunks(store, meta, wanted, place)
    return out


def _read_chunks(store, meta: ArrayMeta, coords: list, place) -> None:
    """Fetch the chunks at ``coords`` and hand each to ``place(coord, region, data)`` on the
    store's pool. A chunk the store does not hold is not placed (the caller's fill stays).
    """
    pool = store._executor()  # raises for a closed store
    reader = store._reader
    regions = store.chunk_regions(meta.name, coords)
    held = [(c, r) for c, r in zip(coords, regions) if r is not None]

    def fetch(group: list) -> tuple[list, list[bytes]]:
        datas = reader.read([r for _, r in group])
        if len(datas) != len(group):  # never let an unread chunk read as the fill value
            raise ZarrError(
                f"the reader returned {len(datas)} results for {len(group)} regions "
                f"of array {meta.name!r}",
                fix="this is a defect in the range reader; report it",
            )
        return group, datas

    # Reads go through a window: at most two groups per worker in flight, and no more
    # than _INFLIGHT_BYTES of compressed bytes read but not yet decoded. A finished read
    # is taken off the queue at once and its bytes live only in its decode tasks.
    window = 2 * store._workers
    waiting = deque((g, sum(r.length for _, r in g)) for g in _io_groups(held))
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
            nbytes = waiting[0][1]
            if inflight and inflight + nbytes > _INFLIGHT_BYTES:
                break
            submit(fetch, "read", nbytes, waiting.popleft()[0])
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
                for (coord, region), data in zip(group, datas, strict=True):
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
    if waiting or inflight:  # every group read and every chunk decoded, or it is a bug
        raise ZarrError(
            f"a read of array {meta.name!r} ended with {len(waiting)} groups unread "
            f"and {inflight} bytes undecoded",
            fix="this is a defect in the native engine; report it",
        )
