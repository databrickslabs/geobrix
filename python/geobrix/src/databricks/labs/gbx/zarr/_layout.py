"""Chunk and shard layouts for the arrays ``gbx_zarr_write`` creates.

An inner chunk is the read and write unit: about ``TARGET_CHUNK_BYTES`` (4 MiB) decoded.
A shard groups inner chunks into one object of at most ``SHARD_CAP_BYTES`` (128 MiB)
decoded. On a Unity Catalog Volume every object costs 40-50 ms to open, and Serverless
tasks run out of memory near 700 MB, which is where both numbers come from.

The last two dimensions are the spatial ones; any before them are leading dimensions
(time, level, ...). The presets differ in how they share the 4 MiB between the two:

- ``maps``: one leading step, large spatial tiles (a map at a time).
- ``series``: 32 x 32 spatial tiles, long runs of leading steps (point time series).
- ``balanced`` (the default): 24 leading steps, spatial tiles sized to fill the rest.

Only the first leading dimension takes the preset's leading size; any other leading
dimension has chunks of 1. A one-dimensional array has a single chunk dimension of about
4 MiB (at least 8 cells). Sizes are clipped to each dimension's length.
"""

from __future__ import annotations

import math

import numpy as np

__all__ = [
    "TARGET_CHUNK_BYTES",
    "SHARD_CAP_BYTES",
    "SHARD_MAX_CHUNKS",
    "PRESETS",
    "check_shard",
    "chunk_layout",
    "coordinate_chunks",
    "shard_layout",
]

TARGET_CHUNK_BYTES = 4 << 20
SHARD_CAP_BYTES = 128 << 20
# A shard holds at most this many inner chunks: its (virtual) index, 16 bytes per chunk,
# stays near 64 KiB, which every read of the shard fetches or builds. Measured: reading one
# inner chunk from a shard's chunk rows costs about 3 us per inner chunk of the shard,
# 13 ms at 4,096 (a third of one Volume object open) and 0.3 s at 93,312.
SHARD_MAX_CHUNKS = 4096
PRESETS = ("maps", "series", "balanced")
_SERIES_TILE = 32
_BALANCED_LEAD = 24
_MIN_SPATIAL = 8


def _clip(size: int, n: int) -> int:
    """``size`` clipped to a dimension of length ``n``, never below 1."""
    return max(1, min(int(size), int(n)))


def _preset_chunks(
    shape: tuple[int, ...], itemsize: int, preset: str
) -> tuple[int, ...]:
    ndim = len(shape)
    if ndim == 0:
        return ()
    if ndim == 1:
        return (_clip(max(_MIN_SPATIAL, TARGET_CHUNK_BYTES // itemsize), shape[0]),)
    lead_dims, (ny, nx) = shape[:-2], shape[-2:]
    lead = [1] * len(lead_dims)
    if preset == "maps":
        s = max(_MIN_SPATIAL, math.isqrt(TARGET_CHUNK_BYTES // itemsize))
        spatial = (_clip(s, ny), _clip(s, nx))
    elif preset == "series":
        spatial = (_clip(_SERIES_TILE, ny), _clip(_SERIES_TILE, nx))
        if lead:
            steps = max(
                1, TARGET_CHUNK_BYTES // (_SERIES_TILE * _SERIES_TILE * itemsize)
            )
            lead[0] = _clip(steps, lead_dims[0])
    else:  # balanced
        if lead:
            lead[0] = _clip(_BALANCED_LEAD, lead_dims[0])
        cells = TARGET_CHUNK_BYTES // (math.prod(lead) * itemsize)
        s = max(_MIN_SPATIAL, math.isqrt(cells))
        spatial = (_clip(s, ny), _clip(s, nx))
    return (*lead, *spatial)


def shard_layout(
    shape: tuple[int, ...], chunks: tuple[int, ...], itemsize: int
) -> tuple[int, ...]:
    """The shard shape for ``chunks`` (always a whole multiple of them).

    The spatial shard starts as the whole spatial extent, rounded up to a multiple of the
    chunk. While a shard holds more than ``SHARD_CAP_BYTES`` the spatial shard is halved,
    in whole chunks. Then the first leading dimension grows by whole chunks while the shard
    stays within the cap, past the dimension's current length if need be: a store created
    small and appended to along time keeps filling the same shards, as the brief's rule
    and the append workload want. A shard never holds more than ``SHARD_MAX_CHUNKS``
    inner chunks: the spatial shard is halved further if it alone has more,
    and the leading growth stops there.
    """
    shape, chunks = tuple(int(n) for n in shape), tuple(int(c) for c in chunks)
    ndim = len(shape)
    if ndim == 0:
        return ()
    spatial = list(range(max(0, ndim - 2), ndim))
    count = [1] * ndim  # chunks per shard along each dimension
    for d in spatial:
        count[d] = max(1, -(-shape[d] // chunks[d]))

    def nbytes() -> int:
        return math.prod(c * k for c, k in zip(chunks, count)) * itemsize

    def too_big() -> bool:
        return nbytes() > SHARD_CAP_BYTES or math.prod(count) > SHARD_MAX_CHUNKS

    while too_big() and any(count[d] > 1 for d in spatial):
        for d in spatial:
            count[d] = -(-count[d] // 2)
    if ndim > 2:
        lead = 0
        slab = nbytes()  # one leading chunk's worth of the shard
        by_chunks = max(1, SHARD_MAX_CHUNKS // math.prod(count))
        count[lead] = max(1, min(SHARD_CAP_BYTES // max(1, slab), by_chunks))
    return tuple(c * k for c, k in zip(chunks, count))


def check_shard(chunks, shards) -> None:
    """Refuse a shard that holds more than ``SHARD_MAX_CHUNKS`` inner chunks."""
    n = math.prod(int(s) // int(c) for s, c in zip(shards, chunks))
    if n > SHARD_MAX_CHUNKS:
        raise ValueError(
            f"a shard of {tuple(shards)} holds {n} inner chunks of {tuple(chunks)}, "
            f"more than the {SHARD_MAX_CHUNKS} a shard may hold (every read of a shard "
            "fetches or builds an index of 16 bytes per inner chunk). "
            "Fix: pass smaller shards= sizes, or larger chunks=."
        )


def chunk_layout(
    shape, dims, dtype, preset="balanced"
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """``(chunks, shards)`` for an array of ``shape`` and ``dtype`` with dimensions ``dims``.

    ``preset`` is ``"maps"``, ``"series"``, ``"balanced"`` or a dict of dimension name to
    chunk size; a dimension the dict does not name takes the ``balanced`` size. Sizes in a
    dict are used as given (an append dimension may want chunks longer than today's data).
    """
    shape = tuple(int(n) for n in shape)
    dims = tuple(dims)
    if len(shape) != len(dims):
        raise ValueError(
            f"shape {shape} and dims {dims} differ in length. "
            "Fix: name one dimension per axis."
        )
    itemsize = np.dtype(dtype).itemsize
    explicit = {}
    if isinstance(preset, dict):
        explicit, preset = dict(preset), "balanced"
        unknown = [d for d in explicit if d not in dims]
        if unknown:
            raise ValueError(
                f"chunks names dimension(s) {unknown} that the array {dims} does not "
                "have. Fix: name only the array's dimensions."
            )
        bad = {
            d: v
            for d, v in explicit.items()
            if isinstance(v, bool) or not isinstance(v, int) or v < 1
        }
        if bad:
            raise ValueError(
                f"chunk sizes must be positive integers, got {bad}. "
                "Fix: pass chunks={'time': 24, ...} with whole numbers."
            )
    elif preset not in PRESETS:
        raise ValueError(
            f"chunks must be one of {PRESETS} or a dict of dimension to size, got "
            f"{preset!r}. Fix: pass chunks='balanced' (the default)."
        )
    chunks = list(_preset_chunks(shape, itemsize, preset))
    for i, d in enumerate(dims):
        if d in explicit:
            chunks[i] = int(explicit[d])
    chunks = tuple(chunks)
    return chunks, shard_layout(shape, chunks, itemsize)


def coordinate_chunks(length: int, dtype) -> tuple[int]:
    """The single chunk of a dimension coordinate: the whole coordinate, and room to grow.

    Coordinates are written unsharded as one chunk. It is at least ``TARGET_CHUNK_BYTES``
    long, so appends along the dimension keep landing in that chunk (a 4 MiB chunk holds
    60 years of hourly int64 times) and concurrent appenders rebase one small chunk.
    """
    itemsize = np.dtype(dtype).itemsize
    return (max(1, int(length), TARGET_CHUNK_BYTES // itemsize),)
