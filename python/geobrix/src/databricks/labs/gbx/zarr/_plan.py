"""Work units: how a read is cut into tasks.

A read touches some inner chunks of an array. ``plan_units`` packs those chunks into
``Unit``s, each small enough that a task decodes it within the memory budget, and gives
every unit only the manifest rows its chunks need, so a task builds its store from the
unit alone (no session, no table). A unit is plain data: ``to_json`` and ``from_json`` are
how it travels to a task.

Chunks are grouped by the shard they lie in, so a unit mostly reads whole neighbouring
files, not one chunk of each.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass

from databricks.labs.gbx.zarr._keys import shard_of
from databricks.labs.gbx.zarr._meta import ArrayMeta
from databricks.labs.gbx.zarr._select import AxisSel, chunk_slices, touched_chunks

__all__ = [
    "Unit",
    "plan_units",
    "chunk_bytes",
    "unit_key",
    "row_unit_key",
    "boxes_of",
    "keys_of_boxes",
    "rows_for_boxes",
]

# The manifest columns a task needs to serve a row. The rest (checksum, write id, tag, the
# metadata text) stay on the driver: a unit travels in a closure, once per task.
_ROW_COLUMNS = (
    "kind",
    "array",
    "key",
    "shard_key",
    "file",
    "offset",
    "length",
    "origin",
    "identity",
)


@dataclass(frozen=True)
class Unit:
    """One task's work: chunks of one array and the manifest rows they need.

    ``chunks`` are chunk grid indices; ``rows`` the shard and chunk rows of the shards
    those chunks lie in (an empty tuple for a plain store, which has no manifest). A table
    read puts the rows of every variable it reads in ``rows``: they carry their array.
    """

    array: str
    chunks: tuple[tuple[int, ...], ...]
    rows: tuple[dict, ...]

    def to_json(self) -> str:
        return json.dumps(
            {
                "array": self.array,
                "chunks": [list(c) for c in self.chunks],
                "rows": [{k: r[k] for k in _ROW_COLUMNS if k in r} for r in self.rows],
            },
            separators=(",", ":"),
        )

    @staticmethod
    def from_json(s: str) -> "Unit":
        d = json.loads(s)
        return Unit(
            array=d["array"],
            chunks=tuple(tuple(c) for c in d["chunks"]),
            rows=tuple(d["rows"]),
        )


def chunk_bytes(meta: ArrayMeta, chunk: tuple[int, ...] | None = None) -> int:
    """Decoded bytes of one inner chunk: the cells of ``chunk`` inside the array's shape (an edge
    chunk, or an inner chunk longer than its array, holds fewer than a whole one), or the whole
    inner chunk when no ``chunk`` is named."""
    n = 1
    for i, c in enumerate(meta.chunks):
        if chunk is not None:
            c = max(0, min(c, meta.shape[i] - chunk[i] * c))
        n *= c
    return n * meta.itemsize


def unit_key(meta: ArrayMeta, chunk: tuple[int, ...]) -> str:
    """The key the manifest rows of ``chunk`` are grouped under: its shard's key for a
    sharded array, its own chunk key otherwise."""
    if meta.shard is not None:
        chunk = shard_of(chunk, meta.shard.cps)
    return meta.key_encoding.chunk_key(chunk)


def row_unit_key(meta: ArrayMeta, row: dict) -> str:
    """``unit_key`` of the chunk a manifest row describes (a row without a ``shard_key``
    on a sharded array takes it from its key, as the store does)."""
    if meta.shard is None:
        return row["key"]
    shard = row.get("shard_key")
    if shard is not None:
        return shard
    if row.get("kind") == "shard":
        return row["key"]
    coords = meta.key_encoding.parse(row["key"], len(meta.shape))
    return meta.key_encoding.chunk_key(shard_of(coords, meta.shard.cps))


def boxes_of(
    meta: ArrayMeta, axes: list[AxisSel], chunk: tuple[int, ...]
) -> list[tuple[tuple[int, int], ...]]:
    """The selected cells of ``chunk`` as boxes of the whole array: ``(start, stop)`` per
    dimension. One box unless a dimension's selection has several ranges in this chunk
    (a list of labels, or a slice across a longitude seam)."""
    out = []
    for within, _ in chunk_slices(axes, chunk, meta.chunks, meta.shape):
        origin = tuple(c * n for c, n in zip(chunk, meta.chunks))
        out.append(tuple((o + s.start, o + s.stop) for o, s in zip(origin, within)))
    return out


def keys_of_boxes(meta: ArrayMeta, boxes) -> list[str]:
    """The ``unit_key``s of every chunk of ``meta`` the boxes touch (any boxes of the array,
    not only chunk-aligned ones), without repeats."""
    keys: dict[str, None] = {}
    for box in boxes:
        lo = [a // n for (a, _), n in zip(box, meta.chunks)]
        hi = [(b - 1) // n for (_, b), n in zip(box, meta.chunks)]
        for chunk in itertools.product(*(range(a, b + 1) for a, b in zip(lo, hi))):
            keys[unit_key(meta, chunk)] = None
    return list(keys)


def rows_for_boxes(
    meta: ArrayMeta, boxes, rows_by_shard: dict[str, list[dict]]
) -> list[dict]:
    """The rows of ``meta``'s array that serve the chunks ``boxes`` touch."""
    return [r for k in keys_of_boxes(meta, boxes) for r in rows_by_shard.get(k, ())]


def plan_units(
    meta: ArrayMeta,
    axes: list[AxisSel],
    rows_by_shard: dict[str, list[dict]],
    budget_bytes: int,
    *,
    slots: int = 1,
    weight: int = 1,
) -> list[Unit]:
    """Pack the chunks ``axes`` select into units of decoded size ``read_unit_bytes``.

    A unit holds as many whole inner chunks as fit (at least one: a chunk bigger than the
    unit size is a unit of its own). The unit size is the read target (``ZARR_READ_UNIT_TARGET``),
    lowered so each of ``slots`` task slots gets a unit when the read is big enough for that, and
    never above ``budget_bytes``, the most a task may decode. A chunk counts the cells it holds
    inside the array. ``weight`` is how many arrays of this shape a unit decodes at once (a table
    read of several variables). Chunks are ordered by shard, then by position, so neighbours
    share a unit. ``rows_by_shard`` maps the key a row is grouped under (a shard's key for a
    sharded array, the chunk key otherwise; see ``unit_key``) to its rows; a unit gets those of
    its chunks only. Nothing selected gives ``[]``.
    """
    from databricks.labs.gbx.pyrx.core.budget import read_unit_bytes

    chunks = touched_chunks(axes, meta.chunks)
    if not chunks:
        return []
    if meta.shard is not None:
        chunks.sort(key=lambda c: (shard_of(c, meta.shard.cps), c))
    sizes = [max(1, chunk_bytes(meta, c)) * max(1, weight) for c in chunks]
    cap = read_unit_bytes(budget_bytes, sum(sizes), slots)
    units = []
    start = held = 0
    for i, size in enumerate(sizes):
        if i > start and held + size > cap:
            units.append(_unit(meta, chunks[start:i], rows_by_shard))
            start, held = i, 0
        held += size
    units.append(_unit(meta, chunks[start:], rows_by_shard))
    return units


def _unit(meta: ArrayMeta, group, rows_by_shard) -> Unit:
    group = tuple(group)
    keys = dict.fromkeys(unit_key(meta, c) for c in group)
    rows = tuple(r for k in keys for r in rows_by_shard.get(k, ()))
    return Unit(meta.name, group, rows)
