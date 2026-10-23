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


def chunk_bytes(meta: ArrayMeta) -> int:
    """Decoded bytes of one whole inner chunk."""
    n = 1
    for c in meta.chunks:
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
) -> list[Unit]:
    """Pack the chunks ``axes`` select into units of at most ``budget_bytes`` decoded.

    A unit holds as many whole inner chunks as fit (at least one: a chunk bigger than the
    budget is a unit of its own). Chunks are ordered by shard, then by position, so
    neighbours share a unit. ``rows_by_shard`` maps the key a row is grouped under (a
    shard's key for a sharded array, the chunk key otherwise; see ``unit_key``) to its
    rows; a unit gets those of its chunks only. Nothing selected gives ``[]``.
    """
    chunks = touched_chunks(axes, meta.chunks)
    if not chunks:
        return []
    per_unit = max(1, int(budget_bytes) // max(1, chunk_bytes(meta)))
    if meta.shard is not None:
        chunks.sort(key=lambda c: (shard_of(c, meta.shard.cps), c))
    units = []
    for i in range(0, len(chunks), per_unit):
        group = tuple(chunks[i : i + per_unit])
        keys = dict.fromkeys(unit_key(meta, c) for c in group)
        rows = tuple(r for k in keys for r in rows_by_shard.get(k, ()))
        units.append(Unit(meta.name, group, rows))
    return units
