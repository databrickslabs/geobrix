"""A table of cells into Zarr: the cells half of ``gbx_zarr_write``.

A DataFrame with one row per cell (a column per dimension, named in ``dim_cols``, and a
column per variable) is turned into the blocks the rest of the writer already commits, so
everything below this module is shared with blocks, tiles and ``xarray`` input.

**What the driver learns first.** At most two small passes over the *dimension columns*
(never the variables' values): the distinct sorted labels of each dimension that has none
yet (a new store's dimensions, an append's new steps; ``coords=`` replaces that pass), and
the storage units (shards, or chunks of an unsharded array) the cells fall in. A table that
fills its grid (as many cells as the product of its labels) needs no second pass. From
these the driver plans every block: one per variable and unit, so a create or an append
needs no position pass over the blocks, and the data pass reads the table once.

**The data pass: combine, then one shuffle.** The driver first picks, per group of variables
that share a layout, the *unit* a task builds (``_choose_unit``): the whole shard when the
shard of all the group's variables fits the task budget, else one inner chunk at a time. Each
input partition locates its cells and gathers them by that unit into *partial blocks* (a dense
block with a mask, or the cells as they came when few), holding at most half the task budget
and shipping the oldest first. The shuffle moves those partial blocks, keyed by unit, not a row
per cell: a table that arrives roughly in unit order (``gbx_zarr_read``'s) ships each unit in
one or a few pieces. The blocks are repartitioned so that each partition owns whole shards, as
evenly as the shards allow (one partition per shard touched, up to four per slot of the
session's parallelism), sorted within the partition, and a streaming merge builds one unit at a
time, dense, with the fill value where no cell came. Its block goes straight to the writer's
pack stage, which encodes it and writes the shard it belongs to. A shard that does not fit the
task's memory is written one inner chunk at a time (a plain store spools the shard's chunks on
local disk and writes it from there): a chunk is then the most a task holds, plus the partials
of that chunk in the batch in hand, whatever the size of the shard. A shard that fits is built
and written whole, which has fewer blocks and less to move; both make the same files.

**Rules.** A cell missing from the table reads as the fill value. Two cells at one position
refuse the write (the task that meets them, on the map side or the merge, raises ``ZarrError``) before anything is
committed. A null variable value is a missing cell; a null dimension value is refused.
``create`` and ``append`` take cells; ``update`` and ``overwrite`` would overwrite stored
cells with fill wherever the table has none, so they take blocks.

Nothing here touches ``spark.conf``, ``sparkContext``, ``.rdd``, ``_jvm`` or ``_jsc``.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field, replace

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.zarr._write_tiles import (
    _SEEN_ARROW,
    Prepared,
    StoreView,
    _append_dim_of,
    _coord_block,
)
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrGridMismatch, ZarrUnsupported

__all__ = ["CellsArgs", "CellsPrepared", "check_columns", "prepare"]

_MIB = 1 << 20
_MAX_LABELS = 20_000_000  # distinct labels of one dimension the driver will hold
_LAT = {"units": "degrees_north", "standard_name": "latitude"}
_LON = {"units": "degrees_east", "standard_name": "longitude"}
_DISTINCT_ARROW = pa.schema(
    [("dim", pa.int32()), ("vals", pa.binary()), ("n", pa.int64())]
)
_KEYS_ARROW = pa.schema([("group", pa.int32()), ("keys", pa.binary())])
_COORDS_FIX = "pass coords={'<dimension>': <values>} for it, or drop the null rows"


@dataclass
class CellsArgs:
    """What a cells write needs beyond the table: the labels the user fixed (``coords``)
    and the layout a new store's arrays get."""

    coords: dict | None = None
    complete: bool = (
        False  # coords fix every label and every unit has cells: skip the survey
    )
    chunks: object = "balanced"
    shards: object = None
    codecs: object = None


@dataclass
class CellsPrepared(Prepared):
    units: dict = field(default_factory=dict)  # variable -> storage unit (a new store)
    chunk_vars: frozenset = frozenset()  # variables whose blocks are single chunks


# --- the columns ----------------------------------------------------------------------------


def _dim_kind(dtype) -> str | None:
    """``M`` for times and dates, ``i`` for integers, ``f`` for other numbers."""
    from pyspark.sql import types as T

    if isinstance(dtype, (T.TimestampType, T.TimestampNTZType, T.DateType)):
        return "M"
    if isinstance(dtype, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return "i"
    if isinstance(dtype, (T.FloatType, T.DoubleType, T.DecimalType)):
        return "f"
    return None


def _var_dtype(dtype) -> np.dtype | None:
    from pyspark.sql import types as T

    table = {
        T.ByteType: "i1",
        T.ShortType: "i2",
        T.IntegerType: "i4",
        T.LongType: "i8",
        T.FloatType: "f4",
        T.DoubleType: "f8",
        T.BooleanType: "?",
    }
    code = table.get(type(dtype))
    return np.dtype(code) if code else None


def check_columns(df, dim_cols, names) -> tuple[dict, list, dict, dict]:
    """``(dim_cols, variables, dim_kinds, var_dtypes)`` of a table of cells: every dimension
    column a number or a time, every variable column a number or a boolean."""
    if not isinstance(dim_cols, dict) or not dim_cols:
        raise ValueError(
            f"dim_cols must map each dimension name to a column name, got {dim_cols!r}. "
            "Fix: pass dim_cols={'time': 'time', 'latitude': 'latitude', 'longitude': "
            "'longitude'}."
        )
    fields = {f.name: f.dataType for f in df.schema.fields}
    kinds = {}
    for dim, col in dim_cols.items():
        if not isinstance(dim, str) or not isinstance(col, str):
            raise ValueError(
                f"dim_cols must map each dimension name to a column name, got {dim_cols!r}. "
                "Fix: pass dim_cols={'time': 'time'}."
            )
        if col not in fields:
            raise ValueError(
                f"dim_cols maps {dim!r} to the column {col!r}, which the table does not "
                f"have (it has {sorted(fields)}). Fix: name a column of the table."
            )
        kind = _dim_kind(fields[col])
        if kind is None:
            raise ZarrUnsupported(
                f"the dim_cols column {col!r} for {dim!r} holds "
                f"{fields[col].simpleString()} values; GeoBrix coordinates are numbers or times",
                fix=f"cast {col!r} to a number or a timestamp",
            )
        kinds[dim] = kind
    if len(set(dim_cols.values())) != len(dim_cols):
        raise ValueError(
            "two dimensions use one column in dim_cols. Fix: give each dimension its own "
            "column."
        )
    taken = set(dim_cols.values())
    wanted = list(names) if names is not None else [c for c in fields if c not in taken]
    for n in wanted:
        if n not in fields:
            raise ValueError(
                f"variables names {n!r}, which the table does not have (it has "
                f"{sorted(c for c in fields if c not in taken)}). Fix: name columns of the "
                "table."
            )
        if n in taken:
            raise ValueError(
                f"variables names {n!r}, a dimension column. Fix: name the value columns."
            )
    if not wanted:
        raise ValueError(
            "the table has no value columns beside its dim_cols columns. Fix: add the "
            "variables as columns."
        )
    dtypes = {}
    for n in wanted:
        dt = _var_dtype(fields[n])
        if dt is None:
            raise ZarrUnsupported(
                f"the column {n!r} holds {fields[n].simpleString()} values; GeoBrix writes "
                "numbers and booleans",
                fix=f"cast {n!r} to a float, an integer or a boolean, or leave it out of variables",
            )
        dtypes[n] = dt
    return dict(dim_cols), wanted, kinds, dtypes


# --- labels ---------------------------------------------------------------------------------


def _raw(col, kind: str, name: str) -> np.ndarray:
    """A dimension column of a batch as int64 (times in ns since the epoch, integers) or
    float64; a null is refused."""
    if col.null_count:
        raise ZarrError(
            f"the {name!r} column has {col.null_count} null value(s) in a batch of cells",
            fix="drop or fill the cells without a position before writing",
        )
    if kind == "M":
        return (
            col.cast(pa.timestamp("ns")).cast(pa.int64()).to_numpy(zero_copy_only=False)
        )
    if kind == "i":
        return col.cast(pa.int64()).to_numpy(zero_copy_only=False)
    return col.cast(pa.float64()).to_numpy(zero_copy_only=False)


class Axis:
    """The labels of one dimension, in array order, and the lookup from a cell's value to its
    position. Times and integers match exactly, other numbers to a relative 1e-9."""

    def __init__(self, name: str, natural: np.ndarray):
        natural = np.asarray(natural)
        if natural.dtype.kind == "M":
            natural = natural.astype("datetime64[ns]")
            self.kind, raw = "M", natural.astype("int64")
        elif natural.dtype.kind in "iu":
            self.kind, raw = "i", natural.astype("int64")
        elif natural.dtype.kind == "f":
            self.kind, raw = "f", natural.astype("float64")
        else:
            raise ZarrUnsupported(
                f"the labels of {name!r} are {natural.dtype}; GeoBrix labels are numbers or "
                "times",
                fix="pass numbers or datetime64 values",
            )
        self.name = name
        self.natural = natural
        self.raw = raw
        self.order = np.argsort(raw, kind="stable")
        self.sorted = raw[self.order]
        if len(raw) > 1 and (np.diff(self.sorted) == 0).any():
            raise ValueError(
                f"the labels of {name!r} repeat a value. Fix: pass distinct labels."
            )

    def __len__(self) -> int:
        return len(self.raw)

    def locate(self, values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """``(position, found)`` of each value among the labels."""
        s = self.sorted
        if not len(s):
            return np.zeros(len(values), np.int64), np.zeros(len(values), bool)
        if self.kind != "f" and values.dtype.kind == "i":
            i = np.clip(np.searchsorted(s, values), 0, len(s) - 1)
            return self.order[i], s[i] == values
        sf, v = s.astype("float64"), values.astype("float64")
        i = np.clip(np.searchsorted(sf, v), 0, len(sf) - 1)
        j = np.clip(i - 1, 0, len(sf) - 1)
        pick = np.where(np.abs(sf[j] - v) < np.abs(sf[i] - v), j, i)
        near = np.isclose(
            sf[pick], v, rtol=1e-9, atol=1e-12 * np.maximum(1.0, np.abs(v))
        )
        return self.order[pick], near

    def label(self, position: int) -> str:
        v = self.natural[position]
        return str(v)


def _axis_from_store(dim: str, labels) -> Axis:
    if labels is None:
        raise ZarrError(
            f"the store has no coordinate array for {dim!r}, so cells cannot be placed by "
            f"their {dim!r} value",
            fix=f"write the {dim!r} coordinate first, or write blocks",
        )
    return Axis(dim, np.asarray(labels))


def _axis_from_coords(dim: str, values) -> Axis:
    arr = np.asarray(values)
    if arr.ndim != 1 or not arr.size:
        raise ValueError(
            f"coords for {dim!r} must be a non-empty one-dimensional array. Fix: pass a list "
            "or array of labels."
        )
    return Axis(dim, arr)


def _natural_from_raw(kind: str, raw: np.ndarray) -> np.ndarray:
    if kind == "M":
        return raw.astype("int64").view("datetime64[ns]")
    return raw.astype("int64" if kind == "i" else "float64")


# --- the passes over the dimension columns --------------------------------------------------


def _unique(values: np.ndarray) -> np.ndarray:
    """The distinct values, sorted: a hash pass over the batch, then a sort of the few."""
    return np.sort(pc.unique(pa.array(values)).to_numpy(zero_copy_only=False))


def _distinct_function(kinds: list, names: list):
    def run(batches):
        acc = [None] * len(kinds)
        n = 0
        for b in batches:
            n += b.num_rows
            for i, (k, name) in enumerate(zip(kinds, names)):
                u = _unique(_raw(b.column(i), k, name))
                acc[i] = u if acc[i] is None else np.union1d(acc[i], u)
        for i, u in enumerate(acc):
            if u is None:
                continue
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([i], pa.int32()),
                    pa.array([u.tobytes()], pa.binary()),
                    pa.array([n if i == 0 else 0], pa.int64()),
                ],
                schema=_DISTINCT_ARROW,
            )

    return run


def _survey_distinct(df, cols: list, kinds: list) -> tuple[list, int]:
    """``(sorted distinct raw values per column, rows)`` in one pass over those columns."""
    from pyspark.sql import functions as F

    table = (
        df.select(*[F.col(c).alias(f"__d{i}") for i, c in enumerate(cols)])
        .mapInArrow(_distinct_function(kinds, cols), "dim int, vals binary, n long")
        .toArrow()
    )
    out: list = [[] for _ in cols]
    rows = 0
    for dim, vals, n in zip(
        *[table.column(c).to_pylist() for c in ("dim", "vals", "n")]
    ):
        dt = np.float64 if kinds[dim] == "f" else np.int64
        out[dim].append(np.frombuffer(vals, dtype=dt))
        rows += n
    merged = []
    for parts in out:
        u = np.unique(np.concatenate(parts)) if parts else np.zeros(0, np.int64)
        merged.append(u)
    return merged, rows


@dataclass
class Group:
    """Variables that share dimensions (in order), unit and window: one pipeline."""

    dims: tuple
    cols: tuple  # the table's column of each dimension
    ckinds: tuple  # the kind of each dimension column
    axes: tuple  # Axis per dimension
    offsets: tuple  # added to a label's position: where the labels start in the array
    unit: tuple  # per dimension (cells), what a task builds one at a time: the shard, or the inner chunk
    part: (
        tuple  # storage unit per dimension (cells): a shard, which one task owns whole
    )
    nu: tuple  # chunks per dimension in the array after the write
    wlo: tuple  # the window being written, per dimension
    whi: tuple
    vars: tuple
    dtypes: dict
    fill: dict
    whole: bool = (
        True  # a task builds a whole shard (``unit`` is ``part``), not a chunk
    )


def _absolute(batch, g: Group):
    """The array position of each cell of a batch, per dimension."""
    out = []
    for i, (axis, kind, name) in enumerate(zip(g.axes, g.ckinds, g.cols)):
        raw = _raw(batch.column(f"__d_{g.dims[i]}"), kind, name)
        if (kind == "M") != (axis.kind == "M"):
            raise ZarrGridMismatch(
                f"the {g.dims[i]!r} column holds {'times' if kind == 'M' else 'numbers'} and "
                f"its labels are {'times' if axis.kind == 'M' else 'numbers'}",
                fix="give the column the type of the labels",
            )
        pos, found = axis.locate(raw)
        if not found.all():
            bad = raw[int(np.flatnonzero(~found)[0])]
            shown = np.datetime64(int(bad), "ns") if kind == "M" else bad
            raise ZarrGridMismatch(
                f"no {g.dims[i]!r} label equals {shown}",
                fix=(
                    f"write values the store's {g.dims[i]!r} labels have, or use "
                    "mode='append' for new steps of the append dimension"
                ),
            )
        out.append(pos.astype(np.int64) + g.offsets[i])
    return out


def _unit_key(g: Group, absolute: list) -> np.ndarray:
    key = np.zeros(len(absolute[0]), dtype=np.int64)
    for a, u, n in zip(absolute, g.unit, g.nu):
        key = key * n + a // u
    return key


def _box_of(g: Group, key: int) -> tuple[tuple, tuple]:
    """The array positions ``[lo, hi)`` the unit of ``key`` holds within the window."""
    idx = np.unravel_index(int(key), g.nu)
    lo = tuple(max(int(i) * u, a) for i, u, a in zip(idx, g.unit, g.wlo))
    hi = tuple(min((int(i) + 1) * u, b) for i, u, b in zip(idx, g.unit, g.whi))
    return lo, hi


def _keys_function(groups: list):
    def run(batches):
        acc = [None] * len(groups)
        for b in batches:
            for gi, g in enumerate(groups):
                u = _unique(_unit_key(g, _absolute(b, g)))
                acc[gi] = u if acc[gi] is None else np.union1d(acc[gi], u)
        for gi, u in enumerate(acc):
            if u is not None:
                yield pa.RecordBatch.from_arrays(
                    [pa.array([gi], pa.int32()), pa.array([u.tobytes()], pa.binary())],
                    schema=_KEYS_ARROW,
                )

    return run


def _survey_units(df, groups: list) -> list:
    """The sorted keys of the units each group's cells fall in, in one pass over the
    dimension columns. The pass also finds a label the store lacks, on the driver."""
    from pyspark.sql import functions as F

    g0 = groups[0]
    table = (
        df.select(*[F.col(c).alias(f"__d_{d}") for d, c in zip(g0.dims, g0.cols)])
        .mapInArrow(_keys_function(groups), "group int, keys binary")
        .toArrow()
    )
    parts: list = [[] for _ in groups]
    for gi, keys in zip(
        table.column("group").to_pylist(), table.column("keys").to_pylist()
    ):
        parts[gi].append(np.frombuffer(keys, dtype=np.int64))
    return [np.unique(np.concatenate(p)) if p else np.zeros(0, np.int64) for p in parts]


# --- the data pass --------------------------------------------------------------------------


def _partial_arrow(g: Group) -> pa.Schema:
    """The rows the map side hands to the shuffle: one per partial block."""
    fields = [
        ("__k", pa.int64()),
        ("__n", pa.int64()),
        ("__box", pa.binary()),
        ("__m", pa.binary()),
    ]
    return pa.schema(fields + [(f"__v{i}", pa.binary()) for i in range(len(g.vars))])


def _placed_arrow(g: Group) -> pa.Schema:
    """A partial block with the partition ``__p`` it is shuffled to."""
    base = _partial_arrow(g)
    return base.append(pa.field("__p", pa.int32())).append(pa.field("__s", pa.int64()))


def _partial_struct(g: Group):
    from pyspark.sql import types as T

    return T.StructType(
        [T.StructField("__k", T.LongType())]
        + [T.StructField("__n", T.LongType())]
        + [T.StructField(n, T.BinaryType()) for n in ("__box", "__m")]
        + [T.StructField(f"__v{i}", T.BinaryType()) for i in range(len(g.vars))]
        + [T.StructField("__p", T.IntegerType())]
        + [T.StructField("__s", T.LongType())]
    )


# --- placing units on partitions -------------------------------------------------------------
#
# ``repartition(n, "__k")`` hashes the key, so n units land on about (1 - 1/e) n partitions
# and a few partitions hold two or three units while the rest sit empty: the stage then takes
# as long as its fullest task. The data pass instead gives every unit a slot (its rank among
# the units touched, modulo the partition count) and shuffles by a column whose hash is that
# slot, so partitions hold the same number of units, give or take one.

_SLOTS_PER_PARALLELISM = (
    4  # more partitions than slots keeps a task to one unit when units are many
)


def _shuffle_width(shards: int, parallelism: int) -> int:
    """Partitions of a group's shuffle: one per shard, up to ``_SLOTS_PER_PARALLELISM`` per slot."""
    return max(1, min(shards, parallelism * _SLOTS_PER_PARALLELISM))


def _spark_hash_int(x: np.ndarray) -> np.ndarray:
    """Spark's ``hash`` (Murmur3, seed 42) of int32 values, as int32: the function a column
    shuffle partitions by, taken modulo the partition count."""
    c1, c2 = np.uint32(0xCC9E2D51), np.uint32(0x1B873593)
    m = np.uint32(0xFFFFFFFF)

    def rotl(v, r):
        return ((v << np.uint32(r)) | (v >> np.uint32(32 - r))) & m

    with np.errstate(over="ignore"):
        k = x.astype(np.int32).view(np.uint32)
        k = rotl(k * c1, 15) * c2
        h = rotl(np.uint32(42) ^ k, 13) * np.uint32(5) + np.uint32(0xE6546B64)
        h = h ^ np.uint32(4)
        h = (h ^ (h >> np.uint32(16))) * np.uint32(0x85EBCA6B)
        h = (h ^ (h >> np.uint32(13))) * np.uint32(0xC2B2AE35)
        h = h ^ (h >> np.uint32(16))
    return h.view(np.int32)


def _slot_keys(n: int) -> np.ndarray:
    """``keys[j]``: an int32 whose shuffle partition among ``n`` is ``j``."""
    keys = np.full(n, -1, dtype=np.int64)
    start, step = 0, max(1024, 8 * n)
    while (keys < 0).any():
        cand = np.arange(start, start + step, dtype=np.int64).astype(np.int32)
        part = np.mod(_spark_hash_int(cand).astype(np.int64), n)
        # first candidate of each residue not yet found
        _, first = np.unique(part, return_index=True)
        for j, i in zip(part[first], first):
            if keys[j] < 0:
                keys[j] = cand[i]
        start += step
    return keys.astype(np.int32)


def _flat_offsets(g: Group, absolute: list) -> np.ndarray:
    """The flat offset of each cell in its unit's window."""
    flat = np.zeros(len(absolute[0]), np.int64)
    for a, u, w0, w1 in zip(absolute, g.unit, g.wlo, g.whi):
        lo = np.maximum(a // u * u, w0)  # the unit's box within the window
        hi = np.minimum(a // u * u + u, w1)
        flat = flat * (hi - lo) + (a - lo)
    return flat


def _values(col, g: Group, v: str) -> np.ndarray:
    """A variable column as numbers: a null is NaN for a float, the fill value otherwise."""
    if col.null_count and g.dtypes[v].kind != "f":
        col = pc.fill_null(col, pa.scalar(g.fill[v], col.type))
    return col.to_numpy(zero_copy_only=False)


def _binary(buf) -> pa.Array:
    """A one-element binary array over ``buf`` (bytes or a numpy array), without a copy."""
    if isinstance(buf, np.ndarray):
        raw = np.ascontiguousarray(buf).reshape(-1).view(np.uint8)
        data = pa.py_buffer(raw)
        size = raw.nbytes
    else:
        data = pa.py_buffer(buf)
        size = len(buf)
    offsets = pa.py_buffer(np.array([0, size], dtype=np.int32))
    return pa.Array.from_buffers(pa.binary(), 1, [None, offsets, data])


def _dup_error(g: Group, lo: tuple, shape: tuple, extra: int) -> ZarrError:
    hi = tuple(a + n for a, n in zip(lo, shape))
    where = ", ".join(
        f"{d} {ax.label(a)}..{ax.label(min(b, len(ax)) - 1)}"
        for d, ax, a, b in zip(g.dims, g.axes, lo, hi)
        if a < len(ax)
    )
    return ZarrError(
        f"{extra} cell(s) of the table share a position with another cell (in the unit "
        f"{where})",
        fix="keep one row per cell. Aggregate or drop the duplicate rows before writing",
    )


def _partial_limit(budget: int) -> int:
    """Bytes of partial blocks one input partition holds before it ships the oldest: half
    the task budget (the other half is the shuffle's own buffers and the batch in hand).
    """
    return max(1, budget // 2)


class _Partial:
    """The cells one input partition has for one storage unit.

    Few cells are kept as they came (flat offset and values). Once they would take as many
    bytes as the unit's dense arrays they are folded into those arrays with a mask. Either
    ships as a partial block: sparse (offsets and values) when that is smaller, else the
    bounding box of the cells that came, dense, with a packed mask unless the box is full.
    """

    def __init__(self, g: Group, key: int):
        self.g, self.key = g, key
        self.lo, hi = _box_of(g, key)
        self.shape = tuple(b - a for a, b in zip(self.lo, hi))
        self.size = int(np.prod(self.shape, dtype=np.int64))
        self.item = sum(g.dtypes[v].itemsize for v in g.vars)
        self.chunks: list = []  # (flat offsets, {variable: values}) while sparse
        self.arrays: dict | None = None
        self.mask: np.ndarray | None = None
        self.cells = 0  # cells taken, a repeated position counted each time
        self.nbytes = 0

    def add(self, flat: np.ndarray, vals: dict) -> int:
        """Take cells (owned arrays); returns the change in bytes held."""
        before = self.nbytes
        self.cells += len(flat)
        if self.arrays is None:
            self.chunks.append((flat, vals))
            self.nbytes += len(flat) * (8 + self.item)
            if self.nbytes >= self.size * (self.item + 1):
                self._densify()
        else:
            self._put(flat, vals)
        return self.nbytes - before

    def _put(self, flat, vals) -> None:
        for v, a in vals.items():
            self.arrays[v][flat] = a
        self.mask[flat] = True

    def _densify(self) -> None:
        g = self.g
        self.arrays = {
            v: np.full(self.size, g.fill[v], dtype=g.dtypes[v]) for v in g.vars
        }
        self.mask = np.zeros(self.size, dtype=bool)
        for flat, vals in self.chunks:
            self._put(flat, vals)
        self.chunks = []
        self.nbytes = self.size * (self.item + 1)

    def emit(self, schema: pa.Schema) -> pa.RecordBatch:
        g = self.g
        if self.arrays is None:
            flat = np.concatenate([c[0] for c in self.chunks])
            vals = {v: np.concatenate([c[1][v] for c in self.chunks]) for v in g.vars}
            return self._row(schema, self.cells, b"", flat, [vals[v] for v in g.vars])
        present = int(self.mask.sum())
        if present != self.cells:
            raise _dup_error(g, self.lo, self.shape, self.cells - present)
        m = self.mask.reshape(self.shape)
        nd = len(self.shape)
        box = []
        for ax in range(nd):
            hit = np.flatnonzero(m.any(axis=tuple(i for i in range(nd) if i != ax)))
            box.append((int(hit[0]), int(hit[-1]) + 1))
        box_cells = int(np.prod([b - a for a, b in box], dtype=np.int64))
        if present * (8 + self.item) < box_cells * self.item + box_cells // 8:
            flat = np.flatnonzero(self.mask)
            return self._row(
                schema, present, b"", flat, [self.arrays[v][flat] for v in g.vars]
            )
        sl = tuple(slice(a, b) for a, b in box)
        mask = b"" if present == box_cells else np.packbits(m[sl].reshape(-1))
        bounds = np.array([a for a, _ in box] + [b for _, b in box], dtype=np.int64)
        return self._row(
            schema,
            present,
            mask,
            None,
            [self.arrays[v].reshape(self.shape)[sl] for v in g.vars],
            bounds,
        )

    def _row(self, schema, n, mask, flat, arrays, bounds=None) -> pa.RecordBatch:
        if (
            flat is not None
        ):  # sparse: the offsets ride where a dense block has its mask
            mask = flat
        cols = [
            pa.array([self.key], pa.int64()),
            pa.array([n], pa.int64()),
            _binary(b"" if bounds is None else bounds),
            _binary(b"" if isinstance(mask, bytes) else mask),
        ] + [_binary(a) for a in arrays]
        return pa.RecordBatch.from_arrays(cols, schema=schema)


def _shard_keys(g: Group, keys: np.ndarray) -> np.ndarray:
    """The key of the storage unit (shard) each chunk key lies in."""
    per = [p // u for p, u in zip(g.part, g.unit)]
    idx = np.unravel_index(np.asarray(keys, dtype=np.int64), g.nu)
    counts = [-(-n // c) for n, c in zip(g.nu, per)]
    return np.ravel_multi_index([i // c for i, c in zip(idx, per)], counts).astype(
        np.int64
    )


def _combine_function(g: Group, limit: int, shards: np.ndarray, n: int):
    """Cells to partial blocks, before the shuffle: each input partition gathers its cells
    by unit and ships a unit's cells as one (or a few) partial blocks instead of a row per
    cell. It holds at most ``limit`` bytes of partials (the oldest ship first); a partition
    that reads in chunk order holds one chunk at a time and ships it near full. ``shards`` are
    the sorted keys of the storage units touched; each partial is tagged with its shard and
    with the shuffle slot among ``n`` partitions that its shard has."""
    schema = _partial_arrow(g)
    placed = _placed_arrow(g)
    slot_key = _slot_keys(n)

    def place(batch):
        k = batch.column(0).to_numpy(zero_copy_only=False)
        s = _shard_keys(g, k)
        p = slot_key[np.searchsorted(shards, s) % n]
        cols = list(batch.columns) + [pa.array(p, pa.int32()), pa.array(s, pa.int64())]
        return pa.RecordBatch.from_arrays(cols, schema=placed)

    def run(batches):
        held: OrderedDict = OrderedDict()
        total = 0
        for b in batches:
            if not b.num_rows:
                continue
            absolute = _absolute(b, g)
            key = _unit_key(g, absolute)
            flat = _flat_offsets(g, absolute)
            vals = {v: _values(b.column(v), g, v) for v in g.vars}
            lo, hi = int(key.min()), int(key.max())
            if lo == hi:
                runs = [(lo, np.arange(len(key)))]
            else:
                order = np.argsort(key, kind="stable")
                ks = key[order]
                cuts = (np.flatnonzero(ks[1:] != ks[:-1]) + 1).tolist()
                runs = [
                    (int(ks[s]), order[s:e])
                    for s, e in zip([0, *cuts], [*cuts, len(ks)])
                ]
            for k, sel in runs:
                p = held.get(k)
                if p is None:
                    p = held[k] = _Partial(g, k)
                else:
                    held.move_to_end(k)
                total += p.add(flat[sel], {v: vals[v][sel] for v in g.vars})
                while total > limit and len(held) > 1:
                    _, old = held.popitem(last=False)
                    total -= old.nbytes
                    yield place(old.emit(schema))
        for p in held.values():
            yield place(p.emit(schema))

    return run


class _Merge:
    """The partial blocks of one unit, merged into the unit's dense arrays."""

    def __init__(self, g: Group, key: int):
        self.g, self.key = g, key
        self.lo, hi = _box_of(g, key)
        self.shape = tuple(b - a for a, b in zip(self.lo, hi))
        self.size = int(np.prod(self.shape, dtype=np.int64))
        self.arrays: dict | None = None
        self.present: np.ndarray | None = None  # None while every cell is there
        self.claimed = 0

    def _fresh(self) -> None:
        g = self.g
        self.arrays = {
            v: np.full(self.size, g.fill[v], dtype=g.dtypes[v]) for v in g.vars
        }
        self.present = np.zeros(self.size, dtype=bool)

    def take(self, batch, i: int) -> None:
        g = self.g
        n = batch.column("__n")[i].as_py()
        box = np.frombuffer(batch.column("__box")[i].as_buffer(), dtype=np.int64)
        mask = batch.column("__m")[i].as_buffer()
        vals = {
            v: np.frombuffer(batch.column(f"__v{j}")[i].as_buffer(), dtype=g.dtypes[v])
            for j, v in enumerate(g.vars)
        }
        self.claimed += n
        nd = len(self.shape)
        whole = (
            len(box)
            and not mask.size
            and tuple(box[:nd]) == (0,) * nd
            and tuple(box[nd:]) == self.shape
        )
        if whole and self.arrays is None:
            self.arrays = vals  # one block for the whole unit, read only: used as it is
            return
        if self.arrays is not None and self.present is None:
            raise _dup_error(g, self.lo, self.shape, n)
        if self.arrays is None:
            self._fresh()
        if not len(box):  # sparse: the mask slot holds the flat offsets
            idx = np.frombuffer(mask, dtype=np.int64)
            for v in g.vars:
                self.arrays[v][idx] = vals[v]
            self.present[idx] = True
            return
        sl = tuple(slice(int(a), int(b)) for a, b in zip(box[:nd], box[nd:]))
        part = tuple(int(b) - int(a) for a, b in zip(box[:nd], box[nd:]))
        pv = self.present.reshape(self.shape)[sl]
        if mask.size:
            m = np.unpackbits(np.frombuffer(mask, np.uint8), count=int(np.prod(part)))
            m = m.astype(bool).reshape(part)
        else:
            m = np.ones(part, dtype=bool)
        for v in g.vars:
            view = self.arrays[v].reshape(self.shape)[sl]
            view[m] = vals[v].reshape(part)[m]
        pv[m] = True

    def finish(self) -> pa.RecordBatch:
        from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

        g = self.g
        have = self.size if self.present is None else int(self.present.sum())
        if have != self.claimed:
            raise _dup_error(g, self.lo, self.shape, self.claimed - have)
        dims = list(g.dims)
        rows = []
        for v in g.vars:
            a = self.arrays[v].reshape(self.shape)
            rows.append(
                {
                    "variable": v,
                    "dims": dims,
                    "start": [x - o for x, o in zip(self.lo, g.offsets)],
                    "stop": [
                        x + n - o for x, n, o in zip(self.lo, self.shape, g.offsets)
                    ],
                    "coord_start": None,
                    "coord_stop": None,
                    "dtype": a.dtype.str,
                    "shape": list(self.shape),
                    "data": a.tobytes(),
                    "fill_value": None,
                    "attrs": None,
                }
            )
        return pa.RecordBatch.from_pylist(rows, schema=_BLOCKS_ARROW)


def _merge_function(g: Group):
    """Sorted partial blocks to blocks: the partials of one unit merged into a dense array
    per variable (the fill value where no cell came), emitted when the unit's rows end. Two
    partials that hold one position refuse the write."""

    def run(batches):
        state = None
        for b in batches:
            if not b.num_rows:
                continue
            ks = b.column("__k").to_numpy(zero_copy_only=False)
            for i in range(b.num_rows):
                key = int(ks[i])
                if state is None or state.key != key:
                    if state is not None:
                        yield state.finish()
                    state = _Merge(g, key)
                state.take(b, i)
        if state is not None:
            yield state.finish()

    return run


# --- planning on the driver -----------------------------------------------------------------


def _attrs_of_dim(dim: str):
    if dim in ("latitude", "lat"):
        return dict(_LAT)
    if dim in ("longitude", "lon"):
        return dict(_LON)
    return None


def _unit_bytes(g: Group) -> int:
    cells = int(
        np.prod(
            [min(u, b - a) for u, a, b in zip(g.unit, g.wlo, g.whi)], dtype=np.int64
        )
    )
    sizes = [g.dtypes[v].itemsize for v in g.vars]
    return cells * (sum(sizes) + max(sizes))  # the dense arrays and one copy of one


def _by_shard(g: Group) -> Group:
    """``g`` with its shard as the unit a task builds: the blocks, partials and keys are whole
    shards, as for an unsharded array's chunks."""
    nu = tuple(
        -(-n // k) for n, k in zip(g.nu, (p // u for p, u in zip(g.part, g.unit)))
    )
    return replace(g, unit=g.part, nu=nu, whole=True)


def _choose_unit(g: Group, budget: int) -> Group:
    """The unit a task builds for ``g``: its whole shard when the shard of all the group's
    variables (the dense arrays and one copy) fits ``budget`` (one block, one write, as
    fast as it gets), else one inner chunk at a time, in shard order. The choice is the
    driver's, from the plan alone, so every task of a group builds the same way."""
    shard = _by_shard(g)
    if _unit_bytes(shard) <= budget:
        return shard
    return replace(g, whole=False)


def _check_budget(g: Group, budget: int) -> None:
    need = _unit_bytes(g)
    if need > budget:
        per = {d: u for d, u in zip(g.dims, g.unit)}
        raise ZarrError(
            f"one chunk of {list(g.vars)} is {per} cells, {need / _MIB:.0f} MiB with its "
            f"copy, over the {budget / _MIB:.0f} MiB a task holds; a task builds one chunk "
            "at a time",
            fix="use smaller chunks (chunks={'<dimension>': <size>}), or fewer "
            "variables per write",
        )


def _find_axes(df, mode, dim_cols, kinds, coords, store, append_dim):
    """``(axes, rows)``: the labels of every dimension (from ``coords``, the store, or one pass
    over the columns that have none yet) and the table's row count when that pass ran.
    """
    axes: dict = {}
    need = []
    for d in dim_cols:
        if d in coords:
            axes[d] = _axis_from_coords(d, coords[d])
        elif mode == "append" and d != append_dim:
            axes[d] = _axis_from_store(d, store.labels.get(d))
        else:
            need.append(d)
    rows = None
    if need:
        merged, rows = _survey_distinct(
            df, [dim_cols[d] for d in need], [kinds[d] for d in need]
        )
        for d, raw in zip(need, merged):
            if not len(raw):
                raise ValueError(
                    "the table has no cells to write. Fix: pass a table with rows."
                )
            if len(raw) > _MAX_LABELS:
                raise ZarrError(
                    f"the {d!r} column has {len(raw)} distinct values, more than the driver "
                    "holds as labels",
                    fix=f"check that {d!r} is a dimension, or pass coords for it",
                )
            axes[d] = Axis(d, _natural_from_raw(kinds[d], raw))
    return axes, rows


def _fill_of(dt: np.dtype, fill):
    """The value of a missing cell: NaN for floats; an integer or boolean array's own fill when
    it is a plain number, else 0 or False."""
    if dt.kind == "f":
        return float("nan")
    ok = isinstance(fill, (int, float, bool, np.number)) and fill == fill
    return fill if ok else (False if dt.kind == "b" else 0)


def _existing_array(store: StoreView, v: str, dims: tuple):
    """``(meta, dims in the array's order)`` of an array a cells append writes."""
    meta = store.arrays.get(v)
    if meta is None:
        raise ZarrError(
            f"the store has no array {v!r}, and a table of cells does not create variables "
            "in an existing store",
            fix="create a new store for them (mode='create'), or write columns of arrays "
            "the store has",
        )
    if v in store.docs.unsupported:
        raise ZarrUnsupported(
            f"array {v!r} cannot be decoded, so it cannot be written: "
            f"{store.docs.unsupported[v]}",
            fix="write the other variables",
        )
    if set(meta.dims) != set(dims):
        raise ValueError(
            f"dim_cols names {list(dims)}, and {v!r} has the dimensions {list(meta.dims)}. "
            "Fix: name exactly the array's dimensions."
        )
    return meta, tuple(meta.dims)


def _plan_groups(
    variables, dtypes, dim_cols, kinds, axes, mode, append_dim, at, store, layout
):
    """``(groups, units)``: the variables that share dimensions, unit and window, as the
    pipelines that write them, and a new store's unit per variable."""
    dims = tuple(dim_cols)
    appending = mode == "append"
    groups: dict = {}
    units: dict = {}
    for v in variables:
        dt = dtypes[v]
        if not appending:
            shape = tuple(len(axes[d]) for d in dims)
            unit, fill, inner = layout(v, dt, dims, shape)
            vdims, units[v] = dims, unit
        else:
            meta, vdims = _existing_array(store, v, dims)
            shape = tuple(
                max(n, at + len(axes[append_dim])) if d == append_dim else n
                for d, n in zip(vdims, meta.shape)
            )
            inner = tuple(meta.chunks)
            unit = inner
            if meta.shard is not None:
                unit = tuple(c * k for c, k in zip(meta.chunks, meta.shard.cps))
            fill = meta.fill_value
        window = tuple(
            (
                (at, at + len(axes[append_dim]))
                if (appending and d == append_dim)
                else (0, n)
            )
            for d, n in zip(vdims, shape)
        )
        groups.setdefault((vdims, tuple(inner), tuple(unit), shape, window), []).append(
            (v, dt, _fill_of(dt, fill))
        )
    out = []
    for (vdims, inner, part, shape, window), members in groups.items():
        out.append(
            Group(
                dims=vdims,
                cols=tuple(dim_cols[d] for d in vdims),
                ckinds=tuple(kinds[d] for d in vdims),
                axes=tuple(axes[d] for d in vdims),
                offsets=tuple(
                    at if (appending and d == append_dim) else 0 for d in vdims
                ),
                unit=inner,
                part=part,
                nu=tuple(-(-n // u) for n, u in zip(shape, inner)),
                wlo=tuple(w[0] for w in window),
                whi=tuple(w[1] for w in window),
                vars=tuple(m[0] for m in members),
                dtypes={m[0]: m[1] for m in members},
                fill={m[0]: m[2] for m in members},
            )
        )
    return out, units


def _partials_frame(df, g: Group, limit: int, shards: np.ndarray, n: int):
    """One group's map side: each input partition locates its cells and gathers them into
    partial blocks per unit (``_combine_function``); a row of this frame is a partial block,
    not a cell."""
    from pyspark.sql import functions as F

    picked = [F.col(c).alias(f"__d_{d}") for d, c in zip(g.dims, g.cols)] + [
        F.col(v) for v in g.vars
    ]
    return df.select(*picked).mapInArrow(
        _combine_function(g, limit, shards, n), _partial_struct(g)
    )


def _group_frame(df, g: Group, ks: np.ndarray, schema, parallelism: int, limit: int):
    """One group's data pass: partial blocks per input partition, shuffled once so that the
    storage units (shards) spread evenly over the partitions (``_shuffle_width``), sorted by
    shard and chunk, and merged into one chunk at a time. A task owns whole shards and holds
    one chunk of one: ``ks`` are the keys of the chunks touched."""
    shards = np.unique(_shard_keys(g, ks))
    n = _shuffle_width(len(shards), parallelism)
    return (
        _partials_frame(df, g, limit, shards, n)
        .repartition(n, "__p")
        .sortWithinPartitions("__s", "__k")
        .mapInArrow(_merge_function(g), schema)
    )


def _window_keys(g: Group) -> np.ndarray:
    """The sorted keys of every unit the window being written touches."""
    ranges = [
        np.arange(w0 // u, -(-w1 // u)) for w0, w1, u in zip(g.wlo, g.whi, g.unit)
    ]
    grid = np.meshgrid(*ranges, indexing="ij")
    return np.sort(np.ravel_multi_index([x.ravel() for x in grid], g.nu))


def _seen_rows(g: Group, ks: np.ndarray) -> list:
    """The position of every block the data pass of ``g`` will make, before the offsets."""
    out = []
    for v in g.vars:
        for k in ks.tolist():
            lo, hi = _box_of(g, k)
            out.append(
                {
                    "variable": v,
                    "dims": list(g.dims),
                    "start": [a - o for a, o in zip(lo, g.offsets)],
                    "stop": [b - o for b, o in zip(hi, g.offsets)],
                    "dtype": g.dtypes[v].str,
                    "shape": [b - a for a, b in zip(lo, hi)],
                    "attrs": None,
                    "fill_value": None,
                }
            )
    return out


def prepare(
    df,
    *,
    mode: str,
    dim_cols,
    names,
    append_dim,
    store: StoreView | None,
    schema,
    args: CellsArgs,
    layout,
    append_start,
    parallelism: int,
    budget: int,
) -> CellsPrepared:
    """Plan a cells write and make its blocks.

    ``store`` is the existing store (``None`` for ``create``); ``layout(name, dtype, dims,
    shape)`` gives a new array's storage unit and fill; ``append_start(dim, base, new)`` where
    an append's labels start; ``parallelism`` caps the shuffle's partitions; ``budget`` is
    what a task may hold, in bytes.
    """
    if mode not in ("create", "append"):
        raise ValueError(
            f"a table of cells writes mode 'create' or 'append', got {mode!r}. Fix: use "
            "one of those, or write blocks (gbx_zarr_read(output='blocks') or an "
            "xarray.Dataset) to update or overwrite."
        )
    dim_cols, variables, kinds, dtypes = check_columns(df, dim_cols, names)
    dims = tuple(dim_cols)
    coords = dict(args.coords or {})
    extra = sorted(set(coords) - set(dims))
    if extra:
        raise ValueError(
            f"coords names {extra}, which dim_cols does not. Fix: name dimensions of dim_cols."
        )
    if mode == "append":
        append_dim = _append_dim_of(dim_cols, store, append_dim)
        stored = sorted(set(coords) - {append_dim})
        if stored:
            raise ValueError(
                f"an append takes the store's labels for {stored}; coords gives the new "
                f"labels of the append dimension {append_dim!r} only. Fix: drop coords for "
                f"{stored}."
            )
    axes, rows = _find_axes(df, mode, dim_cols, kinds, coords, store, append_dim)
    new_labels, at = None, 0
    if mode == "append":
        new_labels = axes[append_dim].natural
        base = (store.labels or {}).get(append_dim)
        if base is None:
            raise ZarrError(
                f"the store has no coordinate array for {append_dim!r}, so the new labels "
                "cannot be placed after its steps",
                fix=f"write the {append_dim!r} labels of the existing steps first",
            )
        at = append_start(append_dim, base, new_labels)
    plan_groups, units = _plan_groups(
        variables, dtypes, dim_cols, kinds, axes, mode, append_dim, at, store, layout
    )
    plan_groups = [_choose_unit(g, budget) for g in plan_groups]
    for g in plan_groups:
        _check_budget(g, budget)
    # the units the cells fall in: every unit of the window when the caller says the table fills
    # it (complete=True with every label fixed: all of a create's, an append's new steps) or the
    # table has as many rows as the grid has cells, else one pass over the dimension columns
    labels_fixed = (
        set(coords) == set(dims) if mode == "create" else append_dim in coords
    )
    full = (args.complete and labels_fixed) or (
        rows is not None
        and not coords
        and rows == int(np.prod([len(axes[d]) for d in dims], dtype=np.int64))
    )
    if full:
        keys = [_window_keys(g) for g in plan_groups]
    else:
        keys = _survey_units(df, plan_groups)
    frames, seen = [], []
    for g, ks in zip(plan_groups, keys):
        if not len(ks):
            raise ValueError(
                "the table has no cells to write. Fix: pass a table with rows."
            )
        seen += _seen_rows(g, ks)
        frames.append(
            _group_frame(df, g, ks, schema, parallelism, _partial_limit(budget))
        )
    frame = frames[0]
    for f in frames[1:]:
        frame = frame.unionByName(f)
    coord_rows, known = [], {}
    if mode == "create":
        for d in dims:
            coord_rows.append(_coord_block(d, axes[d].natural, _attrs_of_dim(d)))
    else:
        coord_rows.append(_coord_block(append_dim, new_labels, None))
        known[append_dim] = (new_labels, 0, {}, new_labels.dtype)
    for c in coord_rows:
        seen.append({k: c[k] for k in _SEEN_ARROW.names})
    return CellsPrepared(
        frame=frame,
        coords=coord_rows,
        seen=pa.Table.from_pylist(seen, schema=_SEEN_ARROW),
        names=sorted(variables),
        known=known,
        append_dim=append_dim if mode == "append" else None,
        units=units,
        chunk_vars=frozenset(v for g in plan_groups if not g.whole for v in g.vars),
    )
