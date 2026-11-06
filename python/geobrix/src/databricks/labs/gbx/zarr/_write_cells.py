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

**The data pass: one shuffle.** Each cell gets the key of its unit and its offset in it; the
cells are repartitioned by that key (one partition per unit touched, no more than the
session's parallelism), sorted within the partition, and a streaming packer builds one unit
at a time, dense, with the fill value where no cell came. Its block (a unit of one variable)
goes straight to the writer's pack stage, which encodes the chunks and writes the shard where
the block was made. A unit is the most a task holds, plus one copy of one variable.

**Rules.** A cell missing from the table reads as the fill value. Two cells at one position
refuse the write (the task that meets them raises ``ZarrError``) before anything is
committed. A null variable value is a missing cell; a null dimension value is refused.
``create`` and ``append`` take cells; ``update`` and ``overwrite`` would overwrite stored
cells with fill wherever the table has none, so they take blocks.

Nothing here touches ``spark.conf``, ``sparkContext``, ``.rdd``, ``_jvm`` or ``_jsc``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

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
    unit: tuple  # storage unit per dimension (cells)
    nu: tuple  # units per dimension in the array after the write
    wlo: tuple  # the window being written, per dimension
    whi: tuple
    vars: tuple
    dtypes: dict
    fill: dict


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


def _locate_function(g: Group):
    """Cells to ``(__k, __c, values...)``: the key of the unit a cell falls in, and its flat
    offset in that unit's window."""

    def run(batches):
        for b in batches:
            if not b.num_rows:
                continue
            absolute = _absolute(b, g)
            key = _unit_key(g, absolute)
            flat = np.zeros(len(key), np.int64)
            for a, u, w0, w1 in zip(absolute, g.unit, g.wlo, g.whi):
                lo = np.maximum(a // u * u, w0)  # the unit's box within the window
                hi = np.minimum(a // u * u + u, w1)
                flat = flat * (hi - lo) + (a - lo)
            yield pa.RecordBatch.from_arrays(
                [pa.array(key), pa.array(flat)] + [b.column(v) for v in g.vars],
                names=["__k", "__c", *g.vars],
            )

    return run


def _pack_function(g: Group):
    """Sorted ``(__k, __c, values...)`` rows to blocks: a dense array per variable for one
    unit at a time (the fill value where no cell came), emitted when the unit's rows end.
    """
    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    dims = list(g.dims)

    def run(batches):
        state = None  # key, lo, shape, arrays, mask, cells

        def begin(k):
            lo, hi = _box_of(g, k)
            shape = tuple(b - a for a, b in zip(lo, hi))
            size = int(np.prod(shape, dtype=np.int64))
            arrays = {v: np.full(size, g.fill[v], dtype=g.dtypes[v]) for v in g.vars}
            return [k, lo, shape, arrays, np.zeros(size, dtype=bool), 0]

        def flush(s):
            k, lo, shape, arrays, mask, cells = s
            if int(mask.sum()) != cells:
                hi = tuple(a + n for a, n in zip(lo, shape))
                where = ", ".join(
                    f"{d} {ax.label(a)}..{ax.label(min(b, len(ax)) - 1)}"
                    for d, ax, a, b in zip(dims, g.axes, lo, hi)
                    if a < len(ax)
                )
                raise ZarrError(
                    f"{cells - int(mask.sum())} cell(s) of the table share a position with "
                    f"another cell (in the unit {where})",
                    fix="keep one row per cell. Aggregate or drop the duplicate rows before "
                    "writing",
                )
            rows = []
            for v in g.vars:
                a = arrays[v].reshape(shape)
                rows.append(
                    {
                        "variable": v,
                        "dims": dims,
                        "start": [x - o for x, o in zip(lo, g.offsets)],
                        "stop": [x + n - o for x, n, o in zip(lo, shape, g.offsets)],
                        "coord_start": None,
                        "coord_stop": None,
                        "dtype": a.dtype.str,
                        "shape": list(shape),
                        "data": a.tobytes(),
                        "fill_value": None,
                        "attrs": None,
                    }
                )
            return pa.RecordBatch.from_pylist(rows, schema=_BLOCKS_ARROW)

        for b in batches:
            if not b.num_rows:
                continue
            k = b.column("__k").to_numpy(zero_copy_only=False)
            c = b.column("__c").to_numpy(zero_copy_only=False)
            cols = {}
            for v in g.vars:
                col = b.column(v)
                if col.null_count and g.dtypes[v].kind != "f":
                    col = pc.fill_null(col, pa.scalar(g.fill[v], col.type))
                cols[v] = col.to_numpy(zero_copy_only=False)
            cuts = np.flatnonzero(k[1:] != k[:-1]) + 1
            for s, e in zip([0, *cuts.tolist()], [*cuts.tolist(), len(k)]):
                key = int(k[s])
                if state is None or state[0] != key:
                    if state is not None:
                        yield flush(state)
                    state = begin(key)
                idx = c[s:e]
                for v in g.vars:
                    state[3][v][idx] = cols[v][s:e]
                state[4][idx] = True
                state[5] += e - s
        if state is not None:
            yield flush(state)

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


def _check_budget(g: Group, budget: int) -> None:
    need = _unit_bytes(g)
    if need > budget:
        per = {d: u for d, u in zip(g.dims, g.unit)}
        raise ZarrError(
            f"one storage unit of {list(g.vars)} is {per} cells, {need / _MIB:.0f} MiB with its "
            f"copy, over the {budget / _MIB:.0f} MiB a task holds; a task builds one unit "
            "at a time",
            fix="use smaller shards (shards={'<dimension>': <size>}) or chunks, or fewer "
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
            unit, fill = layout(v, dt, dims, shape)
            vdims, units[v] = dims, unit
        else:
            meta, vdims = _existing_array(store, v, dims)
            shape = tuple(
                max(n, at + len(axes[append_dim])) if d == append_dim else n
                for d, n in zip(vdims, meta.shape)
            )
            unit = tuple(meta.chunks)
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
        groups.setdefault((vdims, tuple(unit), shape, window), []).append(
            (v, dt, _fill_of(dt, fill))
        )
    out = []
    for (vdims, unit, shape, window), members in groups.items():
        out.append(
            Group(
                dims=vdims,
                cols=tuple(dim_cols[d] for d in vdims),
                ckinds=tuple(kinds[d] for d in vdims),
                axes=tuple(axes[d] for d in vdims),
                offsets=tuple(
                    at if (appending and d == append_dim) else 0 for d in vdims
                ),
                unit=unit,
                nu=tuple(-(-n // u) for n, u in zip(shape, unit)),
                wlo=tuple(w[0] for w in window),
                whi=tuple(w[1] for w in window),
                vars=tuple(m[0] for m in members),
                dtypes={m[0]: m[1] for m in members},
                fill={m[0]: m[2] for m in members},
            )
        )
    return out, units


def _group_frame(df, g: Group, ks: np.ndarray, schema, parallelism: int):
    """One group's data pass: locate each cell, shuffle once by unit key (a partition per unit
    touched, no more than ``parallelism``), sort, and pack one unit at a time. The packed blocks
    are then spread over a task per block (up to ``parallelism``) when the units alone would
    not fill them."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    picked = [F.col(c).alias(f"__d_{d}") for d, c in zip(g.dims, g.cols)] + [
        F.col(v) for v in g.vars
    ]
    located = T.StructType(
        [T.StructField("__k", T.LongType()), T.StructField("__c", T.LongType())]
        + [df.schema[v] for v in g.vars]
    )
    n = max(1, min(len(ks), parallelism))
    packed = (
        df.select(*picked)
        .mapInArrow(_locate_function(g), located)
        .repartition(n, "__k")
        .sortWithinPartitions("__k")
        .mapInArrow(_pack_function(g), schema)
    )
    # a unit packs all its variables in one task, so few units leave cores idle for the encode
    # and write that follow: fan the packed blocks (small beside the cells) out to a task each
    blocks = max(1, min(len(ks) * len(g.vars), parallelism))
    if blocks > n:
        packed = packed.repartition(blocks, "variable", "start")
    return packed


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
    for g in plan_groups:
        _check_budget(g, budget)
    # the units the cells fall in: every unit of a table that fills its grid, else one pass
    full = mode == "create" and (
        (args.complete and set(coords) == set(dims))
        or (
            rows is not None
            and not coords
            and rows == int(np.prod([len(axes[d]) for d in dims], dtype=np.int64))
        )
    )
    if full:
        keys = [np.arange(int(np.prod(g.nu, dtype=np.int64))) for g in plan_groups]
    else:
        keys = _survey_units(df, plan_groups)
    frames, seen = [], []
    for g, ks in zip(plan_groups, keys):
        if not len(ks):
            raise ValueError(
                "the table has no cells to write. Fix: pass a table with rows."
            )
        seen += _seen_rows(g, ks)
        frames.append(_group_frame(df, g, ks, schema, parallelism))
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
    )
