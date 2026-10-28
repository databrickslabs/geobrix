"""``gbx_zarr_sample``: the values of a Zarr store at a DataFrame of points.

Each point is mapped to the cell of the store's latitude/longitude (or projected x/y) grid
that is nearest to it, and the result is one row per point and per selected position of
the other dimensions (time, level), carrying the point's own columns:

1. **Map.** The geometry is read as ``rst_sample`` reads it (``_geom.parse_geom``: WKB,
   EWKB, WKT or EWKT; an embedded SRID wins over ``crs``) and moved to the store's CRS
   (``core.crs``). The nearest cell along each axis is found on the coordinate arrays; a
   point beyond the first or last cell by more than half a step, and a missing or empty
   geometry, has no cell and samples NULL. A global longitude axis wraps.
2. **Group.** Points are grouped by the inner chunk of the grid they fall in. One unit of
   work is one spatial chunk with every selected position of the other dimensions, and it
   ships the manifest rows of exactly the objects it reads.
3. **Read.** A unit reads the cells of its points, not the boxes around them
   (``_engine.read_points``): a plain store through a vectorized index, a manifest store by
   decoding only the chunks that hold a cell. Missing data is masked as ``gbx_zarr_read``
   masks it (``_read._table_values``).

The work runs where it is cheapest:

- Up to ``_POINT_LIMIT`` points are collected to the driver, which maps them. When they lie
  in at most ``_SMALL_UNITS`` chunks and the answer is small, the driver reads and returns
  ``spark.createDataFrame(rows, schema)``: a Spark job has a fixed cost that dwarfs a few
  chunk reads.
- Otherwise the points go through Spark, partitioned and sorted by chunk so a chunk is read
  once, in a ``mapInArrow`` whose closure carries the units. The driver's own mapping is
  sent as columns; more than ``_POINT_LIMIT`` points are never collected: an executor
  ``mapInArrow`` maps them (the coordinate arrays travel in its closure) and a first pass
  over the geometry column alone tells the driver which chunks to plan.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from databricks.labs.gbx.zarr._meta import _check_mask_flag
from databricks.labs.gbx.zarr._plan import Unit, boxes_of, keys_of_boxes, rows_for_boxes
from databricks.labs.gbx.zarr._read import (
    _BATCH_SHARE,
    _READ_THREADS,
    _TABLE_ARROW,
    _arrow_column,
    _budget_bytes,
    _choose_variables,
    _coordinate_of,
    _grid_mapping_attrs,
    _label_spans,
    _open_source,
    _resolve_axes,
    _Source,
    _Span,
    _spark_type,
    _table_columns,
    _table_values,
    _TaskConfig,
    _UnitReader,
    check_access,
    check_version_args,
    classify_source,
    plain_direct_error,
    source_access,
)
from databricks.labs.gbx.zarr._select import AxisSel, _axis_of, touched_chunks
from databricks.labs.gbx.zarr._tiles import _grid_crs, _is_geographic, _spatial_dims
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrGridMismatch, ZarrUnsupported

__all__ = ["gbx_zarr_sample"]

_LOG = logging.getLogger(__name__)

_POINT_LIMIT = 100_000  # points the driver collects and maps itself
_SMALL_UNITS = 4  # chunks a driver-side job may read
_DRIVER_ROWS = (
    1_000_000  # rows the driver may build (a driver job is small in rows too)
)
_MAX_PARTITIONS = 512  # partitions of the Spark stage: its chunks are spread over them
_SPREAD = (
    64  # unmapped points are spread over this many pseudo-chunks, so none is skewed
)
_CELL_BYTES = 64  # working bytes per sampled cell: indices, values and Arrow columns
_UID, _ROW, _COL = "__gbx_uid", "__gbx_r", "__gbx_c"
_INTERNAL = (_UID, _ROW, _COL)
_PERIOD = 360.0
_EDGE_RTOL = 1e-9  # slack, as a fraction of the step, at the first and last cell


# --- the grid: points to cells --------------------------------------------------------


@dataclass(frozen=True)
class _AxisMap:
    """One grid axis: its labels, and how a coordinate finds its nearest cell."""

    labels: np.ndarray  # float64, in array order, monotonic
    origin: (
        float | None
    )  # the first label of a global longitude axis (it wraps), else None

    def nearest(self, v: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """``(index, ok)``: the nearest cell of each coordinate, and whether it is inside the
        axis (the cells are intervals centred on their labels, as the pixels of a raster
        are). A tie goes to the lowest index, as ``argmin`` does. A wrapping axis has no
        outside."""
        v = np.asarray(v, dtype=np.float64)
        finite = np.isfinite(v)
        v = np.where(finite, v, 0.0)
        n = self.labels.size
        if self.origin is not None:
            v = self.origin + (v - self.origin) % _PERIOD
            at = np.searchsorted(self.labels, v)
            left, right = (at - 1) % n, at % n
            d_left = _circular(v - self.labels[left])
            d_right = _circular(self.labels[right] - v)
            return np.where(d_right < d_left, right, left), finite
        if n == 1:
            return np.zeros(v.shape, np.int64), finite & (v == self.labels[0])
        ascending = self.labels[0] <= self.labels[-1]
        num = self.labels if ascending else self.labels[::-1]
        at = np.searchsorted(num, v)
        left, right = np.clip(at - 1, 0, n - 1), np.clip(at, 0, n - 1)
        d_left, d_right = np.abs(v - num[left]), np.abs(num[right] - v)
        if ascending:
            index = np.where(d_right < d_left, right, left)
        else:  # the lower array index is the higher label
            index = n - 1 - np.where(d_left < d_right, left, right)
        lo = num[0] - (num[1] - num[0]) / 2.0
        hi = num[-1] + (num[-1] - num[-2]) / 2.0
        slack = _EDGE_RTOL * max(num[1] - num[0], num[-1] - num[-2])
        return index, finite & (v >= lo - slack) & (v <= hi + slack)


def _circular(delta: np.ndarray) -> np.ndarray:
    d = np.abs(delta) % _PERIOD
    return np.minimum(d, _PERIOD - d)


def _axis_map(array: str, dim: str, coord: np.ndarray, attrs: dict | None) -> _AxisMap:
    axis = _axis_of(dim, np.asarray(coord), attrs)
    num = np.asarray(axis.num, dtype=np.float64)
    if axis.kind not in "fi" or num.size == 0 or not np.isfinite(num).all():
        raise ZarrUnsupported(
            f"dimension {dim!r} of array {array!r} has coordinates that are not a list "
            "of finite numbers, so its cells have no position",
            fix="sample a store whose latitude/longitude (or y/x) labels are numbers",
        )
    if num.size > 1 and axis.direction == 0:
        raise ZarrUnsupported(
            f"the {dim!r} coordinates of array {array!r} neither rise nor fall, so the "
            "nearest cell of a point is not defined",
            fix="sample a store whose latitude/longitude (or y/x) labels are monotonic",
        )
    period = axis.period
    return _AxisMap(num, None if period is None else period[0])


def _parse(values) -> np.ndarray:
    """The geometries of ``values`` as an object array (``None`` for a missing one)."""
    from databricks.labs.gbx._geom import parse_geom

    out = np.empty(len(values), dtype=object)
    for i, v in enumerate(values):
        out[i] = parse_geom(v)
    return out


def _resolve(srid, crs):
    from databricks.labs.gbx.core.crs import resolve_crs

    try:
        return resolve_crs(srid if crs is None else crs)
    except Exception as e:  # noqa: BLE001 - whatever the resolver raises is one refusal
        raise ValueError(
            f"cannot read the CRS {srid if crs is None else crs!r} ({type(e).__name__}: "
            f"{e}). Fix: use an EPSG or ESRI code, a CRS string or WKT."
        ) from None


def _move(transformer, x: np.ndarray, y: np.ndarray):
    """``transformer`` over arrays of coordinates (pyproj warns on a one-element array)."""
    if x.size == 1:
        return transformer.transform(float(x[0]), float(y[0]))
    return transformer.transform(x, y)


@dataclass(frozen=True)
class _Grid:
    """The spatial grid of the store and how its chunks number: all a task needs to map."""

    y: _AxisMap
    x: _AxisMap
    crs: str  # the canonical CRS of the grid
    chunks: tuple[int, int]  # the reference array's inner chunk (y, x)
    n_chunks_x: int

    def points(self, values, crs) -> tuple[np.ndarray, np.ndarray]:
        """``(x, y)`` of each geometry in the CRS of the grid; NaN where there is no point.

        An embedded SRID wins over ``crs``; a geometry with neither is taken to be in the
        grid's CRS already.
        """
        import shapely

        from databricks.labs.gbx.core.crs import crs_equal, get_transformer

        geoms = _parse(values)
        geoms[shapely.is_empty(geoms)] = None  # an empty point has no x or y
        kinds = shapely.get_type_id(geoms)  # -1 for a missing geometry
        wrong = (kinds >= 0) & (kinds != 0)
        if wrong.any():
            seen = shapely.get_type_id(geoms[wrong][:1])[0]
            raise ValueError(
                f"gbx_zarr_sample takes POINT geometries; got geometry type {seen}. "
                "Fix: pass points (use a centroid for other shapes)."
            )
        x, y = shapely.get_x(geoms), shapely.get_y(geoms)
        srid = shapely.get_srid(geoms)
        fallback = None if crs is None else _resolve(None, crs)
        dst = _resolve(None, self.crs)
        ok = np.isfinite(x) & np.isfinite(y)
        for s in np.unique(srid[ok]):
            src = _resolve(int(s), None) if s > 0 else fallback
            if src is None or crs_equal(src, dst):
                continue
            here = ok & (srid == s)
            x[here], y[here] = _move(get_transformer(src, dst), x[here], y[here])
        return x, y

    def map(self, values, crs) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """``(uid, row, col)`` per geometry: the grid cell of the nearest cell and the number
        of the chunk it lies in. A point with no cell has a negative ``uid``, spread over
        ``_SPREAD`` values so that no partition gets all of them."""
        x, y = self.points(values, crs)
        row, ok_y = self.y.nearest(y)
        col, ok_x = self.x.nearest(x)
        ok = ok_y & ok_x & np.isfinite(x) & np.isfinite(y)
        uid = (row // self.chunks[0]) * self.n_chunks_x + (col // self.chunks[1])
        uid = np.where(ok, uid, -1 - np.arange(len(uid)) % _SPREAD).astype(np.int64)
        return (
            uid,
            np.where(ok, row, 0).astype(np.int32),
            np.where(ok, col, 0).astype(np.int32),
        )


# --- the plan -------------------------------------------------------------------------


@dataclass
class _SampleConfig:
    """What the driver and the tasks need besides the units; it travels in a closure."""

    read: _TaskConfig  # how a unit opens its arrays
    grid: _Grid
    variables: tuple[str, ...]
    ypos: int  # positions of the grid's y and x among the dimensions of the arrays
    xpos: int
    other: tuple[int, ...]  # positions of the other dimensions
    steps: (
        np.ndarray
    )  # (len(other), n_steps): the selected positions, one column per row
    dim_columns: tuple[tuple[str, str], ...]  # (name, kind) of each other dimension
    dim_paths: tuple[str | None, ...]  # the coordinate array of each
    spans: dict[str, _Span]  # the labels of each, over the selection
    var_columns: tuple[tuple[str, str], ...]  # (name, kind) of each variable
    batch_cells: int  # cells a batch holds
    crs: object  # the caller's crs, for points without an SRID

    @property
    def n_steps(self) -> int:
        return int(self.steps.shape[1])

    @property
    def columns(self) -> tuple[tuple[str, str], ...]:
        return self.dim_columns + self.var_columns

    @property
    def extra_schema(self) -> pa.Schema:
        return pa.schema([(n, _TABLE_ARROW[k]) for n, k in self.columns])


def _plan_sample(src: _Source, names, sel, crs, flag) -> _SampleConfig:
    arrays = src.arrays
    ref = arrays[names[0]]
    if any((arrays[v].dims, arrays[v].shape) != (ref.dims, ref.shape) for v in names):
        shown = "; ".join(f"{v}{arrays[v].dims}={arrays[v].shape}" for v in names)
        raise ZarrGridMismatch(
            f"the variables must be on the same grid, but they differ: {shown}",
            fix="sample variables with the same dimensions, or sample them separately",
        )
    paths = [_coordinate_of(d, names[0], src) for d in ref.dims]
    attrs = {d: arrays[c].attrs for d, c in zip(ref.dims, paths) if c in arrays}
    try:
        ypos, xpos = _spatial_dims(ref, attrs)
    except ZarrUnsupported as e:
        raise ZarrUnsupported(
            e.message, fix="name the dimensions latitude/longitude or y/x"
        ) from None
    ydim, xdim = ref.dims[ypos], ref.dims[xpos]
    if any(d in (ydim, xdim) for d in (sel or {})):
        raise ValueError(
            f"sel selects {[d for d in (ydim, xdim) if d in (sel or {})]}, which the points "
            "choose. Fix: select only the other dimensions (time, level) with sel."
        )

    dim_coords, stored, coords, axes = _resolve_axes(src, names, sel, None, True, flag)
    labels = {}
    for pos, dim in ((ypos, ydim), (xpos, xdim)):
        path = dim_coords[names[0]][pos]
        if path not in coords:
            raise ZarrUnsupported(
                f"dimension {dim!r} of array {names[0]!r} has no coordinate array that "
                "GeoBrix can read, so its cells have no position",
                fix="add a coordinate array for it",
            )
        labels[dim] = coords[path]
    mapping = _grid_mapping_attrs(src, ref)
    grid = _Grid(
        _axis_map(ref.name, ydim, labels[ydim], attrs.get(ydim)),
        _axis_map(ref.name, xdim, labels[xdim], attrs.get(xdim)),
        _grid_crs(
            ref,
            mapping,
            _is_geographic(ydim, attrs.get(ydim))
            and _is_geographic(xdim, attrs.get(xdim)),
        ),
        (ref.chunks[ypos], ref.chunks[xpos]),
        -(-ref.shape[xpos] // ref.chunks[xpos]),
    )

    typed = {p: _table_values(v, arrays[p], True, flag) for p, v in stored.items()}
    columns = _table_columns(names, arrays, dim_coords, typed, True, flag)
    other = tuple(p for p in range(len(ref.dims)) if p not in (ypos, xpos))
    ndim = len(ref.dims)
    positions = [
        np.concatenate([np.arange(a, b) for a, b in axes[names[0]][p].ranges] or [[]])
        for p in other
    ]
    steps = (
        np.stack([g.reshape(-1) for g in np.meshgrid(*positions, indexing="ij")])
        if other
        else np.zeros((0, 1), dtype=np.int64)
    ).astype(np.int64)
    spans = _label_spans(typed, names, dim_coords, axes)
    dim_paths = tuple(dim_coords[names[0]][p] for p in other)

    read_arrays = {v: arrays[v] for v in names}
    budget = _budget_bytes()
    read = _TaskConfig(
        kind=src.kind,
        output="table",
        root=src.root,
        base_root=src.base_root,
        docs={
            k: raw
            for k, raw in src.metas.items()
            if k.rpartition("/")[0] in read_arrays
        },
        arrays=read_arrays,
        axes={v: tuple(a) for v, a in axes.items()},
        coords={},
        labels={},
        dim_coords=dim_coords,
        variables=tuple(names),
        decode=True,
        flag=flag,
        batch_bytes=max(1, budget // _BATCH_SHARE),
    )
    return _SampleConfig(
        read=read,
        grid=grid,
        variables=tuple(names),
        ypos=ypos,
        xpos=xpos,
        other=other,
        steps=steps,
        dim_columns=tuple(columns[p] for p in other),
        dim_paths=dim_paths,
        spans={p: spans[p] for p in dim_paths if p in spans},
        var_columns=tuple(columns[ndim:]),
        batch_cells=max(1, read.batch_bytes // _CELL_BYTES),
        crs=crs,
    )


def _units(src: _Source, cfg: _SampleConfig, uids) -> dict[int, str]:
    """The unit of each chunk number in ``uids``, as JSON: the manifest rows of the objects
    it reads (none for a plain store). A unit is one spatial chunk of the first variable
    with every selected position of the other dimensions."""
    arrays = src.arrays
    ref = arrays[cfg.variables[0]]
    axes = list(cfg.read.axes[ref.name])
    wanted: dict[int, tuple[list, list]] = {}
    for uid in uids:
        cy, cx = divmod(int(uid), cfg.grid.n_chunks_x)
        here = list(axes)
        for pos, c in ((cfg.ypos, cy), (cfg.xpos, cx)):
            lo = c * ref.chunks[pos]
            here[pos] = AxisSel(
                ref.dims[pos], ((lo, min(lo + ref.chunks[pos], ref.shape[pos])),), False
            )
        chunks = touched_chunks(here, ref.chunks)
        wanted[int(uid)] = (chunks, [b for c in chunks for b in boxes_of(ref, here, c)])
    rows_of: dict[str, dict] = {}
    if src.kind == "table":
        for v in cfg.variables:
            keys = {
                k
                for _, boxes in wanted.values()
                for k in keys_of_boxes(arrays[v], boxes)
            }
            rows_of[v] = src.rows(arrays[v], sorted(keys))
    return {
        uid: Unit(
            ref.name,
            tuple(chunks),
            tuple(
                r
                for v in cfg.variables
                for r in rows_for_boxes(arrays[v], boxes, rows_of.get(v, {}))
            ),
        ).to_json()
        for uid, (chunks, boxes) in wanted.items()
    }


# --- reading: points to rows ----------------------------------------------------------


def _positions(cfg: _SampleConfig, n_points: int, k: int) -> np.ndarray:
    """The ``k``-th other dimension's selected position at every row of ``n_points`` points."""
    return np.tile(cfg.steps[k], n_points)


def _dimension_columns(cfg: _SampleConfig, n_points: int) -> list[pa.Array]:
    out = []
    for k, ((_, kind), path) in enumerate(zip(cfg.dim_columns, cfg.dim_paths)):
        pos, span = _positions(cfg, n_points, k), cfg.spans.get(path)
        out.append(
            _arrow_column(pos, kind)
            if span is None
            else _arrow_column(span.at(pos), kind, span.missing(pos))
        )
    return out


def _cells(
    cfg: _SampleConfig, meta, row: np.ndarray, col: np.ndarray
) -> list[np.ndarray]:
    """One coordinate array per axis of ``meta``: every selected step of every point."""
    n_points = row.size
    out = []
    for p in range(len(meta.dims)):
        if p == cfg.ypos:
            out.append(np.repeat(row, cfg.n_steps))
        elif p == cfg.xpos:
            out.append(np.repeat(col, cfg.n_steps))
        else:
            out.append(_positions(cfg, n_points, cfg.other.index(p)))
    return out


def _sampled(cfg: _SampleConfig, reader: _UnitReader, pool, row, col) -> pa.RecordBatch:
    from databricks.labs.gbx.zarr._engine import read_points

    def one(v: str):
        meta = cfg.read.arrays[v]
        stored = read_points(reader.array(v), _cells(cfg, meta, row, col))
        return _table_values(stored, meta, True, cfg.read.flag)

    read = list(pool.map(one, cfg.variables))
    columns = _dimension_columns(cfg, row.size)
    for (_, kind), (values, mask) in zip(cfg.var_columns, read):
        columns.append(_arrow_column(values, kind, mask))
    return pa.RecordBatch.from_arrays(columns, schema=cfg.extra_schema)


def _unsampled(cfg: _SampleConfig, n_points: int) -> pa.RecordBatch:
    """The columns of points that have no cell: their steps, and NULL for every variable."""
    columns = _dimension_columns(cfg, n_points)
    columns += [
        pa.nulls(n_points * cfg.n_steps, _TABLE_ARROW[k]) for _, k in cfg.var_columns
    ]
    return pa.RecordBatch.from_arrays(columns, schema=cfg.extra_schema)


def _sample_points(
    cfg: _SampleConfig,
    units: Mapping[int, str],
    uid: np.ndarray,
    row: np.ndarray,
    col: np.ndarray,
    engine: str,
) -> Iterator[tuple[np.ndarray, pa.RecordBatch]]:
    """``(point, columns)`` pairs: ``point`` indexes the arguments, once per output row, and
    ``columns`` are those rows' dimension and variable columns (``cfg.extra_schema``).

    A point's rows are consecutive and in step order; points of one chunk come together and
    the chunk is opened once. A batch holds at most ``cfg.batch_cells`` cells.
    """
    uid = np.asarray(uid, dtype=np.int64)
    row, col = np.asarray(row, dtype=np.int64), np.asarray(col, dtype=np.int64)
    per_batch = max(1, cfg.batch_cells // max(1, cfg.n_steps * len(cfg.variables)))
    lost = np.flatnonzero(uid < 0)
    for lo in range(0, lost.size, per_batch):
        part = lost[lo : lo + per_batch]
        yield np.repeat(part, cfg.n_steps), _unsampled(cfg, part.size)
    have = np.flatnonzero(uid >= 0)
    if not have.size:
        return
    have = have[np.argsort(uid[have], kind="stable")]
    cuts = np.flatnonzero(np.diff(uid[have])) + 1
    with ThreadPoolExecutor(
        max_workers=max(1, min(_READ_THREADS, len(cfg.variables)))
    ) as pool:
        for group in np.split(have, cuts):
            unit = units.get(int(uid[group[0]]))
            if unit is None:
                raise ZarrError(
                    "the points changed between planning the sample and reading it: a "
                    "point lies in a chunk the plan did not include",
                    fix="make points_df deterministic (write it to a table first), "
                    "then sample that",
                )
            reader = _UnitReader(cfg.read, Unit.from_json(unit), engine)
            try:
                for name in cfg.variables:
                    reader.array(name)  # open before the threads start
                for lo in range(0, group.size, per_batch):
                    part = group[lo : lo + per_batch]
                    yield np.repeat(part, cfg.n_steps), _sampled(
                        cfg, reader, pool, row[part], col[part]
                    )
            finally:
                reader.close()


# --- the Spark stages -----------------------------------------------------------------


def _map_task(cfg: _SampleConfig, geom_col: str):
    """The ``mapInArrow`` function that adds the chunk number and cell of each point."""
    grid, crs = cfg.grid, cfg.crs

    def run(batches):
        for batch in batches:
            uid, row, col = grid.map(batch.column(geom_col).to_pylist(), crs)
            fields = [
                (_UID, pa.int64()),
                (_ROW, pa.int32()),
                (_COL, pa.int32()),
            ]
            yield pa.RecordBatch.from_arrays(
                [*batch.columns, pa.array(uid), pa.array(row), pa.array(col)],
                schema=pa.schema([*batch.schema, *fields]),
            )

    return run


def _sample_task(cfg: _SampleConfig, units: dict[int, str]):
    """The ``mapInArrow`` function that samples the points of a partition sorted by chunk."""

    def run(batches):
        for batch in batches:
            carry = batch.select([n for n in batch.schema.names if n not in _INTERNAL])
            for point, extra in _sample_points(
                cfg,
                units,
                batch.column(_UID).to_numpy(zero_copy_only=False),
                batch.column(_ROW).to_numpy(zero_copy_only=False),
                batch.column(_COL).to_numpy(zero_copy_only=False),
                "zarr",
            ):
                taken = carry.take(pa.array(point))
                yield pa.RecordBatch.from_arrays(
                    [*taken.columns, *extra.columns],
                    schema=pa.schema([*carry.schema, *extra.schema]),
                )

    return run


def _spark_schema(points_df, cfg: _SampleConfig):
    from pyspark.sql import types as T

    return T.StructType(
        [
            *points_df.schema.fields,
            *(T.StructField(n, _spark_type(k), True) for n, k in cfg.columns),
        ]
    )


def _through_spark(df, cfg, units, schema):
    """``df`` (points with ``_UID``, ``_ROW``, ``_COL``) sampled in tasks, one chunk's points
    together: hashed over at most ``_MAX_PARTITIONS`` partitions and sorted within each.
    """
    parts = max(1, min(len(units), _MAX_PARTITIONS))
    return (
        df.repartition(parts, _UID)
        .sortWithinPartitions(_UID)
        .mapInArrow(_sample_task(cfg, units), schema)
    )


def gbx_zarr_sample(
    spark,
    points_df,
    source: str,
    *,
    geom_col: str = "geom",
    crs=None,
    variables=None,
    sel=None,
    method: str = "nearest",
    version_as_of=None,
    timestamp_as_of=None,
    access: str = "auto",
    use_zarr_fill_value_as_mask: bool | None = None,
):
    """Sample a Zarr store at the points of a DataFrame.

    ``points_df`` has a POINT geometry column ``geom_col`` (WKB, EWKB, WKT or EWKT, as
    ``rst_sample`` reads it). ``source`` is the manifest table of a managed or registered
    store or the path of a plain store (``version_as_of`` / ``timestamp_as_of`` pin a table
    read, as in ``gbx_zarr_read``). ``crs`` is the CRS of points that carry no SRID of their
    own (an EWKB/EWKT SRID wins); a point with neither is taken to be in the store's CRS.
    Points are moved to the store's CRS and read at the nearest cell.

    The result has every column of ``points_df``, then a column per dimension of the store
    other than latitude/longitude (``x``/``y``) that ``sel`` leaves in (time as
    ``TIMESTAMP``), then a column per variable, typed as in ``gbx_zarr_read(output="table")``.
    A point is repeated for each selected position of those dimensions, so a time series
    is one point's rows. ``sel`` selects those dimensions by label (as in ``gbx_zarr_read``);
    the points choose the location. ``variables`` defaults to every gridded variable, which
    must share a grid. A point outside the grid (further than half a cell beyond its first or
    last cell), a missing or empty geometry, and a cell equal to the array's fill value or
    ``_FillValue`` / ``missing_value`` (where ``use_zarr_fill_value_as_mask`` applies, as in
    ``gbx_zarr_read``) all give ``NULL``. A point that is not a POINT raises ``ValueError``.

    ``method`` is ``"nearest"``; ``"bilinear"`` is planned and raises ``ZarrUnsupported``.
    ``access`` is ``"auto"``, ``"fuse"`` or ``"direct"``, as in ``gbx_zarr_read`` (direct
    storage access with Unity Catalog credentials where they can be had). A plain-path store
    is read through its path (FUSE only), so ``"direct"`` raises for it: register the store
    to use direct access.

    Up to 100,000 points are collected to the driver; if they lie in at most four chunks of
    the grid and the answer is small the driver reads them and returns the result, with no
    Spark job. Otherwise Spark reads each chunk once for all its points. Row order is not
    defined. ``points_df`` is read more than once when it has more than 100,000 points, so
    it must be deterministic.
    """
    if method != "nearest":
        raise ZarrUnsupported(
            f"method={method!r} is not available: points are read at the nearest cell",
            fix='use method="nearest"; bilinear is planned',
        )
    check_access(access)
    if access == "direct" and classify_source(source) == "path":
        raise plain_direct_error(source)  # a plain store has no files to reach directly
    check_version_args(version_as_of, timestamp_as_of)
    _check_mask_flag(use_zarr_fill_value_as_mask)
    if geom_col not in points_df.columns:
        raise ValueError(
            f"points_df has no column {geom_col!r} (it has {points_df.columns}). "
            "Fix: pass geom_col=<the geometry column>."
        )
    from pyspark.sql import types as T

    if not isinstance(
        points_df.schema[geom_col].dataType, (T.BinaryType, T.StringType)
    ):
        raise ValueError(
            f"column {geom_col!r} is {points_df.schema[geom_col].dataType.simpleString()}, "
            "not a geometry. Fix: pass WKB/EWKB (BINARY) or WKT/EWKT (STRING) points."
        )
    if crs is not None:
        _resolve(None, crs)

    kind = classify_source(source)
    src = _open_source(spark, source, kind, version_as_of, timestamp_as_of)
    names = _choose_variables(variables, src)
    cfg = _plan_sample(src, names, sel, crs, use_zarr_fill_value_as_mask)
    cfg = dataclasses.replace(
        cfg,
        read=dataclasses.replace(cfg.read, access=source_access(spark, src, access)),
    )
    clash = sorted({n for n, _ in cfg.columns} & {*points_df.columns, *_INTERNAL})
    if clash:
        raise ValueError(
            f"points_df already has column(s) {clash}, which the sample adds. "
            "Fix: rename them in points_df, or sample other variables."
        )
    schema = _spark_schema(points_df, cfg)
    if cfg.n_steps == 0:
        return spark.createDataFrame([], schema)

    rows = points_df.limit(_POINT_LIMIT + 1).collect()
    if len(rows) > _POINT_LIMIT:
        return _sample_large(spark, points_df, geom_col, src, cfg, schema)
    uid, row, col = cfg.grid.map([r[geom_col] for r in rows], crs)
    chunks = np.unique(uid[uid >= 0])
    units = _units(src, cfg, chunks)
    if len(chunks) <= _SMALL_UNITS and len(rows) * cfg.n_steps <= _DRIVER_ROWS:
        return _sample_on_driver(spark, rows, cfg, units, (uid, row, col), schema)
    fields = [(_UID, T.LongType()), (_ROW, T.IntegerType()), (_COL, T.IntegerType())]
    mapped = spark.createDataFrame(
        [(*r, int(u), int(a), int(b)) for r, u, a, b in zip(rows, uid, row, col)],
        T.StructType(
            [*points_df.schema.fields, *(T.StructField(n, t) for n, t in fields)]
        ),
    )
    return _through_spark(mapped, cfg, units, schema)


def _sample_on_driver(spark, rows, cfg, units, mapped, schema):
    out = []
    for point, extra in _sample_points(cfg, units, *mapped, "auto"):
        for i, values in zip(point.tolist(), extra.to_pylist()):
            out.append((i, (*rows[i], *values.values())))
    out.sort(key=lambda item: item[0])  # stable: a point's rows stay in step order
    return spark.createDataFrame([values for _, values in out], schema)


def _sample_large(spark, points_df, geom_col, src, cfg, schema):
    """More points than the driver collects: executors map them, and the driver plans the
    chunks from a first pass over the geometry column alone."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    fields = [(_UID, T.LongType()), (_ROW, T.IntegerType()), (_COL, T.IntegerType())]
    add_cells = _map_task(cfg, geom_col)

    def mapped_schema(df):
        return T.StructType(
            [*df.schema.fields, *(T.StructField(n, t) for n, t in fields)]
        )

    geoms = points_df.select(geom_col)
    found = (
        geoms.mapInArrow(add_cells, mapped_schema(geoms))
        .where(F.col(_UID) >= 0)
        .select(_UID)
        .distinct()
        .collect()
    )
    units = _units(src, cfg, sorted(r[_UID] for r in found))
    mapped = points_df.mapInArrow(add_cells, mapped_schema(points_df))
    return _through_spark(mapped, cfg, units, schema)
