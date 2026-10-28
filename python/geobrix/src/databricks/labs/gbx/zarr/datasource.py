"""``zarr_gbx``: a read-only, session-free Spark DataSource for plain Zarr stores.

``spark.read.format("zarr_gbx").load(path)`` reads a vendor Zarr store (one that has no
GeoBrix manifest) like every other light reader: no table, no session, no JAR. A managed or
registered store needs its manifest, which a Python DataSource cannot read (it has no
session on Connect and Serverless), so those are read with ``gbx_zarr_read``.

The reader plans where ``gbx_zarr_read`` plans and decodes where it decodes (``_read``):
``partitions()`` opens the store on its bare path and returns one ``InputPartition`` per
work unit (chunk indices, no rows and no bytes); ``read(partition)`` decodes one unit into
Arrow batches. What every task shares with the planner (the arrays, the selection, the
labels) is not carried in the partitions, which would copy it once per unit. It is derived
again from the options, once per Python worker (``_task_state``), because the reader is
pickled before ``partitions()`` runs and so cannot carry what ``partitions()`` computes.

Options (camelCase, like the other ``*_gbx`` readers); every value is a string:

``variables``  comma list of arrays to read (default: every gridded variable).
``sel``        JSON, dimension to a label: ``{"latitude": [60, 50], "time": "2023-01-01T05"}``.
               A list of two is an inclusive slice; a scalar the nearest label; a list of
               any other length those labels.
``isel``       JSON, dimension to a position, with the same shapes (a list of two is a
               ``start:stop`` slice, stop excluded).
``output``     ``blocks`` (default), ``tiles`` or ``table`` (``gbx_zarr_read``'s outputs).
``virtualTiles``  ``true`` (default) or ``false``; ``output=tiles`` only.
``dimIndex``   ``dim=index,...``: pin dimensions to one position (NetCDF reader semantics).
``fanout``     ``dim,...``; ``output=tiles`` only. As in the NetCDF reader the dimensions
               named are expanded, one tile for each position, and every other non-spatial
               dimension that is not pinned, selected or stacked is read at index 0. Without
               ``fanout`` every selected position of every such dimension is expanded.
``bandDim``    one dimension to stack as the bands of each tile; ``output=tiles`` only.
``tileSize``   ``w,h`` (or one ``n``) in cells; ``output=tiles`` only. Default: one inner chunk.
``access``     ``auto`` (default) or ``fuse``: the store is read through its path. ``direct``
               raises ``ZarrAccessUnavailable``: it needs credentials Unity Catalog vends to
               a session, and this reader has none.
``engine``     ``auto`` (default, zarr-python), ``zarr`` or ``tensorstore``: how a task opens
               the arrays of a ``blocks`` or ``table`` read.

It defines no ``pushFilters``: on Serverless env 6 a reader that merely defines it fails
with ``DATA_SOURCE_PUSHDOWN_DISABLED``. Pruning is option-based (``sel``, ``isel``,
``variables``).
"""

from __future__ import annotations

import dataclasses
import functools
from dataclasses import dataclass
from typing import Iterator, Mapping, Sequence

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

# Nothing from databricks.labs.gbx.ds is imported at module level: importing any ds module
# runs ds/__init__, which imports ds.register, which imports this module. A worker that
# unpickles the reader imports this module first, so a top-level ds import here is a cycle.

__all__ = ["ZarrGbxDataSource", "ZarrGbxReader", "ZarrGbxOptions", "parse_options"]

_OUTPUTS = ("blocks", "tiles", "table")
_ENGINES = ("auto", "tensorstore", "zarr")


@dataclass(frozen=True)
class ZarrGbxOptions:
    """The options of a ``zarr_gbx`` read, parsed."""

    path: str
    variables: tuple[str, ...] | None
    sel: dict | None
    isel: dict | None  # as written; ``dim_index`` is separate
    dim_index: dict
    output: str
    virtual_tiles: bool
    fanout: tuple[str, ...] | None
    band_dim: str | None
    tile_size: tuple[int, int] | None
    engine: str


def _bad(message: str, fix: str) -> ValueError:
    return ValueError(f"zarr_gbx: {message} Fix: {fix}")


def _helper(parse, raw: str, name: str, fix: str):
    """``parse(raw)`` with the reader's name on its error: the NetCDF and raster helpers
    that parse ``dimIndex`` and ``tileSize`` name their own readers in theirs."""
    try:
        return parse(raw)
    except ValueError as e:
        text = str(e)
        for prefix in ("netcdf_gbx: ", "raster_gbx: "):
            text = text.removeprefix(prefix)
        raise _bad(f"{name} is not valid: {text.rstrip('.')}.", fix) from None


def _json(raw: str, name: str):
    import json

    try:
        value = json.loads(raw)
    except ValueError as e:
        raise _bad(
            f"{name} is not valid JSON ({e}).",
            f"write it like {name}='{{\"time\": [0, 24]}}'",
        ) from None
    if not isinstance(value, dict):
        raise _bad(
            f"{name} must be a JSON object of dimension to selection, got {raw[:40]!r}.",
            f"write it like {name}='{{\"time\": [0, 24]}}'",
        )
    return value


def _selector(name: str, dim: str, value):
    """One ``sel``/``isel`` value as ``gbx_zarr_read`` takes it: a list of two is a slice."""
    if isinstance(value, dict):
        raise _bad(
            f"{name}[{dim!r}] is an object; a selection is a number, a string or a list.",
            "give a value, [start, stop] or a list of values",
        )
    if isinstance(value, list):
        return slice(value[0], value[1]) if len(value) == 2 else list(value)
    return value


def _selection(raw: str | None, name: str) -> dict | None:
    if raw is None:
        return None
    return {d: _selector(name, d, v) for d, v in _json(raw, name).items()}


def _flag(value: str, name: str) -> bool:
    text = value.strip().lower()
    if text not in ("true", "false"):
        raise _bad(
            f"{name} must be true or false, got {value!r}.",
            f"pass {name}=true or false",
        )
    return text == "true"


def _choice(value: str, name: str, allowed: Sequence[str]) -> str:
    text = value.strip().lower()
    if text not in allowed:
        raise _bad(
            f"{name} must be one of {tuple(allowed)}, got {value!r}.",
            f"pass one of {', '.join(allowed)}",
        )
    return text


def _direct_error():
    """What ``access="direct"`` raises here: the error and ``Fix:`` of ``resolve_access``."""
    from databricks.labs.gbx.zarr._access import _unavailable

    return _unavailable(
        "direct storage access is not available to the zarr_gbx reader: it gets Unity "
        "Catalog volume credentials from a session, and a Python data source has none "
        "(gbx_zarr_read has one)"
    )


def parse_options(options: Mapping[str, str]) -> ZarrGbxOptions:
    """The parsed, checked options of a ``zarr_gbx`` read (names are case-insensitive).

    Raises ``ValueError`` (its message ends with ``Fix:``) for an option that is not valid
    or that does not go with another, and ``ZarrAccessUnavailable`` for ``access="direct"``.
    """
    from databricks.labs.gbx.ds.netcdf import _parse_dim_index, _parse_fanout
    from databricks.labs.gbx.ds.raster import _as_tile_size

    raw = {str(k).lower(): str(v) for k, v in dict(options).items()}

    def get(name: str, default: str | None = None) -> str | None:
        value = raw.get(name.lower())
        return default if value is None else value

    path = (get("path") or "").strip()
    if not path:
        raise _bad(
            "no store path.",
            "load the path of a Zarr store: spark.read.format('zarr_gbx').load(path)",
        )
    output = _choice(get("output", "blocks"), "output", _OUTPUTS)
    access = _choice(get("access", "auto"), "access", ("auto", "fuse", "direct"))
    if access == "direct":
        raise _direct_error()
    engine = _choice(get("engine", "auto"), "engine", _ENGINES)

    names = [n.strip() for n in (get("variables") or "").split(",") if n.strip()]
    sel, isel = _selection(get("sel"), "sel"), _selection(get("isel"), "isel")
    dim_index = (
        _helper(
            _parse_dim_index,
            get("dimIndex"),
            "dimIndex",
            "write dimIndex=time=2,level=1",
        )
        if get("dimIndex")
        else {}
    )
    fanout = tuple(_parse_fanout(get("fanout") or "")) or None
    band_dim = (get("bandDim") or "").strip() or None
    tile_size = (
        _helper(_as_tile_size, get("tileSize"), "tileSize", "write tileSize=256,256")
        if get("tileSize")
        else None
    )
    virtual_tiles = _flag(get("virtualTiles", "true"), "virtualTiles")

    for dim in dim_index:
        if dim in (sel or {}) or dim in (isel or {}):
            raise _bad(
                f"dimension {dim!r} is in dimIndex and also in sel or isel.",
                "select each dimension one way",
            )
    if set(dim_index) & set(fanout or ()):
        raise _bad(
            f"dimension(s) {sorted(set(dim_index) & set(fanout))} are in dimIndex and fanout: "
            "a dimension cannot be both pinned and expanded.",
            "remove them from one of the two",
        )
    if band_dim and (band_dim in dim_index or band_dim in (fanout or ())):
        raise _bad(
            f"bandDim={band_dim!r} is also in dimIndex or fanout: a dimension cannot be both "
            "stacked as bands and pinned or expanded.",
            "stack a dimension that is in neither",
        )
    if output != "tiles":
        given = {"fanout": fanout, "bandDim": band_dim, "tileSize": tile_size}
        for name, value in given.items():
            if value is not None:
                raise _bad(
                    f"{name} applies to output='tiles', and this read is output={output!r}.",
                    f"drop {name}",
                )
    elif engine != "auto":
        raise _bad(
            "engine applies to output='blocks' and 'table'; tiles are read by the tile engine.",
            "drop engine",
        )
    return ZarrGbxOptions(
        path=path,
        variables=tuple(names) or None,
        sel=sel,
        isel=isel,
        dim_index=dim_index,
        output=output,
        virtual_tiles=virtual_tiles,
        fanout=fanout,
        band_dim=band_dim,
        tile_size=tile_size,
        engine=engine,
    )


# --- planning ------------------------------------------------------------------------


def _non_spatial(src, name: str) -> tuple[str, ...]:
    """The dimensions of array ``name`` that are not its latitude and longitude (the ones
    a tile spans); none for an array that has no clear pair, which is not tiled."""
    from databricks.labs.gbx.zarr import _read, _tiles
    from databricks.labs.gbx.zarr.errors import ZarrUnsupported

    meta = src.arrays[name]
    attrs = {}
    for dim in meta.dims:
        coord = _read._coordinate_of(dim, name, src)
        if coord in src.arrays:
            attrs[dim] = src.arrays[coord].attrs
    try:
        y, x = _tiles._spatial_dims(meta, attrs)
    except ZarrUnsupported:
        return ()
    return tuple(d for i, d in enumerate(meta.dims) if i not in (y, x))


def _cut(
    opts: ZarrGbxOptions, src, names: list[str]
) -> tuple[dict | None, dict | None]:
    """``(sel, isel)`` of the read: the options' own, plus ``dimIndex``, plus (with
    ``fanout``) index 0 for each non-spatial dimension that nothing else names."""
    sel, isel = dict(opts.sel or {}), dict(opts.isel or {})
    isel.update(opts.dim_index)
    if opts.fanout:
        known = {d for n in names for d in src.arrays[n].dims}
        unknown = [d for d in opts.fanout if d not in known]
        if unknown:
            raise _bad(
                f"fanout names unknown dimension(s) {unknown}; the variables have {sorted(known)}.",
                "name dimensions of the variables",
            )
        named = {*sel, *isel, *opts.fanout, *([opts.band_dim] if opts.band_dim else [])}
        for n in names:
            isel.update({d: 0 for d in _non_spatial(src, n) if d not in named})
    return sel or None, isel or None


def _open(opts: ZarrGbxOptions):
    from databricks.labs.gbx.zarr import _read

    src = _read._open_source(None, opts.path, "path", None, None)
    names = _read._choose_variables(
        list(opts.variables) if opts.variables else None, src
    )
    return src, names, *_cut(opts, src, names)


def _unit_plan(opts: ZarrGbxOptions, *, units: bool):
    """``(units, config)`` of a blocks or table read, as ``gbx_zarr_read`` plans it."""
    from databricks.labs.gbx.zarr import _read
    from databricks.labs.gbx.zarr._access import FUSE

    src, names, sel, isel = _open(opts)
    planned, cfg = _read._plan(
        None, src, names, sel, isel, opts.output, True, None, units=units
    )
    return planned, dataclasses.replace(cfg, access=FUSE)


def _tile_plans(opts: ZarrGbxOptions):
    """The ``TilePlan`` of each variable, as ``gbx_zarr_read`` plans a tiles read.

    A session-free reader cannot tell Serverless from classic, so the largest tile a task
    may hold is the classic cap (``GBX_STREAM_MAX_BYTES`` overrides it).
    """
    from databricks.labs.gbx.zarr import _read

    src, names, sel, isel = _open(opts)
    return _read._plan_tile_set(
        None,
        src,
        names,
        opts.variables is not None,
        sel,
        isel,
        True,
        None,
        opts.band_dim,
        opts.tile_size,
    )


@functools.lru_cache(maxsize=4)
def _task_state(key: tuple):
    """What a task shares: the config of a blocks or table read, or the tile plans.

    Derived from the options and cached per Python worker, so a worker that runs many
    units plans once. ``key`` is the options as sorted ``(name, value)`` pairs.
    """
    opts = parse_options(dict(key))
    return (
        _tile_plans(opts)
        if opts.output == "tiles"
        else _unit_plan(opts, units=False)[1]
    )


# --- the source ----------------------------------------------------------------------


class _UnitPartition(InputPartition):
    """One work unit: chunks of one array (``zarr._plan.Unit``): paths and indices only."""

    def __init__(self, unit):
        super().__init__(unit)
        self.unit = unit


class _TilePartition(InputPartition):
    """Tiles ``start`` to ``stop`` (exclusive) of the numbering that runs over the tiles of
    every variable in turn."""

    def __init__(self, start: int, stop: int):
        super().__init__((start, stop))
        self.start, self.stop = start, stop


class ZarrGbxReader(DataSourceReader):
    def __init__(self, options: Mapping[str, str]):
        self._key = tuple(
            sorted((str(k).lower(), str(v)) for k, v in dict(options).items())
        )
        self._opts = parse_options(dict(self._key))

    def partitions(self) -> Sequence[InputPartition]:
        opts = self._opts
        if opts.output == "tiles":
            from databricks.labs.gbx.zarr import _read

            total = sum(p.n_tiles for p in _tile_plans(opts))
            per_task = (
                _read._TILES_PER_TASK
                if opts.virtual_tiles
                else _read._MATERIALIZED_PER_TASK
            )
            return [
                _TilePartition(lo, min(lo + per_task, total))
                for lo in range(0, total, per_task)
            ]
        units, _ = _unit_plan(opts, units=True)
        return [_UnitPartition(u) for u in units]

    def read(self, partition) -> Iterator:
        if partition is None:  # Spark hands one to a read that planned no partition
            return
        from databricks.labs.gbx.zarr import _read

        state = _task_state(self._key)
        if isinstance(partition, _TilePartition):
            import pyarrow as pa

            from databricks.labs.gbx.zarr._access import FUSE

            ids = pa.array(range(partition.start, partition.stop), pa.int64())
            task = _read._tile_task(state, self._opts.virtual_tiles, FUSE)
            yield from task([pa.record_batch({"id": ids})])
        else:
            engine = "zarr" if self._opts.engine == "auto" else self._opts.engine
            yield from _read._run_unit(state, partition.unit, engine)


class ZarrGbxDataSource(DataSource):
    """``spark.read.format("zarr_gbx")``: see the module docstring for the options."""

    @classmethod
    def name(cls) -> str:
        return "zarr_gbx"

    def schema(self) -> StructType | str:
        opts = parse_options(self.options)
        if opts.output == "tiles":
            from databricks.labs.gbx.ds.raster import reader_schema_v2

            return reader_schema_v2()
        from databricks.labs.gbx.zarr import _read

        if opts.output == "blocks":
            return _read.BLOCKS_SCHEMA
        return _read._table_struct(_unit_plan(opts, units=False)[1])

    def reader(self, schema: StructType) -> DataSourceReader:
        return ZarrGbxReader(self.options)
