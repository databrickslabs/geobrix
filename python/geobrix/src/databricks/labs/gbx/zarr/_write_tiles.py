"""GeoBrix tiles into Zarr: the tile half of ``gbx_zarr_write``.

A DataFrame with one tile-struct column is turned into the blocks the rest of the writer
already commits (two-phase write, CAS, rebase), so everything below this module is shared
with blocks and ``xarray`` input.

**Two passes over the tiles, one decode each.**

1. *Facts* (``collect_facts``): one small row per tile, made by ``mapInArrow`` from the
   tile's header (no pixel is read for a virtual tile; a tile with a pending clip or warp
   is decoded, because only the decoded raster knows its grid). Their geotransform, CRS,
   size and dtype, the Zarr provenance in ``metadata`` and the ``dim_cols`` values come to
   the driver, which plans the whole write from them: the grid, the position of every
   tile, the coordinate arrays. A tile that does not fit raises ``ZarrGridMismatch``
   there, on the driver, before any file is written.
2. *Blocks* (``blocks_function``): each tile is decoded through ``open_tile`` (so pending
   instructions and clips apply, and a virtual tile is materialized) into blocks at the
   positions the plan gave it. Both passes derive a tile's positions with ``place``, so the
   write's digest check sees the plan again.

**Where a tile goes.** Spatially, by its decoded geotransform against the target grid (the
store's own coordinates; for ``create``, the first tile's). A tile with Zarr provenance
(``zarr_array``, ``sliceDims``, ``window`` in ``metadata``) must also come from a plane
whose grid equals the target's, and takes its other positions from ``sliceDims``. Along a
non-spatial dimension named in ``dim_cols`` the column's value is looked up in the store's
coordinate (``update``/``overwrite``), is a new label (``append``'s dimension) or a sorted
label of the new store (``create``). Tiles without provenance need ``dim_cols`` for every
non-spatial dimension.

Values keep the tile's type in a new store; in an existing array they are the decoded
values, with the tile's NoData as NaN, so the writer's CF encoding casts them to the array's
type (floats to integers round half to even) and puts the array's ``_FillValue`` where the
tile has NoData.

Nothing here touches ``spark.conf``, ``sparkContext``, ``.rdd``, ``_jvm`` or ``_jsc``.
"""

from __future__ import annotations

import functools
import json
import math
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from databricks.labs.gbx.zarr.errors import ZarrError, ZarrGridMismatch, ZarrUnsupported

__all__ = ["Prepared", "StoreView", "prepare", "tile_column"]

_PIXEL = 1e-6  # how far from the grid, in cells, a tile may be
_F32_EPS = float(np.finfo(np.float32).eps)
_FACTS_ARROW = pa.schema([("facts", pa.string())])
_SEEN_ARROW = pa.schema(
    [
        ("variable", pa.string()),
        ("dims", pa.list_(pa.string())),
        ("start", pa.list_(pa.int64())),
        ("stop", pa.list_(pa.int64())),
        ("dtype", pa.string()),
        ("shape", pa.list_(pa.int64())),
        ("attrs", pa.string()),
        ("fill_value", pa.string()),
    ]
)
_BLOCK_FIELDS = (
    "variable",
    "dims",
    "start",
    "stop",
    "coord_start",
    "coord_stop",
    "dtype",
    "shape",
    "data",
    "fill_value",
    "attrs",
)
_MAPPING = (
    "spatial_ref"  # the CF grid-mapping variable of a new store that is not EPSG:4326
)
_DEFAULT_VARIABLE = (
    "data"  # as the NetCDF writer: no name given, a new store's variable
)
_LAT = ("latitude", {"units": "degrees_north", "standard_name": "latitude"})
_LON = ("longitude", {"units": "degrees_east", "standard_name": "longitude"})
_Y = ("y", {"standard_name": "projection_y_coordinate", "units": "m"})
_X = ("x", {"standard_name": "projection_x_coordinate", "units": "m"})
_DIM_COLS_FIX = (
    "pass dim_cols={'<dimension>': '<column>'} for each non-spatial dimension, with the "
    "column holding that dimension's value for each tile"
)


# --- recognizing the tile column and the dim_cols ---------------------------------------


def tile_column(df) -> str | None:
    """The name of the tile-struct column of ``df`` (the v2 tile struct's fields, by schema),
    ``None`` when it has none; more than one is refused."""
    from pyspark.sql.types import StructType

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    want = V2_TILE_SCHEMA.fieldNames()
    found = [
        f.name
        for f in df.schema.fields
        if isinstance(f.dataType, StructType) and f.dataType.fieldNames() == want
    ]
    if len(found) > 1:
        raise ValueError(
            f"the DataFrame has {len(found)} tile columns {found}, and a write takes one. "
            "Fix: select the tile column to write (df.select('tile', ...))."
        )
    return found[0] if found else None


def check_dim_cols(dim_cols, df, tile_col) -> dict:
    """``dim_cols`` as a plain ``{dimension: column}`` dict, every column a number or a
    time; ``{}`` for ``None``."""
    from pyspark.sql import types as T

    if dim_cols is None:
        return {}
    if not isinstance(dim_cols, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in dim_cols.items()
    ):
        raise ValueError(
            f"dim_cols must map a dimension name to a column name, got {dim_cols!r}. "
            "Fix: pass dim_cols={'time': 'acq_time'}."
        )
    fields = {f.name: f.dataType for f in df.schema.fields}
    numeric = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
        T.TimestampType,
        T.TimestampNTZType,
        T.DateType,
    )
    for dim, col in dim_cols.items():
        if col not in fields or col == tile_col:
            raise ValueError(
                f"dim_cols maps {dim!r} to the column {col!r}, which the DataFrame "
                f"does not have beside its tile column (it has {sorted(set(fields) - {tile_col})}). "
                "Fix: name a column of the DataFrame."
            )
        if not isinstance(fields[col], numeric):
            raise ZarrUnsupported(
                f"the dim_cols column {col!r} for {dim!r} holds {fields[col].simpleString()} "
                "values; GeoBrix coordinates are numbers or times",
                fix=f"cast {col!r} to a number or a timestamp",
            )
    return dict(dim_cols)


# --- one tile, as the passes see it -------------------------------------------------------


@dataclass
class Fact:
    """What the plan needs of one tile."""

    variable: str | None  # the tile's zarr_array; None for a tile without provenance
    gt: list  # the decoded tile's geotransform, GDAL order
    crs: str | None
    height: int
    width: int
    count: int
    dtype: str
    nodata: float | None
    slices: dict  # provenance: dimension -> index (sliceDims)
    band_dim: str | None  # provenance: the dimension stacked as bands
    bands: list  # and the indices along it
    plane_gt: list | None  # provenance: the grid of the plane the tile was cut from
    plane_crs: str | None
    dims: dict = field(default_factory=dict)  # dim_cols: dimension -> [kind, value]


@dataclass(frozen=True)
class TileConfig:
    """What a pass needs to know; travels in the ``mapInArrow`` closures."""

    dims: tuple = ()  # the dim_cols dimensions, in the order of the columns d0, d1, ...
    names: tuple | None = (
        None  # variables to write; provenance tiles of others are left out
    )
    default: str | None = None  # the variable of a tile without provenance
    targets: dict | None = None  # the data pass: variable -> Target


def _virtual(tile: dict):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    row = dict(tile)
    md = row.get("metadata")
    row["metadata"] = dict(md) if md else {}
    return VirtualTile.from_row(row)


def _slices_of(text: str | None) -> dict:
    out = {}
    for part in (text or "").split(","):
        name, eq, index = part.partition("=")
        if eq:
            out[name] = int(index)
    return out


def _provenance(md: dict) -> str | None:
    """The array a tile was cut from, ``None`` for a tile that has no Zarr provenance."""
    return md.get("zarr_array") or None


@contextmanager
def _opened(vt, header_only: bool):
    """The tile as a dataset: its header when that says everything (no clip, no warp), else
    the decoded raster."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    if header_only and vt.clip_polygon is None and not vt.crs:
        with ot.open_header(vt) as ds:
            yield ds
    else:
        with ot.open_tile(vt) as ds:
            yield ds


def _fact_of(ds, vt, dims: dict) -> Fact:
    from databricks.labs.gbx.core.crs import crs_to_canonical

    md = vt.metadata or {}
    plane = md.get("zarr_gt") if _provenance(md) else None
    band_dim = md.get("bandDim") or None
    nodata = ds.nodata
    return Fact(
        variable=_provenance(md),
        gt=[float(v) for v in ds.transform.to_gdal()],
        crs=None if ds.crs is None else crs_to_canonical(ds.crs),
        height=int(ds.height),
        width=int(ds.width),
        count=int(ds.count),
        dtype=np.dtype(ds.dtypes[0]).name,
        nodata=None if nodata is None else float(nodata),
        slices=_slices_of(md.get("sliceDims")),
        band_dim=band_dim,
        bands=([int(i) for i in md["zarr_band_index"].split(",")] if band_dim else []),
        plane_gt=None if plane is None else [float(v) for v in plane.split(",")],
        plane_crs=md.get("zarr_crs") if plane is not None else None,
        dims=dims,
    )


def _column_values(col, name: str) -> list:
    """``[kind, value]`` of each cell of a dim_cols column: ``M`` a time as nanoseconds since
    the epoch (UTC), ``i`` an integer, ``f`` a float."""
    col = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    if col.null_count:
        raise ValueError(
            f"the dim_cols column {name!r} has null values. Fix: give every tile a value "
            "(df.where(col.isNotNull()))."
        )
    t = col.type
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        ns = pc.cast(col, pa.timestamp("ns", tz=getattr(t, "tz", None)))
        return [["M", int(v)] for v in pc.cast(ns, pa.int64()).to_pylist()]
    if pa.types.is_integer(t):
        return [["i", int(v)] for v in col.to_pylist()]
    values = [float(v) for v in pc.cast(col, pa.float64()).to_pylist()]
    if not all(math.isfinite(v) for v in values):
        raise ValueError(
            f"the dim_cols column {name!r} has NaN or infinite values. Fix: give every "
            "tile a finite value."
        )
    return [["f", v] for v in values]


def _batches_of_tiles(batches, cfg: TileConfig):
    """``(tile dict, variable, dims)`` of each tile of the Arrow batches the passes receive.
    Null tiles and tiles of variables not wanted are left out."""
    for batch in batches:
        tiles = batch.column("tile").to_pylist()
        cols = [
            _column_values(batch.column(f"d{i}"), dim) for i, dim in enumerate(cfg.dims)
        ]
        for i, tile in enumerate(tiles):
            if tile is None or (
                tile.get("raster") is None and tile.get("path") is None
            ):
                continue  # nothing to write: a null or empty tile
            md = dict(tile.get("metadata") or {})
            name = _provenance(md)
            if name is not None and cfg.names is not None and name not in cfg.names:
                continue
            yield tile, {dim: cols[j][i] for j, dim in enumerate(cfg.dims)}


def facts_function(cfg: TileConfig):
    def run(batches):
        for tile, dims in _batches_of_tiles(batches, cfg):
            vt = _virtual(tile)
            with _opened(vt, header_only=True) as ds:
                fact = _fact_of(ds, vt, dims)
            yield pa.RecordBatch.from_pylist(
                [{"facts": json.dumps(asdict(fact))}], schema=_FACTS_ARROW
            )

    return run


# --- the target a tile is placed on ---------------------------------------------------------


@dataclass(frozen=True)
class Target:
    """One variable's array (existing or to be created) and its plane grid."""

    name: str
    dims: tuple
    y: int  # positions of the plane's axes among dims
    x: int
    gt: tuple  # the plane's north-up geotransform (GDAL order)
    crs: str
    height: int
    width: int
    flip_y: bool  # the array's axis runs the other way from the plane's rows
    flip_x: bool
    slack: float  # cells; the rounding of the store's labels, added to the tolerance
    maps: dict  # dimension -> {(kind, value): index} for the dim_cols dimensions
    existing: bool  # an existing array: provenance tiles must come from its grid and
    # their sliceDims indices are positions in it
    attrs: str | None = None  # a new store: JSON attributes of the variable's blocks
    stored: bool = False  # a new store: the blocks hold the tile's values as they are

    @property
    def others(self) -> tuple:
        return tuple(d for i, d in enumerate(self.dims) if i not in (self.y, self.x))


@functools.lru_cache(maxsize=256)
def _same_crs(a: str, b: str) -> bool:
    from databricks.labs.gbx.core.crs import crs_equal

    return a == b or crs_equal(a, b)


def _show(key) -> str:
    kind, value = key
    return str(np.datetime64(int(value), "ns")) if kind == "M" else str(value)


def place(fact: Fact, t: Target) -> list:
    """The boxes of one tile in ``t``: ``[(band index in the tile, start, stop)]``, ``start``
    and ``stop`` in the array's dimension order. Raises for a tile that does not fit."""
    gt, name = fact.gt, t.name
    if gt[2] or gt[4] or gt[1] <= 0 or gt[5] >= 0:
        raise ZarrGridMismatch(
            f"a tile for {name!r} is rotated or not north-up (geotransform {tuple(gt)})",
            fix="warp the tiles north-up first (rst_transform), then write them",
        )
    if fact.crs is None or not _same_crs(fact.crs, t.crs):
        raise ZarrGridMismatch(
            f"a tile for {name!r} is in {fact.crs or 'no CRS'}, the grid in {t.crs}",
            fix="reproject the tiles to the grid's CRS (rst_transform), or write to a "
            "store on the tiles' grid",
        )
    cell_x, cell_y = t.gt[1], -t.gt[5]
    tol = _PIXEL + t.slack
    # a tile cut from a Zarr plane: that plane's grid must be the array's
    if fact.plane_gt is not None and t.existing:
        pg = fact.plane_gt
        same = (
            abs(pg[1] - t.gt[1]) <= _PIXEL * cell_x
            and abs(pg[5] - t.gt[5]) <= _PIXEL * cell_y
            and abs(pg[0] - t.gt[0]) <= tol * cell_x
            and abs(pg[3] - t.gt[3]) <= tol * cell_y
            and fact.plane_crs is not None
            and _same_crs(fact.plane_crs, t.crs)
        )
        if not same:
            raise ZarrGridMismatch(
                f"the tiles for {name!r} come from a grid (origin {pg[0]}, {pg[3]}, cell "
                f"{pg[1]} x {-pg[5]}) that is not the target array's (origin {t.gt[0]}, "
                f"{t.gt[3]}, cell {cell_x} x {cell_y})",
                fix="write tiles into an array on their own grid, or resample them onto "
                "the target's",
            )
    if abs(gt[1] - cell_x) > _PIXEL * cell_x or abs(-gt[5] - cell_y) > _PIXEL * cell_y:
        raise ZarrGridMismatch(
            f"a tile for {name!r} has cells of {gt[1]} x {-gt[5]}, the grid's are "
            f"{cell_x} x {cell_y}",
            fix="resample the tiles to the grid's resolution, or write to a store on "
            "the tiles' grid",
        )
    col = (gt[0] - t.gt[0]) / cell_x
    row = (gt[3] - t.gt[3]) / t.gt[5]
    c0, r0 = round(col), round(row)
    if abs(col - c0) > tol or abs(row - r0) > tol:
        raise ZarrGridMismatch(
            f"a tile for {name!r} is not aligned to the grid: its origin lies "
            f"{col - math.floor(col):.6f} / {row - math.floor(row):.6f} of a cell off",
            fix="shift or resample the tiles onto the grid's cell edges",
        )
    h, w = fact.height, fact.width
    if c0 < 0 or r0 < 0 or c0 + w > t.width or r0 + h > t.height:
        raise ZarrGridMismatch(
            f"a tile for {name!r} covers columns {c0}..{c0 + w} and rows {r0}..{r0 + h}, "
            f"outside the array's {t.width} x {t.height} grid",
            fix="write tiles inside the store's grid",
        )
    stray = [d for d in fact.slices if d not in t.others]
    if stray or (fact.band_dim is not None and fact.band_dim not in t.others):
        raise ZarrGridMismatch(
            f"the tiles for {name!r} are cut along {sorted({*stray, fact.band_dim} - {None})}, "
            f"which are not dimensions of the array ({list(t.dims)})",
            fix="write them to the array they were read from, or map the dimensions "
            "with dim_cols",
        )
    if fact.band_dim is None and fact.count != 1:
        raise ZarrUnsupported(
            f"a tile for {name!r} has {fact.count} bands and no band dimension",
            fix="write one band per tile (rst_separatebands, or rst_band), or read the "
            "tiles with band_dim",
        )
    fixed, bands = {}, [None]
    for d in t.others:
        if d in t.maps:
            key = tuple(fact.dims[d])
            if key not in t.maps[d]:
                raise ZarrError(
                    f"a tile has the {d!r} value {_show(key)}, which the plan did not see",
                    fix="materialize the DataFrame first (write it to Delta, then pass "
                    "spark.table(...))",
                )
            fixed[d] = t.maps[d][key]
        elif d == fact.band_dim:
            bands = list(fact.bands)
        elif d in fact.slices and t.existing:
            fixed[d] = fact.slices[d]
        else:
            raise ZarrError(
                f"a tile for {name!r} has no position along {d!r}"
                + (
                    ""
                    if d not in fact.slices
                    else f" (its own {d!r} index is a position in the store it was read from)"
                ),
                fix=_DIM_COLS_FIX,
            )
    y0 = t.height - (r0 + h) if t.flip_y else r0
    x0 = t.width - (c0 + w) if t.flip_x else c0
    out = []
    for i, band in enumerate(bands):
        start, stop = [], []
        for pos, d in enumerate(t.dims):
            if pos == t.y:
                a, n = y0, h
            elif pos == t.x:
                a, n = x0, w
            elif d == fact.band_dim:
                a, n = band, 1
            else:
                a, n = fixed[d], 1
            start.append(int(a))
            stop.append(int(a + n))
        out.append((i, tuple(start), tuple(stop)))
    return out


# --- the data pass ---------------------------------------------------------------------------


def _plane_values(arr: np.ndarray, nodata, t: Target) -> np.ndarray:
    """A band as the writer should get it: in a new store the tile's own values; in an
    existing array decoded values, NaN where the tile has NoData (the writer's CF encoding
    then casts them and puts the array's ``_FillValue`` there)."""
    if t.stored:
        return arr
    if nodata is None:
        return arr
    if arr.dtype.kind == "f":
        if math.isnan(nodata):
            return arr
        return np.where(arr == arr.dtype.type(nodata), np.nan, arr).astype(arr.dtype)
    if not (arr == nodata).any():
        return arr
    wide = np.promote_types(arr.dtype, np.float32)
    out = arr.astype(wide)
    out[arr == nodata] = np.nan
    return out


def _block_rows(fact: Fact, arr: np.ndarray, t: Target) -> list:
    rows = []
    for i, start, stop in place(fact, t):
        plane = _plane_values(arr[i], fact.nodata, t)
        if t.flip_y:
            plane = plane[::-1, :]
        if t.flip_x:
            plane = plane[:, ::-1]
        if t.y > t.x:
            plane = plane.T
        shape = tuple(b - a for a, b in zip(start, stop))
        values = np.ascontiguousarray(plane).reshape(shape)
        rows.append(
            {
                "variable": t.name,
                "dims": list(t.dims),
                "start": list(start),
                "stop": list(stop),
                "coord_start": None,
                "coord_stop": None,
                "dtype": values.dtype.str,
                "shape": list(shape),
                "data": values.tobytes(),
                "fill_value": None,
                "attrs": t.attrs,
            }
        )
    return rows


def blocks_function(cfg: TileConfig):
    """Tiles to blocks: each tile is decoded and cut to the boxes its plan gave it."""
    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW

    def run(batches):
        for tile, dims in _batches_of_tiles(batches, cfg):
            vt = _virtual(tile)
            with _opened(vt, header_only=False) as ds:
                fact = _fact_of(ds, vt, dims)
                arr = ds.read()
            name = fact.variable or cfg.default
            rows = _block_rows(fact, arr, cfg.targets[name])
            yield pa.RecordBatch.from_pylist(rows, schema=_BLOCKS_ARROW)

    return run


# --- planning on the driver ----------------------------------------------------------------


@dataclass
class StoreView:
    """An existing store as the plan needs it: its arrays and decoded coordinates."""

    arrays: dict  # name -> ArrayMeta
    labels: dict  # dimension -> decoded coordinate values
    docs: object  # _write._Docs (documents), for the grid mapping


@dataclass
class Prepared:
    frame: object  # the data blocks (a DataFrame)
    coords: list  # coordinate blocks the driver made (rows of the blocks schema)
    seen: (
        pa.Table | None
    )  # the positions of every block: what a create or append plans from
    names: list  # the variables written
    known: dict  # coordinate values the driver holds: name -> (values, start, attrs, dtype)
    append_dim: str | None


def _collect_facts(df, tile_col, dim_cols, cfg: TileConfig) -> list:
    from pyspark.sql import functions as F

    cols = [F.col(tile_col).alias("tile")] + [
        F.col(col).alias(f"d{i}") for i, col in enumerate(dim_cols.values())
    ]
    out = df.select(*cols).mapInArrow(facts_function(cfg), "facts STRING")
    table = out.toArrow()
    facts = [Fact(**json.loads(s)) for s in table.column("facts").to_pylist()]
    if not facts:
        raise ValueError(
            "the DataFrame has no tiles to write (none, or all null or of other "
            "variables). Fix: pass the tiles to write."
        )
    return facts


def _resolve_variables(facts: list, names, mode: str) -> str | None:
    """The variable of the tiles without provenance (and the check that they have one)."""
    default = None
    if names is not None and len(names) == 1:
        default = names[0]
    elif names is None and mode == "create":
        default = _DEFAULT_VARIABLE
    bare = [f for f in facts if f.variable is None]
    if bare and default is None:
        raise ValueError(
            "tiles without Zarr provenance are written as one variable, and "
            + (
                f"variables names {len(names)}"
                if names is not None
                else "variables is not given"
            )
            + ". Fix: pass variables='<name>' (a single name) with these tiles."
        )
    return default


def _sorted_keys(dim: str, facts: list) -> list:
    keys = {tuple(f.dims[dim]) for f in facts}
    if len({k[0] for k in keys}) > 1:
        raise ZarrError(
            f"the dim_cols column for {dim!r} mixes times and numbers",
            fix="give the column one type",
        )
    return sorted(keys, key=lambda k: k[1])


def _label_array(keys: list) -> np.ndarray:
    kind = keys[0][0]
    values = [k[1] for k in keys]
    if kind == "M":
        return np.array(values, dtype="int64").view("datetime64[ns]")
    return np.array(values, dtype="int64" if kind == "i" else "float64")


def _find_label(labels: np.ndarray, key) -> int | None:
    kind, value = key
    labels = np.asarray(labels)
    if kind == "M":
        if labels.dtype.kind != "M":
            return None
        ns = labels.astype("datetime64[ns]")
        hits = np.flatnonzero((ns.astype("int64") == value) & ~np.isnat(ns))
    elif labels.dtype.kind in "iu" and kind == "i":
        hits = np.flatnonzero(labels == value)
    elif labels.dtype.kind in "iuf":
        v = float(value)
        hits = np.flatnonzero(
            np.isclose(
                labels.astype("float64"), v, rtol=1e-9, atol=1e-12 * max(1, abs(v))
            )
        )
    else:
        return None
    return int(hits[0]) if hits.size else None


def _match_labels(dim: str, keys: list, labels) -> dict:
    """Index in the store's ``dim`` coordinate of each key; every one must be a label."""
    if labels is None:
        raise ZarrError(
            f"the store has no coordinate array for {dim!r}, so tiles cannot be placed by "
            f"their {dim!r} value",
            fix=f"write the {dim!r} coordinate first, or use the tiles' own sliceDims "
            "(tiles read from this store)",
        )
    out = {}
    for key in keys:
        at = _find_label(labels, key)
        if at is None:
            raise ZarrGridMismatch(
                f"no {dim!r} label of the store equals {_show(key)}",
                fix=f"use mode='append' to add new {dim!r} steps, or write values the "
                "store has",
            )
        out[key] = at
    return out


def _array_target(meta, store: StoreView, maps_for) -> Target:
    """The plane grid of an existing array (``plan_tiles``' rules) and its dim_cols maps."""
    from databricks.labs.gbx.zarr import _tiles as zt
    from databricks.labs.gbx.zarr._read import _grid_mapping_attrs

    coord_attrs = {d: store.arrays[d].attrs for d in meta.dims if d in store.arrays}
    y, x = zt._spatial_dims(meta, coord_attrs)
    ydim, xdim = meta.dims[y], meta.dims[x]
    for d in (ydim, xdim):
        if d not in store.labels:
            raise ZarrUnsupported(
                f"dimension {d!r} of array {meta.name!r} has no coordinate array, so its "
                "cells have no position",
                fix="add a coordinate array for it, or write blocks",
            )
    ly, lx = np.asarray(store.labels[ydim]), np.asarray(store.labels[xdim])
    gt = zt.affine_of(ly, lx)
    if gt is None:
        raise ZarrUnsupported(
            f"the {ydim!r} and {xdim!r} coordinates of array {meta.name!r} are not a "
            "regular grid (a constant step along each, at least two labels)",
            fix="write blocks to an irregular grid",
        )
    geographic = zt._is_geographic(ydim, coord_attrs.get(ydim)) and zt._is_geographic(
        xdim, coord_attrs.get(xdim)
    )
    crs = zt._grid_crs(meta, _grid_mapping_attrs(store.docs, meta), geographic)
    slack = 0.0
    for lab, step in ((ly, gt[5]), (lx, gt[1])):
        if lab.dtype.kind == "f":
            eps = float(np.finfo(lab.dtype).eps)
            slack = max(slack, 4.0 * eps * float(np.abs(lab).max()) / abs(step))
    return Target(
        name=meta.name,
        dims=tuple(meta.dims),
        y=y,
        x=x,
        gt=gt,
        crs=crs,
        height=int(meta.shape[y]),
        width=int(meta.shape[x]),
        flip_y=bool(ly[0] < ly[-1]),
        flip_x=bool(lx[0] > lx[-1]),
        slack=slack,
        maps=maps_for(meta),
        existing=True,
    )


def _existing_targets(facts, names_of, store, dim_cols, mode, append_dim):
    """``(targets, append_dim, known, new_keys)`` for tiles written into existing arrays."""
    keys = {dim: _sorted_keys(dim, facts) for dim in dim_cols}
    known: dict = {}
    fixed: dict = {}
    if mode == "append":
        append_dim = _append_dim_of(dim_cols, store, append_dim)
        new = keys[append_dim]
        fixed[append_dim] = {k: i for i, k in enumerate(new)}
        values = _label_array(new)
        known[append_dim] = (values, 0, {}, values.dtype)
    targets: dict = {}
    for name in sorted(set(names_of)):
        meta = store.arrays.get(name)
        if meta is None:
            raise ZarrError(
                f"the store has no array {name!r}, and tiles do not create variables in an "
                "existing store",
                fix="create a new store for them (mode='create'), or write to an array "
                "the store has",
            )
        if name in store.docs.unsupported:
            raise ZarrUnsupported(
                f"array {name!r} cannot be decoded, so it cannot be written: "
                f"{store.docs.unsupported[name]}",
                fix="write the other variables",
            )

        def maps_for(meta, name=name):
            maps = {}
            for dim in dim_cols:
                if dim not in meta.dims:
                    raise ValueError(
                        f"dim_cols names {dim!r}, which is not a dimension of {name!r} "
                        f"(it has {list(meta.dims)}). Fix: name a dimension of the array."
                    )
                maps[dim] = (
                    fixed[dim]
                    if dim in fixed
                    else _match_labels(dim, keys[dim], store.labels.get(dim))
                )
            return maps

        targets[name] = _array_target(meta, store, maps_for)
    return targets, append_dim, known


def _append_dim_of(dim_cols: dict, store: StoreView, append_dim) -> str:
    from databricks.labs.gbx.zarr._meta import is_time_coord

    if append_dim is not None:
        if append_dim not in dim_cols:
            raise ValueError(
                f"append_dim {append_dim!r} has no column in dim_cols, so the tiles carry "
                f"no {append_dim!r} labels. Fix: pass dim_cols={{{append_dim!r}: '<column>'}}."
            )
        return append_dim
    if not dim_cols:
        raise ValueError(
            "appending tiles needs dim_cols for the dimension they extend. "
            f"Fix: {_DIM_COLS_FIX}."
        )
    for dim in dim_cols:
        meta = store.arrays.get(dim)
        if meta is not None and is_time_coord(meta):
            return dim
    if len(dim_cols) == 1:
        return next(iter(dim_cols))
    raise ValueError(
        f"append needs append_dim: none of the dim_cols dimensions {list(dim_cols)} is "
        "labelled by CF times. Fix: pass append_dim='<dimension>'."
    )


def _create_target_attrs(facts: list, mapping: str | None) -> tuple[str | None, bool]:
    """``(JSON attributes, stored)`` of the blocks of a new variable: the NoData as the
    array's ``_FillValue``, and its grid mapping. The tiles of one variable must agree on
    type and NoData."""
    first = facts[0]
    for f in facts:
        if f.dtype != first.dtype or not _nodata_same(f.nodata, first.nodata):
            raise ZarrGridMismatch(
                f"the tiles disagree on their type or NoData ({first.dtype}, "
                f"{first.nodata} against {f.dtype}, {f.nodata})",
                fix="give every tile the same dtype and NoData (rst_updatetype, "
                "rst_setnodata)",
            )
    nodata, attrs = first.nodata, {}
    if mapping is not None:
        attrs["grid_mapping"] = mapping
    stored = nodata is not None and not math.isnan(nodata)
    if stored:
        kind = np.dtype(first.dtype).kind
        attrs["_FillValue"] = int(nodata) if kind in "iu" else float(nodata)
    return (json.dumps(attrs) if attrs else None), stored


def _nodata_same(a, b) -> bool:
    return (a is None and b is None) or (
        a is not None
        and b is not None
        and (a == b or (math.isnan(a) and math.isnan(b)))
    )


def _create_targets(facts, names_of, dim_cols):
    """``(targets, coordinate blocks)`` for a new store: the grid is the first tile's,
    extended to the extent of all tiles; each dim_cols dimension's labels are the sorted
    distinct values of its column."""
    from databricks.labs.gbx.core.crs import resolve_crs

    ref = facts[0]
    for f in facts:
        if f.band_dim is not None:
            raise ZarrUnsupported(
                "tiles read with band_dim stack a dimension that a new store cannot "
                "derive",
                fix="write them into the store they were read from, or read them without "
                "band_dim and pass dim_cols",
            )
    # the reference grid: the first tile's origin, the cell, and the CRS
    rg = ref.gt
    if ref.crs is None:
        raise ZarrGridMismatch(
            "a tile has no CRS, so the new store's grid has none",
            fix="set the tiles' CRS (rst_setsrid) first",
        )
    probe = Target(
        name="",
        dims=(),
        y=0,
        x=0,
        gt=tuple(rg),
        crs=ref.crs,
        height=1 << 40,
        width=1 << 40,
        flip_y=False,
        flip_x=False,
        slack=0.0,
        maps={},
        existing=False,
    )
    west, north, east, south = math.inf, -math.inf, -math.inf, math.inf
    for f in facts:
        _check_on_grid(f, probe)
        west = min(west, f.gt[0])
        north = max(north, f.gt[3])
        east = max(east, f.gt[0] + f.width * f.gt[1])
        south = min(south, f.gt[3] + f.height * f.gt[5])
    cx, cy = rg[1], -rg[5]
    width, height = round((east - west) / cx), round((north - south) / cy)
    gt = (west, rg[1], 0.0, north, 0.0, rg[5])
    geographic = bool(resolve_crs(ref.crs).is_geographic)
    (ydim, yattrs), (xdim, xattrs) = (_LAT, _LON) if geographic else (_Y, _X)
    taken = {ydim, xdim} & set(dim_cols)
    if taken:
        raise ValueError(
            f"dim_cols names {sorted(taken)}, which the new store uses for the tiles' "
            "plane. Fix: name a non-spatial dimension (time, level, ...)."
        )
    keys = {dim: _sorted_keys(dim, facts) for dim in dim_cols}
    maps = {dim: {k: i for i, k in enumerate(ks)} for dim, ks in keys.items()}
    dims = (*dim_cols, ydim, xdim)
    # the reader takes a latitude/longitude grid to be EPSG:4326: any other CRS is carried
    # by a CF grid-mapping variable the data variables name
    mapped = not (geographic and _same_crs(ref.crs, "EPSG:4326"))
    coords = []
    for dim, ks in keys.items():
        coords.append(_coord_block(dim, _label_array(ks), None))
    coords.append(_coord_block(ydim, gt[3] + (np.arange(height) + 0.5) * gt[5], yattrs))
    coords.append(_coord_block(xdim, gt[0] + (np.arange(width) + 0.5) * gt[1], xattrs))
    if mapped:
        wkt = resolve_crs(ref.crs).to_wkt()
        coords.append(
            _scalar_block(
                _MAPPING, {"crs_wkt": wkt, "spatial_ref": wkt, "crs": ref.crs}
            )
        )
    targets = {}
    for name in sorted(set(names_of)):
        mine = [f for f, n in zip(facts, names_of) if n == name]
        attrs, stored = _create_target_attrs(mine, _MAPPING if mapped else None)
        targets[name] = Target(
            name=name,
            dims=dims,
            y=len(dims) - 2,
            x=len(dims) - 1,
            gt=gt,
            crs=ref.crs,
            height=int(height),
            width=int(width),
            flip_y=False,
            flip_x=False,
            slack=0.0,
            maps=maps,
            existing=False,
            attrs=attrs,
            stored=stored,
        )
    return targets, coords


def _check_on_grid(f: Fact, probe: Target) -> None:
    """A new store's tiles share one CRS, one cell size and one set of cell edges."""
    cx, cy = probe.gt[1], -probe.gt[5]
    if f.crs is None or not _same_crs(f.crs, probe.crs):
        raise ZarrGridMismatch(
            f"the tiles are in different CRSs ({probe.crs} and {f.crs or 'none'})",
            fix="reproject them to one CRS (rst_transform)",
        )
    if f.gt[2] or f.gt[4] or f.gt[1] <= 0 or f.gt[5] >= 0:
        raise ZarrGridMismatch(
            "a tile is rotated or not north-up",
            fix="warp the tiles north-up first (rst_transform)",
        )
    if abs(f.gt[1] - cx) > _PIXEL * cx or abs(-f.gt[5] - cy) > _PIXEL * cy:
        raise ZarrGridMismatch(
            f"the tiles have different cell sizes ({cx} x {cy} and {f.gt[1]} x {-f.gt[5]})",
            fix="resample them to one resolution",
        )
    col = (f.gt[0] - probe.gt[0]) / cx
    row = (f.gt[3] - probe.gt[3]) / probe.gt[5]
    if abs(col - round(col)) > _PIXEL or abs(row - round(row)) > _PIXEL:
        raise ZarrGridMismatch(
            "the tiles are not aligned to one grid (an origin lies "
            f"{col - math.floor(col):.6f} / {row - math.floor(row):.6f} of a cell off)",
            fix="shift or resample them onto common cell edges",
        )


def _coord_block(name: str, values: np.ndarray, attrs: dict | None) -> dict:
    values = np.ascontiguousarray(values)
    return {
        "variable": name,
        "dims": [name],
        "start": [0],
        "stop": [int(values.size)],
        "coord_start": None,
        "coord_stop": None,
        "dtype": values.dtype.str,
        "shape": [int(values.size)],
        "data": values.tobytes(),
        "fill_value": None,
        "attrs": json.dumps(attrs) if attrs else None,
    }


def _scalar_block(name: str, attrs: dict) -> dict:
    """A scalar variable (CF grid mapping) as one block; its value is not used."""
    return {
        "variable": name,
        "dims": [],
        "start": [],
        "stop": [],
        "coord_start": None,
        "coord_stop": None,
        "dtype": "<i8",
        "shape": [],
        "data": np.zeros(1, dtype="<i8").tobytes(),
        "fill_value": None,
        "attrs": json.dumps(attrs),
    }


def _seen_table(facts, names_of, targets, coords) -> pa.Table:
    """The position of every block the data pass will produce (and of the coordinate
    blocks): what ``create`` and ``append`` plan from, without reading the tiles again.
    """
    rows, seen_at = [], set()
    for f, name in zip(facts, names_of):
        t = targets[name]
        for _, start, stop in place(f, t):
            if (name, start) in seen_at:
                raise ZarrError(
                    f"two tiles are placed at the same position of {name!r} ({start})",
                    fix="give the tiles different dim_cols values, or drop the duplicates",
                )
            seen_at.add((name, start))
            rows.append(
                {
                    "variable": name,
                    "dims": list(t.dims),
                    "start": list(start),
                    "stop": list(stop),
                    "dtype": np.dtype(f.dtype).str,
                    "shape": [b - a for a, b in zip(start, stop)],
                    "attrs": t.attrs,
                    "fill_value": None,
                }
            )
    for c in coords:
        rows.append({k: c[k] for k in _SEEN_ARROW.names})
    return pa.Table.from_pylist(rows, schema=_SEEN_ARROW)


@dataclass
class Plan:
    """A tile write, planned from the tiles' facts."""

    targets: dict  # variable -> Target
    default: str | None  # the variable of tiles without provenance
    coords: list  # coordinate blocks the driver made
    seen: pa.Table  # the position of every block
    names: list  # the variables written
    known: dict
    append_dim: str | None


def plan_write(
    facts: list, *, mode: str, dim_cols: dict, names, append_dim, store
) -> Plan:
    """Place every tile (raising for one that does not fit) and make the coordinates."""
    default = _resolve_variables(facts, names, mode)
    names_of = [f.variable or default for f in facts]
    if store is None:
        for f in facts:
            for d in f.slices:
                if d not in dim_cols:
                    raise ZarrError(
                        f"a tile read from a Zarr store has a {d!r} position that a new "
                        "store has no label for",
                        fix=_DIM_COLS_FIX,
                    )
        targets, coords = _create_targets(facts, names_of, dim_cols)
        known: dict = {}
        used_append = None
    else:
        targets, used_append, known = _existing_targets(
            facts, names_of, store, dim_cols, mode, append_dim
        )
        coords = []
        if mode == "append":
            coords.append(_coord_block(used_append, known[used_append][0], None))
    # every tile is checked against its target here, on the driver, before any file is written
    seen = _seen_table(facts, names_of, targets, coords)
    return Plan(
        targets=targets,
        default=default,
        coords=coords,
        seen=seen,
        names=sorted(set(names_of)),
        known=known,
        append_dim=used_append,
    )


def prepare(
    df,
    tile_col: str,
    *,
    mode: str,
    dim_cols: dict,
    names,
    append_dim,
    store: StoreView | None,
    schema,
) -> Prepared:
    """Plan a tile write and make its blocks.

    ``names`` are the variables wanted (a list, or ``None`` for all), ``store`` the
    existing store (``None`` for ``create``), ``schema`` the blocks ``StructType`` of the
    ``mapInArrow`` stage.
    """
    from pyspark.sql import functions as F

    cfg = TileConfig(
        dims=tuple(dim_cols), names=None if names is None else tuple(names)
    )
    facts = _collect_facts(df, tile_col, dim_cols, cfg)
    plan = plan_write(
        facts,
        mode=mode,
        dim_cols=dim_cols,
        names=names,
        append_dim=append_dim,
        store=store,
    )
    run_cfg = TileConfig(
        dims=cfg.dims, names=cfg.names, default=plan.default, targets=plan.targets
    )
    cols = [F.col(tile_col).alias("tile")] + [
        F.col(col).alias(f"d{i}") for i, col in enumerate(dim_cols.values())
    ]
    frame = df.select(*cols).mapInArrow(blocks_function(run_cfg), schema)
    return Prepared(
        frame=frame,
        coords=plan.coords,
        seen=plan.seen,
        names=plan.names,
        known=plan.known,
        append_dim=plan.append_dim,
    )
