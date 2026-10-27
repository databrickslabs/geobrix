"""GeoBrix tiles backed by Zarr chunks: ``gbx_zarr_read(..., output="tiles")``.

A Zarr tile is a standard v2 tile struct that is *virtual*: ``raster`` is null, ``path`` is
the store and ``window`` is a pixel window of the variable's 2-D (y, x) plane. Everything
needed to read it travels in ``metadata`` (see ``tile_rows``), so a table of tiles can be
persisted and read back by tasks that have no session and no manifest:

- ``zarr_meta`` is the array's metadata document (cut to what decoding needs) and how it is
  read; ``zarr_refs`` the storage objects each slice of the tile needs (file, offset,
  length), taken from the manifest at the pinned version; ``zarr_root`` where the files are.
- ``open_tile`` (pyrx) calls ``zarr_tile_dataset_bytes`` for a tile that ``is_zarr_tile``:
  the window is read through the store, CF-decoded and written as in-memory GeoTIFF bytes,
  and the pending instructions of the tile (warp, clip, band, NoData, SRID) apply to those
  bytes exactly as they do to a window of a GeoTIFF. ``open_header`` answers from
  ``zarr_tile_header`` and never reads a chunk.

The grid is the 2-D plane of the variable's latitude/longitude (or ``y``/``x``) coordinate
arrays, which must be regular (``affine_of``). A tile is north-up: when latitude ascends the
rows are flipped, so row 0 is always the northernmost one and the geotransform's row step is
negative. Windows are one inner chunk wide by default and never cross the longitude seam
(each range of a wrapped ``sel`` is tiled on its own).

This module imports nothing heavy at load (``is_zarr_tile`` is called from the tile
chokepoint of every pyrx function); rasterio, zarr and the engines load when a tile is read.
"""

from __future__ import annotations

import functools
import itertools
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import numpy as np

from databricks.labs.gbx.zarr.errors import (
    ZarrAccessUnavailable,
    ZarrError,
    ZarrUnsupported,
)

__all__ = [
    "ZARR_FORMAT",
    "TilePlan",
    "affine_of",
    "check_tile_args",
    "is_zarr_tile",
    "plan_tiles",
    "source_of",
    "tile_rows",
    "zarr_tile_dataset_bytes",
    "zarr_tile_header",
    "zarr_tile_header_view",
]

ZARR_FORMAT = "zarr"  # tile.metadata["format"]
ZARR_DRIVER = "Zarr"  # tile.metadata["driver"]
_CELLID = -1  # an un-tessellated tile (ds._encode.CELLID_FRESH)
_IRREGULAR_FIX = (
    'use output="blocks" or output="table" for irregular or curvilinear grids'
)
_MOUNT_FIX = "materialize the tiles, or keep the store on a Volume"
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:/")
_CLASSIC_CAP = 256 * 1024**2  # materialize_decision's classic cap, for a tile with none
_INDEX_RUN = (
    2  # a band stack is read as one box when it spans at most this many x its bands
)

# attributes a tile keeps of its array: what CF decoding consumes, and what describes it
_TILE_ATTRS = (
    "_FillValue",
    "missing_value",
    "scale_factor",
    "add_offset",
    "_Unsigned",
    "dtype",
    "units",
    "long_name",
    "standard_name",
    "grid_mapping",
)
_TAG_ATTRS = ("units", "long_name", "standard_name")
_LAT_NAMES = frozenset({"lat", "latitude", "y"})
_LAT_UNITS = frozenset(
    {"degrees_north", "degree_north", "degrees_n", "degree_n", "degreesn", "degreen"}
)
_LAT_STANDARD = frozenset({"latitude", "projection_y_coordinate"})
_LON_STANDARD = frozenset({"longitude", "projection_x_coordinate"})
_GEOGRAPHIC_NAMES = frozenset({"lat", "latitude", "lon", "longitude"})
_ROW_FIELDS = ("kind", "key", "file", "offset", "length", "origin")


# --- recognizing a tile ----------------------------------------------------------------


def _get(tile, name):
    """Field ``name`` of a ``VirtualTile``, a Row or a dict."""
    if isinstance(tile, Mapping):
        return tile.get(name)
    try:
        return getattr(tile, name)
    except AttributeError:
        return tile[name]


def is_zarr_tile(tile) -> bool:
    """Whether ``tile`` is a virtual tile read from Zarr chunks: its ``format`` is ``zarr``
    and it holds no raster bytes (a materialized Zarr tile is an ordinary GeoTIFF tile).
    """
    try:
        if _get(tile, "raster") is not None:
            return False
        metadata = _get(tile, "metadata")
    except (AttributeError, KeyError, TypeError, IndexError):
        return False
    return bool(metadata) and metadata.get("format") == ZARR_FORMAT


# --- the grid --------------------------------------------------------------------------


def _axis_of(coords, rtol: float) -> tuple[float, float, float] | None:
    """``(lowest label, highest label, cell size)`` of a regular axis, else ``None``."""
    arr = np.asarray(coords)
    if arr.ndim != 1 or arr.size < 2 or arr.dtype.kind not in "iuf":
        return None
    eps = float(np.finfo(arr.dtype).eps) if arr.dtype.kind == "f" else 0.0
    v = arr.astype(np.float64)
    if not np.isfinite(v).all():
        return None
    step = (v[-1] - v[0]) / (v.size - 1)
    if step == 0.0:
        return None
    slack = max(rtol * abs(step), 4.0 * eps * float(np.abs(v).max()))
    if not (np.abs(np.diff(v) - step) <= slack).all():
        return None
    return float(min(v[0], v[-1])), float(max(v[0], v[-1])), abs(step)


def affine_of(coords_y, coords_x, *, rtol: float = 1e-6) -> tuple[float, ...] | None:
    """The north-up geotransform of a regular grid, or ``None`` if it is not one.

    ``coords_y`` and ``coords_x`` are the cell-centre labels of the two axes, each
    monotonic with a constant step (to ``rtol`` of the step, plus the rounding error of the
    label dtype: float32 labels of a 0.1 degree grid differ from their mean step by about
    1e-5). The cell size is the step and the origin is the first label minus half a step,
    taken at the north-west corner whichever way an axis runs: the result is GDAL's
    ``(x_origin, x_size, 0, y_origin, 0, -y_size)``, so its sixth number is negative. An
    axis with fewer than two labels has no cell size and gives ``None``.
    """
    ys, xs = _axis_of(coords_y, rtol), _axis_of(coords_x, rtol)
    if ys is None or xs is None:
        return None
    (_, y_hi, y_step), (x_lo, _, x_step) = ys, xs
    gt = (x_lo - x_step / 2.0, x_step, 0.0, y_hi + y_step / 2.0, 0.0, -y_step)
    return tuple(float(v) for v in gt)


def _is_lat_like(dim: str, attrs: dict | None) -> bool:
    if dim.lower() in _LAT_NAMES:
        return True
    attrs = attrs or {}
    return (
        str(attrs.get("units", "")).lower() in _LAT_UNITS
        or str(attrs.get("standard_name", "")).lower() in _LAT_STANDARD
        or str(attrs.get("axis", "")).upper() == "Y"
    )


def _is_lon_like(dim: str, attrs: dict | None) -> bool:
    from databricks.labs.gbx.zarr._select import _is_lon_like as lon_like

    return (
        lon_like(dim, attrs)
        or str((attrs or {}).get("standard_name", "")).lower() in _LON_STANDARD
    )


def _is_geographic(dim: str, attrs: dict | None) -> bool:
    """Whether an axis is latitude or longitude in degrees, not a projected coordinate."""
    attrs = attrs or {}
    return (
        dim.lower() in _GEOGRAPHIC_NAMES
        or str(attrs.get("units", "")).lower().startswith("degree")
        or str(attrs.get("standard_name", "")).lower() in _GEOGRAPHIC_NAMES
    )


def _spatial_dims(meta, coord_attrs: dict) -> tuple[int, int]:
    """Positions of the y and x dimensions of the array, from their names and attributes."""
    ys = [i for i, d in enumerate(meta.dims) if _is_lat_like(d, coord_attrs.get(d))]
    xs = [i for i, d in enumerate(meta.dims) if _is_lon_like(d, coord_attrs.get(d))]
    if len(ys) != 1 or len(xs) != 1:
        raise ZarrUnsupported(
            f"array {meta.name!r} has dimensions {list(meta.dims)}, from which GeoBrix "
            "cannot tell the latitude/y axis from the longitude/x axis",
            fix="name them latitude/longitude or y/x (or give the coordinates a "
            'standard_name, units or axis attribute), or use output="blocks"',
        )
    return ys[0], xs[0]


def _mapping_crs(attrs: dict) -> str | None:
    """The CRS a CF ``grid_mapping`` variable carries, as text GeoBrix resolves, or ``None``."""
    for key in ("crs_canonical", "crs_wkt", "spatial_ref"):
        value = attrs.get(key)
        if isinstance(value, str) and value.strip():
            return value
    for key in ("epsg_code", "spatial_epsg"):
        found = re.search(r"(\d+)\s*$", str(attrs.get(key, "")))
        if found:
            return f"EPSG:{int(found.group(1))}"
    return None


def _grid_crs(meta, mapping: dict | None, geographic: bool) -> str:
    """The canonical CRS of the grid: its ``grid_mapping``, else EPSG:4326 for a regular
    latitude/longitude grid."""
    from databricks.labs.gbx.core.crs import crs_to_canonical, resolve_crs

    if mapping is not None:
        text = _mapping_crs(mapping)
        if text is None:
            raise ZarrUnsupported(
                f"the grid_mapping of array {meta.name!r} names no CRS GeoBrix can read "
                "(it has no crs_wkt, spatial_ref or epsg_code)",
                fix='add a crs_wkt or an epsg_code attribute to it, or use output="blocks"',
            )
    elif geographic:
        text = "EPSG:4326"
    else:
        raise ZarrUnsupported(
            f"array {meta.name!r} has projected x/y axes and no grid_mapping, so its tiles "
            "would have no CRS",
            fix="add a grid_mapping variable with a crs_wkt or an epsg_code, or use "
            'output="blocks"',
        )
    try:
        return crs_to_canonical(resolve_crs(text))
    except (
        Exception
    ) as e:  # noqa: BLE001 - whatever the resolver raises is the same refusal
        raise ZarrUnsupported(
            f"the CRS of array {meta.name!r} cannot be resolved ({type(e).__name__}: {e})",
            fix='correct its grid_mapping, or use output="blocks"',
        ) from None


# --- the array spec: what a task needs to decode ----------------------------------------


@dataclass(frozen=True)
class _Spec:
    """The ``zarr_meta`` of a tile, parsed."""

    meta: object  # ArrayMeta
    decode: bool
    flag: bool | None  # use_zarr_fill_value_as_mask
    dtype: str  # the dtype of the tile
    nodata: float | None
    y: int  # positions of the y and x dimensions among the array's
    x: int
    flip_y: bool  # the axis runs south to north / east to west in the array
    flip_x: bool
    cap: int  # the largest tile (decoded bytes) a task may hold


def _tile_doc(meta) -> dict:
    """The array's metadata document with its attributes cut to ``_TILE_ATTRS``."""

    def keep(attrs):
        return {k: v for k, v in attrs.items() if k in _TILE_ATTRS}

    if meta.zarr_format == 3:
        doc = dict(meta.doc)
        doc["attributes"] = keep(doc.get("attributes") or {})
        return {"fmt": 3, "doc": doc}
    zattrs = meta.doc.get(".zattrs") or {}
    kept = keep(zattrs)
    if "_ARRAY_DIMENSIONS" in zattrs:  # the dimension names
        kept["_ARRAY_DIMENSIONS"] = zattrs["_ARRAY_DIMENSIONS"]
    return {"fmt": 2, "zarray": meta.doc[".zarray"], "zattrs": kept}


def _gtiff_dtype(dtype: np.dtype, name: str) -> str:
    """The numpy dtype name a GeoTIFF holds ``dtype`` as."""
    if dtype.kind == "b":
        return "uint8"
    if dtype.kind == "f" and dtype.itemsize == 2:
        return "float32"
    if dtype.kind in "iufc":
        return dtype.newbyteorder("=").name
    raise ZarrUnsupported(
        f"array {name!r} holds {dtype} values, which a GeoTIFF band cannot",
        fix='use output="blocks" or output="table", or leave it out of variables',
    )


def _decoded_dtype(meta, decode: bool, flag) -> np.dtype:
    """The dtype CF decoding gives the array's cells, probed on one cell."""
    from databricks.labs.gbx.zarr._meta import cf_decode

    stored = np.dtype(meta.dtype).newbyteorder("=")
    probe = np.zeros((1,) * max(1, len(meta.shape)), dtype=stored)
    if not decode:
        return probe.dtype
    return cf_decode(probe, meta, use_zarr_fill_value_as_mask=flag).dtype


def _nodata_of(meta, decode: bool, flag, dtype: str) -> float | None:
    """NaN for a decoded floating-point tile; for stored values (``decode=False``) the
    ``_FillValue``; else none."""
    np_dtype = np.dtype(dtype)
    if decode:
        return math.nan if np_dtype.kind == "f" else None
    from databricks.labs.gbx.zarr._meta import _cf_attrs

    fill = _cf_attrs(meta, flag).get("_FillValue")
    if isinstance(fill, bool) or not isinstance(fill, (int, float, np.number)):
        return None
    fill = float(fill)
    if np_dtype.kind in "iu":
        info = np.iinfo(np_dtype)
        if not (fill.is_integer() and info.min <= fill <= info.max):
            return None
    return fill


def _spec_json(meta, decode, flag, dtype, nodata, yx, flips, cap) -> str:
    body = {
        **_tile_doc(meta),
        "decode": bool(decode),
        "mask": flag,
        "dtype": dtype,
        "nodata": None if nodata is None else repr(float(nodata)),
        "yx": list(yx),
        "flip": [bool(f) for f in flips],
        "cap": cap,
    }
    return json.dumps(body, separators=(",", ":"), sort_keys=True)


@functools.lru_cache(maxsize=64)
def _spec(text: str, array: str) -> _Spec:
    """Parse ``zarr_meta`` (cached: the tiles of a task share one)."""
    from databricks.labs.gbx.zarr._meta import parse_v2, parse_v3

    body = json.loads(text)
    meta = (
        parse_v3(array, body["doc"])
        if body["fmt"] == 3
        else parse_v2(array, body["zarray"], body["zattrs"])
    )
    nodata = body["nodata"]
    return _Spec(
        meta=meta,
        decode=body["decode"],
        flag=body["mask"],
        dtype=body["dtype"],
        nodata=None if nodata is None else float(nodata),
        y=body["yx"][0],
        x=body["yx"][1],
        flip_y=body["flip"][0],
        flip_x=body["flip"][1],
        cap=body["cap"] or _CLASSIC_CAP,
    )


# --- the plan --------------------------------------------------------------------------


@dataclass(frozen=True)
class _Dim:
    """A non-spatial dimension a tile spans: its position among the array's, its name, the
    indices selected along it (in order) and their labels (``None`` without a coordinate
    array)."""

    pos: int
    name: str
    indices: tuple[int, ...]
    labels: tuple[str, ...] | None


@dataclass(frozen=True)
class TilePlan:
    """Everything ``tile_rows`` needs to make the tiles of one variable.

    It is the closure of the task that makes the tiles: a plain value that pickles, with no
    session and no manifest in it. Tile ``i`` is window ``i // n_slices`` of slice
    ``i % n_slices``, so the tiles of one window (one chunk) are neighbours. What it
    carries grows with the manifest rows of the shards the selection touches
    (``chunk_rows``, ``shard_rows``), not with the number of tiles.
    """

    array: str
    spec: str  # the zarr_meta text
    root: str  # where the manifest's gbx-origin files are (a plain store: the store)
    base_root: str  # a registered store's own files; "" when none
    table: str  # "" for a plain store
    version: str
    crs: str
    gt: tuple[float, ...]  # the geotransform of the whole plane
    width: int  # the whole plane
    height: int
    row_pieces: tuple[tuple[int, int], ...]  # (start, stop) of the windows' rows
    col_pieces: tuple[tuple[int, int], ...]  # and columns: a window is one of each
    fan: tuple[_Dim, ...]  # dimensions fanned out into tiles
    band: _Dim | None  # the dimension stacked as bands
    chunk_rows: dict  # chunk key -> manifest row (unsharded chunks, and overrides)
    shard_rows: dict  # shard key -> manifest row

    @property
    def n_windows(self) -> int:
        return len(self.row_pieces) * len(self.col_pieces)

    def window(self, i: int) -> tuple[int, int, int, int]:
        """Window ``i``, ``(col_off, row_off, width, height)``: rows outer, columns inner."""
        r, c = divmod(i, len(self.col_pieces))
        (r0, r1), (c0, c1) = self.row_pieces[r], self.col_pieces[c]
        return (c0, r0, c1 - c0, r1 - r0)

    @property
    def n_slices(self) -> int:
        return math.prod(len(d.indices) for d in self.fan)

    @property
    def n_tiles(self) -> int:
        return self.n_windows * self.n_slices

    @property
    def count(self) -> int:
        return len(self.band.indices) if self.band else 1

    @property
    def path(self) -> str:
        """The store, as the user knows it: a registered store's own root."""
        return self.base_root or self.root


def _pieces(
    rng: tuple[int, int], chunk: int, n: int, flip: bool, size: int | None
) -> list[tuple[int, int]]:
    """The windows ``(start, stop)`` along one axis of the plane that cover the data range
    ``rng`` of an axis of ``n`` cells, in plane order (``flip``: the axis runs the other way).

    By default a window is the part of one inner chunk the range covers; with ``size`` it is
    that many cells, counted from the range's plane start.
    """
    s, e = rng
    if size is None:
        cuts = [s, *range((s // chunk + 1) * chunk, e, chunk), e]
        data = zip(cuts[:-1], cuts[1:])
        return sorted((n - b, n - a) if flip else (a, b) for a, b in data)
    o0, o1 = (n - e, n - s) if flip else (s, e)
    return [(a, min(a + size, o1)) for a in range(o0, o1, size)]


def _data_range(start: int, stop: int, n: int, flip: bool) -> tuple[int, int]:
    """The range of the array's axis (of ``n`` cells) that plane cells ``[start, stop)`` are."""
    return (n - stop, n - start) if flip else (start, stop)


def _label(value) -> str:
    """A coordinate value as tile metadata spells it (as the NetCDF reader does)."""
    if isinstance(value, np.datetime64):
        return str(value)
    if isinstance(value, np.generic):
        value = value.item()
    return str(float(value)) if isinstance(value, float) else str(value)


def _fan_dim(pos: int, name: str, axis, coords: dict) -> _Dim:
    indices = tuple(i for s, e in axis.ranges for i in range(s, e))
    values = coords.get(name)
    labels = None if values is None else tuple(_label(values[i]) for i in indices)
    return _Dim(pos, name, indices, labels)


def check_tile_args(band_dim, tile_size) -> tuple[int, int] | None:
    """Validate ``band_dim`` and ``tile_size`` of a tiles read; ``tile_size`` as ``(w, h)``."""
    if band_dim is not None and not (isinstance(band_dim, str) and band_dim):
        raise ValueError(
            f"band_dim must be the name of a dimension, got {band_dim!r}. "
            "Fix: pass band_dim='time', or leave it None."
        )
    if tile_size is None:
        return None
    ok = (
        isinstance(tile_size, (tuple, list))
        and len(tile_size) == 2
        and all(
            isinstance(n, (int, np.integer)) and not isinstance(n, bool) and n > 0
            for n in tile_size
        )
    )
    if not ok:
        raise ValueError(
            f"tile_size must be (width, height) in cells, two positive integers, "
            f"got {tile_size!r}. Fix: pass tile_size=(256, 256), or leave it None for "
            "one tile per chunk."
        )
    return int(tile_size[0]), int(tile_size[1])


def plan_tiles(
    meta,
    coords,
    axes,
    rows_by_shard,
    *,
    root,
    table="",
    version="",
    tile_size=None,
    band_dim=None,
    base_root=None,
    coord_attrs=None,
    grid_mapping=None,
    decode=True,
    flag=None,
    cap_bytes=None,
) -> TilePlan:
    """The plan of the tiles of one variable.

    ``meta`` is its ``ArrayMeta``; ``coords`` the decoded coordinate values by dimension
    name (the two spatial ones are required); ``axes`` the selection along each dimension
    (``_select.resolve``); ``rows_by_shard`` the manifest rows of the shards it touches,
    keyed as ``_plan.unit_key`` (empty for a plain store); ``root`` and ``base_root`` where
    the files are; ``table`` and ``version`` the pinned manifest ("" for a plain store).
    ``tile_size`` is ``(width, height)`` in cells, or ``None`` for one inner chunk per tile;
    ``band_dim`` names a dimension to stack as bands. ``grid_mapping`` holds the attributes
    of the CF grid-mapping variable the array names, if any (the CRS comes from it; a
    latitude/longitude grid without one is EPSG:4326). ``coord_attrs`` are the attributes
    of each dimension's coordinate array. ``decode`` and ``flag`` are the CF decoding.
    ``cap_bytes`` is the largest tile a task may hold, decoded, as the driver measured it.

    Raises ``ZarrUnsupported`` for a grid that is not regular, an array with no clear
    latitude and longitude, or a CRS it cannot determine.
    """
    from databricks.labs.gbx.zarr._meta import is_time_coord

    if is_time_coord(meta):
        raise ZarrUnsupported(
            f"array {meta.name!r} holds times, which a GeoTIFF band cannot",
            fix="leave it out of variables",
        )
    coord_attrs = coord_attrs or {}
    tile_wh = check_tile_args(band_dim, tile_size)
    y, x = _spatial_dims(meta, coord_attrs)
    ydim, xdim = meta.dims[y], meta.dims[x]
    for dim in (ydim, xdim):
        if dim not in coords:
            raise ZarrUnsupported(
                f"dimension {dim!r} of array {meta.name!r} has no coordinate array that "
                "GeoBrix can read, so its cells have no position",
                fix='add a coordinate array for it, or use output="blocks"',
            )
    gt = affine_of(coords[ydim], coords[xdim])
    if gt is None:
        raise ZarrUnsupported(
            f"the {ydim!r} and {xdim!r} coordinates of array {meta.name!r} are not a "
            "regular grid (a constant step along each, at least two labels)",
            fix=_IRREGULAR_FIX,
        )
    cy, cx = np.asarray(coords[ydim]), np.asarray(coords[xdim])
    flip_y, flip_x = bool(cy[0] < cy[-1]), bool(cx[0] > cx[-1])
    height, width = int(meta.shape[y]), int(meta.shape[x])
    crs = _grid_crs(
        meta,
        grid_mapping,
        _is_geographic(ydim, coord_attrs.get(ydim))
        and _is_geographic(xdim, coord_attrs.get(xdim)),
    )

    band = None
    if band_dim is not None and band_dim in meta.dims:
        bp = meta.dims.index(band_dim)
        if bp in (y, x):
            raise ValueError(
                f"band_dim {band_dim!r} is a spatial dimension of array {meta.name!r}. "
                "Fix: stack a dimension such as time or level."
            )
        band = _fan_dim(bp, band_dim, axes[bp], coords)
    fan = tuple(
        _fan_dim(p, d, axes[p], coords)
        for p, d in enumerate(meta.dims)
        if p not in (y, x) and not (band and p == band.pos)
    )

    tw, th = tile_wh if tile_wh else (None, None)
    row_pieces = tuple(
        itertools.chain.from_iterable(
            _pieces(rng, meta.chunks[y], height, flip_y, th) for rng in axes[y].ranges
        )
    )
    col_pieces = tuple(
        itertools.chain.from_iterable(
            _pieces(rng, meta.chunks[x], width, flip_x, tw) for rng in axes[x].ranges
        )
    )

    dtype = _gtiff_dtype(_decoded_dtype(meta, decode, flag), meta.name)
    nodata = _nodata_of(meta, decode, flag, dtype)
    chunk_rows: dict = {}
    shard_rows: dict = {}
    for group in rows_by_shard.values():
        for row in group:
            slim = {k: row.get(k) for k in _ROW_FIELDS}
            (shard_rows if row["kind"] == "shard" else chunk_rows)[row["key"]] = slim
    return TilePlan(
        array=meta.name,
        spec=_spec_json(
            meta, decode, flag, dtype, nodata, (y, x), (flip_y, flip_x), cap_bytes
        ),
        root=str(root),
        base_root=str(base_root or ""),
        table=table or "",
        version=str(version or ""),
        crs=crs,
        gt=gt,
        width=width,
        height=height,
        row_pieces=row_pieces,
        col_pieces=col_pieces,
        fan=fan,
        band=band,
        chunk_rows=chunk_rows,
        shard_rows=shard_rows,
    )


# --- making the tiles ------------------------------------------------------------------


def _ref(row: dict, shard: bool, inner) -> dict:
    ref = {
        "shard": shard,
        "file": row["file"],
        "offset": row["offset"],
        "length": row["length"],
        "inner": inner,
        "key": row["key"],
    }
    if row.get("origin") == "external":  # a registered store's own file
        ref["origin"] = "external"
    return ref


def _refs_of(plan: TilePlan, meta, chunks: Iterable[tuple[int, ...]]) -> list[dict]:
    """The storage objects that hold ``chunks`` (inner chunk coordinates) of the array.

    An unsharded array: each chunk's row. A sharded one: the row of each shard the chunks
    lie in, with the indices of the inner chunks wanted, and the rows that override or
    delete one inner chunk of it. A chunk with no row was never written and reads as fill.
    """
    from databricks.labs.gbx.zarr._keys import inner_index, shard_of

    enc, out = meta.key_encoding, []
    if meta.shard is None:
        for c in chunks:
            row = plan.chunk_rows.get(enc.chunk_key(c))
            if row is not None:
                out.append(_ref(row, False, None))
        return out
    cps = meta.shard.cps
    by_shard: dict[str, list[tuple[str, int]]] = {}
    for c in chunks:
        by_shard.setdefault(enc.chunk_key(shard_of(c, cps)), []).append(
            (enc.chunk_key(c), inner_index(c, cps))
        )
    for shard_key, wanted in by_shard.items():
        row = plan.shard_rows.get(shard_key)
        if row is not None:
            out.append(_ref(row, True, sorted({i for _, i in wanted})))
        for chunk_key, i in wanted:
            override = plan.chunk_rows.get(chunk_key)
            if override is not None:
                out.append(_ref(override, False, [i]))
    return out


def _slice_refs(plan, spec, ybox, xbox, fan_at: dict, bands) -> list[list[dict]]:
    """``zarr_refs``: for each slice (band) of the tile, the objects its window needs."""
    meta, out = spec.meta, []
    for b in bands if plan.band else [None]:
        ranges = []
        for p, n in enumerate(meta.chunks):
            if p == spec.y:
                lo, hi = ybox
            elif p == spec.x:
                lo, hi = xbox
            elif plan.band is not None and p == plan.band.pos:
                lo, hi = b, b + 1
            else:
                lo, hi = fan_at[p], fan_at[p] + 1
            ranges.append(range(lo // n, (hi - 1) // n + 1))
        out.append(_refs_of(plan, meta, itertools.product(*ranges)))
    return out


def _tile_row(plan: TilePlan, spec: _Spec, window, s: int) -> dict:
    """The v2 tile row of ``window`` in slice ``s`` (an index into the fan-out)."""
    c0, r0, w, h = window
    picks, rest = [], s
    for d in reversed(plan.fan):  # the last fanned dimension varies fastest
        rest, k = divmod(rest, len(d.indices))
        picks.append((d, k))
    picks.reverse()
    fan_at = {d.pos: d.indices[k] for d, k in picks}
    ybox = _data_range(r0, r0 + h, plan.height, spec.flip_y)
    xbox = _data_range(c0, c0 + w, plan.width, spec.flip_x)
    bands = plan.band.indices if plan.band else ()
    refs = _slice_refs(plan, spec, ybox, xbox, fan_at, bands)

    meta = {
        "format": ZARR_FORMAT,
        "driver": ZARR_DRIVER,
        "sourcePath": plan.path,
        "zarr_array": plan.array,
        "zarr_meta": plan.spec,
        "zarr_refs": json.dumps(refs, separators=(",", ":"), sort_keys=True),
        "zarr_root": plan.root,
        "zarr_table": plan.table,
        "zarr_version": plan.version,
        "zarr_gt": ",".join(repr(float(v)) for v in plan.gt),
        "zarr_crs": plan.crs,
        "width": str(plan.width),
        "height": str(plan.height),
        "count": str(plan.count),
    }
    if plan.base_root:
        meta["zarr_base_root"] = plan.base_root
    if picks:
        meta["sliceDims"] = ",".join(
            f"{d.name}={d.indices[k]}"
            for d, k in sorted(picks, key=lambda p: p[0].name)
        )
        for d, k in picks:
            if d.labels is not None:
                meta[f"sliceCoord_{d.name}"] = d.labels[k]
    if plan.band:
        b = plan.band
        names = b.labels if b.labels is not None else tuple(map(str, b.indices))
        meta["bandDim"] = b.name
        meta["zarr_band_index"] = ",".join(map(str, b.indices))
        meta["bandDescriptions"] = ",".join(f"{b.name}={n}" for n in names)
        if b.labels is not None:
            meta["bandCoords"] = ",".join(b.labels)
    return {
        "cellid": _CELLID,
        "raster": None,
        "path": plan.path,
        "path_mode": None,
        "window": {"col_off": c0, "row_off": r0, "width": w, "height": h},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,  # the field is the warp target of a pending rst_transform
        "metadata": meta,
    }


def tile_rows(plan: TilePlan, ids: Iterable[int]) -> list[dict]:
    """The v2 tile rows (``VirtualTile.to_row`` dicts) of tile numbers ``ids``.

    A pure function of the plan and the ids: it reads nothing. ``tile.metadata`` holds
    ``format`` (``zarr``), ``driver``, ``zarr_array``, ``sliceDims`` (``time=12,level=3``,
    the NetCDF reader's form) and ``sliceCoord_<dim>``, ``zarr_meta``, ``zarr_refs``,
    ``zarr_root`` (and ``zarr_base_root`` for a registered store), ``zarr_table`` and
    ``zarr_version``, ``zarr_gt``, ``zarr_crs``, and ``width``, ``height`` (the whole
    plane) and ``count``. A ``band_dim`` stack adds ``bandDim``, ``bandCoords``,
    ``bandDescriptions`` and ``zarr_band_index``.
    """
    spec = _spec(plan.spec, plan.array)
    n_slices, n_tiles = plan.n_slices, plan.n_tiles
    out = []
    for i in ids:
        i = int(i)
        if not 0 <= i < n_tiles:
            raise IndexError(f"tile {i} is outside the plan's {n_tiles} tiles")
        w, s = divmod(i, n_slices)
        out.append(_tile_row(plan, spec, plan.window(w), s))
    return out


def source_of(row: dict) -> str:
    """The ``source`` column of a tile row: ``ZARR:"<store>":<array>[<sliceDims>]``."""
    meta = row["metadata"]
    dims = meta.get("sliceDims")
    return f'ZARR:"{row["path"]}":{meta["zarr_array"]}' + (f"[{dims}]" if dims else "")


# --- reading a tile --------------------------------------------------------------------


@dataclass(frozen=True)
class _Read:
    """What one tile says about itself, parsed (``zarr_meta`` is shared and cached)."""

    spec: _Spec
    array: str
    root: str
    base_root: str
    table: str
    refs: list  # per slice: [ref]
    slice_at: dict  # position of a fanned dimension -> index
    band_pos: int | None
    bands: tuple[int, ...]
    gt: tuple[float, ...]
    crs: str
    width: int  # the whole plane
    height: int
    count: int
    tags: dict


def _parse(tile) -> _Read:
    md = tile.metadata
    array = md["zarr_array"]
    spec = _spec(md["zarr_meta"], array)
    dims = spec.meta.dims
    slice_at = {}
    for part in (md.get("sliceDims") or "").split(","):
        if part:
            name, _, index = part.partition("=")
            slice_at[dims.index(name)] = int(index)
    band_pos, bands = None, ()
    if md.get("bandDim"):
        band_pos = dims.index(md["bandDim"])
        bands = tuple(int(i) for i in md["zarr_band_index"].split(","))
    tags = {"zarr_array": array}
    tags.update(
        {k: v for k, v in md.items() if k == "sliceDims" or k.startswith("sliceCoord_")}
    )
    tags.update(
        {
            k: spec.meta.attrs[k]
            for k in _TAG_ATTRS
            if isinstance(spec.meta.attrs.get(k), str)
        }
    )
    return _Read(
        spec=spec,
        array=array,
        root=md["zarr_root"],
        base_root=md.get("zarr_base_root", ""),
        table=md.get("zarr_table", ""),
        refs=json.loads(md.get("zarr_refs") or "[]"),
        slice_at=slice_at,
        band_pos=band_pos,
        bands=bands,
        gt=tuple(float(v) for v in md["zarr_gt"].split(",")),
        crs=md["zarr_crs"],
        width=int(md["width"]),
        height=int(md["height"]),
        count=int(md.get("count", 1)),
        tags=tags,
    )


def zarr_tile_header(tile) -> dict:
    """The header of the tile's 2-D plane, from its metadata alone (no chunk is read).

    ``width`` and ``height`` are those of the whole plane (the tile's ``window`` is a part of
    it), ``transform`` its geotransform (an ``Affine``), ``crs`` the canonical CRS; also
    ``count``, ``dtype`` and ``nodata`` of the tile's bands.
    """
    from affine import Affine

    rd = _parse(tile)
    return {
        "width": rd.width,
        "height": rd.height,
        "count": rd.count,
        "dtype": rd.spec.dtype,
        "crs": rd.crs,
        "transform": Affine.from_gdal(*rd.gt),
        "nodata": rd.spec.nodata,
        "driver": ZARR_DRIVER,
        "tags": dict(rd.tags),
    }


class _ZarrHeader:
    """A read-free stand-in for the open dataset of a tile's plane.

    It has the header attributes of a rasterio dataset (``width``, ``height``, ``count``,
    ``dtypes``, ``crs``, ``transform``, ``bounds``, ``nodata``, ``profile``, ``tags()``...),
    so ``open_header`` treats it as it treats a GeoTIFF it opened by path: a tile with a
    ``window`` sees the window's part of it. ``read`` is not available.
    """

    driver = ZARR_DRIVER
    subdatasets: list = []
    closed = False

    def __init__(self, header: dict):
        from rasterio.coords import BoundingBox
        from rasterio.enums import ColorInterp

        from databricks.labs.gbx.core.crs import resolve_crs

        self.width, self.height = header["width"], header["height"]
        self.count = header["count"]
        self.transform = header["transform"]
        self.crs = resolve_crs(header["crs"])
        self.nodata = header["nodata"]
        self.dtypes = (header["dtype"],) * self.count
        self.nodatavals = (self.nodata,) * self.count
        self.indexes = tuple(range(1, self.count + 1))
        self.descriptions = (None,) * self.count
        self.colorinterp = (
            (ColorInterp.undefined,) * self.count
            if self.count > 1
            else (ColorInterp.gray,)
        )
        t = self.transform
        self.bounds = BoundingBox(
            t.c, t.f + t.e * self.height, t.c + t.a * self.width, t.f
        )
        self._tags = header["tags"]

    @property
    def shape(self) -> tuple[int, int]:
        return (self.height, self.width)

    @property
    def res(self) -> tuple[float, float]:
        return (abs(self.transform.a), abs(self.transform.e))

    @property
    def profile(self) -> dict:
        return {
            "driver": self.driver,
            "dtype": self.dtypes[0],
            "nodata": self.nodata,
            "width": self.width,
            "height": self.height,
            "count": self.count,
            "crs": self.crs,
            "transform": self.transform,
        }

    def tags(self, bidx=0, **_) -> dict:
        return dict(self._tags) if not bidx else {}

    def window_transform(self, window):
        from rasterio.windows import transform

        return transform(window, self.transform)

    def read(self, *_, **__):
        raise AttributeError(
            "a Zarr tile header has no pixels; open the tile to read them"
        )

    def close(self) -> None:
        pass


def zarr_tile_header_view(tile) -> _ZarrHeader:
    """``zarr_tile_header`` as a dataset-like object (see ``_ZarrHeader``)."""
    return _ZarrHeader(zarr_tile_header(tile))


def _mounted(root: str) -> str:
    """``root`` as a path the process can open, or ``ZarrAccessUnavailable``.

    A tile reads its files through the file system (the Volume mount): ``dbfs:`` and
    ``file:`` spellings are bare paths, any other scheme (``s3://``, ``abfss://``) is not.
    """
    from databricks.labs.gbx.ds._listing import to_local_path

    local = to_local_path(root)
    if _SCHEME.match(local):
        raise ZarrAccessUnavailable(
            f"the tile's store {root} is not on a mounted file system, and tiles read "
            "their chunks through the Volume mount",
            fix=_MOUNT_FIX,
        )
    return local


def _rows_of(array: str, refs: list[list[dict]]) -> list[dict]:
    """The manifest rows (``ManifestStore`` input) the refs of all slices stand for."""
    rows: dict[tuple, dict] = {}
    for ref in itertools.chain.from_iterable(refs):
        shard = ref["shard"]
        rows[(shard, ref["key"])] = {
            "kind": "shard" if shard else "chunk",
            "array": array,
            "key": ref["key"],
            "shard_key": ref["key"] if shard else None,
            "file": ref["file"],
            "offset": ref["offset"],
            "length": ref["length"],
            "origin": ref.get("origin", "gbx"),
        }
    return list(rows.values())


def _read_box(rd: _Read, box: list[tuple[int, int]]) -> np.ndarray:
    """The stored values of ``box`` (a ``(start, stop)`` per dimension of the array)."""
    from databricks.labs.gbx.zarr._engine import (
        open_manifest_array,
        open_plain_array,
        read_box,
    )

    root = _mounted(rd.root)
    slices = tuple(slice(a, b) for a, b in box)
    if not rd.table:  # a plain store: whatever bytes are there now
        return read_box(open_plain_array(root, rd.array, engine="zarr"), slices)
    from databricks.labs.gbx.zarr._access import FuseRangeReader
    from databricks.labs.gbx.zarr._meta import doc_bytes
    from databricks.labs.gbx.zarr._store import ManifestStore

    meta = rd.spec.meta
    base = _mounted(rd.base_root) if rd.base_root else None
    store = ManifestStore(
        {f"{rd.array}/{name}": raw for name, raw in doc_bytes(meta).items()},
        _rows_of(rd.array, rd.refs),
        FuseRangeReader(root, base_root=base),
        {rd.array: meta},
    )
    try:
        return read_box(open_manifest_array(store, rd.array), slices)
    finally:
        store.close()


def _plane(block: np.ndarray, rd: _Read, rel: list[int] | None) -> np.ndarray:
    """``block`` (decoded, a box of the array) as ``(bands, rows, cols)``, north-up.

    Every dimension but the plane's and the band dimension has one cell in the box; the
    bands ``rel`` of the band dimension are kept, in order.
    """
    spec = rd.spec
    keep = [spec.y, spec.x] + ([rd.band_pos] if rd.band_pos is not None else [])
    sub = block[tuple(slice(None) if p in keep else 0 for p in range(block.ndim))]
    order = sorted(keep)
    ya, xa = order.index(spec.y), order.index(spec.x)
    if rd.band_pos is None:
        sub = np.moveaxis(sub, (ya, xa), (0, 1))[None]
    else:
        sub = np.moveaxis(sub, (order.index(rd.band_pos), ya, xa), (0, 1, 2))
        sub = sub[rel]
    if spec.flip_y:
        sub = sub[:, ::-1, :]
    if spec.flip_x:
        sub = sub[:, :, ::-1]
    return sub


def _band_groups(bands: tuple[int, ...]) -> list[tuple[int, int, list[int]]]:
    """``(start, stop, positions)`` reads that cover the band indices: one box when the
    indices are close together, else one read each."""
    if not bands:
        return [(0, 1, [0])]
    lo, hi = min(bands), max(bands) + 1
    if hi - lo <= _INDEX_RUN * len(bands):
        return [(lo, hi, [b - lo for b in bands])]
    return [(b, b + 1, [0]) for b in bands]


def zarr_tile_dataset_bytes(tile) -> bytes:
    """GeoTIFF bytes of the tile's window, read from the store and CF-decoded.

    The window is read through the store (the manifest rows in ``zarr_refs`` for a managed
    or registered store, the files themselves for a plain one), decoded as ``decode`` says
    (missing values to NaN, scale and offset applied), arranged north-up with the band
    dimension as bands, and written with the light compression policy. The pending
    instructions of the tile are NOT applied here; ``open_tile`` does that to these bytes,
    as it does to a window of a file. A window larger than the task may hold decoded
    (``materialize_decision``) raises ``ZarrError``.
    """
    from rasterio.enums import ColorInterp
    from rasterio.transform import Affine

    from databricks.labs.gbx.core.crs import resolve_crs
    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.pyrx.core.compression import write_gtiff
    from databricks.labs.gbx.zarr._meta import cf_decode

    rd = _parse(tile)
    spec, meta = rd.spec, rd.spec.meta
    c0, r0, w, h = (
        tile.window if tile.window is not None else (0, 0, rd.width, rd.height)
    )
    if not (0 <= c0 and 0 <= r0 and c0 + w <= rd.width and r0 + h <= rd.height):
        raise ZarrError(
            f"the tile's window {tuple(tile.window)} lies outside its {rd.width} x "
            f"{rd.height} grid",
            fix="read the tiles again with gbx_zarr_read",
        )
    nbytes = rd.count * w * h * np.dtype(spec.dtype).itemsize
    if materialize_decision(nbytes, "read", cap_bytes=spec.cap) != "stream":
        raise ZarrError(
            f"the tile holds {nbytes} bytes decoded, more than the {spec.cap} a task can "
            "hold",
            fix="read the tiles with a smaller tile_size, or stack fewer bands (band_dim)",
        )
    ybox = _data_range(r0, r0 + h, rd.height, spec.flip_y)
    xbox = _data_range(c0, c0 + w, rd.width, spec.flip_x)
    planes = []
    for lo, hi, rel in _band_groups(rd.bands):
        box = []
        for p in range(len(meta.shape)):
            if p == spec.y:
                box.append(ybox)
            elif p == spec.x:
                box.append(xbox)
            elif p == rd.band_pos:
                box.append((lo, hi))
            else:
                box.append((rd.slice_at[p], rd.slice_at[p] + 1))
        block = _read_box(rd, box)
        if spec.decode:
            block = cf_decode(block, meta, use_zarr_fill_value_as_mask=spec.flag)
        planes.append(_plane(block, rd, rel))
        del block
    data = planes[0] if len(planes) == 1 else np.concatenate(planes)
    data = np.ascontiguousarray(data, dtype=spec.dtype)

    profile = {
        "driver": "GTiff",
        "width": w,
        "height": h,
        "count": rd.count,
        "dtype": spec.dtype,
        "crs": resolve_crs(rd.crs),
        "transform": Affine.from_gdal(*rd.gt) * Affine.translation(c0, r0),
    }
    if spec.nodata is not None:
        profile["nodata"] = spec.nodata
    # The bands of a variable are data, none of them alpha (GDAL labels band 4 of a 4-band
    # byte raster alpha, and bands 1-3 of a 3-band one RGB): label them all undefined.
    labels = [ColorInterp.undefined] * rd.count if rd.count > 1 else None
    return write_gtiff(profile, data, alpha_like=labels, all_labels=True)
