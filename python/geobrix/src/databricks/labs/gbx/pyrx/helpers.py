"""DataFrame helpers that keep pixel bytes out of rows and out of shuffles.

``retile_virtual`` and ``bng_tessellate_virtual`` plan windows from raster headers and hand back
virtual tiles (a path, a window and, for tessellation, a clip: no pixels). ``merge_by_key`` and
``combine_by_key`` shuffle by key and fold each group once, in a sorted Arrow stream: memory is the
current group, and each member is decoded once.

Each helper wraps what the light tier already has. The planners are the raster reader's: the window
grid of ``tiling.plan_grid_windows`` (as Spark SQL), ``window_for_geom`` and the clip a virtual tile
carries (what ``clipPolygons`` plans), and the BNG cell enumerator of the BNG mosaic. The folds are
the aggregates' reducers (``agg.merge_tiles``, ``agg.combineavg_tiles``, ``agg.combine_max_tiles``)
fed by the aggregates' own member rule, and the ``vrt`` merge is ``minted_vrt`` streamed through
``cog_convert_file``. Compression and the materialize caps are theirs too, and a group's members
are bounded by the canonical tile budget. Connect-safe: no ``.rdd``,
``_jvm``, ``_jsc`` or ``spark.conf.set``.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StructField, StructType

__all__ = [
    "retile_virtual",
    "bng_tessellate_virtual",
    "merge_by_key",
    "combine_by_key",
]

_FIX_VIRTUAL = (
    "{fn} takes virtual tiles (a path, no raster bytes). Fix: use gbx_rst_retile / "
    "gbx_rst_bng_tessellate for materialized tiles, or read with virtualTiles=true"
)
_BNG = "EPSG:27700"
# EPSG:27700 cells exist only inside this envelope (what `pygx._bng.is_valid` accepts).
_BNG_EAST_MAX, _BNG_NORTH_MAX = 700_000.0, 1_300_000.0
# A source that would plan more cells than this is the wrong size for a driver-side plan.
_MAX_PLAN_CELLS = 2_000_000


# --- shared planning pieces ---------------------------------------------------------------------


def _positive(value, name: str) -> int:
    n = int(value)
    if n <= 0:
        raise ValueError(f"{name} must be a positive integer; got {value!r}")
    return n


def _columns(df: DataFrame, names: Sequence[str]) -> None:
    missing = [c for c in names if c not in df.columns]
    if missing:
        raise ValueError(f"no such column(s) {missing}; the DataFrame has {df.columns}")


def _require_virtual(df: DataFrame, tile_col: str, fn: str) -> DataFrame:
    """A row with raster bytes stops the query when it runs (not when the DataFrame is built)."""
    t = F.col(tile_col)
    refuse = F.when(
        t["raster"].isNotNull(), F.raise_error(F.lit(_FIX_VIRTUAL.format(fn=fn)))
    ).otherwise(F.lit(True))
    return df.where(refuse)


def _header_dims(paths: Sequence[str]):
    """(path, width, height) from one header open per path; no pixel is read."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    dims = []
    for p in paths:
        try:
            with ot.open_header(VirtualTile(cellid=0, path=p)) as ds:
                dims.append((p, int(ds.width), int(ds.height)))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                f"cannot read the header of {p!r}: {exc}. Fix: check that the file exists and that "
                f"it is a raster the light tier can open"
            ) from exc
    return dims


def _fill_windows(df: DataFrame, tile_col: str) -> DataFrame:
    """Whole-file virtual tiles (window NULL) get (0, 0, W, H) from one header open per path."""
    t = F.col(tile_col)
    todo = df.where(t["window"].isNull() & t["path"].isNotNull())
    paths = [r[0] for r in todo.select(t["path"]).distinct().collect()]
    if not paths:
        return df
    dims = df.sparkSession.createDataFrame(
        _header_dims(paths), "_gbx_p string, _gbx_w int, _gbx_h int"
    )
    full = F.struct(
        F.lit(0).alias("col_off"),
        F.lit(0).alias("row_off"),
        F.col("_gbx_w").alias("width"),
        F.col("_gbx_h").alias("height"),
    )
    return (
        df.join(dims, t["path"] == F.col("_gbx_p"), "left")
        .withColumn(tile_col, t.withField("window", F.coalesce(t["window"], full)))
        .drop("_gbx_p", "_gbx_w", "_gbx_h")
    )


# --- retile_virtual -----------------------------------------------------------------------------


def retile_virtual(
    df: DataFrame, tile_col: str = "tile", tile_width: int = 512, tile_height: int = 512
) -> DataFrame:
    """One virtual sub-tile per window of each virtual input tile, with no pixel read.

    The windows are the grid ``tiling.plan_grid_windows`` plans (row by row, the last row and column
    clamped to the tile), computed in Spark SQL from the tile's own window. A whole-file tile (window
    NULL) takes its size from one header open per distinct path, on the driver. An error tile (no
    raster, no path) comes through unchanged. A tile with raster bytes stops the query with a
    ``Fix:`` message when the query runs.

    Args:
        df: a DataFrame with a tile column.
        tile_col: the tile column.
        tile_width, tile_height: the sub-tile size in pixels.
    """
    w, h = _positive(tile_width, "tile_width"), _positive(tile_height, "tile_height")
    _columns(df, [tile_col])
    df = _fill_windows(_require_virtual(df, tile_col, "retile_virtual"), tile_col)
    t = F.col(tile_col)
    win = t["window"]
    planned = win.isNotNull()
    nx = F.greatest(
        F.when(planned, F.ceil(win["width"] / F.lit(w))).otherwise(F.lit(1)), F.lit(1)
    )
    ny = F.greatest(
        F.when(planned, F.ceil(win["height"] / F.lit(h))).otherwise(F.lit(1)), F.lit(1)
    )
    col_px, row_px = F.col("_gbx_c") * w, F.col("_gbx_r") * h
    sub = F.struct(
        (win["col_off"] + col_px).cast("int").alias("col_off"),
        (win["row_off"] + row_px).cast("int").alias("row_off"),
        F.least(F.lit(w), win["width"] - col_px).cast("int").alias("width"),
        F.least(F.lit(h), win["height"] - row_px).cast("int").alias("height"),
    )
    return (
        df.withColumn("_gbx_r", F.explode(F.sequence(F.lit(0), (ny - 1).cast("int"))))
        .withColumn("_gbx_c", F.explode(F.sequence(F.lit(0), (nx - 1).cast("int"))))
        .withColumn(tile_col, F.when(planned, t.withField("window", sub)).otherwise(t))
        .drop("_gbx_r", "_gbx_c")
    )


# --- bng_tessellate_virtual ---------------------------------------------------------------------


def _window_tuple(win):
    return (
        None
        if win is None
        else (win["col_off"], win["row_off"], win["width"], win["height"])
    )


def _clip_in_bng(clip: bytes, clip_crs: Optional[str], src_crs):
    """A tile's clip polygon in EPSG:27700. Its CRS follows the clip rule of a virtual tile: the SRID
    the geometry carries, else `clip_crs`, else the raster's own."""
    import shapely
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape

    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.core.crs import crs_equal, resolve_source_crs

    geom = parse_geom(bytes(clip))
    crs = resolve_source_crs(shapely.get_srid(geom), crs=clip_crs) or src_crs
    if crs_equal(crs, _BNG):
        return geom
    return shape(transform_geom(crs, _BNG, mapping(geom)))


def _plan_cells(sources, resolution: int):
    """(path, window, clip, clip CRS, cellid, cell window, cell WKB) per BNG cell over each source.

    A source is a tile's (path, window, clip, clip CRS). One header open per path. The cells are the
    BNG cells over the source's bounds (the enumerator the BNG mosaic uses); the cell window is the
    part of the source the cell covers, and the cell polygon, cut to the tile's own clip when it has
    one, is the clip the new tile will carry. A cell the source or its clip does not reach has no
    window and is left out.
    """
    import math

    import shapely
    from rasterio.errors import WindowError
    from rasterio.warp import transform_bounds
    from rasterio.windows import Window
    from rasterio.windows import bounds as window_bounds
    from shapely.geometry import box

    from databricks.labs.gbx.ds._bng_grid import bng_cells_for_bounds
    from databricks.labs.gbx.ds._window import window_for_geom
    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    size = _bng.get_edge_size(resolution)
    by_path = {}
    for path, win, clip, clip_crs in sources:
        by_path.setdefault(path, []).append((win, clip, clip_crs))
    plan, budget = [], _MAX_PLAN_CELLS
    for path, tiles in by_path.items():
        with ot.open_header(VirtualTile(cellid=0, path=path)) as src:
            if src.crs is None:
                raise ValueError(
                    f"{path!r} has no CRS, so it has no place on the British National Grid. "
                    f"Fix: label it first (gbx_rst_setsrid)"
                )
            for win, clip, clip_crs in tiles:
                part = (
                    Window(*win)
                    if win is not None
                    else Window(0, 0, src.width, src.height)
                )
                west, south, east, north = transform_bounds(
                    src.crs, _BNG, *window_bounds(part, src.transform)
                )
                if not all(math.isfinite(v) for v in (west, south, east, north)):
                    continue  # no usable footprint on the grid (e.g. far outside its projection)
                west, south = max(west, 0.0), max(south, 0.0)
                east, north = min(east, _BNG_EAST_MAX), min(north, _BNG_NORTH_MAX)
                bound = None if clip is None else _clip_in_bng(clip, clip_crs, src.crs)
                if bound is not None:
                    cw_, cs_, ce_, cn_ = bound.bounds
                    west, south = max(west, cw_), max(south, cs_)
                    east, north = min(east, ce_), min(north, cn_)
                if west > east or south > north:
                    continue  # outside Great Britain, or outside the clip: no cell
                budget -= (math.floor((east - west) / size) + 2) * (
                    math.floor((north - south) / size) + 2
                )
                if budget < 0:
                    raise ValueError(
                        f"{path!r} would plan more than {_MAX_PLAN_CELLS:,} BNG cells at this "
                        f"resolution. Fix: use a coarser resolution, or retile_virtual first"
                    )
                for cell in bng_cells_for_bounds(
                    (west, south, east, north), resolution
                ):
                    geom = box(cell.west, cell.south, cell.east, cell.north)
                    if bound is not None:
                        geom = geom.intersection(bound)
                        if geom.is_empty or geom.area == 0:
                            continue  # the clip does not reach this cell
                    cw = window_for_geom(src, geom, _BNG)
                    if cw is not None and win is not None:
                        try:
                            cw = cw.intersection(part)
                        except WindowError:  # the cell is outside the tile's window
                            cw = None
                    if cw is None or cw.width < 1 or cw.height < 1:
                        continue
                    plan.append(
                        (
                            path,
                            win,
                            clip,
                            clip_crs,
                            int(_bng.parse(cell.cellid)),
                            (
                                int(cw.col_off),
                                int(cw.row_off),
                                int(cw.width),
                                int(cw.height),
                            ),
                            shapely.to_wkb(geom),
                        )
                    )
    return plan


def bng_tessellate_virtual(
    df: DataFrame, tile_col: str = "tile", resolution=3
) -> DataFrame:
    """One virtual tile per British National Grid cell over each virtual input tile, with no pixel read.

    Each output tile keeps its source's path and carries the cell's window, the cell polygon as its
    clip (``clip_crs`` EPSG:27700) and the cell id as ``cellid``; the pixels are read, and clipped to
    the cell, when something downstream needs them. The tile keeps its own grid and CRS, so the
    cells are those over its footprint (the per-row ``gbx_rst_bng_tessellate`` warps the raster to
    EPSG:27700 and may add one-pixel edge cells). A tile that already has a clip keeps it: its cells
    are cut to the clip, and a cell the clip does not reach is left out. The driver plans from
    headers, one open per distinct path, so the cost follows sources times cells and not pixels. A
    source outside Great Britain has no cells. An error tile (no raster, no path) comes through
    unchanged. A tile with raster bytes stops the query with a ``Fix:`` message when the query runs,
    and so does a file whose header cannot be read.

    Args:
        df: a DataFrame with a tile column.
        tile_col: the tile column.
        resolution: a BNG resolution, an index (1 to 6, or -1 to -6 for quadrants) or a name ("1km").
    """
    from databricks.labs.gbx.pygx import _bng

    res = _bng.get_resolution(resolution)
    _columns(df, [tile_col])
    df = _require_virtual(df, tile_col, "bng_tessellate_virtual")
    t = F.col(tile_col)
    sources = [
        (r["p"], _window_tuple(r["w"]), r["k"], r["kc"])
        for r in df.where(t["path"].isNotNull())
        .select(
            t["path"].alias("p"),
            t["window"].alias("w"),
            t["clip_polygon"].alias("k"),
            t["clip_crs"].alias("kc"),
        )
        .distinct()
        .collect()
    ]
    win_type = "struct<col_off:int,row_off:int,width:int,height:int>"
    plan = df.sparkSession.createDataFrame(
        [
            (
                p,
                (
                    None
                    if w is None
                    else dict(zip(("col_off", "row_off", "width", "height"), w))
                ),
                None if k is None else bytes(k),
                kc,
                cid,
                dict(zip(("col_off", "row_off", "width", "height"), cw)),
                wkb,
            )
            for p, w, k, kc, cid, cw, wkb in _plan_cells(sources, res)
        ],
        f"_gbx_p string, _gbx_w {win_type}, _gbx_k binary, _gbx_kc string, "
        f"_gbx_cell bigint, _gbx_cw {win_type}, _gbx_g binary",
    )
    cells = (
        t.withField("window", F.col("_gbx_cw"))
        .withField("cellid", F.col("_gbx_cell"))
        .withField("clip_polygon", F.col("_gbx_g"))
        .withField("clip_crs", F.lit(_BNG))
    )
    same_source = (
        (t["path"] == F.col("_gbx_p"))
        & t["window"].eqNullSafe(F.col("_gbx_w"))
        & t["clip_polygon"].eqNullSafe(F.col("_gbx_k"))
        & t["clip_crs"].eqNullSafe(F.col("_gbx_kc"))
    )
    joined = df.where(t["path"].isNotNull()).join(plan, same_source)
    tessellated = joined.withColumn(tile_col, cells).drop(
        "_gbx_p", "_gbx_w", "_gbx_k", "_gbx_kc", "_gbx_cell", "_gbx_cw", "_gbx_g"
    )
    return tessellated.unionByName(df.where(t["path"].isNull()))


# --- merge_by_key and combine_by_key ------------------------------------------------------------


def _by_key(df: DataFrame, keys, tile_col: str, num_partitions: Optional[int]):
    keys = [keys] if isinstance(keys, str) else list(keys)
    if not keys:
        raise ValueError("keys must name at least one column")
    _columns(df, [*keys, tile_col])
    n = None if num_partitions is None else _positive(num_partitions, "num_partitions")
    return keys, n


def _sorted_by_key(
    df: DataFrame, keys: List[str], num_partitions: Optional[int]
) -> DataFrame:
    """One shuffle by the keys (parallelism = `num_partitions`, else the session default), then
    sorted within each partition, so every group is contiguous."""
    part = (
        df.repartition(num_partitions, *keys)
        if num_partitions
        else df.repartition(*keys)
    )
    return part.sortWithinPartitions(*keys)


def _out_schema(df: DataFrame, keys: Sequence[str]) -> StructType:
    return StructType(
        [df.schema[k] for k in keys] + [StructField("raster", BinaryType())]
    )


def _decoded_size(ds) -> int:
    """Decoded bytes of an open raster, from its header."""
    import numpy as np

    return (
        int(ds.count)
        * int(ds.width)
        * int(ds.height)
        * int(np.dtype(ds.dtypes[0]).itemsize)
    )


def _decoded_bytes(tile) -> int:
    """Decoded size of a virtual tile's window, from its header."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    with ot.open_header(tile) as ds:
        return _decoded_size(ds)


def _tile_member(tile, cap_bytes: int, decoded_budget: int, fix: str):
    """The raster bytes of one group member; None for a member the aggregates leave out.

    A NULL tile, an empty or error tile (no raster, no path) and bytes that do not open are dropped,
    as ``gbx_rst_merge_agg`` and ``gbx_rst_combineavg_agg`` drop them. A virtual member is decoded
    here, under the materialize cap. Every member's decoded size, from its header, must fit the
    decoded-tile budget, for bytes already in the row as much as for a path: the fold decodes it.
    """
    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.functions import _tile_raster_bytes

    if tile is None:
        return None
    if tile.get("raster") is None and tile.get("path"):
        size = _decoded_bytes(tile)
        if materialize_decision(size, "ingest", cap_bytes=cap_bytes) == "error":
            raise ValueError(
                f"a member tile of {tile['path']!r} is {size / 2**20:.0f} MiB decoded, over the "
                f"{cap_bytes / 2**20:.0f} MiB cap for decoding a tile into a row. Fix: use "
                f"merge_by_key(method='vrt') (paths only), or retile_virtual first"
            )
    candidate = _tile_raster_bytes(tile)
    if candidate is None:
        return None
    try:
        with _serde.open_tile(candidate) as ds:
            size = _decoded_size(ds)
    except Exception:  # noqa: BLE001 - dropped, as the aggregates drop it
        return None
    if size > decoded_budget:
        raise ValueError(
            f"a member tile is {size / 2**20:.1f} MiB decoded, over the "
            f"{decoded_budget / 2**20:.0f} MiB tile budget of this session. Fix: {fix}"
        )
    return candidate


def _group_stream(
    batches,
    keys: List[str],
    value_col: str,
    member,
    finish,
    cap_bytes: int,
    group_budget: int,
    fix: str,
    weigh=len,
):
    """Yield one Arrow batch (the key values and the folded raster) per finished key group.

    The input is sorted by key within the partition, so a group is contiguous, even across batches;
    memory is the current group's members, and a group whose members (`weigh` each, their bytes)
    pass `group_budget` stops the query with `fix`, before the fold copies them. A group with no
    usable member gets a NULL raster, as an aggregate over an empty group does.
    """
    import pyarrow as pa

    from databricks.labs.gbx.ds.file_gbx import materialize_decision

    started, current, scalars, members, held = False, None, None, [], 0

    def emit():
        raster = finish(members)
        if (
            raster is not None
            and materialize_decision(len(raster), "ingest", cap_bytes=cap_bytes)
            == "error"
        ):
            raise ValueError(
                f"the result for key {current!r} is {len(raster) / 2**20:.0f} MiB, over the "
                f"{cap_bytes / 2**20:.0f} MiB cap for bytes in a row. Fix: write the result to files "
                f"instead (the cog_gbx writer with vrtMosaic=true), or split the groups"
            )
        arrays = [pa.array([s.as_py()], type=s.type) for s in scalars]
        arrays.append(pa.array([raster], type=pa.binary()))
        return pa.RecordBatch.from_arrays(arrays, names=[*keys, "raster"])

    for batch in batches:
        names = batch.schema.names
        key_cols = [batch.column(names.index(k)) for k in keys]
        values = batch.column(names.index(value_col))
        for i in range(batch.num_rows):
            key_scalars = [c[i] for c in key_cols]
            key = tuple(s.as_py() for s in key_scalars)
            if not started or key != current:
                if started:
                    yield emit()
                started, current, scalars, members, held = (
                    True,
                    key,
                    key_scalars,
                    [],
                    0,
                )
            item = member(values[i].as_py())
            if item is not None:
                held += weigh(item)
                if held > group_budget:
                    raise ValueError(
                        f"the tiles for key {current!r} hold more than "
                        f"{group_budget / 2**20:.0f} MiB of encoded bytes, the cap for a group of "
                        f"tiles in this session. Fix: {fix}"
                    )
                members.append(item)
    if started:
        yield emit()


def _cap_bytes(df: DataFrame) -> int:
    """The connect-aware cap, taken on the driver (a worker cannot see which kind of session it serves)."""
    from databricks.labs.gbx.ds.file_gbx import _connect_aware_lru_sizing

    return _connect_aware_lru_sizing(df.sparkSession)[0]


def _decoded_budget(df: DataFrame) -> int:
    """The decoded-tile budget (64 MiB on Serverless and Connect, 1.5 GiB on classic): what one
    member may decode to. Taken on the driver."""
    from databricks.labs.gbx.manager.capabilities import is_connect
    from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

    return decoded_budget_bytes("serverless" if is_connect(df.sparkSession) else "auto")


_FIX_SPLIT = (
    "split the key so each group is smaller (add a sub-key, such as a spatial block)"
)
_FIX_CUT = "cut the tiles smaller first (retile_virtual)"


def _fold_by_key(
    df,
    keys,
    tile_col,
    num_partitions,
    finish,
    fix_group=_FIX_SPLIT,
    fix_member=_FIX_CUT,
):
    """Shuffle (keys, tile) by the keys and fold each group once with `finish(list of GTiff bytes)`.

    A key's members may hold the materialize cap in encoded bytes (64 MiB on Serverless and Connect,
    256 MiB on classic); each member's decoded size must fit the decoded-tile budget.
    """
    cap, decoded = _cap_bytes(df), _decoded_budget(df)
    shuffled = _sorted_by_key(df.select(*keys, tile_col), keys, num_partitions)

    def run(batches):
        return _group_stream(
            batches,
            keys,
            tile_col,
            lambda tile: _tile_member(tile, cap, decoded, fix_member),
            finish,
            cap,
            cap,
            fix_group,
        )

    return shuffled.mapInArrow(run, _out_schema(df, keys))


def _vrt_mosaic(members, cap_bytes: int):
    """One COG of the mosaic of whole-file members: a VRT over their paths, streamed to a COG block
    by block, so no member is decoded into memory. The COG's size is checked against the cap before
    it is read back."""
    import os
    import shutil

    import rasterio

    from databricks.labs.gbx.ds._mosaic import minted_vrt
    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file
    from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

    if not members:
        return None
    for path, window in members:
        if window is not None:
            with rasterio.open(path) as src:
                if tuple(window) != (0, 0, src.width, src.height):
                    raise ValueError(
                        f"merge_by_key(method='vrt') mosaics whole files, and a member is the window "
                        f"{tuple(window)} of {path!r}. Fix: use method='fold', which applies windows"
                    )
    scratch = new_local_temp_dir("gbx_merge_vrt")
    try:
        out = os.path.join(scratch, "mosaic.tif")
        with minted_vrt(sorted({p for p, _ in members})) as vrt:
            cog_convert_file(vrt, out, bigtiff="IF_SAFER")
        size = os.path.getsize(out)
        if materialize_decision(size, "ingest", cap_bytes=cap_bytes) == "error":
            raise ValueError(
                f"the merged mosaic is {size / 2**20:.0f} MiB, over the {cap_bytes / 2**20:.0f} MiB "
                f"cap for bytes in a row. Fix: write the mosaic to files instead (the cog_gbx writer "
                f"with vrtMosaic=true), or split the groups"
            )
        with open(out, "rb") as fh:
            return fh.read()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def merge_by_key(
    df: DataFrame,
    keys: List[str],
    tile_col: str = "tile",
    method: str = "fold",
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """Mosaic each key's tiles once: ``DataFrame[keys..., raster BINARY]``, a GeoTIFF per key.

    ``method='fold'`` shuffles the tiles by key and merges each group in a sorted Arrow stream with
    the merge ``gbx_rst_merge_agg`` runs (overlaps resolve the same way; a member that is NULL, an
    error tile or unreadable is left out; a group with no usable member gets a NULL raster). Materialized
    and virtual tiles both work, and a virtual member's window and instructions are applied.

    ``method='vrt'`` takes virtual tiles that are whole files and shuffles only their paths: the
    mosaic is a VRT over the files, streamed to a COG, so no member is decoded into memory. The
    members must share a CRS, pixel size, band count and data type. A member that is a smaller window
    of its file, or that has a clip, stops the query with a ``Fix:`` message.

    The two methods agree wherever the members do not overlap. Where they overlap, ``fold`` lets the
    member whose encoded GeoTIFF bytes sort last win, as the aggregate does, and ``vrt`` lets the
    last path in sorted order win: the aggregate's order comes from the decoded, re-encoded tiles, which this
    method never reads, so it cannot follow it. The ``vrt`` result is a COG (DEFLATE, with overviews;
    a BigTIFF from about 2 GiB uncompressed, GDAL's ``IF_SAFER`` rule), where ``fold`` gives a plain
    zstd GeoTIFF. Use ``fold`` where the aggregate's winner matters.

    For ``fold``, a key's members may hold the materialize cap in encoded bytes (64 MiB on Serverless
    and Connect, 256 MiB on classic), and each member's decoded size, from its header, must fit the
    decoded-tile budget (``decoded_budget_bytes``: 64 MiB on Serverless and Connect, 1.5 GiB on
    classic), for tiles with bytes as much as for paths. Over either, the query stops with a ``Fix:``
    to split the key, cut the tiles smaller, or use ``method='vrt'``. The Arrow batches that carry the
    rows are sized by Spark and held in memory besides.

    Args:
        df: a DataFrame with the key columns and a tile column.
        keys: the key column names.
        tile_col: the tile column.
        method: ``'fold'`` or ``'vrt'``.
        num_partitions: the shuffle's parallelism (``repartition(N, keys)``); the session default if None.
    """
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    if method not in ("fold", "vrt"):
        raise ValueError(
            f"merge_by_key: method must be 'fold' or 'vrt'; got {method!r}"
        )
    keys, n = _by_key(df, keys, tile_col, num_partitions)
    if method == "fold":
        return _fold_by_key(
            df,
            keys,
            tile_col,
            n,
            agg_core.merge_tiles,
            _FIX_SPLIT + ", or use method='vrt', which reads paths only",
            _FIX_CUT + ", or use method='vrt', which reads paths only",
        )
    t = F.col(tile_col)
    cap = _cap_bytes(df)
    paths = _sorted_by_key(
        _require_virtual(df, tile_col, "merge_by_key(method='vrt')").select(
            *keys,
            F.struct(
                t["path"].alias("path"),
                t["window"].alias("window"),
                t["clip_polygon"].isNotNull().alias("clipped"),
            ).alias("_gbx_m"),
        ),
        keys,
        n,
    )

    def member(m):
        if m is None or m["path"] is None:
            return None
        if m["clipped"]:
            raise ValueError(
                f"merge_by_key(method='vrt') mosaics whole files, and a member of {m['path']!r} has a "
                f"clip. Fix: use method='fold', which applies clips"
            )
        w = m["window"]
        return m["path"], (
            None if w is None else (w["col_off"], w["row_off"], w["width"], w["height"])
        )

    def run(batches):
        return _group_stream(
            batches,
            keys,
            "_gbx_m",
            member,
            lambda members: _vrt_mosaic(members, cap),
            cap,
            cap,
            _FIX_SPLIT,
            weigh=lambda m: 0,  # paths only: no tile bytes are held
        )

    return paths.mapInArrow(run, _out_schema(df, keys))


def combine_by_key(
    df: DataFrame,
    keys: List[str],
    tile_col: str = "tile",
    stat: str = "avg",
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """Per-pixel ``avg`` or ``max`` over each key's tiles: ``DataFrame[keys..., raster BINARY]``.

    The tiles of a key must share a grid (size, transform and CRS) for both statistics; a key whose
    tiles differ stops the query with a ``Fix:`` message to align them first with
    ``gbx_rst_align_to``. NoData is left out of the statistic, as in ``gbx_rst_combineavg_agg`` and
    ``gbx_rst_combinemax``, whose reducers this runs: each tile is decoded once per key, one at a
    time, and the result encoded once. A member that is NULL, an error tile or unreadable is left
    out; a group with no usable member gets a NULL raster.

    A key's members may hold the materialize cap in encoded bytes (64 MiB on Serverless and Connect,
    256 MiB on classic), and each member's decoded size, from its header, must fit the decoded-tile
    budget (``decoded_budget_bytes``: 64 MiB on Serverless and Connect, 1.5 GiB on classic). Over
    either, the query stops with a ``Fix:`` to split the key or cut the tiles smaller. The Arrow
    batches that carry the rows (Spark caps them at 256 MiB) are held in memory besides.

    Args:
        df: a DataFrame with the key columns and a tile column.
        keys: the key column names.
        tile_col: the tile column.
        stat: ``'avg'`` or ``'max'``.
        num_partitions: the shuffle's parallelism (``repartition(N, keys)``); the session default if None.
    """
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    reducers = {"avg": agg_core.combineavg_tiles, "max": agg_core.combine_max_tiles}
    if stat not in reducers:
        raise ValueError(f"combine_by_key: stat must be 'avg' or 'max'; got {stat!r}")
    keys, n = _by_key(df, keys, tile_col, num_partitions)
    return _fold_by_key(
        df, keys, tile_col, n, _aligned(reducers[stat], check_grid=stat == "avg")
    )


def _aligned(reducer, check_grid: bool):
    """`reducer` with a `Fix:` on the error a group of tiles on different grids raises.

    The average reducer only compares shapes, so it would average two tiles of one size on a shifted
    transform or another CRS; `check_grid` holds it to the check the other statistics make (size,
    transform and CRS, from the headers).
    """
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    def run(members):
        try:
            if check_grid and len(members) > 1:
                memfiles, datasets = agg_core._open_all(members)
                try:
                    agg_core._require_aligned(datasets, "combine_by_key")
                finally:
                    agg_core._close_all(memfiles, datasets)
            return reducer(members)
        except ValueError as exc:
            if "align" in str(exc) and "Fix:" not in str(exc):
                lead = "" if "combine_by_key" in str(exc) else "combine_by_key: "
                raise ValueError(
                    f"{lead}{exc}. Fix: align the tiles of each key first (gbx_rst_align_to)"
                ) from exc
            raise

    return run
