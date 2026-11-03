"""RasterX DataFrame helpers: the Python-only API that has no SQL name.

The helpers of the first group keep pixel bytes out of rows and out of shuffles; the second group
is the per-tower viewshed, the partition-scoped (``*_grouped``) pixel operations, bounded point
binning and the H3 grid canvas. Every function here takes or returns a DataFrame.

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

Grouped merge and combine: ``merge_by_key`` and ``combine_by_key`` are the default on a dedicated
cluster. Mind the memory: a fold holds a whole group in a task, and measured at 1000 groups its peak
execution memory was 40.3 GB, where the aggregate ``gbx_rst_merge_agg`` streamed the same groups in
7.4 GB. On Serverless the aggregate streams under the worker's read cap. A result over the cap stops
with a ``Fix:`` that names the VRT-mosaic route: keep the tiles as files on a Volume and build the
mosaic with ``databricks.labs.gbx.ds._mosaic.mint_vrt(paths, out="/Volumes/...")`` (or the cog_gbx
writer's mosaic mode), then read the VRT, because ``merge_by_key`` also returns BINARY bounded by
the same cap. ``merge_by_key(..., method="vrt")`` is the route for a group whose result fits the
cap but whose members do not fit in memory: it reads paths only and streams the mosaic to a COG
without decoding a member (1000 tiles completed in 28 minutes). ``combine_by_key`` folds under the
same budget.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import functions as f
from pyspark.sql.types import BinaryType, DoubleType, StructField, StructType

from databricks.labs.gbx._gdal_policy import open_data

__all__ = [  # noqa: F822  (tile_range and prepare_cogs load on first use)
    "retile_virtual",
    "bng_tessellate_virtual",
    "merge_by_key",
    "combine_by_key",
    "rst_viewshed_towers",
    "rst_memsize_grouped",
    "rst_clip_grouped",
    "rst_transform_grouped",
    "rst_to_webmercator_grouped",
    "rst_transformcrs_grouped",
    "rst_resample_grouped",
    "rst_resample_to_size_grouped",
    "rst_resample_to_res_grouped",
    "rst_threshold_grouped",
    "rst_updatetype_grouped",
    "rst_slope_grouped",
    "rst_aspect_grouped",
    "rst_hillshade_grouped",
    "bin_points_tiled",
    "rst_h3_gridspec",
    "tile_range",
    "prepare_cogs",
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

    This is an option for cutting virtual tiles into windows; it is not the way to handle a large
    raster. On Serverless environment 6 it runs out of memory on large sources: read a large raster
    with ``virtualTiles=true`` and a tile size instead.

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

    Only the named install uses this (it is not part of a plain ``register()``), and it has not been
    measured on a Serverless run with an EPSG:27700 tile, so treat it as an option, not as a default.

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
        with _serde.open_tile(
            candidate, gate=False
        ) as ds:  # the size probe: the budget check below judges it
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

    # the driver's read cap goes to the workers with the closure (a standard-access worker cannot work it out itself):
    # the gate the folds apply to each member is the one the session names, as for a registered function
    from databricks.labs.gbx._gdal_policy import carrying
    from databricks.labs.gbx.ds.file_gbx import session_read_cap

    read_cap = session_read_cap(df.sparkSession)
    return shuffled.mapInArrow(
        run if read_cap is None else carrying(run, None, read_cap),
        _out_schema(df, keys),
    )


def _vrt_mosaic(members, cap_bytes: int):
    """One COG of the mosaic of whole-file members: a VRT over their paths, streamed to a COG block
    by block, so no member is decoded into memory. The COG's size is checked against the cap before
    it is read back."""
    import os
    import shutil

    from databricks.labs.gbx.ds._mosaic import minted_vrt
    from databricks.labs.gbx.ds.file_gbx import materialize_decision
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file
    from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

    if not members:
        return None
    for path, window in members:
        if window is not None:
            with open_data(path) as src:
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
                f"cap for bytes in a row. Fix: keep the tiles as files on a Volume and build the mosaic over "
                f'them with databricks.labs.gbx.ds._mosaic.mint_vrt(paths, out="/Volumes/...") (or the cog_gbx '
                f"writer with vrtMosaic=true), then read the VRT; or split the groups"
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

    This is the default for a grouped merge on a dedicated cluster. Its memory follows the group: a
    fold holds the group in a task (measured at 1000 groups: 40.3 GB peak execution memory, against
    7.4 GB for the ``gbx_rst_merge_agg`` aggregate). On Serverless, ``method='vrt'`` is the route for
    a group whose result fits the worker's read cap but whose members do not fit in memory. The result
    is BINARY, bounded by that same cap: a mosaic over the cap is kept as files instead (the cog_gbx
    writer's mosaic mode, or ``databricks.labs.gbx.ds._mosaic.mint_vrt(paths, out="/Volumes/...")``,
    then read the VRT), and this call stops with a ``Fix:`` that says so.

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

    This is the default for a grouped combine on a dedicated cluster. Its memory follows the group,
    so size the key to the task. On Serverless the members must fit the budgets below; a group over
    them stops with a ``Fix:``.

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
                memfiles, datasets = agg_core._open_all(
                    members, gate=False
                )  # headers only
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


# --- Python-only API: per-tower viewsheds, partition-scoped (grouped) pixel ops, bounded point binning, H3 canvases ---
# The private implementations stay in databricks.labs.gbx.pyrx; these functions are the public API.


def rst_viewshed_towers(
    towers_df: DataFrame,
    dsm_tiles_df: DataFrame,
    radius_m: float,
    observer_height,
    *,
    x0: float,
    y0: float,
    tile_m: float,
    target_height: float = 1.6,
    to_crs: Optional[int] = None,
    vrt_dir: Optional[str] = None,
    tower_id_col: str = "tower_cellid",
    x_col: str = "x",
    y_col: str = "y",
    num_partitions: Optional[int] = None,
    _levels: Optional[str] = None,
) -> DataFrame:
    """Per-tower raster viewshed over a tiled DSM — one scalar row per tower.

    Packages the failure-avoiding pipeline validated on Databricks Serverless
    light: for each tower it selects the in-radius DSM tiles, mints ONE
    bytes-free VRT over them, runs a single pixel-level :func:`rst_viewshed`
    (materialized once, in-memory), and derives coverage counts from a
    histogram. No raster bytes ever cross a shuffle — each tower row carries a
    ~100-byte VRT path — so there is no FUSE read-back and no whole-AOI merge.

    The tile window is computed per tower with :func:`tile_range` (``x0``/``y0``
    grid origin, ``tile_m`` tile size — the grid the ``tx``/``ty`` indices in
    ``dsm_tiles_df`` reference), then the viewshed, histogram, georeference and
    optional reproject are chained as Spark ``withColumn`` steps that stay in a
    single post-``repartition`` stage so the mint and the DEM read run in the
    same task (the minted VRT is read before any further shuffle).

    Args:
        towers_df:    DataFrame of tower sites. Must carry ``tower_id_col`` plus
                      the projected observer coordinate columns ``x_col``/``y_col``
                      (EPSG:3857 metres, matching the DSM grid). When
                      ``observer_height`` is a column name, that column too.
        dsm_tiles_df: DSM tile manifest; must have columns ``path`` (absolute COG
                      path), ``tx`` and ``ty`` (integer grid indices). This is a
                      bytes-free manifest (paths, not raster bytes) and is
                      collected once on the driver.
        radius_m:     Viewshed analysis radius in projected metres (> 0). Also
                      caps the DSM window and the ``rst_viewshed`` max-distance.
        observer_height: Observer/mast height above the DSM surface — a scalar
                      (applied to every tower) or a column name (per-tower).
        x0, y0:       EPSG:3857 tile-grid origin (the ``tx``/``ty`` reference).
        tile_m:       Tile size in metres (> 0).
        target_height: Receiver height above the DSM at each tested cell.
                      Defaults to 1.6.
        to_crs:       Optional EPSG code to additionally reproject the viewshed
                      into (``rst_transform``, materialized once); when set, the
                      result gains ``visible_px_reproj`` / ``total_px_reproj``.
        vrt_dir:      Optional durable, worker-readable directory (e.g. a UC
                      Volume) for the per-tower VRTs (``tower_<id>.vrt``). When
                      omitted the VRTs are minted transiently on the worker and
                      read within the same task.
        tower_id_col: Tower identity column name. Defaults to "tower_cellid".
        x_col, y_col: Observer coordinate column names. Default "x" / "y".
        num_partitions: Fan-out for the heavy per-tower stage (``repartition`` by
                      ``tower_id_col``). Defaults to the distinct tower count
                      (~one tower per task).
        _levels:      Testing only. The level to run: ``"L3"`` (the default: all
                      light), ``"L1-steps"`` (the viewshed and the histogram are
                      the heavyweight's Catalyst steps) or ``"JVM-DF"`` (the whole
                      per-tower pipeline is one Scala DataFrame step through py4j;
                      no ``to_crs``, and ``vrt_dir`` is not used). The two heavy
                      levels need a dedicated cluster with the geobrix JAR; where
                      the session cannot run one, a line says why and L3 runs.

    The viewshed of a tower is the window around it, as :func:`rst_viewshed` returns
    it with a max distance: ``total_px`` is the window's pixel count (``radius_m``
    each way from the tower's pixel, clipped to the tiles) and ``visible_px`` the
    visible part of it. A tower stands on the DSM tile its coordinates fall in
    (tile ``i`` covers ``[origin + i * tile_m, origin + (i + 1) * tile_m)``): one
    outside the DSM, in a grid cell that has no tile, has no row. A NULL
    ``observer_height`` is 0.0. An unreadable DSM tile fails the query.

    Returns:
        DataFrame with one row per tower that has in-radius DSM coverage:
        ``tower_id_col``, ``observer_x``, ``observer_y``, ``visible_px`` (long),
        ``total_px`` (long), ``frac`` (double, visible/total), and — when
        ``to_crs`` is set — ``visible_px_reproj`` and ``total_px_reproj``.
    """
    from databricks.labs.gbx.pyrx.core import analysis as analysis_core
    from databricks.labs.gbx.pyrx.functions import (  # noqa: PLC0415
        _fromfile_extra,
        _light_rst_histogram,
        _raise_if_null,
        _tower_window_paths,
    )

    analysis_core._validate_towers_inputs(dsm_tiles_df.columns, radius_m)
    if tile_m is None or tile_m <= 0:
        raise ValueError(f"tile_m must be > 0 (got {tile_m!r}).")
    from databricks.labs.gbx.manager import levels as manager_levels

    if (
        to_crs is not None
        and (_levels or manager_levels.PREFERENCE["rst_viewshed_towers"][0]) == "JVM-DF"
    ):
        raise ValueError("_levels='JVM-DF' does not support to_crs")
    level = manager_levels.towers_level(towers_df.sparkSession, _levels)

    # Bytes-free tile manifest -> {(tx, ty): path}. Only tile index + path
    # strings reach the driver (never raster bytes), so this is Connect/Serverless
    # safe and bounded by the DSM tile count, not the pixel volume.
    tile_rows = dsm_tiles_df.select("tx", "ty", "path").collect()
    tile_map = {(int(r["tx"]), int(r["ty"])): r["path"] for r in tile_rows if r["path"]}
    if not tile_map:
        raise ValueError(
            "dsm_tiles_df yielded no (tx, ty) -> path entries; every path is NULL "
            "(a materialized DSM STRUCT table?) — point dsm_tiles_df at the DSM "
            "COG directory with dsm_<tx>_<ty>.tif tiles."
        )

    _x0, _y0, _tile_m = float(x0), float(y0), float(tile_m)
    _radius = float(radius_m)
    _vrt_dir = vrt_dir

    window_paths = _tower_window_paths(tile_map, _x0, _y0, _tile_m, _radius)

    @f.udf("string")
    def _mint_window_vrt(tower_id, obs_x, obs_y):
        # Worker-side: pick the in-radius tiles via tile_range and mint ONE VRT
        # over their paths (bytes-free: the VRT is tiny XML referencing the COGs
        # by absolute path). Runs in the same task as the downstream DEM read.
        import os

        from databricks.labs.gbx.ds._mosaic import mint_vrt

        paths = window_paths(obs_x, obs_y)
        if not paths:
            return None
        if _vrt_dir:
            os.makedirs(_vrt_dir, exist_ok=True)
            return mint_vrt(paths, out=os.path.join(_vrt_dir, f"tower_{tower_id}.vrt"))
        return mint_vrt(paths)

    @f.udf("string")
    def _window_tiles(obs_x, obs_y):
        return ", ".join(window_paths(obs_x, obs_y) or [])

    base = towers_df.select(
        f.col(tower_id_col).alias("tower_id"),
        f.col(x_col).cast(DoubleType()).alias("observer_x"),
        f.col(y_col).cast(DoubleType()).alias("observer_y"),
        # A NULL mast height is 0.0 at every level (the light viewshed reads it so).
        f.coalesce(
            analysis_core._observer_height_col(observer_height).cast("double"),
            f.lit(0.0),
        ).alias("observer_height"),
    )

    def with_frac(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "frac",
            f.when(
                f.col("total_px") > 0, f.col("visible_px") / f.col("total_px")
            ).otherwise(f.lit(0.0)),
        )

    # One tower per task: an explicit repartition by tower id is the only fan-out
    # Serverless AQE cannot coalesce. Repartition BEFORE the mint so the mint and
    # the DEM read share a task (no shuffle between them -> the local/Volume VRT
    # is readable where it was written).
    if num_partitions is None:
        num_partitions = max(base.select("tower_id").distinct().count(), 1)
    if level == "JVM-DF":
        from databricks.labs.gbx.manager import jvm

        # The Scala step repartitions by tower id itself. It takes the id as a string; as on L3, a tower with
        # no coordinate has no row.
        in_df = base.where(
            f.col("observer_x").isNotNull() & f.col("observer_y").isNotNull()
        ).select(
            f.col("tower_id").cast("string"),
            "observer_x",
            "observer_y",
            "observer_height",
        )
        done = jvm.viewshed_towers_jvm(
            towers_df.sparkSession,
            in_df,
            tile_map,
            _x0,
            _y0,
            _tile_m,
            _radius,
            float(target_height),
            int(num_partitions),
        )
        return with_frac(
            done.select(
                f.col("tower_id")
                .cast(base.schema["tower_id"].dataType)
                .alias(tower_id_col),
                "observer_x",
                "observer_y",
                "visible_px",
                "total_px",
            )
        )
    base = base.repartition(int(num_partitions), "tower_id")

    # Fixed-point WKT POINT (never scientific notation) in the raster CRS.
    observer_wkt = f.format_string(
        "POINT (%.6f %.6f)", f.col("observer_x"), f.col("observer_y")
    )

    # L3 is all light. At L1-steps the viewshed and the histogram are the heavyweight's Catalyst steps, which
    # take raster bytes: the DEM is materialized (through the same cap) instead of left a path.
    heavy = level == "L1-steps"

    def viewshed():
        args = (
            observer_wkt,
            f.col("observer_height"),
            f.lit(float(target_height)),
            f.lit(_radius),
        )
        if heavy:
            return f.call_function("gbx__l1_rst_viewshed", f.col("_dem"), *args)
        from databricks.labs.gbx.rasterx import functions as rx_api

        return rx_api.rst_viewshed(f.col("_dem"), *args, materialize=True)

    def histogram(tile_col: str):
        args = (f.lit(2), f.lit(0.0), f.lit(256.0), f.lit(False))
        if heavy:
            return f.call_function("gbx__l1_rst_histogram", f.col(tile_col), *args)
        return _light_rst_histogram(tile_col, *args)

    def cells(tile_col: str, vis, invis):
        """The cells of the viewshed: the two histogram classes, or at L1-steps the size of the raster. The
        heavyweight viewshed marks 0 as NoData and its histogram leaves NoData out, so it counts what is visible.
        """
        if heavy:
            w = f.call_function("gbx__l1_rst_width", f.col(tile_col))
            return w.cast("long") * f.call_function(
                "gbx__l1_rst_height", f.col(tile_col)
            )
        return invis + vis

    piped = (
        base.withColumn(
            "_vrt", _mint_window_vrt("tower_id", "observer_x", "observer_y")
        )
        .where(f.col("_vrt").isNotNull())
        .withColumn(
            "_dem", _fromfile_extra(f.col("_vrt"), f.lit("GTiff"), f.lit(bool(heavy)))
        )
    )
    if heavy:
        # The light read gives NULL for a tile it cannot read, and a heavyweight step gives an error tile for a
        # failure: either would become a (0, 0) row, where L3 fails the query. Fail it here, naming the cause.
        tower = f.col("tower_id").cast("string")
        piped = piped.withColumn(
            "_dem",
            _raise_if_null(
                f.col("_dem"),
                f.concat(
                    f.lit("rst_viewshed_towers: a DSM tile for tower "),
                    tower,
                    f.lit(" could not be read, one of: "),
                    _window_tiles("observer_x", "observer_y"),
                ),
                f.col("_dem"),
            ),
        ).withColumn("_vs", viewshed())
    else:
        piped = piped.withColumn("_vs", viewshed())
    # An unreadable DSM tile fails the query at every level: the viewshed of a tile op that cannot read its DEM is the
    # row's error tile (no raster), which the histogram would count as a (0, 0) row.
    tower = f.col("tower_id").cast("string")
    piped = piped.withColumn(
        "_vs",
        _raise_if_null(
            f.col("_vs")["raster"],
            f.concat(
                f.lit("rst_viewshed_towers: the viewshed of tower "),
                tower,
                f.lit(" failed: "),
                f.coalesce(
                    f.col("_vs")["metadata"]["last_error"], f.lit("no reason given")
                ),
            ),
            f.col("_vs"),
        ),
    )
    piped = piped.withColumn("_hist", histogram("_vs"))

    # Histogram bucket 1 = [128, 256) = the visible class (255); bucket 0 = invisible (0).
    vis = f.col("_hist")["band_1"].getItem(1)
    invis = f.col("_hist")["band_1"].getItem(0)
    out_cols = [
        f.col("tower_id").alias(tower_id_col),
        f.col("observer_x"),
        f.col("observer_y"),
        f.coalesce(vis, f.lit(0)).cast("long").alias("visible_px"),
        f.coalesce(cells("_vs", vis, invis), f.lit(0)).cast("long").alias("total_px"),
    ]

    if to_crs is not None:
        from databricks.labs.gbx.rasterx.functions import rst_transform

        piped = piped.withColumn(
            "_vs_reproj", rst_transform("_vs", f.lit(int(to_crs)), materialize=True)
        ).withColumn("_hist_reproj", histogram("_vs_reproj"))
        vis_r = f.col("_hist_reproj")["band_1"].getItem(1)
        invis_r = f.col("_hist_reproj")["band_1"].getItem(0)
        out_cols += [
            f.coalesce(vis_r, f.lit(0)).cast("long").alias("visible_px_reproj"),
            f.coalesce(cells("_vs_reproj", vis_r, invis_r), f.lit(0))
            .cast("long")
            .alias("total_px_reproj"),
        ]

    return with_frac(piped.select(*out_cols))


def rst_memsize_grouped(df, *, tile_col: str = "tile", out_col: str = "memsize"):
    """Partition-scoped rst_memsize for virtual/FILE tiles: amortizes source opens
    across a partition via the grouped executor.

    Returns the same value as per-row ``rst_memsize`` for **virtual** tiles (the
    decoded-window footprint = ``count * width * height * itemsize``).  This is
    the intended input: path-backed tiles where the open cost dominates.

    Note — for already-materialized tiles per-row ``rst_memsize`` returns the
    serialized buffer length instead; the grouped form (which decodes via rasterio)
    is **not** equivalent for that case and is not intended for it."""
    import numpy as np
    from pyspark.sql.types import LongType, StructField

    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    def _core(ds, cellid):  # noqa: ARG001 — cellid passed by executor contract
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, LongType()),
        tile_col=tile_col,
    )


def rst_clip_grouped(
    df,
    geom_wkb,
    all_touched: bool = False,
    clip_crs=None,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_clip via the grouped executor (FILE pixel fast path).

    Clips every tile in *df* to *geom_wkb* (WKB, EWKB, WKT, or EWKT) by routing
    pixel reads through ``grouped_tile_map`` with ``view="pixels"``.  Source opens
    are amortized per partition: each unique FILE source is opened once and reused
    for all windows that share it, making this the preferred form for virtual/FILE
    tiles where the open cost dominates.

    *geom_wkb* is a driver-side constant (one geometry for all tiles in the group),
    not a per-row column.  Per-row geometry is handled by the scalar ``rst_clip``.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct); null when the clip geometry does not overlap the tile.
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import edit
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    # Capture raw bytes in closure; parse inside _core so shapely objects are
    # never pickled across the Spark worker boundary.
    def _core(ds, cellid):
        from databricks.labs.gbx._geom import parse_geom

        geom = parse_geom(geom_wkb)
        if geom is None:
            return None
        new_bytes = edit.clip_to_geom(ds, geom, bool(all_touched), geom_crs=clip_crs)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_transform_grouped(
    df,
    target_srid,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_transform via the grouped executor (FILE pixel fast path).

    Reprojects every tile in *df* to *target_srid* (int EPSG code) by routing
    pixel reads through ``grouped_tile_map`` with ``view="pixels"``.  Source opens
    are amortized per partition, making this the preferred form for virtual/FILE
    tiles where the open cost dominates.

    *target_srid* is a driver-side constant (one EPSG code for all tiles).

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import warp
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    def _core(ds, cellid):
        new_bytes = warp.reproject_to_srid(ds, int(target_srid))
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_to_webmercator_grouped(
    df,
    resampling: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_to_webmercator via the grouped executor (FILE pixel fast path).

    Reprojects every tile in *df* to EPSG:3857 (web mercator) with the given
    *resampling* algorithm by routing pixel reads through ``grouped_tile_map``
    with ``view="pixels"``.  Source opens are amortized per partition.

    *resampling* is a driver-side constant (default ``"bilinear"``).

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import warp
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _resampling = str(resampling)

    def _core(ds, cellid):
        new_bytes = warp.reproject_to_srid(ds, 3857, resampling=_resampling)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_transformcrs_grouped(
    df,
    crs_value,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_transformcrs via the grouped executor (FILE pixel fast path).

    Reprojects every tile in *df* to *crs_value* (CRS string such as ``"EPSG:3857"``
    or a WKT string) by routing pixel reads through ``grouped_tile_map`` with
    ``view="pixels"``.  Source opens are amortized per partition.

    *crs_value* is a driver-side constant (one CRS for all tiles).

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import warp
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _crs = str(crs_value)

    def _core(ds, cellid):
        new_bytes = warp.reproject_to_crs(ds, _crs)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_resample_grouped(
    df,
    factor,
    algorithm: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_resample via the grouped executor (FILE pixel fast path).

    Resamples every tile in *df* by a multiplicative *factor* (>1 upsamples,
    0<factor<1 downsamples) using the given *algorithm*.  Pixel reads are routed
    through ``grouped_tile_map`` with ``view="pixels"``; source opens are amortized
    per partition.

    *factor* and *algorithm* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import resample
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _alg = str(algorithm)

    def _core(ds, cellid):
        new_bytes = resample.resample_by_factor(ds, float(factor), _alg)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_resample_to_size_grouped(
    df,
    width_px,
    height_px,
    algorithm: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_resample_to_size via the grouped executor (FILE pixel fast path).

    Resamples every tile in *df* to exactly *width_px* × *height_px* pixels using
    *algorithm*.  Pixel reads are routed through ``grouped_tile_map`` with
    ``view="pixels"``; source opens are amortized per partition.

    *width_px*, *height_px*, and *algorithm* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import resample
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _alg = str(algorithm)

    def _core(ds, cellid):
        new_bytes = resample.resample_to_size(ds, int(width_px), int(height_px), _alg)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_resample_to_res_grouped(
    df,
    x_res,
    y_res,
    algorithm: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_resample_to_res via the grouped executor (FILE pixel fast path).

    Resamples every tile in *df* to the target pixel resolution (*x_res*, *y_res*)
    using *algorithm*.  Pixel reads are routed through ``grouped_tile_map`` with
    ``view="pixels"``; source opens are amortized per partition.

    *x_res*, *y_res*, and *algorithm* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import resample
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _alg = str(algorithm)

    def _core(ds, cellid):
        new_bytes = resample.resample_to_res(ds, float(x_res), float(y_res), _alg)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_threshold_grouped(
    df,
    op: str = ">",
    value: float = 0.0,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_threshold via the grouped executor (FILE pixel fast path).

    Applies a pixel threshold to every tile in *df*, setting pixels that do not
    satisfy ``pixel <op> value`` to NoData.  Pixel reads are routed through
    ``grouped_tile_map`` with ``view="pixels"``; source opens are amortized per
    partition.

    *op* (one of ``">"``, ``"<"``, ``">="`` ``"<="`` ``"=="`` ``"!="``) and
    *value* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import edit
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _op = str(op)
    _val = float(value)

    def _core(ds, cellid):
        new_bytes = edit.threshold(ds, _op, _val)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_updatetype_grouped(
    df,
    new_type,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_updatetype via the grouped executor (FILE pixel fast path).

    Casts all raster bands in every tile to *new_type* (GDAL data type string such
    as ``"Float64"`` or ``"Int16"``).  Pixel reads are routed through
    ``grouped_tile_map`` with ``view="pixels"``; source opens are amortized per
    partition.

    *new_type* is a driver-side constant.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import edit
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _new_type = str(new_type)

    def _core(ds, cellid):
        new_bytes = edit.update_type(ds, _new_type)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_slope_grouped(
    df,
    unit: str = "degrees",
    xscale=None,
    yscale=None,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_slope via the grouped executor (FILE pixel fast path).

    Computes terrain slope (Horn's 3x3 method) for every tile in *df*.  Pixel
    reads are routed through ``grouped_tile_map`` with ``view="pixels"``; source
    opens are amortized per partition.

    *unit* (``"degrees"`` or ``"percent"``), *xscale*, and *yscale* are
    driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import terrain
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _unit = str(unit)
    _xs = None if xscale is None else float(xscale)
    _ys = None if yscale is None else float(yscale)

    def _core(ds, cellid):
        new_bytes = terrain.slope(ds, unit=_unit, xscale=_xs, yscale=_ys)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_aspect_grouped(
    df,
    trigonometric=False,
    zero_for_flat=False,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_aspect via the grouped executor (FILE pixel fast path).

    Computes terrain aspect (Horn's 3x3 method) for every tile in *df*.  Pixel
    reads are routed through ``grouped_tile_map`` with ``view="pixels"``; source
    opens are amortized per partition.

    *trigonometric* and *zero_for_flat* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import terrain
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _trig = bool(trigonometric)
    _zff = bool(zero_for_flat)

    def _core(ds, cellid):
        new_bytes = terrain.aspect(ds, trigonometric=_trig, zero_for_flat=_zff)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_hillshade_grouped(
    df,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z_factor: float = 1.0,
    xscale=None,
    yscale=None,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_hillshade via the grouped executor (FILE pixel fast path).

    Computes hillshade (Horn's 3x3 method) for every tile in *df*.  Pixel reads
    are routed through ``grouped_tile_map`` with ``view="pixels"``; source opens
    are amortized per partition.

    *azimuth*, *altitude*, *z_factor*, *xscale*, and *yscale* are driver-side
    constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core import terrain
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
    from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

    _az = float(azimuth)
    _alt = float(altitude)
    _zf = float(z_factor)
    _xs = None if xscale is None else float(xscale)
    _ys = None if yscale is None else float(yscale)

    def _core(ds, cellid):
        new_bytes = terrain.hillshade(
            ds,
            azimuth=_az,
            altitude=_alt,
            z_factor=_zf,
            xscale=_xs,
            yscale=_ys,
        )
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def bin_points_tiled(
    df, *, x, y, z, by, xmin, ymin, xmax, ymax, width, height, srid, stat="max"
):
    """Bounded-memory tiled point binning — memory is the raster grid (W×H), not the
    point count. Two-stage native pre-aggregation: per-point pixel index -> native
    groupBy(tile, cell).agg(stat) -> place the <=W*H per-cell values via
    _light_rst_binpoints_agg. Output matches df.groupBy(by).agg(_light_rst_binpoints_agg(x,y,z,...,
    stat)) for max/min/mean/count. median/percentile delegate to that buffered
    aggregator (memory-heavy; use decimate or smaller tiles). Returns [*by, 'tile']."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx.functions import (  # noqa: PLC0415
        _light_rst_binpoints_agg,
    )

    if isinstance(by, str):
        by = [by]
    w, h, sid, st = int(width), int(height), int(srid), str(stat).lower()

    if st == "median" or st.startswith("percentile"):
        return df.groupBy(*by).agg(
            _light_rst_binpoints_agg(
                x, y, z, xmin, ymin, xmax, ymax, F.lit(w), F.lit(h), F.lit(sid), stat
            ).alias("tile")
        )

    xr = F.col(xmax) - F.col(xmin)
    yr = F.col(ymax) - F.col(ymin)
    col = F.floor((F.col(x) - F.col(xmin)) / xr * F.lit(w)).cast("int")
    row = F.floor((F.col(ymax) - F.col(y)) / yr * F.lit(h)).cast("int")
    celled = (
        df.withColumn("_col", col)
        .withColumn("_row", row)
        .where(
            (F.col("_col") >= 0)
            & (F.col("_col") < F.lit(w))
            & (F.col("_row") >= 0)
            & (F.col("_row") < F.lit(h))
        )
        .withColumn("_cx", F.col(xmin) + (F.col("_col") + F.lit(0.5)) * xr / F.lit(w))
        .withColumn("_cy", F.col(ymax) - (F.col("_row") + F.lit(0.5)) * yr / F.lit(h))
    )

    # NULL-z parity with the reference (rst_binpoints / binning.py): count includes
    # every in-bounds point regardless of z, and a NULL z contaminates the cell's
    # mean to NoData (emit NaN, which the placement's fmax skips -> the cell stays
    # NoData). max/min already match — Spark max/min ignore NULL like the reference.
    reducer = {
        "max": F.max(F.col(z)),
        "min": F.min(F.col(z)),
        "mean": F.when(
            F.max(F.col(z).isNull().cast("int")) == 1, F.lit(float("nan"))
        ).otherwise(F.avg(F.col(z))),
        "count": F.count(F.lit(1)).cast("double"),
    }
    if st not in reducer:
        raise ValueError(f"bin_points_tiled: unsupported statistic {stat!r}")
    per_cell = celled.groupBy(*by, xmin, ymin, xmax, ymax, "_cx", "_cy").agg(
        reducer[st].alias("_v")
    )

    return per_cell.groupBy(*by).agg(
        _light_rst_binpoints_agg(
            "_cx",
            "_cy",
            "_v",
            xmin,
            ymin,
            xmax,
            ymax,
            F.lit(w),
            F.lit(h),
            F.lit(sid),
            "max",
        ).alias("tile")
    )


def rst_h3_gridspec(
    df,
    cell_col="cellid",
    *group_cols,
    out_srid=4326,
    pixel_size=None,
    mode="centroids",
    kring_pad=1,
    out_crs=None,
):
    """Snapped shared-canvas grid spec per group of H3 cells.

    Returns the grouped DataFrame with a ``grid`` column of type
    ``STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
    pixel_size DOUBLE, width INT, height INT, srid INT>``.

    Implemented as per-cell bbox expansion (via a scalar UDF) + native Spark
    min/max aggregation + snap arithmetic from ``cellraster.snap_bounds``,
    so it is Serverless-safe (no ``spark.conf.set``, no JVM access).

    Args:
        df:          Input DataFrame.
        cell_col:    Column name holding H3 cell IDs (integer).
        *group_cols: Additional grouping columns (e.g. a tile/region key).
        out_srid:    Output CRS as an EPSG or ESRI code (int). Defaults to 4326.
        pixel_size:  Ground resolution in CRS units. ``None`` = auto from H3
                     resolution (edge-length heuristic, same as
                     ``cellraster.compute_gridspec``).
        mode:        ``"centroids"`` (default) or ``"spatial_envelope"``.
        kring_pad:   k-ring padding applied per cell before computing its bbox.
                     Defaults to 1 (matches ``compute_gridspec`` default).
        out_crs:     Output CRS string (``EPSG:x`` / ``ESRI:x``); wins over
                     ``out_srid``. The ``grid.srid`` field carries its EPSG code
                     when it has one (else ``out_srid``), while the bbox
                     coordinates are computed in the resolved CRS.

    Returns:
        DataFrame grouped by *group_cols* with a ``grid`` struct column added.
    """
    from databricks.labs.gbx.pyrx._udf import _col
    from databricks.labs.gbx.pyrx.core import cellraster as cellraster_core
    from databricks.labs.gbx.pyrx.functions import (  # noqa: PLC0415
        _GRID_SCHEMA,
        _h3_cell_bbox_udf,
    )

    # Resolve the output-CRS spec once (out_crs wins). The bbox coords use the
    # resolved CRS; the grid.srid INT field carries its EPSG code when available.
    _out_spec = out_crs if out_crs is not None else out_srid
    _resolved = cellraster_core._norm_out_crs(_out_spec)
    _is_geo = bool(_resolved.is_geographic)
    from databricks.labs.gbx.core.crs import authority_srid_of

    _epsg = authority_srid_of(_resolved)
    _srid_field = int(_epsg) if _epsg is not None else int(out_srid)
    # Sample one cell on the driver to obtain the H3 resolution for auto pixel_size.
    # An empty input is always an error: there is nothing to rasterize onto.
    _res = None
    _pixel_size = pixel_size
    if _pixel_size is None:
        first_row = df.select(cell_col).first()
        if first_row is None or first_row[0] is None:
            raise ValueError("empty cell set")
        sample_str = cellraster_core._h3_str(int(first_row[0]))
        import h3 as _h3_driver

        _res = _h3_driver.get_resolution(sample_str)

    # Per-cell expanded bbox (kring_pad applied inside the scalar UDF). The bbox is
    # computed in the resolved output CRS: pass out_crs when set (string wins),
    # else the int out_srid. _h3_cell_bbox_udf's out_crs wins over its srid arg.
    if out_crs is not None:
        b = _h3_cell_bbox_udf(
            _col(cell_col), f.lit(None), f.lit(mode), f.lit(kring_pad), f.lit(out_crs)
        )
    else:
        b = _h3_cell_bbox_udf(
            _col(cell_col), f.lit(out_srid), f.lit(mode), f.lit(kring_pad)
        )
    gcols = list(group_cols)
    enriched = df.withColumn("_bb", b)
    agg_expr = enriched.groupBy(*gcols) if gcols else enriched.groupBy()
    bounds = agg_expr.agg(
        f.min("_bb.xmin").alias("_xmin"),
        f.min("_bb.ymin").alias("_ymin"),
        f.max("_bb.xmax").alias("_xmax"),
        f.max("_bb.ymax").alias("_ymax"),
    )

    # Capture closure values for the snap UDF (driver-side constants).
    _srid_val = _srid_field

    @f.udf(_GRID_SCHEMA)
    def _snap_to_grid(bxmin, bymin, bxmax, bymax):
        if bxmin is None:
            return None
        import math as _math

        import h3 as _h3

        ps = _pixel_size
        if ps is None:
            mid_lat = (float(bymin) + float(bymax)) / 2.0
            edge_m = (
                _h3.average_hexagon_edge_length(_res, unit="m")
                if _res is not None
                else 1.0
            )
            if _is_geo:
                ps = edge_m / (111320.0 * max(_math.cos(_math.radians(mid_lat)), 1e-6))
            else:
                ps = edge_m

        bxmin_f = float(bxmin)
        bymin_f = float(bymin)
        bxmax_f = float(bxmax)
        bymax_f = float(bymax)

        xmin = _math.floor(bxmin_f / ps) * ps
        ymax = _math.ceil(bymax_f / ps) * ps
        width = max(1, int(_math.ceil((bxmax_f - xmin) / ps)))
        height = max(1, int(_math.ceil((ymax - bymin_f) / ps)))
        xmax = xmin + width * ps
        ymin = ymax - height * ps
        return (xmin, ymin, xmax, ymax, ps, width, height, _srid_val)

    return bounds.withColumn(
        "grid",
        _snap_to_grid(f.col("_xmin"), f.col("_ymin"), f.col("_xmax"), f.col("_ymax")),
    ).drop("_xmin", "_ymin", "_xmax", "_ymax")


def __getattr__(name: str):
    """``tile_range`` and ``prepare_cogs`` load on first use: their modules need rasterio, and importing the helpers
    must not. ``prepare_cogs`` is the one public path of the driver-side COG preparer (same signature and docstring).
    """
    if name == "tile_range":
        from databricks.labs.gbx.pyrx.core.tiling import tile_range

        return tile_range
    if name == "prepare_cogs":
        from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs

        return prepare_cogs
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
