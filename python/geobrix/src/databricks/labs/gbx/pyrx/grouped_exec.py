"""Partition-scoped grouped execution for FILE/virtual tiles (light tier).

Amortizes the dominant cost (the source OPEN) by grouping a source raster's
tiles into one partition, contiguous, and reading them from one cached open
resource. Pure Python + mapInPandas -- no .rdd / sc / spark.conf.set.

The FILE read engine (LRU, opener context, staging) has been relocated to
databricks.labs.gbx.ds.file_gbx for unified access. This module re-exports them
and the consumer functions (grouped_tile_map, _run_file_fast_path) for backward
compatibility.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from databricks.labs.gbx._gdal_policy import open_data
from databricks.labs.gbx.ds.file_gbx import (  # noqa: F401 (re-exported for backward compatibility)
    GBX_LRU_MAX_BYTES,
    GBX_STREAM_MAX_BYTES,
    STREAM_NOMINAL_BYTES,
    OpenResourceLRU,
    _connect_aware_lru_sizing,
    _open_via_file_ref,
    _OpenerContext,
    file_ref_arg,
    file_supported,
)
from databricks.labs.gbx.zarr._tiles import is_zarr_tile

# Sentinel for "core_fn has not been called yet for this tile".
# Distinct from None so a core_fn that legitimately returns None
# is not re-routed through the fallback (M1 fix).
_MISS = object()


def _tile_is_windowless_virtual(tile) -> bool:
    """True if *tile* is a virtual tile (path set, raster None) with no window.

    Used in the fallback path to route windowless virtual tiles to ``open_header``
    (which returns the full-source footprint) instead of ``_open`` (which raises
    ``ValueError`` for windowless virtual tiles).
    """
    try:
        return (
            tile["raster"] is None
            and tile["path"] is not None
            and tile["window"] is None
        )
    except (KeyError, TypeError):
        return False


def _has_clip(tile) -> bool:
    """True if *tile* carries a ``clip_polygon``.

    On a virtual tile that is an instruction (a pending operation); only ``open_tile``
    applies it.
    """
    try:
        return tile["clip_polygon"] is not None
    except (KeyError, TypeError):
        return False


def align_partitions(
    df: DataFrame, *, n: int, path_col: str = "tile.path"
) -> DataFrame:
    """Hash-by-path repartition + sort so each source FILE is saturated in one
    partition and contiguous within it. `n` is parallelism-sized by the caller
    (3-5x worker cores on classic; a parallelism target on Serverless) -- never
    n_files, never sc-derived."""
    if n <= 0:
        raise ValueError(f"n must be a positive parallelism target, got {n}")
    col = F.col(path_col)
    return df.repartition(n, col).sortWithinPartitions(col)


def _run_file_fast_path(
    fr_holder, size_holder, lru, uri, fr, tile, cellid, view_mode, core_fn
):
    """Execute the FILE fast-path for one tile. Returns result; raises on failure.

    Sets ``fr_holder[0] = fr`` and ``size_holder[0] = <metadata_size>`` before
    calling ``lru.get(uri)`` so the opener can stream-open the source on a cache
    miss and the weigher can prefer the pre-known size. Dispatches to ``core_fn``
    via either a read-free ``_WindowHeaderView`` (``view_mode="header"``) or a
    real windowed ``DatasetReader`` materialised from the cached source
    (``view_mode="pixels"``).

    Returns ``_MISS`` when the tile's target ``crs`` differs from its source's: the
    window of the source as it is would be the wrong raster, and ``open_tile`` (the
    fallback) reprojects. A pending clip is excluded earlier (``_file_fast_path_uri``).

    Caller wraps in ``try/except Exception`` and sets ``result = _MISS`` so the
    fallback path can take over on any open / stream / view-construction failure.
    """
    fr_holder[0] = fr
    from ._env import FOLDER_FILES_KEY, note_folder_files
    from .core.open_tile import (
        _has_pending,
        _parse_pending,
        _read_size_key,
        _to_virtual_tile,
        _warp_pending,
        _WindowHeaderView,
    )

    vt = _to_virtual_tile(tile)
    # the reader's count of files in the tile's folder: lets the opener skip listing a
    # folder past GDAL's listing limit (a hint only; see _env.note_folder_files)
    note_folder_files(vt.path, (vt.metadata or {}).get(FOLDER_FILES_KEY))
    bands, raw_nodata, srid, crs_str = _parse_pending(vt.metadata)
    # Extract metadata-derived size (path_file_size or tile_size) before lru.get()
    # so the weigher can use it instead of calling fr.size.
    metadata_size = _read_size_key(vt.metadata, "path_file_size") or _read_size_key(
        vt.metadata, "tile_byte_size"
    )
    size_holder[0] = metadata_size
    src = lru.get(uri)
    if _warp_pending(vt, src):
        # The tile asks for another CRS than its source has; this path reads the
        # source as it is, so open_tile (via the fallback) applies the reprojection.
        return _MISS
    from rasterio.windows import Window

    pending_count = len(bands) if bands else None
    pending_crs = None
    if crs_str is not None or srid is not None:
        from .core.crs import resolve_crs

        pending_crs = resolve_crs(crs_str if crs_str is not None else srid)
    win = None
    if vt.window is not None:
        c, r_off, w, h = vt.window
        win = Window(c, r_off, w, h)
    elif _has_pending(vt.metadata):
        # A whole-file tile with a pending instruction (rst_initnodata, rst_band,
        # rst_setsrid, rst_setcrs) is its whole extent as a window: the instruction is
        # applied to it, as open_tile does, not skipped.
        win = Window(0, 0, src.width, src.height)
    if win is not None:
        if view_mode == "pixels":
            # Materialise the window into GTiff bytes, open as a real
            # DatasetReader inside an ExitStack, and pass to core_fn.
            from contextlib import ExitStack

            import rasterio

            from .core.open_tile import _window_dataset_bytes

            pending = (bands, raw_nodata, srid, crs_str)
            b = _window_dataset_bytes(src, win, pending)
            with ExitStack() as _stk:
                ds = _stk.enter_context(open_data(rasterio.io.MemoryFile(b)))
                return core_fn(ds, cellid)
        else:
            # view="header": read-free header view.
            header_view = _WindowHeaderView(
                src,
                win,
                pending_count=pending_count,
                pending_crs=pending_crs,
                pending_nodata=raw_nodata,
            )
            return core_fn(header_view, cellid)
    else:
        # No sub-window and nothing pending: full source; src is a complete
        # DatasetReader. For view="pixels" src is already a full DatasetReader; for
        # view="header" we pass it directly (no sub-window means full-source
        # dims are correct — a WindowHeaderView would be a no-op wrapper).
        return core_fn(src, cellid)


def _run_fallback_tile(tile, cellid, view_mode, core_fn):
    """Execute the per-tile fallback for one tile. Returns result; raises on failure.

    Covers:
      (a) materialized tiles (raster inline; a ``path`` they carry is provenance)
      (b) virtual tiles when driver did not add _file_ref (FILE not supported)
      (c) FILE open/stream/view failure (graceful degrade from fast path)
      (d) virtual tiles with a pending clip or target CRS (the fast path reads the
          source as it is; ``open_tile`` applies the instructions)

    A windowless virtual tile with no clip goes to ``open_header``, which returns the
    full-source footprint (consistent with per-row ``rst_memsize``) unless its target
    ``crs`` asks for a reprojection of that source. With a clip, or such a ``crs``, it
    goes to ``_open``: the footprint is the clipped or reprojected one. With
    ``view="pixels"`` and a pending instruction (``rst_initnodata``, ``rst_band``,
    ``rst_setsrid``, ``rst_setcrs``) it goes to ``_open`` as well: ``open_header``
    shows the instruction's effect on the header but refuses ``read``.

    view contract:
      ``view="header"``: wrap the open DatasetReader in a full-extent
        ``_WindowHeaderView`` so ``core_fn`` consistently receives a read-blocking
        view regardless of tile type.
      ``view="pixels"``: pass the real DatasetReader directly.

    Caller wraps in ``try/except Exception`` and sets ``result = None`` on failure
    so per-tile errors degrade gracefully rather than crashing the whole partition.
    """
    from .core.open_tile import (
        _has_pending,
        _open,
        _to_virtual_tile,
        _warp_pending,
        open_header,
    )

    # Pending instructions only concern a virtual tile, so they are read only for one: a
    # materialized tile with a malformed pending key is read from its bytes, not nulled.
    windowless = _tile_is_windowless_virtual(tile) and not _has_clip(tile)
    if windowless and (
        view_mode == "header" or not _has_pending(_to_virtual_tile(tile).metadata)
    ):
        with open_header(tile) as ds:
            # The header view of a relabelled source wraps the file's own dataset; the
            # reprojection rule compares the target with the file's CRS (see open_tile).
            if not _warp_pending(_to_virtual_tile(tile), getattr(ds, "_src", ds)):
                return _call_core(ds, cellid, view_mode, core_fn)
        # a target CRS the source does not have: read it through open_tile
    with _open(tile) as ds:
        return _call_core(ds, cellid, view_mode, core_fn)


def _call_core(ds, cellid, view_mode, core_fn):
    """Run *core_fn* on an open dataset under the fallback's view contract: a
    read-blocking full-extent ``_WindowHeaderView`` for ``view="header"``, the real
    dataset for ``view="pixels"``."""
    if view_mode == "header":
        from rasterio.windows import Window as _Win

        from .core.open_tile import _WindowHeaderView

        return core_fn(_WindowHeaderView(ds, _Win(0, 0, ds.width, ds.height)), cellid)
    return core_fn(ds, cellid)


def _file_fast_path_uri(tile):
    """The source path the FILE fast path may read for *tile*, else ``None``.

    Only a virtual tile (``raster`` is ``None``) reads its source: there the
    path IS the data. A materialized tile's ``raster`` holds its computed
    bytes, and any ``path`` it carries is provenance (reader tiles,
    ``materialize=True`` outputs, whole-input retile outputs). The source may
    differ from the bytes (a baked-in ``rst_setsrid``, ``rst_band`` or
    ``rst_initnodata``, or computed pixels), so such a tile is read from its bytes.

    A virtual tile with a pending ``clip_polygon`` is not read from its source as it
    is either (``open_tile``'s FILE branch makes the same exception): the fallback
    applies the clip. (A target ``crs`` needs the open source to compare with; see
    ``_run_file_fast_path``.)
    """
    try:
        if tile["raster"] is not None:
            return None
    except (KeyError, TypeError):
        pass
    if is_zarr_tile(tile):  # its path is a store of chunks, not a file to open
        return None
    if _has_clip(tile):
        return None
    try:
        return tile["path"] or None
    except (KeyError, TypeError):
        return None


def _dispatch_tile(tile, fr, ctx, lru, view_mode, core_fn):
    """Run *core_fn* on one tile: FILE fast path when it applies, else fallback.

    *fr* is the tile's FileRef (``None`` when the driver minted none).
    Returns the core_fn value, or ``None`` when the tile cannot be read.
    """
    # Extract cellid from the tile struct for passing to core_fn.
    try:
        cellid = tile["cellid"]
    except (KeyError, TypeError):
        cellid = None
    uri = _file_fast_path_uri(tile)

    result = _MISS
    if uri and fr is not None:
        # FILE fast path: inject FileRef so the LRU opener can
        # stream-open the source on a cache miss.
        try:
            result = _run_file_fast_path(
                ctx.fr_holder,
                ctx.size_holder,
                lru,
                uri,
                fr,
                tile,
                cellid,
                view_mode,
                core_fn,
            )
        except Exception:
            # Any open/stream/view-construction failure →
            # degrade gracefully to the per-tile fallback.
            result = _MISS

    if result is _MISS:
        try:
            result = _run_fallback_tile(tile, cellid, view_mode, core_fn)
        except Exception:
            result = None
    return result


def grouped_tile_map(
    df: DataFrame,
    core_fn,
    *,
    return_field: StructField,
    tile_col: str = "tile",
    view: str = "header",
) -> DataFrame:
    """Partition-scoped ``mapInPandas`` executor for light-tier tiles.

    For each tile in the partition, opens the raster source, applies
    ``core_fn(ds_or_view, cellid) -> value`` on the open ``DatasetReader`` (or a
    read-free ``_WindowHeaderView`` on the FILE fast path), and stores the result
    in a new column ``return_field.name``.

    ``cellid`` is the tile's ``cellid`` field, passed so tile-returning ops can
    stamp the output tile with the input cell identity.

    The ``view`` keyword controls what ``core_fn`` receives on the FILE fast path:

    - ``view="header"`` (default): ``core_fn`` receives a ``_WindowHeaderView``
      whose ``.read()`` raises — header attributes only, no pixel I/O.
    - ``view="pixels"``: the window is materialised from the cached source via
      ``_window_dataset_bytes``, opened as a real ``DatasetReader``, and passed
      to ``core_fn``.  On the fallback path (materialized tiles / non-FILE /
      open failure) ``core_fn`` always receives a real ``DatasetReader``
      regardless of ``view``.

    Tile dispatch:

    - **Materialized tiles** (``raster`` set): opened per-row via ``_open``
      from their bytes; the LRU is not consulted — bytes are already inline,
      nothing to amortise.  A materialized tile may still carry ``path`` as
      provenance of where its bytes came from; it is never read in their place.
      This is the path exercised by the local unit test.
    - **Virtual tiles** (``raster`` None, ``path`` set) + FILE-capable: before
      ``mapInPandas`` a ``_file_ref`` column is added holding a ``try_to_file(path)``
      FileRef for each row.  Inside the partition the LRU caches the open rasterio
      dataset (opened from the FileRef's seekable stream) keyed by ``uri``; on the
      FILE fast path the view contract (header/pixels) determines what is passed to
      ``core_fn``.  ``_file_ref`` is stripped from the output so the output
      schema is unchanged.  On any FileRef open or view-construction failure,
      degrades per-tile to the fallback path below.  A tile with a pending
      ``clip_polygon``, or a target ``crs`` its source does not have, skips the fast
      path: only ``open_tile`` applies those instructions.
    - **Virtual tiles** + fallback (non-FILE / open failure / pending clip or CRS):
      opened per-row via ``_open``; no amortisation — each tile's window is unique.

    The per-row choice between these paths is ``_dispatch_tile``'s, and nothing else's.

    ``core_fn(ds_or_view, cellid)`` must not hold a reference to its first arg
    after returning.

    Output schema is ``df.schema`` with ``return_field`` appended (or replacing
    an existing field of the same name); ``_file_ref`` never leaks.
    """
    # Capture original fields BEFORE adding _file_ref so the output schema
    # matches the caller's expectation exactly.
    original_fields = list(df.schema.fields)
    # Replace a colliding field rather than appending a duplicate.  When
    # out_col equals an existing input column name (e.g. out_col="tile" —
    # the default for rst_clip_grouped and the 11 pixel-op _grouped variants),
    # the pandas _map yields a df with ONE column of that name (via .assign
    # overwrite), so out_schema must also have ONE field.  Appending a
    # duplicate causes Arrow schema mismatch on real clusters.
    out_schema = StructType(
        [f for f in original_fields if f.name != return_field.name] + [return_field]
    )
    out_name = return_field.name

    # Add the FileRef column on the driver before mapInPandas.  Each row
    # gets try_to_file(tile.path); a tile without a path gets NULL.  A materialized
    # tile may carry a path as provenance and gets a FileRef too, which is never
    # used: its bytes are read, not its source (see _file_fast_path_uri).
    # Pass df.sparkSession explicitly so file_supported() doesn't need to
    # resolve the session via getActiveSession() (which can return None on
    # Spark Connect / DBR 14+ in some threading contexts).
    # file_supported() is memoized per SparkSession and is cheap after the
    # first call.
    try:
        _driver_spark = df.sparkSession
    except Exception:
        _driver_spark = None
    file_ok_driver = file_supported(_driver_spark)
    if file_ok_driver:
        # file_ref_arg expects the TILE column and extracts ["path"] internally.
        df = df.withColumn(
            "_file_ref", file_ref_arg(F.col(tile_col), spark=_driver_spark)
        )

    # Compute Connect-aware sizing on the driver; capture in the closure.
    _stream_max, _lru_max, _lru_count = _connect_aware_lru_sizing(_driver_spark)

    from databricks.labs.gbx.core import proj_grids

    _grid_dirs = tuple(proj_grids.get_registered_dirs())  # captured at build → pickled

    def _map(pdf_iter):
        from . import _env

        _env.configure_gdal_env(extra_proj_dirs=_grid_dirs)
        _ctx = _OpenerContext(stream_max_bytes=_stream_max)
        lru = OpenResourceLRU(
            opener=_ctx.open,
            closer=_ctx.close,
            weigher=_ctx.weigh,
            max_bytes=_lru_max,
            max_count=_lru_count,
        )
        # Capture the view kwarg in the closure so the inner loop can use it
        # without shadowing the local variable 'view' with a _WindowHeaderView
        # instance.
        _view_mode = view
        try:
            for pdf in pdf_iter:
                results = []
                # has_fr_col is the worker-side FILE capability signal: the driver
                # added _file_ref only when file_ok_driver was True (checked via an
                # explicit df.sparkSession).  We key off column presence rather than
                # a worker-side file_supported() call because getActiveSession()
                # returns None on Spark-Connect worker threads (e.g. Serverless
                # DBR 19.5), which would silently defeat the FILE fast path.
                has_fr_col = "_file_ref" in pdf.columns
                for _, row in pdf.iterrows():
                    fr = row["_file_ref"] if has_fr_col else None
                    results.append(
                        _dispatch_tile(
                            row[tile_col], fr, _ctx, lru, _view_mode, core_fn
                        )
                    )

                # Strip _file_ref from the output so the schema matches
                # original_fields + [return_field] exactly.
                out_pdf = pdf.drop(columns=["_file_ref"], errors="ignore")
                yield out_pdf.assign(**{out_name: results})
        finally:
            lru.close_all()

    return df.mapInPandas(_map, schema=out_schema)
