"""Partition-scoped grouped execution for FILE/virtual tiles (light tier).

Amortizes the dominant cost (the source OPEN) by grouping a source raster's
tiles into one partition, contiguous, and reading them from one cached open
resource. Pure Python + mapInPandas -- no .rdd / sc / spark.conf.set.

The FILE read engine (LRU, opener context, staging) has been relocated to
databricks.labs.gbx.ds.file_gbx for unified access. This module re-exports them
and the consumer functions (grouped_tile_map, _run_file_fast_path) for backward
compatibility.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

# Re-export the FILE read engine from the unified base for backward compatibility
from databricks.labs.gbx.ds.file_gbx import (  # noqa: F401
    GBX_LRU_MAX_BYTES,
    GBX_STREAM_MAX_BYTES,
    STREAM_NOMINAL_BYTES,
    OpenResourceLRU,
    _connect_aware_lru_sizing,
    _open_via_file_ref,
    _OpenerContext,
)

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

    Caller wraps in ``try/except Exception`` and sets ``result = _MISS`` so the
    fallback path can take over on any open / stream / view-construction failure.
    """
    fr_holder[0] = fr
    from .core.open_tile import (
        _parse_pending,
        _read_size_key,
        _to_virtual_tile,
        _WindowHeaderView,
    )

    vt = _to_virtual_tile(tile)
    bands, raw_nodata, srid, crs_str = _parse_pending(vt.metadata)
    # Extract metadata-derived size (path_file_size or tile_size) before lru.get()
    # so the weigher can use it instead of calling fr.size.
    metadata_size = _read_size_key(vt.metadata, "path_file_size") or _read_size_key(
        vt.metadata, "tile_byte_size"
    )
    size_holder[0] = metadata_size
    src = lru.get(uri)
    from rasterio.windows import Window

    pending_count = len(bands) if bands else None
    pending_crs = None
    if crs_str is not None or srid is not None:
        from .core.crs import resolve_crs

        pending_crs = resolve_crs(crs_str if crs_str is not None else srid)
    if vt.window is not None:
        c, r_off, w, h = vt.window
        win = Window(c, r_off, w, h)
        if view_mode == "pixels":
            # Materialise the window into GTiff bytes, open as a real
            # DatasetReader inside an ExitStack, and pass to core_fn.
            from contextlib import ExitStack

            import rasterio

            from .core.open_tile import _window_dataset_bytes

            pending = (bands, raw_nodata, srid, crs_str)
            b = _window_dataset_bytes(src, win, pending)
            with ExitStack() as _stk:
                ds = _stk.enter_context(rasterio.open(rasterio.io.MemoryFile(b)))
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
        # No sub-window: full source; src is a complete DatasetReader.
        # For view="pixels" src is already a full DatasetReader; for
        # view="header" we pass it directly (no sub-window means full-source
        # dims are correct — a WindowHeaderView would be a no-op wrapper).
        return core_fn(src, cellid)


def _run_fallback_tile(tile, cellid, view_mode, core_fn):
    """Execute the per-tile fallback for one tile. Returns result; raises on failure.

    Covers:
      (a) materialized tiles (raster inline, path None)
      (b) virtual tiles when driver did not add _file_ref (FILE not supported)
      (c) FILE open/stream/view failure (graceful degrade from fast path)

    For windowless virtual tiles, ``_open`` raises ``ValueError``; ``open_header``
    returns the full-source footprint (consistent with per-row ``rst_memsize``).

    view contract:
      ``view="header"``: wrap the open DatasetReader in a full-extent
        ``_WindowHeaderView`` so ``core_fn`` consistently receives a read-blocking
        view regardless of tile type.
      ``view="pixels"``: pass the real DatasetReader directly.

    Caller wraps in ``try/except Exception`` and sets ``result = None`` on failure
    so per-tile errors degrade gracefully rather than crashing the whole partition.
    """
    from .core.open_tile import _open, open_header

    if _tile_is_windowless_virtual(tile):
        with open_header(tile) as ds:
            if view_mode == "header":
                from rasterio.windows import Window as _Win

                from .core.open_tile import _WindowHeaderView

                _hv = _WindowHeaderView(ds, _Win(0, 0, ds.width, ds.height))
                return core_fn(_hv, cellid)
            else:
                return core_fn(ds, cellid)
    else:
        with _open(tile) as ds:
            if view_mode == "header":
                from rasterio.windows import Window as _Win

                from .core.open_tile import _WindowHeaderView

                _hv = _WindowHeaderView(ds, _Win(0, 0, ds.width, ds.height))
                return core_fn(_hv, cellid)
            else:
                return core_fn(ds, cellid)


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

    - **Materialized tiles** (``raster`` set, ``path`` None): opened per-row via
      ``_open``; the LRU is not consulted — bytes are already inline, nothing
      to amortise.  This is the path exercised by the local unit test.
    - **Virtual tiles** (``path`` set) + FILE-capable: before ``mapInPandas`` a
      ``_file_ref`` column is added holding a ``try_to_file(path)`` FileRef for
      each row.  Inside the partition the LRU caches the open rasterio dataset
      (opened from the FileRef's seekable stream) keyed by ``uri``; on the FILE
      fast path the view contract (header/pixels) determines what is passed to
      ``core_fn``.  ``_file_ref`` is stripped from the output so the output
      schema is unchanged.  On any FileRef open or view-construction failure,
      degrades per-tile to the fallback path below.
    - **Virtual tiles** + fallback (non-FILE / open failure): opened per-row via
      ``_open``; no amortisation — each tile's window is unique.

    ``core_fn(ds_or_view, cellid)`` must not hold a reference to its first arg
    after returning.

    Output schema is ``df.schema`` with ``return_field`` appended (or replacing
    an existing field of the same name); ``_file_ref`` never leaks.
    """
    from . import _file_ref as _fr_mod

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
    # gets try_to_file(tile.path); materialized tiles (path=null) get NULL.
    # Pass df.sparkSession explicitly so file_supported() doesn't need to
    # resolve the session via getActiveSession() (which can return None on
    # Spark Connect / DBR 14+ in some threading contexts).
    # file_supported() is memoized per SparkSession and is cheap after the
    # first call.
    try:
        _driver_spark = df.sparkSession
    except Exception:
        _driver_spark = None
    file_ok_driver = _fr_mod.file_supported(_driver_spark)
    if file_ok_driver:
        # file_ref_arg expects the TILE column and extracts ["path"] internally.
        df = df.withColumn(
            "_file_ref", _fr_mod.file_ref_arg(F.col(tile_col), spark=_driver_spark)
        )

    # Compute Connect-aware sizing on the driver; capture in the closure.
    _stream_max, _lru_max, _lru_count = _connect_aware_lru_sizing(_driver_spark)

    def _map(pdf_iter):
        from . import _env

        _env.configure_gdal_env()
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
                    tile = row[tile_col]
                    # Extract cellid from the tile struct for passing to core_fn.
                    try:
                        cellid = tile["cellid"]
                    except (KeyError, TypeError):
                        cellid = None
                    # Resolve uri: None for materialized tiles, path string for virtual.
                    try:
                        uri = tile["path"] or None
                    except (KeyError, TypeError):
                        uri = None

                    result = _MISS
                    if uri and has_fr_col:
                        fr = row["_file_ref"]
                        if fr is not None:
                            # FILE fast path: inject FileRef so the LRU opener can
                            # stream-open the source on a cache miss.
                            try:
                                result = _run_file_fast_path(
                                    _ctx.fr_holder,
                                    _ctx.size_holder,
                                    lru,
                                    uri,
                                    fr,
                                    tile,
                                    cellid,
                                    _view_mode,
                                    core_fn,
                                )
                            except Exception:
                                # Any open/stream/view-construction failure →
                                # degrade gracefully to the per-tile fallback.
                                result = _MISS

                    if result is _MISS:
                        try:
                            result = _run_fallback_tile(
                                tile, cellid, _view_mode, core_fn
                            )
                        except Exception:
                            result = None

                    results.append(result)

                # Strip _file_ref from the output so the schema matches
                # original_fields + [return_field] exactly.
                out_pdf = pdf.drop(columns=["_file_ref"], errors="ignore")
                yield out_pdf.assign(**{out_name: results})
        finally:
            lru.close_all()

    return df.mapInPandas(_map, schema=out_schema)
