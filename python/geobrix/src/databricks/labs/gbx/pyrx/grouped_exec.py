"""Partition-scoped grouped execution for FILE/virtual tiles (light tier).

Amortizes the dominant cost (the source OPEN) by grouping a source raster's
tiles into one partition, contiguous, and reading them from one cached open
resource. Pure Python + mapInPandas -- no .rdd / sc / spark.conf.set.
"""

import os
from collections import OrderedDict
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

GBX_LRU_MAX_BYTES = int(os.environ.get("GBX_LRU_MAX_BYTES", 4 * 1024**3))  # 4 GiB
GBX_STREAM_MAX_BYTES = int(
    os.environ.get("GBX_STREAM_MAX_BYTES", 256 * 1024**2)
)  # 256 MiB
STREAM_NOMINAL_BYTES = 16 * 1024**2  # fallback nominal when size is unknown

# Sentinel for "core_fn has not been called yet for this tile".
# Distinct from None so a core_fn that legitimately returns None
# is not re-routed through the fallback (M1 fix).
_MISS = object()


def _connect_aware_lru_sizing(spark) -> tuple[int, int, int]:
    """Return Connect-aware LRU sizing (stream_max_bytes, lru_max_bytes, max_count).

    On Spark Connect (Serverless), tasks have ~1 GB RAM; class instance buffers
    (stream opens) become a major constraint. This function returns reduced sizing
    to fit serverless RAM budgets while favoring FUSE (which doesn't buffer) over
    stream opens.

    Benchmarks show that on serverless, FUSE (74 ms/tile) is FASTER than stream
    (110 ms/tile) per FILE-for-raster benchmarks; lower stream_max_bytes favors
    the FUSE path automatically.

    Args:
        spark: Spark session (SparkSession or PySpark Connect session), or None.

    Returns:
        (stream_max_bytes, lru_max_bytes, max_count):
        - Classic Spark: (256 MiB, 4 GiB, 4) — existing behavior.
        - Spark Connect: (64 MiB, 512 MiB, 4) — serverless-safe.
        - Env overrides win in both: GBX_STREAM_MAX_BYTES, GBX_LRU_MAX_BYTES.
        - Serverless worst case: 4 × 64 MiB = 256 MiB, well under ~1 GB task.
    """
    is_connect = spark is not None and "connect" in type(spark).__module__

    # Read raw os.environ to distinguish "explicitly set" from "default".
    # Module-level constants already baked the env-or-default at import.
    stream_max_bytes_env = os.environ.get("GBX_STREAM_MAX_BYTES")
    if stream_max_bytes_env is not None:
        stream_max_bytes = int(stream_max_bytes_env)
    else:
        stream_max_bytes = 64 * 1024**2 if is_connect else 256 * 1024**2

    lru_max_bytes_env = os.environ.get("GBX_LRU_MAX_BYTES")
    if lru_max_bytes_env is not None:
        lru_max_bytes = int(lru_max_bytes_env)
    else:
        lru_max_bytes = 512 * 1024**2 if is_connect else 4 * 1024**3

    max_count = 4

    return stream_max_bytes, lru_max_bytes, max_count


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


class OpenResourceLRU:
    """Per-partition BYTE-BUDGETED LRU of open resources keyed by source uri/path.

    Amortizes the OPEN cost across a source's windows. Instead of a fixed count,
    entries are held up to a byte budget (default 4 GiB) with a max_count handle
    guard, so many small sources stay warm (e.g. ~128 x 32 MiB under 4 GiB) while
    a few huge ones don't blow the budget. Each entry carries a weight: a staged
    local copy weighs its file size (so this budget IS the staged-disk-fill guard,
    and eviction deletes the temp); an open stream/dataset weighs a small nominal
    so the count guard governs. The current (most-recent) entry is never evicted.
    Evicted and remaining resources are always closed (evict + close_all)."""

    def __init__(
        self,
        *,
        max_bytes: int = GBX_LRU_MAX_BYTES,
        max_count: int = 4,
        opener: Callable[[str], Any],
        closer: Callable[[Any], None],
        weigher: Callable[[Any, str], int],
    ):
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if max_count < 1:
            raise ValueError("max_count must be >= 1")
        self.max_bytes = max_bytes
        self.max_count = max_count
        self._opener = opener
        self._closer = closer
        self._weigher = weigher
        self._store: "OrderedDict[str, tuple]" = (
            OrderedDict()
        )  # key -> (resource, weight)
        self.opens = 0
        self.evictions = 0
        self.bytes = 0

    def get(self, key: str) -> Any:
        if key in self._store:
            self._store.move_to_end(key)
            return self._store[key][0]
        res = self._opener(key)
        self.opens += 1
        weight = int(self._weigher(res, key))
        self._store[key] = (res, weight)
        self.bytes += weight
        # evict oldest while over budget, but never the current (most-recent) entry
        while len(self._store) > 1 and (
            self.bytes > self.max_bytes or len(self._store) > self.max_count
        ):
            _, (evicted, w) = self._store.popitem(last=False)
            self.bytes -= w
            self.evictions += 1
            self._closer(evicted)
        return res

    def close_all(self) -> None:
        while self._store:
            _, (res, w) = self._store.popitem(last=False)
            self.bytes -= w
            self._closer(res)


def _open_via_file_ref(fr, rasterio, stream_max_bytes=GBX_STREAM_MAX_BYTES):
    """Open a rasterio DatasetReader from *fr* using the size-adaptive strategy.

    Returns ``(ds, stream)`` when opened via byte-range stream (tiled/COG path),
    or ``(ds, None)`` when opened via FUSE (``as_local_file``).

    - Small file (≤ stream_max_bytes) AND tiled layout: stream into /vsimem via
      ``fr.open()`` — efficient for tiled/COG sources that support random window reads.
    - Large file OR striped layout: lazy FUSE open via ``fr.as_local_file()`` — blocks
      are fetched on demand without loading the whole file into RAM.

    On the striped fallthrough, both the interim DatasetReader and the stream are
    closed before falling back to FUSE; the returned ``(ds, None)`` pair is FUSE-only.

    Raises on any failure; caller must handle and fall back to staging.

    Args:
        fr: FileRef object with size, open(), as_local_file() methods.
        rasterio: rasterio module (imported on worker).
        stream_max_bytes: threshold in bytes for stream vs FUSE (default: GBX_STREAM_MAX_BYTES).
    """
    if fr.size <= stream_max_bytes:
        stream = fr.open()
        ds = rasterio.open(stream)
        if ds.profile.get("tiled", False):
            return ds, stream  # tiled/COG: seekable stream is efficient
        ds.close()
        stream.close()  # striped: release the stream before FUSE fallback
    local_path = fr.as_local_file()
    return rasterio.open(local_path), None  # FUSE path: no stream


class _OpenerContext:
    """Per-partition opener/closer/weigher state for the FILE-capable LRU.

    Runs on the worker; all imports inside methods are worker-local (GDAL env
    configured before this is instantiated).

    ``fr_holder`` is a one-element list ``[None]``.  The partition loop sets
    ``fr_holder[0] = <current FileRef>`` before each ``lru.get(uri)`` call so the
    opener can stream-open the source on a cache miss.

    ``size_holder`` is a one-element list ``[None]``. The partition loop sets
    ``size_holder[0] = <metadata_size>`` (int or None) before each ``lru.get()``
    call so the weigher can prefer the pre-known size over a per-tile stat.

    FILE capability is signaled by the presence of the ``_file_ref`` column, NOT
    by a worker-side ``file_supported()`` call (which uses ``getActiveSession()``
    and returns ``None`` on Spark-Connect worker threads, e.g. Serverless DBR 19.5).

    Tracking dicts:
    - ``_staged_temps``: id(ds) → temp_path for staging-fallback cleanup.
    - ``_streams``: id(ds) → stream for tiled/COG opens — closed by ``close`` to
      prevent stream leaks alongside the DatasetReader.
    - ``_fuse_sources``: set of id(ds) for FUSE-opened handles — FUSE handles don't
      buffer into executor RAM, so ``weigh`` returns a small nominal for them.
    """

    def __init__(self, stream_max_bytes: int = GBX_STREAM_MAX_BYTES):
        # Mutable FileRef slot; a list so the partition loop can rebind it.
        self.fr_holder: "list[Any]" = [None]
        # Mutable size slot for metadata-derived tile size; a list to mirror fr_holder.
        self.size_holder: "list[int | None]" = [None]
        self._staged_temps: "dict[int, str]" = {}
        self._streams: "dict[int, Any]" = {}
        self._fuse_sources: "set[int]" = set()
        self._stream_max_bytes = stream_max_bytes

    def open(self, uri: str):
        import rasterio

        fr = self.fr_holder[0]
        if fr is not None:
            try:
                ds, stream = _open_via_file_ref(fr, rasterio, self._stream_max_bytes)
                if stream is not None:
                    self._streams[id(ds)] = stream
                else:
                    self._fuse_sources.add(id(ds))
                return ds
            except Exception:
                pass  # all FILE paths failed → staging fallback
        # Staging fallback: fr unavailable or all FILE paths raised.
        from .core.preparer import _stage_local_if_needed

        local_path, is_temp = _stage_local_if_needed(uri)
        ds = rasterio.open(local_path)
        if is_temp:
            self._staged_temps[id(ds)] = local_path
        return ds

    def close(self, src) -> None:
        tmp = self._staged_temps.pop(id(src), None)
        stream = self._streams.pop(id(src), None)
        self._fuse_sources.discard(id(src))
        try:
            src.close()
        except Exception:
            pass
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass
        if tmp:
            try:
                os.remove(tmp)
            except Exception:
                pass

    def weigh(self, src, key: str) -> int:
        # FUSE-opened handles don't buffer into executor RAM — use a small nominal
        # so the LRU budget isn't inflated by large-file FUSE references.
        if id(src) in self._fuse_sources:
            return STREAM_NOMINAL_BYTES
        # Prefer metadata-derived size (tile.metadata["path_file_size"] or ["tile_byte_size"])
        # when available; this avoids a per-tile stat on serverless.
        metadata_size = self.size_holder[0]
        if metadata_size is not None:
            return int(metadata_size)
        # Stream/COG path: fall back to FileRef's declared byte size (actual RAM use).
        fr = self.fr_holder[0]
        if fr is not None:
            try:
                return int(fr.size)
            except Exception:
                pass
        # Staged fallback: use the on-disk size of the temp copy.
        tmp = self._staged_temps.get(id(src))
        if tmp:
            try:
                return os.path.getsize(tmp)
            except Exception:
                pass
        return STREAM_NOMINAL_BYTES


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
