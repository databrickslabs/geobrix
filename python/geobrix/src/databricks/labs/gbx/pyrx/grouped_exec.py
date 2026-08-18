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
STREAM_NOMINAL_BYTES = 16 * 1024**2  # resident estimate for an open stream/dataset

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
        max_count: int = 64,
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


def _make_opener():
    """Capability-adaptive opener factory.

    Returns ``(fr_holder, opener, closer, weigher)``.

    Runs on the worker; all imports are worker-local (GDAL env configured
    before this is called).

    FILE capability is signaled by the presence of the ``_file_ref`` column
    (``has_fr_col`` in the caller), NOT by a worker-side ``file_supported()``
    call.  The driver gated column addition on ``file_ok_driver`` using an
    explicit ``df.sparkSession``; a worker-side ``file_supported()`` would
    rely on ``getActiveSession()``, which returns ``None`` on Spark-Connect
    worker threads (e.g. Serverless on DBR 19.5), silently disabling the FILE
    fast path even when the driver correctly determined FILE is supported.
    The ``_file_ref`` column's presence IS the worker capability signal.

    The LRU is keyed by ``uri`` (the source-path string) — hashable, and the
    identity that amortises opens.

    ``fr_holder`` is a one-element list ``[None]``.  Before each ``lru.get(uri)``
    call the partition loop sets ``fr_holder[0] = <current FileRef>``.  On an LRU
    cache miss the opener reads ``fr_holder[0]`` to stream-open the source via
    ``fr.open()`` (a seekable stream — the FILE fast path).  On a cache hit the
    opener is never called and ``fr_holder[0]`` is irrelevant.

    opener calls ``fr.open()`` → ``rasterio.open(stream)``; on any stream-open
    failure degrades to local staging so the LRU still gets an entry.
    """
    # fr_holder: mutable slot so the partition loop can inject the current row's
    # FileRef before lru.get(uri).  A list is used because closures can't rebind
    # a bare name in the outer scope.
    fr_holder = [None]

    def opener(uri: str):
        import rasterio

        fr = fr_holder[0]
        if fr is not None:
            try:
                stream = fr.open()
                return rasterio.open(stream)
            except Exception:
                pass  # fall through to staging fallback
        # Staging fallback: fr unavailable or stream-open failed.
        from .core.preparer import _stage_local_if_needed

        local_path, _ = _stage_local_if_needed(uri)
        return rasterio.open(local_path)

    def closer(src) -> None:
        try:
            src.close()
        except Exception:
            pass

    def weigher(src, key: str) -> int:
        # Nominal weight: an open stream/dataset holds minimal resident RAM;
        # the count guard (max_count=64) governs the LRU eviction policy.
        return STREAM_NOMINAL_BYTES

    return fr_holder, opener, closer, weigher


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

    Output schema is ``df.schema + [return_field]``; ``_file_ref`` never leaks.
    """
    from . import _file_ref as _fr_mod

    # Capture original fields BEFORE adding _file_ref so the output schema
    # matches the caller's expectation exactly.
    original_fields = list(df.schema.fields)
    out_schema = StructType(original_fields + [return_field])
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

    def _map(pdf_iter):
        from . import _env

        _env.configure_gdal_env()
        fr_holder, opener, closer, weigher = _make_opener()
        lru = OpenResourceLRU(opener=opener, closer=closer, weigher=weigher)
        # Capture the view kwarg in the closure so the inner loop can use it
        # without shadowing the local variable 'view' with a _WindowHeaderView
        # instance (the FILE fast-path loop used to do that).
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
                            # stream-open the source on a cache miss, then hand
                            # core_fn either a read-free header view or a real
                            # windowed DatasetReader depending on _view_mode.
                            try:
                                fr_holder[0] = fr
                                src = lru.get(uri)
                                from rasterio.windows import Window

                                from .core.open_tile import (
                                    _parse_pending,
                                    _to_virtual_tile,
                                    _WindowHeaderView,
                                )

                                vt = _to_virtual_tile(tile)
                                bands, raw_nodata, srid, crs_str = _parse_pending(
                                    vt.metadata
                                )
                                pending_count = len(bands) if bands else None
                                pending_crs = None
                                if crs_str is not None or srid is not None:
                                    from .core.crs import resolve_crs

                                    pending_crs = resolve_crs(
                                        crs_str if crs_str is not None else srid
                                    )
                                if vt.window is not None:
                                    c, r_off, w, h = vt.window
                                    win = Window(c, r_off, w, h)
                                    if _view_mode == "pixels":
                                        # Materialise the window into GTiff bytes,
                                        # open as a real DatasetReader inside an
                                        # ExitStack, and pass to core_fn.
                                        import rasterio
                                        from contextlib import ExitStack

                                        from .core.open_tile import (
                                            _window_dataset_bytes,
                                        )

                                        pending = (bands, raw_nodata, srid, crs_str)
                                        b = _window_dataset_bytes(src, win, pending)
                                        with ExitStack() as _stk:
                                            ds = _stk.enter_context(
                                                rasterio.open(
                                                    rasterio.io.MemoryFile(b)
                                                )
                                            )
                                            result = core_fn(ds, cellid)
                                    else:
                                        # view="header": read-free header view.
                                        header_view = _WindowHeaderView(
                                            src,
                                            win,
                                            pending_count=pending_count,
                                            pending_crs=pending_crs,
                                            pending_nodata=raw_nodata,
                                        )
                                        result = core_fn(header_view, cellid)
                                else:
                                    # No sub-window: full source; src is correct.
                                    # For view="pixels" src is already a full
                                    # DatasetReader; for view="header" we pass it
                                    # directly (no sub-window means full-source dims
                                    # are correct — a WindowHeaderView would be a
                                    # no-op wrapper here).
                                    result = core_fn(src, cellid)
                            except Exception:
                                # Any open/stream/view-construction failure →
                                # degrade gracefully to the per-tile fallback below.
                                result = _MISS

                    if result is _MISS:
                        # Fallback path: covers
                        #   (a) materialized tiles (raster inline, path None)
                        #   (b) virtual tiles when driver did not add _file_ref
                        #       (FILE not supported on this cluster)
                        #   (c) FILE open/stream/view failure (graceful degrade)
                        # For windowless virtual tiles, _open raises ValueError;
                        # open_header returns the full-source footprint (consistent
                        # with per-row rst_memsize).  Per-tile errors degrade to
                        # None rather than crashing the whole partition.
                        #
                        # view contract on fallback path:
                        #   view="header": wrap the open DatasetReader in a full-
                        #     extent _WindowHeaderView so core_fn consistently gets
                        #     a read-blocking view regardless of tile type.
                        #   view="pixels": pass the real DatasetReader directly.
                        from .core.open_tile import _open, open_header

                        try:
                            if _tile_is_windowless_virtual(tile):
                                with open_header(tile) as ds:
                                    if _view_mode == "header":
                                        from rasterio.windows import Window as _Win
                                        from .core.open_tile import _WindowHeaderView
                                        _hv = _WindowHeaderView(
                                            ds,
                                            _Win(0, 0, ds.width, ds.height),
                                        )
                                        result = core_fn(_hv, cellid)
                                    else:
                                        result = core_fn(ds, cellid)
                            else:
                                with _open(tile) as ds:
                                    if _view_mode == "header":
                                        from rasterio.windows import Window as _Win
                                        from .core.open_tile import _WindowHeaderView
                                        _hv = _WindowHeaderView(
                                            ds,
                                            _Win(0, 0, ds.width, ds.height),
                                        )
                                        result = core_fn(_hv, cellid)
                                    else:
                                        result = core_fn(ds, cellid)
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
