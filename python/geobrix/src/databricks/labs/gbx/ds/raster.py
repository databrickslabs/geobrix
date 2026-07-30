"""raster_gbx — catch-all pure-Python DataSource V2 raster reader.

1:1 swap-out for the Scala ``gdal`` reader: recursively lists files, splits each
into layout-aware tiles determined by a decoded-memory budget, re-encodes each
tile as GTiff, emits (source, tile) rows matching pyrx._serde.TILE_SCHEMA.
Pure Python (Serverless).

Split strategy (``splitStrategy`` option, default ``none``):
  - ``none``       — no split; one tile per file (default — halo mode).
  - ``auto``       — resolves to ``serverless`` or ``classic`` by env probe.
  - ``serverless`` — 128 MiB decoded budget per tile (opt-in split).
  - ``classic``    — 1 536 MiB decoded budget per tile (opt-in split).

**Halo mode** (recommended for large rasters): prepare a master COG via
``cog_gbx`` writer, then read windows with ``bbox``/``splitStrategy``.
COG creation is a writer concern; the reader always emits plain GTiff tiles.

Power-user override: ``sizeInMB`` (positive integer) overrides the budget in
MiB, bypassing the strategy-derived budget. Implies opt-in split.

Fast path: when a source is a single whole-raster GTiff tile, the original file
bytes are passed through unchanged — pixels are identical (parity-safe) and
~80x cheaper per tile.

One-tile-per-partition architecture: the tile window plan is computed at the
driver in ``partitions()``, and each ``_TilePartition`` covers exactly one
(file, window) pair.  Each ``read()`` call emits exactly ONE row and then
releases all buffers — this eliminates the per-file tile-accumulation that
previously caused Serverless OOM when all N tile bytes for a file were held
concurrently in a single partition task.

Worker-local staging cache: source files that require windowed reads are staged
to worker-local disk once per (worker process, file path) via a module-level
dict.  Tasks for the same file on the same worker reuse the staged copy without
re-copying (no 2.4 TiB re-staging tax).  Staged files are removed at process
exit via ``atexit``.

Limitation: per-band masks/alpha and source colormaps are not yet propagated to
the re-encoded tiles (band data + nodata/dtype/crs/transform are). Sources that
rely on a colormap or per-band mask will differ structurally from the heavy
reader; tracked as a follow-up.
"""

from __future__ import annotations

import atexit
import logging
import os
import shutil
import tempfile
import threading
from typing import Dict, Iterator, Optional, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.gbx.ds import _encode, _listing
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import budget

logger = logging.getLogger(__name__)

# Spark BinaryType cells are bounded by the JVM 2 GiB array limit; a single
# whole-image tile larger than this cannot be materialized and would otherwise
# fail deep in the writer with an opaque error. Guard the no-split path against
# it (conservative ~1.9 GiB) so users get an actionable "set sizeInMB" message.
_MAX_TILE_BYTES = 1932735283  # ~1.8 GiB

# ---------------------------------------------------------------------------
# Worker-local staging cache (module-level, process-global)
# ---------------------------------------------------------------------------
# Key: original source file_path.  Value: absolute local path of the staged copy.
# Guarded by _STAGE_LOCK for thread safety (multiple Spark tasks in one worker).
_STAGED_FILES: Dict[str, str] = {}
_STAGE_LOCK = threading.Lock()
# Directory holding all staged files for this process — cleaned at exit.
_STAGE_DIR: Optional[str] = None


def _ensure_stage_dir() -> str:
    global _STAGE_DIR
    if _STAGE_DIR is None:
        _STAGE_DIR = tempfile.mkdtemp(prefix="gbx_stage_")
        atexit.register(_cleanup_stage_dir)
    return _STAGE_DIR


def _cleanup_stage_dir() -> None:
    global _STAGE_DIR
    if _STAGE_DIR and os.path.isdir(_STAGE_DIR):
        shutil.rmtree(_STAGE_DIR, ignore_errors=True)
        _STAGE_DIR = None


def _get_or_stage_file(file_path: str) -> str:
    """Return worker-local path for *file_path*, staging it if not yet cached.

    Staging is a single sequential ``shutil.copyfileobj`` pass (FUSE-safe on
    Databricks UC Volumes which don't support per-window seeks).  The local copy
    is reused by all tile tasks for the same source on the same worker process.
    """
    with _STAGE_LOCK:
        if file_path in _STAGED_FILES:
            return _STAGED_FILES[file_path]

        stage_dir = _ensure_stage_dir()
        # Disambiguate in case two different dirs share a basename.
        basename = os.path.basename(file_path) or "raster.tif"
        safe_name = f"{abs(hash(file_path)):x}_{basename}"
        local_path = os.path.join(stage_dir, safe_name)

        # UC Volume FUSE can transiently raise FileNotFoundError even when the
        # file exists (eventual-consistency lag after write). Retry up to 10×.
        def _do_stage() -> None:
            with (
                open(file_path, "rb") as _src,
                open(local_path, "wb") as _dst,
            ):
                shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)

        _listing._retry_transient(_do_stage)

        _STAGED_FILES[file_path] = local_path
        return local_path


def _numpy_itemsize(dtype: str) -> int:
    import numpy as np

    try:
        return np.dtype(dtype).itemsize
    except TypeError:
        return 1


def _estimate_tile_bytes(
    width: int, height: int, count: int, dtype, file_size: int
) -> int:
    """Conservative encoded-size estimate for a single whole-image tile.

    Uses the larger of the raw pixel-array size (width*height*bands*itemsize) and
    the on-disk encoded size, so neither a highly compressed source nor an
    expanded re-encode slips past the cell-limit guard.
    """
    raw = width * height * max(count, 1) * _numpy_itemsize(str(dtype))
    return max(raw, file_size)


def reader_schema() -> StructType:
    """(source, tile) — tile from the single-source TILE_SCHEMA."""
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("tile", _serde.TILE_SCHEMA, nullable=False),
        ]
    )


def _resolve_emit_format(tile_format: str, split: bool) -> str:
    """Resolve the actual emit format for a tile.

    ``cog``   -> always ``"cog"``
    ``gtiff`` -> always ``"gtiff"``
    ``auto``  -> ``"cog"`` when splitting (sub-tiles); ``"gtiff"`` when whole-image.
    """
    fmt = (tile_format or "auto").lower()
    if fmt == "cog":
        return "cog"
    if fmt == "gtiff":
        return "gtiff"
    # auto
    return "cog" if split else "gtiff"


class _TilePartition(InputPartition):
    """One (source file, tile window) = one partition (picklable).

    ``window`` is ``None`` for passthrough tiles (whole-file GTiff fast path)
    or bbox-clipped single-window tiles.  For split tiles it is
    ``(col_off, row_off, win_w, win_h)``.

    ``is_passthrough`` signals the whole-file GTiff fast path (no decode/
    re-encode — original file bytes emitted directly).

    ``is_whole`` signals a single-tile whole-image encode (not a sub-tile of
    a larger split plan).
    """

    __slots__ = (
        "file_path",
        "window",
        "is_passthrough",
        "is_whole",
        "emit_fmt",
        "cog_blocksize",
        "cog_overview_resampling",
        "all_parents",
        # Kept for backward compat with tests that still inspect budget_bytes.
        "budget_bytes",
        "size_mib",
        "tile_format",
    )

    def __init__(
        self,
        file_path: str,
        window: Optional[Tuple[int, int, int, int]],
        *,
        is_passthrough: bool = False,
        is_whole: bool = False,
        emit_fmt: str = "gtiff",
        cog_blocksize: int = 512,
        cog_overview_resampling: str = "AVERAGE",
        all_parents: str = "",
        # Legacy fields so old tests that pass _FilePartition kwargs still work.
        budget_bytes: int = 0,
        size_mib: int = -1,
        tile_format: str = "auto",
    ):
        self.file_path = file_path
        self.window = window
        self.is_passthrough = is_passthrough
        self.is_whole = is_whole
        self.emit_fmt = emit_fmt
        self.cog_blocksize = cog_blocksize
        self.cog_overview_resampling = cog_overview_resampling
        self.all_parents = all_parents
        self.budget_bytes = budget_bytes
        self.size_mib = size_mib
        self.tile_format = tile_format


# ---------------------------------------------------------------------------
# Backward-compatible alias: old code that constructs _FilePartition directly
# (notably test_raster_datasource.py and test_raster_large.py) still works.
# _FilePartition is now a thin factory that returns a _TilePartition configured
# to reproduce the OLD per-file behaviour — read() will compute the tile plan
# inline when it sees a _TilePartition with window=None and is_passthrough=False
# and is_whole=False.
# ---------------------------------------------------------------------------


class _FilePartition(_TilePartition):
    """Legacy: one source file = one partition.  Retained for test compatibility.

    Constructed like the old _FilePartition; ``read()`` detects this via
    ``_is_legacy`` and runs the full tile-plan-inline path (one call can yield
    multiple tiles, mirroring the old behaviour).  New code should construct
    ``_TilePartition`` objects via ``RasterGbxReader.partitions()``.
    """

    _is_legacy = True

    def __init__(
        self,
        file_path: str,
        size_mib: int,
        budget_bytes: int = 0,
        tile_format: str = "auto",
        cog_blocksize: int = 512,
        cog_overview_resampling: str = "AVERAGE",
    ):
        super().__init__(
            file_path=file_path,
            window=None,
            is_passthrough=False,
            is_whole=False,
            emit_fmt=_resolve_emit_format(tile_format, split=False),
            cog_blocksize=cog_blocksize,
            cog_overview_resampling=cog_overview_resampling,
            all_parents="",
            budget_bytes=budget_bytes,
            size_mib=size_mib,
            tile_format=tile_format,
        )


def _plan_partitions_for_file(
    file_path: str,
    budget_bytes: int,
    bbox: Optional[Tuple[float, float, float, float]],
    bbox_crs: Optional[str],
) -> Sequence["_TilePartition"]:
    """Driver-side: open a raster header and return one _TilePartition per tile.

    All window planning happens here so each worker task handles exactly one
    (file, window) pair.
    """
    import rasterio

    size_bytes = os.path.getsize(file_path)

    # ------------------------------------------------------------------
    # bbox path: single windowed tile.  No splitting.
    # ------------------------------------------------------------------
    if bbox is not None:
        from databricks.labs.gbx.ds._window import window_for_bbox

        with rasterio.open(file_path) as ds:
            win = window_for_bbox(ds, bbox, bbox_crs)
        if win is None:
            return []  # source does not overlap AOI
        return [
            _TilePartition(
                file_path=file_path,
                window=(
                    int(win.col_off),
                    int(win.row_off),
                    int(win.width),
                    int(win.height),
                ),
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
            )
        ]

    # ------------------------------------------------------------------
    # Normal path: read header metadata, decide whole vs split.
    # ------------------------------------------------------------------
    with rasterio.open(file_path) as ds:
        width, height, driver = ds.width, ds.height, ds.driver
        bands = ds.count
        itemsize = _numpy_itemsize(ds.dtypes[0])
        tiled = bool(ds.profile.get("tiled", False))
        blockxsize = ds.profile.get("blockxsize")
        blockysize = ds.profile.get("blockysize")

    whole = budget_bytes <= 0 or (width * height * bands * itemsize <= budget_bytes)

    if whole:
        est = _estimate_tile_bytes(width, height, bands, "float32", size_bytes)
        if est > _MAX_TILE_BYTES:
            raise ValueError(
                f"raster {file_path} is ~{est // (1024 * 1024)} MB "
                f"as a single tile, which exceeds the ~2 GB Spark cell limit; "
                f"set the reader option sizeInMB=<n> (a positive MB value) to "
                f"tile it into smaller pieces."
            )

    # Reader always emits plain GTiff (COG is a writer concern — use cog_gbx).
    # Passthrough: whole GTiff — emit original bytes unchanged.
    if whole and driver == "GTiff":
        return [
            _TilePartition(
                file_path=file_path,
                window=None,
                is_passthrough=True,
                is_whole=True,
                emit_fmt="gtiff",
            )
        ]

    if whole:
        # Single whole-image re-encode (non-GTiff source or explicit re-encode).
        return [
            _TilePartition(
                file_path=file_path,
                window=(0, 0, width, height),
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
            )
        ]

    # Split: one partition per tile window, all plain GTiff.
    plan = budget.plan_layout(
        width,
        height,
        bands,
        itemsize,
        tiled,
        blockxsize,
        blockysize,
        budget_bytes,
    )
    if plan.degraded:
        logger.warning(
            "raster %s: layout plan hit the 512-tile cap; some tiles "
            "may exceed the decoded-memory budget.",
            file_path,
        )
    return [
        _TilePartition(
            file_path=file_path,
            window=(col, row, w, h),
            is_passthrough=False,
            is_whole=False,
            emit_fmt="gtiff",
        )
        for col, row, w, h in plan.tiles
    ]


class RasterGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("raster_gbx requires a 'path' (e.g. .load(path)).")
        self.size_mib = int(options.get("sizeInMB", "-1"))
        self.filter_regex = options.get("filterRegex", ".*")
        # Split strategy: default is "none" (halo mode — prepare a COG via the
        # cog_gbx writer, then read windows). Opt-in split: splitStrategy=serverless
        # or splitStrategy=classic, or sizeInMB>0. COG creation is a writer concern;
        # split tiles are always emitted as plain GTiff.
        self.strategy = budget.resolve_strategy(options.get("splitStrategy", "none"))
        # Optional AOI window-on-read. `bbox` is "minx,miny,maxx,maxy" in the source
        # CRS by default; `bboxCrs` (e.g. "EPSG:4326") declares the bbox CRS and the
        # window primitive reprojects it. None = read the whole raster (prior behavior).
        bbox_opt = options.get("bbox")
        if bbox_opt:
            parts = [float(v) for v in str(bbox_opt).split(",")]
            if len(parts) != 4:
                raise ValueError(
                    "raster bbox option must be 'minx,miny,maxx,maxy'; got "
                    f"'{bbox_opt}'"
                )
            self.bbox = tuple(parts)
        else:
            self.bbox = None
        self.bbox_crs = options.get("bboxCrs")

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        # Resolve the decoded-memory budget ONCE at the driver.
        # sizeInMB > 0 is a power-user byte-level override; it wins over strategy.
        if self.size_mib > 0:
            resolved_budget = self.size_mib * 1024 * 1024
        else:
            resolved_budget = budget.decoded_budget_bytes(self.strategy)

        result: list = []
        for f in files:
            result.extend(
                _plan_partitions_for_file(
                    file_path=f,
                    budget_bytes=resolved_budget,
                    bbox=self.bbox,
                    bbox_crs=self.bbox_crs,
                )
            )
        return result

    def read(self, partition: "_TilePartition") -> Iterator[Tuple]:
        import rasterio

        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()

        # Spark's Python DataSource V2 can call read(None) when the partition
        # list is empty (e.g. bbox misses all files). Guard and emit nothing.
        if partition is None:
            return

        # ------------------------------------------------------------------
        # Legacy _FilePartition: the partition was built by tests with the old
        # API — run the inline tile-plan path (may yield multiple rows).
        # ------------------------------------------------------------------
        if getattr(partition, "_is_legacy", False):
            yield from self._read_legacy(partition)
            return

        source = _listing.to_spark_uri(partition.file_path)

        # ------------------------------------------------------------------
        # Passthrough tile: emit original file bytes, no decode.
        # ------------------------------------------------------------------
        if partition.is_passthrough:
            with rasterio.open(partition.file_path) as ds:
                width, height = ds.width, ds.height
                compression = str(ds.profile.get("compress") or "DEFLATE").upper()
            cellid, raster_bytes, meta = _encode.passthrough_tile(
                partition.file_path,
                width,
                height,
                source_path=partition.file_path,
                all_parents=partition.all_parents,
                compression=compression,
            )
            yield (source, (cellid, raster_bytes, meta))
            return

        # ------------------------------------------------------------------
        # Windowed / whole-image encode: stage source once, read one window.
        # ------------------------------------------------------------------
        local_path = _get_or_stage_file(partition.file_path)

        with rasterio.Env(GDAL_CACHEMAX=128):
            with rasterio.open(local_path) as ds:
                cellid, raster_bytes, meta = _encode.encode_tile(
                    ds,
                    window=partition.window,
                    source_path=partition.file_path,
                    all_parents=partition.all_parents,
                    tile_format=partition.emit_fmt,
                    cog_blocksize=partition.cog_blocksize,
                    cog_overview_resampling=partition.cog_overview_resampling,
                )
        yield (source, (cellid, raster_bytes, meta))

    # ------------------------------------------------------------------
    # Legacy path (backward compat with tests that use _FilePartition directly)
    # ------------------------------------------------------------------
    def _read_legacy(self, partition: "_FilePartition") -> Iterator[Tuple]:
        """Reproduce original one-file-yields-all-tiles behaviour for _FilePartition."""
        import rasterio

        source = _listing.to_spark_uri(partition.file_path)

        # bbox path
        if self.bbox is not None:
            from databricks.labs.gbx.ds._window import window_for_bbox

            staged_dir = tempfile.mkdtemp(prefix="gbx_raster_")
            try:
                local_path = os.path.join(
                    staged_dir,
                    os.path.basename(partition.file_path) or "raster.tif",
                )
                with (
                    open(partition.file_path, "rb") as _src,
                    open(local_path, "wb") as _dst,
                ):
                    shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
                with rasterio.open(local_path) as ds:
                    win = window_for_bbox(ds, self.bbox, self.bbox_crs)
                    if win is None:
                        return
                    cellid, raster_bytes, meta = _encode.encode_tile(
                        ds,
                        window=(
                            int(win.col_off),
                            int(win.row_off),
                            int(win.width),
                            int(win.height),
                        ),
                        source_path=partition.file_path,
                        all_parents="",
                        tile_format="gtiff",
                    )
                    yield (source, (cellid, raster_bytes, meta))
            finally:
                shutil.rmtree(staged_dir, ignore_errors=True)
            return

        size_bytes = os.path.getsize(partition.file_path)
        with rasterio.open(partition.file_path) as ds:
            width, height, driver = ds.width, ds.height, ds.driver
            bands = ds.count
            itemsize = _numpy_itemsize(ds.dtypes[0])
            whole = partition.budget_bytes <= 0 or (
                width * height * bands * itemsize <= partition.budget_bytes
            )
            if whole:
                est = _estimate_tile_bytes(
                    width, height, bands, ds.dtypes[0], size_bytes
                )
                if est > _MAX_TILE_BYTES:
                    raise ValueError(
                        f"raster {partition.file_path} is ~{est // (1024 * 1024)} MB "
                        f"as a single tile, which exceeds the ~2 GB Spark cell limit; "
                        f"set the reader option sizeInMB=<n> (a positive MB value) to "
                        f"tile it into smaller pieces."
                    )
            if whole and driver == "GTiff":
                compression = str(ds.profile.get("compress") or "DEFLATE").upper()
                cellid, raster_bytes, meta = _encode.passthrough_tile(
                    partition.file_path,
                    width,
                    height,
                    source_path=partition.file_path,
                    all_parents="",
                    compression=compression,
                )
                yield (source, (cellid, raster_bytes, meta))
                return
            tiled = bool(ds.profile.get("tiled", False))
            blockxsize = ds.profile.get("blockxsize")
            blockysize = ds.profile.get("blockysize")

        staged_dir = tempfile.mkdtemp(prefix="gbx_raster_")
        try:
            local_path = os.path.join(
                staged_dir,
                os.path.basename(partition.file_path) or "raster.tif",
            )
            with (
                open(partition.file_path, "rb") as _src,
                open(local_path, "wb") as _dst,
            ):
                shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
            with rasterio.open(local_path) as ds:
                if whole:
                    plan_tiles = [(0, 0, width, height)]
                else:
                    plan = budget.plan_layout(
                        width,
                        height,
                        bands,
                        itemsize,
                        tiled,
                        blockxsize,
                        blockysize,
                        partition.budget_bytes,
                    )
                    if plan.degraded:
                        logger.warning(
                            "raster %s: layout plan hit the 512-tile cap; some tiles "
                            "may exceed the decoded-memory budget.",
                            partition.file_path,
                        )
                    plan_tiles = plan.tiles
                for col, row, w, h in plan_tiles:
                    cellid, raster_bytes, meta = _encode.encode_tile(
                        ds,
                        window=(col, row, w, h),
                        source_path=partition.file_path,
                        all_parents="",
                        tile_format="gtiff",
                    )
                    yield (source, (cellid, raster_bytes, meta))
        finally:
            shutil.rmtree(staged_dir, ignore_errors=True)


class RasterGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "raster_gbx"

    def schema(self) -> StructType:
        return reader_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return RasterGbxReader(self.options)

    def writer(
        self, schema: StructType, overwrite: bool
    ) -> "DataSourceWriter":  # noqa: F821
        from pyspark.sql.datasource import DataSourceWriter  # noqa: F401

        from databricks.labs.gbx.ds.writer import RasterGbxWriter

        path = self.options.get("path")
        if not path:
            raise ValueError("raster_gbx writer requires an output path (.save(path)).")
        return RasterGbxWriter(
            path,
            schema,
            overwrite,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            force_driver=None,
            cog=str(self.options.get("cog", "false")).lower() == "true",
            cog_blocksize=int(self.options.get("cogBlockSize", "512")),
            cog_overview_resampling=self.options.get(
                "cogOverviewResampling", "AVERAGE"
            ),
            cog_compression=self.options.get("cogCompression", "DEFLATE"),
        )
