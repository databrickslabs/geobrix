"""raster_gbx — catch-all pure-Python DataSource V2 raster reader.

1:1 swap-out for the Scala ``gdal`` reader: recursively lists files, splits each
into layout-aware tiles determined by a decoded-memory budget, re-encodes each
tile as GTiff or COG, emits (source, tile) rows matching pyrx._serde.TILE_SCHEMA.
Pure Python (Serverless).

Split strategy (``splitStrategy`` option, default ``auto``):
  - ``auto``       — resolves to ``serverless`` or ``classic`` by env probe.
  - ``serverless`` — 512 MiB decoded budget per tile.
  - ``classic``    — 1 536 MiB decoded budget per tile.
  - ``none``       — no split; one tile per file (pre-0.4.4 behavior).

Tile format (``tileFormat`` option, default ``auto``):
  - ``auto``       — whole-file: passthrough source format; split: COG.
  - ``gtiff``      — always emit plain GTiff (no overviews).
  - ``cog``        — always emit COG with overviews.

Power-user override: ``sizeInMB`` (positive integer) overrides the budget in
bytes. ``sizeInMB=-1`` is equivalent to ``splitStrategy=none``.

Fast path: when a source is a single whole-raster GTiff tile and tileFormat is
``auto`` or ``gtiff``, the original file bytes are passed through unchanged —
pixels are identical (parity-safe) and ~80x cheaper per tile.

Limitation: per-band masks/alpha and source colormaps are not yet propagated to
the re-encoded tiles (band data + nodata/dtype/crs/transform are). Sources that
rely on a colormap or per-band mask will differ structurally from the heavy
reader; tracked as a follow-up.
"""

from __future__ import annotations

import logging
from typing import Dict, Iterator, Sequence, Tuple

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


class _FilePartition(InputPartition):
    """One source file = one partition (picklable)."""

    def __init__(
        self,
        file_path: str,
        size_mib: int,
        budget_bytes: int = 0,
        tile_format: str = "auto",
        cog_blocksize: int = 512,
        cog_overview_resampling: str = "AVERAGE",
    ):
        self.file_path = file_path
        self.size_mib = size_mib
        self.budget_bytes = budget_bytes
        self.tile_format = tile_format
        self.cog_blocksize = cog_blocksize
        self.cog_overview_resampling = cog_overview_resampling


class RasterGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("raster_gbx requires a 'path' (e.g. .load(path)).")
        self.size_mib = int(options.get("sizeInMB", "-1"))
        self.filter_regex = options.get("filterRegex", ".*")
        # Split strategy + tile format options (new in 0.4.4).
        self.strategy = budget.resolve_strategy(options.get("splitStrategy", "auto"))
        self.tile_format = options.get("tileFormat", "auto")
        self.cog_blocksize = int(options.get("cogBlockSize", "512"))
        self.cog_overview_resampling = options.get("cogOverviewResampling", "AVERAGE")
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
        return [
            _FilePartition(
                f,
                self.size_mib,
                budget_bytes=resolved_budget,
                tile_format=self.tile_format,
                cog_blocksize=self.cog_blocksize,
                cog_overview_resampling=self.cog_overview_resampling,
            )
            for f in files
        ]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        import os
        import shutil
        import tempfile

        import rasterio

        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()

        # rasterio reads the BARE FUSE path; only the emitted source column is
        # scheme-qualified to match binaryFile / heavy gdal (dbfs:/Volumes/...),
        # so a light-produced DataFrame joins cleanly against that convention.
        source = _listing.to_spark_uri(partition.file_path)

        # AOI window-on-read: stage to worker-local disk (FUSE-safe sequential copy --
        # Volume FUSE cannot serve the per-window seeks), then window from local disk.
        # bbox disables the whole-image fast path and the multi-tile split.
        if self.bbox is not None:
            from databricks.labs.gbx.ds._window import window_for_bbox

            staged_dir = tempfile.mkdtemp(prefix="gbx_raster_")
            try:
                local_path = os.path.join(
                    staged_dir, os.path.basename(partition.file_path) or "raster.tif"
                )
                with (
                    open(partition.file_path, "rb") as _src,
                    open(local_path, "wb") as _dst,
                ):
                    shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
                with rasterio.open(local_path) as ds:
                    win = window_for_bbox(ds, self.bbox, self.bbox_crs)
                    if win is None:
                        return  # source does not overlap the AOI -> emit nothing
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
                    )
                    yield (source, (cellid, raster_bytes, meta))
            finally:
                shutil.rmtree(staged_dir, ignore_errors=True)
            return

        size_bytes = os.path.getsize(partition.file_path)
        # Phase 1 (FUSE-safe): open the source to read metadata + compute the split, and
        # serve the whole-image GTiff fast path (sequential byte read). Reading the header
        # is small/sequential, so it is fine directly on a UC Volume.
        with rasterio.open(partition.file_path) as ds:
            width, height, driver = ds.width, ds.height, ds.driver
            bands = ds.count
            itemsize = _numpy_itemsize(ds.dtypes[0])
            # Determine whole-image vs split based on decoded-memory budget.
            # budget_bytes <= 0 means no split (strategy=none or sizeInMB unset under
            # old-style call). Otherwise compare raw decoded size to budget.
            whole = partition.budget_bytes <= 0 or (
                width * height * bands * itemsize <= partition.budget_bytes
            )
            # Large-raster safety: a single whole-image tile that would exceed
            # Spark's ~2 GiB BinaryType cell limit must fail with an actionable
            # message rather than producing a giant (or unmaterializable) cell.
            if whole:
                est = _estimate_tile_bytes(width, height, bands, ds.dtypes[0], size_bytes)
                if est > _MAX_TILE_BYTES:
                    raise ValueError(
                        f"raster {partition.file_path} is ~{est // (1024 * 1024)} MB "
                        f"as a single tile, which exceeds the ~2 GB Spark cell limit; "
                        f"set the reader option sizeInMB=<n> (a positive MB value) to "
                        f"tile it into smaller pieces."
                    )
            # Fast path: whole-image AND a GTiff source AND tileFormat is not forcing COG
            # -> emit the original file bytes (sequential read, no decode/re-encode).
            # Pixels are identical (parity-safe). Sub-tiles / non-GTiff / tileFormat=cog
            # fall through to phase 2.
            emit_fmt = _resolve_emit_format(partition.tile_format, split=not whole)
            if whole and driver == "GTiff" and emit_fmt != "cog":
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
            # Capture tiled layout metadata for plan_layout (needed in phase 2).
            tiled = bool(ds.profile.get("tiled", False))
            blockxsize = ds.profile.get("blockxsize")
            blockysize = ds.profile.get("blockysize")

        # Phase 2 (windowed split or whole-image re-encode): per-window ds.read() SEEKS,
        # which UC Volume FUSE can't serve. Stage to worker-local disk with a SEQUENTIAL
        # copy (FUSE-safe), then window from local disk. Each ds.read(window) loads only
        # that window's blocks, so per-task RAM stays ~tile-sized regardless of source
        # size. The staged file is created, fully consumed, and removed — all within this
        # generator (one executor task); only encoded tile BYTES cross into the columnar
        # output, so nothing local leaks downstream.
        staged_dir = tempfile.mkdtemp(prefix="gbx_raster_")
        try:
            local_path = os.path.join(
                staged_dir, os.path.basename(partition.file_path) or "raster.tif"
            )
            with (
                open(partition.file_path, "rb") as _src,
                open(local_path, "wb") as _dst,
            ):
                shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
            with rasterio.open(local_path) as ds:
                if whole:
                    # tileFormat=cog on a whole-image tile: encode the full raster as COG.
                    plan_tiles = [(0, 0, width, height)]
                else:
                    plan = budget.plan_layout(
                        width, height, bands, itemsize,
                        tiled, blockxsize, blockysize,
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
                        tile_format=emit_fmt,
                        cog_blocksize=partition.cog_blocksize,
                        cog_overview_resampling=partition.cog_overview_resampling,
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
        )
