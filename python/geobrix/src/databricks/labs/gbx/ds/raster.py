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
``cog_gbx`` writer, then read windows with ``splitStrategy``.
COG creation is a writer concern; the reader always emits plain GTiff tiles.

AOI selection (mutually exclusive, single value or list of values):
  - ``clipPolygons`` — one tile per polygon whose envelope intersects the raster
    (materialized tiles are pre-clipped to the polygon; virtual tiles carry the
    clip as an instruction). Spark options are string-typed, so pass a **single
    WKT/EWKT string** for one polygon, or a **JSON-array string** for a list,
    e.g. ``json.dumps([wkt1, wkt2])`` -> ``'["<wkt1>","<wkt2>"]'`` (auto-detected:
    a value that ``json.loads`` to a list is a list; a bare WKT is one geometry).
    Raw WKB/EWKB bytes are accepted only from programmatic callers passing a
    Python list via the options dict.
  - ``windows`` — one tile per pixel window ``(col,row,w,h)``; partial windows
    clip to the raster extent, fully-outside windows are skipped. Over
    ``.option()`` a single window is a JSON 4-int array ``"[0,0,256,256]"``; a
    list is a JSON array of them ``"[[0,0,256,256],[256,0,256,256]]"``.
  - ``clipCrs`` — CRS for clipPolygons lacking an embedded SRID (precedence:
    embedded EWKB/EWKT SRID → clipCrs → raster CRS).

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
import json
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


def reader_schema_v2() -> StructType:
    """(source, tile) — tile is the v2 VirtualTile struct (raster nullable)."""
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("tile", V2_TILE_SCHEMA, nullable=False),
        ]
    )


def _v2_tile_row(
    cellid,
    raster,
    path,
    window,
    metadata,
    clip_polygon=None,
    clip_crs=None,
    crs=None,
) -> tuple:
    """Assemble one v2 tile tuple in V2_TILE_SCHEMA field order.

    ``window`` is a (col_off, row_off, width, height) tuple or None; it is
    serialized to the nested struct dict Spark expects (or None). This is the
    SINGLE place the reader assembles a v2 tile — both the virtual and
    materialized paths route through it.
    """
    win = None
    if window is not None:
        c, r, w, h = window
        win = {"col_off": int(c), "row_off": int(r), "width": int(w), "height": int(h)}
    return (
        int(cellid),
        raster,
        path,
        None,  # path_mode: materialized/unknown; effective_path_mode() infers downstream
        win,
        clip_polygon,
        clip_crs,
        crs,
        metadata,
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

    ``window`` is ``None`` for passthrough tiles (whole-file GTiff fast path).
    For split, clipPolygons, and windows tiles it is
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
        "emit_virtual",
        "cog_blocksize",
        "cog_overview_resampling",
        "all_parents",
        # clipPolygons selection: the clip geometry (WKB) and its resolved CRS.
        "clip_polygon",
        "clip_crs",
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
        emit_virtual: bool = False,
        cog_blocksize: int = 512,
        cog_overview_resampling: str = "AVERAGE",
        all_parents: str = "",
        clip_polygon: Optional[bytes] = None,
        clip_crs: Optional[str] = None,
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
        self.emit_virtual = emit_virtual
        self.cog_blocksize = cog_blocksize
        self.cog_overview_resampling = cog_overview_resampling
        self.all_parents = all_parents
        self.clip_polygon = clip_polygon
        self.clip_crs = clip_crs
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


def _resolve_clip_crs(geom, reader_clip_crs):
    """Reader CRS precedence: embedded SRID > reader clipCrs > raster CRS (None)."""
    import shapely

    srid = shapely.get_srid(geom)
    if srid and srid > 0:
        return f"EPSG:{srid}"
    return reader_clip_crs or None


def _clip_partitions(
    file_path: str,
    clip_polygons: Sequence,
    clip_crs: Optional[str],
    *,
    emit_virtual: bool,
) -> list:
    """One _TilePartition per clipPolygon whose envelope intersects the raster
    (skip disjoint). Shared by the virtual and materialized planning branches;
    the two differ only by the ``emit_virtual`` flag on the emitted partition.

    Fields: window=envelope, clip_polygon=<WKB bytes>, clip_crs=<resolved>.
    """
    import rasterio
    import shapely.wkb

    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.ds._window import window_for_geom

    parts: list = []
    with rasterio.open(file_path) as ds:
        for raw in clip_polygons:
            geom = parse_geom(raw)
            if geom is None:
                raise ValueError(f"raster_gbx: unparseable clipPolygons entry: {raw!r}")
            resolved = _resolve_clip_crs(geom, clip_crs)
            win = window_for_geom(ds, geom, geom_crs=resolved)
            if win is None:
                continue  # envelope disjoint -> no tile
            parts.append(
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
                    emit_virtual=emit_virtual,
                    clip_polygon=(
                        raw
                        if isinstance(raw, (bytes, bytearray))
                        else shapely.wkb.dumps(geom)
                    ),
                    clip_crs=resolved,
                )
            )
    return parts


def _plan_partitions_for_file(
    file_path: str,
    budget_bytes: int,
    *,
    clip_polygons: Sequence = (),
    clip_crs: Optional[str] = None,
    windows: Sequence = (),
    tile_size=None,
    overlap_percent: int = 0,
    emit_virtual: bool = False,
) -> Sequence["_TilePartition"]:
    """Driver-side: open a raster header and return one _TilePartition per tile.

    All window planning happens here so each worker task handles exactly one
    (file, window) pair.
    """
    import rasterio

    # ------------------------------------------------------------------
    # virtualTiles short-circuit: bytes-free partitions.
    #  - no clip: one whole-file partition.
    #  - clipPolygons: one bytes-free partition per polygon whose envelope
    #    intersects the raster, carrying the clip as instructions that
    #    open_tile applies at read time (raster stays null).
    # tileSize takes precedence: if tile_size is set, fall through to the
    # tileSize grid-planning branch which threads emit_virtual correctly.
    # ------------------------------------------------------------------
    if emit_virtual and not tile_size:
        if clip_polygons:
            return _clip_partitions(
                file_path, clip_polygons, clip_crs, emit_virtual=True
            )

        # Approach 3 — lazy planning: skip the header open at plan time.
        # Reuses the existing null-window slot (window=None already means
        # "passthrough GTiff fast path" for materialized tiles), gated by
        # emit_virtual=True. read() dispatches emit_virtual first, so there
        # is no collision with the materialized passthrough case (is_passthrough=True).
        # This avoids N rasterio.open calls when loading a directory of N files.
        return [
            _TilePartition(
                file_path=file_path,
                window=None,  # filled lazily by read() on the executor
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
                emit_virtual=True,
            )
        ]

    # ------------------------------------------------------------------
    # clipPolygons: one tile per polygon whose envelope intersects the raster.
    # ------------------------------------------------------------------
    if clip_polygons:
        return _clip_partitions(file_path, clip_polygons, clip_crs, emit_virtual=False)

    # ------------------------------------------------------------------
    # windows: one tile per pixel window, clipped to extent (skip fully-outside).
    # ------------------------------------------------------------------
    if windows:
        from rasterio.windows import Window as _W

        parts = []
        with rasterio.open(file_path) as ds:
            full = _W(0, 0, ds.width, ds.height)
            for c, r, w, h in windows:
                try:
                    iw = _W(c, r, w, h).intersection(full)
                except Exception:
                    continue  # disjoint
                if iw.width < 1 or iw.height < 1:
                    continue
                parts.append(
                    _TilePartition(
                        file_path=file_path,
                        window=(
                            int(iw.col_off),
                            int(iw.row_off),
                            int(iw.width),
                            int(iw.height),
                        ),
                        is_passthrough=False,
                        is_whole=True,
                        emit_fmt="gtiff",
                    )
                )
        return parts

    # ------------------------------------------------------------------
    # tileSize: plan a regular grid of (tw x th) windows using
    # plan_grid_windows.  Materialized tiles are guarded against the
    # ~2 GB Spark cell limit at plan time; virtual tiles are bytes-free
    # so no guard is applied (it fires later at materialize time if ever).
    # ------------------------------------------------------------------
    if tile_size:
        from databricks.labs.gbx.pyrx.core.tiling import plan_grid_windows

        tw, th = int(tile_size[0]), int(tile_size[1])
        with rasterio.open(file_path) as ds:
            W, H = ds.width, ds.height
            bands = ds.count
            itemsize = _numpy_itemsize(ds.dtypes[0])
        if not emit_virtual:
            # materialized-only guard: the nominal cell must fit the ~2 GB Spark
            # cell limit (overlap changes stride, not tile size). Virtual tiles
            # carry no bytes -> no guard here (fires later at materialize time).
            cell = tw * th * max(bands, 1) * itemsize
            if cell > _MAX_TILE_BYTES:
                raise ValueError(
                    f"raster_gbx: tileSize {tw}x{th} materialized cell is "
                    f"~{cell // (1024 * 1024)} MB, exceeding the ~2 GB Spark cell "
                    f"limit; use a smaller tileSize or virtualTiles=true."
                )
        return [
            _TilePartition(
                file_path=file_path,
                window=(c, r, w, h),
                is_passthrough=False,
                is_whole=True,
                emit_fmt="gtiff",
                emit_virtual=emit_virtual,
            )
            for c, r, w, h in plan_grid_windows(W, H, tw, th, overlap_percent)
        ]

    size_bytes = os.path.getsize(file_path)

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


def _as_geom_list(val) -> list:
    """Normalize a clipPolygons option to a list of geometry inputs.

    Spark ``.option()`` values are strings, so a list is passed as a JSON-array
    string ``'["<wkt1>","<wkt2>"]'`` and auto-detected: ``json.loads`` that
    yields a list is used as the list; anything else (a bare WKT/EWKT string,
    which is never valid JSON) is treated as ONE geometry. Programmatic callers
    may pass a real Python list (used as-is) or single bytes/str.
    """
    if val is None or val == "":
        return []
    if isinstance(val, (bytes, bytearray)):
        return [val]
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
        except (ValueError, TypeError):
            return [val]  # bare WKT/EWKT (not JSON) -> single geometry
        return list(parsed) if isinstance(parsed, list) else [val]
    return list(val)  # already a sequence of geometry inputs (programmatic)


def _as_tile_size(val):
    """Parse a tileSize option ("w,h" or bare "n" -> (n,n)) to (w,h) or None."""
    if val is None or val == "":
        return None
    if isinstance(val, (tuple, list)) and len(val) == 2:
        w, h = int(val[0]), int(val[1])
    else:
        parts = [p for p in str(val).split(",") if p.strip() != ""]
        if len(parts) == 1:
            w = h = int(parts[0])
        elif len(parts) == 2:
            w, h = int(parts[0]), int(parts[1])
        else:
            raise ValueError(
                f"raster_gbx: 'tileSize' must be 'w,h' or a single int; got {val!r}"
            )
    if w <= 0 or h <= 0:
        raise ValueError(f"raster_gbx: 'tileSize' must be positive; got {val!r}")
    return (w, h)


def _as_window_list(val) -> list:
    """Normalize a windows option to a list of (col,row,w,h) int tuples.

    Over ``.option()`` a single window is a JSON 4-int array ``"[0,0,256,256]"``
    and a list is a JSON array of 4-int arrays ``"[[..],[..]]"`` (auto-detected
    via ``json.loads``: list-of-lists -> many; flat 4-int list -> one).
    Programmatic callers may pass a 4-int tuple/list or a list of them.
    """
    if val is None or val == "":
        return []
    if isinstance(val, str):
        try:
            val = json.loads(val)  # JSON 4-int array or array-of-4-int-arrays
        except ValueError as exc:
            raise ValueError(
                "raster_gbx: 'windows' must be a JSON 4-int array '[c,r,w,h]' or a "
                f"JSON array of them '[[..],[..]]'; got {val!r}"
            ) from exc
    # single (c,r,w,h)?
    if (
        isinstance(val, (tuple, list))
        and len(val) == 4
        and all(isinstance(v, (int, float)) for v in val)
    ):
        return [tuple(int(v) for v in val)]
    return [tuple(int(v) for v in w) for w in val]  # list of windows


# ---------------------------------------------------------------------------
# Budget resolution helper (shared by partitions() and _partitions_from_tile_rows)
# ---------------------------------------------------------------------------


def _resolved_budget(size_mib: int, strategy) -> int:
    """Return the decoded-memory budget in bytes.

    ``size_mib > 0`` is a power-user override that wins over ``strategy``.
    ``size_mib <= 0`` defers to the strategy-derived budget (0 = no split).
    """
    if size_mib > 0:
        return size_mib * 1024 * 1024
    return budget.decoded_budget_bytes(strategy)


# ---------------------------------------------------------------------------
# Manifest / tile-table helpers (Approach 1 — pre-computed tile input)
# ---------------------------------------------------------------------------


def _read_manifest_rows(manifest_path: str) -> list:
    """Read tile rows from a JSON or Parquet manifest file.

    JSON: a list of dicts; each must have ``path`` (str) and optionally
    ``window`` ([col_off, row_off, width, height]), ``width``, ``height``,
    ``bands``, ``dtype``, ``srid``.

    Parquet: read via ``spark.table()`` / ``spark.read.parquet(...)`` on the
    driver (Connect-safe); returns a list of ``pyspark.sql.Row`` objects.
    """
    import os as _os

    local = _listing.to_local_path(manifest_path)
    if not _os.path.exists(local):
        raise FileNotFoundError(f"raster_gbx: manifest not found: {manifest_path!r}")
    if local.endswith(".parquet"):
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "raster_gbx: reading a Parquet manifest requires an active SparkSession."
            )
        return spark.read.parquet(local).collect()
    # JSON (default)
    with open(local) as fh:
        return json.load(fh)


def _row_to_window(w) -> Optional[Tuple[int, int, int, int]]:
    """Normalise a manifest row's ``window`` value to a 4-int tuple or None.

    Accepts: None, a 4-element list/tuple [c, r, w, h], or a Spark Row /
    namedtuple with fields ``col_off``, ``row_off``, ``width``, ``height``.
    """
    if w is None:
        return None
    if isinstance(w, (list, tuple)):
        return tuple(int(v) for v in w)  # type: ignore[return-value]
    # Spark Row / namedtuple
    return (int(w.col_off), int(w.row_off), int(w.width), int(w.height))


def _partitions_from_tile_rows(
    rows,
    *,
    emit_virtual: bool,
    budget_bytes: int,
    clip_polygons: Sequence = (),
    clip_crs: Optional[str] = None,
    windows: Sequence = (),
    tile_size=None,
    overlap_percent: int = 0,
) -> list:
    """Build ``_TilePartition`` objects from pre-computed tile rows.

    Each row is a dict (JSON manifest) or a ``pyspark.sql.Row`` (Parquet /
    table). Required field: ``path``. Optional fields: ``window`` (4-element),
    ``width``, ``height`` (whole-file dims when window absent).

    Decision tree per row:
    - Row has ``window``  → build ``_TilePartition`` directly; NO rasterio.open.
    - Row has ``width`` + ``height`` (no window) → use ``(0, 0, w, h)``; NO open.
    - Row has only ``path`` → call ``_plan_partitions_for_file``; header read
      occurs for that file only when ``emit_virtual=False``.  When
      ``emit_virtual=True``, planning defers the header open to the executor
      (lazy ``window=None``), so no header open happens at planning time even
      for path-only rows.
    """
    result: list = []
    for row in rows:
        r: dict = row.asDict() if hasattr(row, "asDict") else dict(row)
        path = str(r.get("path") or "")
        if not path:
            raise ValueError(
                f"raster_gbx manifest/table: row missing 'path' field: {r!r}"
            )

        win = _row_to_window(r.get("window"))

        # Also handle flat column layout: col_off/row_off/width/height at top level
        # (common for tilesTable results that store window fields as separate columns).
        if win is None and all(
            k in r for k in ("col_off", "row_off", "width", "height")
        ):
            win = (
                int(r["col_off"]),
                int(r["row_off"]),
                int(r["width"]),
                int(r["height"]),
            )

        if win is None:
            # Try whole-file dims as fallback
            w_val = r.get("width")
            h_val = r.get("height")
            if w_val is not None and h_val is not None:
                win = (0, 0, int(w_val), int(h_val))

        if win is not None:
            # Window is known → build partition directly, no header open
            result.append(
                _TilePartition(
                    file_path=path,
                    window=win,
                    is_passthrough=False,
                    is_whole=True,
                    emit_fmt="gtiff",
                    emit_virtual=emit_virtual,
                )
            )
        else:
            # Path only → open header for this specific file
            result.extend(
                _plan_partitions_for_file(
                    file_path=path,
                    budget_bytes=budget_bytes,
                    clip_polygons=clip_polygons,
                    clip_crs=clip_crs,
                    windows=list(windows),
                    tile_size=tile_size,
                    overlap_percent=overlap_percent,
                    emit_virtual=emit_virtual,
                )
            )
    return result


def _parse_vrt_members(vrt_path: str) -> list:
    """Parse a GDAL VRT XML and return deduplicated absolute member file paths.

    Handles both ``relativeToVRT="1"`` (path is relative to the VRT's directory)
    and absolute paths (``relativeToVRT="0"`` or attribute absent).

    Multi-band VRTs reference the same tile once per ``VRTRasterBand`` — the
    resulting duplicates are removed while preserving first-occurrence order.

    Connect-safe: pure Python / stdlib I/O; no Spark session, no ``_jvm``.
    No ``osgeo`` required — parses VRT XML directly.

    Parameters
    ----------
    vrt_path:
        Path to the ``.vrt`` file (local, FUSE Volume, or ``dbfs:`` URI).

    Returns
    -------
    list of str
        Ordered, deduplicated absolute paths of all ``SourceFilename`` members.

    Raises
    ------
    ValueError
        When the VRT contains no ``SourceFilename`` elements.
    """
    # Prefer defusedxml for external-origin XML (guards against XXE / XML bombs).
    # Fall back to stdlib when defusedxml is not installed.
    try:
        import defusedxml.ElementTree as _ET
    except ImportError:
        import xml.etree.ElementTree as _ET  # type: ignore[no-redef]

    local_vrt = _listing.to_local_path(vrt_path)
    tree = _ET.parse(local_vrt)
    vrt_dir = os.path.dirname(os.path.abspath(local_vrt))

    seen: set = set()
    paths: list = []
    for sf in tree.getroot().iter("SourceFilename"):
        text = (sf.text or "").strip()
        if not text:
            continue
        rel = sf.get("relativeToVRT", "0")
        if rel == "1":
            abs_path = os.path.normpath(os.path.join(vrt_dir, text))
        elif os.path.isabs(text):
            abs_path = text
        else:
            abs_path = os.path.abspath(text)
        if abs_path not in seen:
            seen.add(abs_path)
            paths.append(abs_path)

    if not paths:
        raise ValueError(
            f"raster_gbx: VRT at {vrt_path!r} contains no SourceFilename members"
        )
    return paths


class RasterGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("raster_gbx requires a 'path' (e.g. .load(path)).")
        self.size_mib = int(options.get("sizeInMB", "-1"))
        self.filter_regex = options.get("filterRegex", ".*")
        # Unified enumeration options (Task 4: session-free file_gbx core).
        # These are parsed once in __init__ and consumed by _list_source_files().
        self.recursive = str(options.get("recursive", "true")).lower() == "true"
        self.include_hidden = (
            str(options.get("includeHidden", "false")).lower() == "true"
        )
        _exts = options.get("extensions")
        self.extensions = (
            tuple(e.strip() for e in _exts.split(",") if e.strip()) if _exts else None
        )
        self.path_glob_filter = options.get("pathGlobFilter") or None
        # Split strategy: default is "none" (halo mode — prepare a COG via the
        # cog_gbx writer, then read windows). Opt-in split: splitStrategy=serverless
        # or splitStrategy=classic, or sizeInMB>0. COG creation is a writer concern;
        # split tiles are always emitted as plain GTiff.
        self.strategy = budget.resolve_strategy(options.get("splitStrategy", "none"))
        # AOI selection (mutually exclusive): clipPolygons (arbitrary geometry,
        # single or list) OR windows (pixel (col,row,w,h), single or list) OR
        # tileSize (regular grid, (tw,th) in pixels, optional overlapPercent).
        self.clip_polygons = _as_geom_list(options.get("clipPolygons"))
        self.windows = _as_window_list(options.get("windows"))
        self.clip_crs = options.get("clipCrs")
        self.tile_size = _as_tile_size(options.get("tileSize"))
        self.overlap_percent = int(options.get("overlapPercent", "0"))
        if not (0 <= self.overlap_percent < 100):
            raise ValueError(
                f"raster_gbx: 'overlapPercent' must be 0..99; got {self.overlap_percent}"
            )
        _selectors = [
            bool(self.clip_polygons),
            bool(self.windows),
            bool(self.tile_size),
        ]
        if sum(_selectors) > 1:
            raise ValueError(
                "raster_gbx: 'clipPolygons', 'windows', and 'tileSize' are mutually "
                "exclusive; supply at most one."
            )
        if self.overlap_percent > 0 and not self.tile_size:
            raise ValueError(
                "raster_gbx: 'overlapPercent' requires 'tileSize' (it modifies the "
                "regular tiling grid only)."
            )
        self.emit_virtual = str(options.get("virtualTiles", "true")).lower() == "true"

        # Approach 1 — pre-computed tile input.
        # When manifest or tilesTable is present, partitions() reads tile rows
        # from those sources instead of walking self.path.
        self.manifest = options.get("manifest")
        self.tiles_table = options.get("tilesTable")
        # skipOrdering: when True, suppress the T8 sort-by-source-path at the end
        # of partitions() so rows are returned in manifest/table/walk enumeration
        # order.  Default False preserves today's always-on sort (B-ORDERHOME).
        self.skip_ordering = str(options.get("skipOrdering", "false")).lower() == "true"
        if self.manifest and self.tiles_table:
            raise ValueError(
                "raster_gbx: 'manifest' and 'tilesTable' are mutually exclusive; "
                "supply at most one."
            )

    def _list_source_files(self) -> list:
        """Enumerate source files via the session-free file_gbx core.

        Unified options (recursive / includeHidden / extensions / pathGlobFilter)
        drive the shared enumerator; ``filterRegex`` (if set to something other
        than the default ".*") is applied as an ADDITIONAL full-path regex filter
        on top, preserving the historical power-tool surface used by samples/bench.
        """
        from databricks.labs.gbx.ds.file_gbx import list_local_files

        files = list_local_files(
            self.path,
            recursive=self.recursive,
            include_hidden=self.include_hidden,
            extensions=self.extensions,
            path_glob_filter=self.path_glob_filter,
        )
        if self.filter_regex and self.filter_regex != ".*":
            import re as _re

            pat = _re.compile(self.filter_regex)
            files = [f for f in files if pat.match(f)]
        # A .vrt is a mosaic INDEX, not a walkable raster member. Reading it during
        # a directory walk double-counts: the VRT spans the union of the tiles it
        # indexes, and those tile_*.tif are also walked. A .vrt is honored only
        # when the load path points directly at it (the .vrt branch in partitions()).
        files = [f for f in files if not f.lower().endswith(".vrt")]
        return files

    def partitions(self) -> Sequence[InputPartition]:
        resolved_budget = _resolved_budget(self.size_mib, self.strategy)

        # Approach 1: pre-computed tile input bypasses os.walk + per-file header opens.
        if self.manifest or self.tiles_table:
            if self.manifest:
                tile_rows = _read_manifest_rows(self.manifest)
            else:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if spark is None:
                    raise RuntimeError(
                        "raster_gbx: 'tilesTable' requires an active SparkSession."
                    )
                tile_rows = spark.table(self.tiles_table).collect()
            parts = _partitions_from_tile_rows(
                tile_rows,
                emit_virtual=self.emit_virtual,
                budget_bytes=resolved_budget,
                clip_polygons=self.clip_polygons,
                clip_crs=self.clip_crs,
                windows=self.windows,
                tile_size=self.tile_size,
                overlap_percent=self.overlap_percent,
            )
            # Layout convention (T8): sort by source file path so tiles from the
            # same file are adjacent. Maximises per-worker open/stage cache reuse
            # when a worker processes consecutive partitions from the same source.
            # None-safe: place None values at the end (deterministic ordering).
            # skipOrdering=True bypasses this sort (opt-out for pre-ordered tables).
            if self.skip_ordering:
                return parts
            return sorted(parts, key=lambda p: (p.file_path is None, p.file_path or ""))

        # VRT expansion: when the path is a .vrt file, parse the VRT XML to
        # enumerate member paths instead of walking a directory.  Each member
        # is treated as an independent whole-file source — one _TilePartition
        # per member (the existing whole-file virtual-tile path handles the
        # actual emission in read()).  This is the payoff of the mini-COG
        # mosaic write: the reader expands the VRT back into per-member tile
        # rows so all downstream rst_* ops work unchanged.
        #
        # Primary load convention: point at the .vrt directly.
        #   spark.read.format("raster_gbx").load("/mosaic/mosaic.vrt")
        # Directory walk (unmodified, .vrt files included as rasterio sources)
        # remains available when the user points at the containing directory.
        #
        # Connect-safe: _parse_vrt_members() does pure Python / stdlib I/O with
        # no Spark session, no _jvm, no .rdd access.
        if self.path.lower().endswith(".vrt"):
            member_paths = _parse_vrt_members(self.path)
            result: list = []
            for member_path in member_paths:
                result.extend(
                    _plan_partitions_for_file(
                        file_path=member_path,
                        budget_bytes=resolved_budget,
                        clip_polygons=self.clip_polygons,
                        clip_crs=self.clip_crs,
                        windows=self.windows,
                        tile_size=self.tile_size,
                        overlap_percent=self.overlap_percent,
                        emit_virtual=self.emit_virtual,
                    )
                )
            if self.skip_ordering:
                return result
            return sorted(
                result, key=lambda p: (p.file_path is None, p.file_path or "")
            )

        # Default path: walk self.path and plan partitions per file.
        # Layout convention (T8): sort the final list by file_path so tiles
        # from the same source are always adjacent, regardless of walk order.
        # Maximises per-worker rasterio open/stage cache reuse.
        # To control task parallelism or re-align after a join, use
        #   df.repartition(n, 'source').sortWithinPartitions('source')
        # on the read result; n is a parallelism signal (classic ≈ 3–5× cores),
        # never n = file_count.
        files = self._list_source_files()
        result: list = []
        for f in files:
            result.extend(
                _plan_partitions_for_file(
                    file_path=f,
                    budget_bytes=resolved_budget,
                    clip_polygons=self.clip_polygons,
                    clip_crs=self.clip_crs,
                    windows=self.windows,
                    tile_size=self.tile_size,
                    overlap_percent=self.overlap_percent,
                    emit_virtual=self.emit_virtual,
                )
            )
        # None-safe: place None values at the end (deterministic ordering).
        # skipOrdering=True bypasses this sort (opt-out for pre-ordered input).
        if self.skip_ordering:
            return result
        return sorted(result, key=lambda p: (p.file_path is None, p.file_path or ""))

    def read(self, partition: "_TilePartition") -> Iterator[Tuple]:
        import rasterio

        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()

        # Spark's Python DataSource V2 can call read(None) when the partition
        # list is empty (e.g. all clip polygons miss). Guard and emit nothing.
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
        # Virtual tile: bytes-free emission (path + whole-file window).
        # No staging, no encode — header opened locally for metadata.
        # NOTE: tile.crs is intentionally left None for virtual tiles.
        # The field doubles as the open_tile warp-target instruction; setting
        # it to the source CRS here would activate the warp path when a user
        # subsequently calls rst_setsrid (pending_srid != tile.crs -> spurious
        # warp).  For virtual tiles the source CRS is implicit in the path.
        # ------------------------------------------------------------------
        if getattr(partition, "emit_virtual", False):
            # ---------------------------------------------------------------
            # Whole-file virtual tile (window=None, no clip): DEFER the
            # rasterio.open.  The per-tile FUSE header open is the dominant
            # Serverless cost (~74s/1k tiles); deferring it removes ~1000 FUSE
            # opens from the DataSource read path.
            #
            # os.path.getsize is kept: it is a single cheap metadata stat
            # (not a file open) and is required for path_file_size so the
            # LRU size-aware scheduler (grouped_exec.py / file_gbx.py) still
            # receives source size without a separate per-tile call.
            #
            # width/height/count/driver are OMITTED from metadata: they are
            # resolved lazily at the first pixel op via the FILE-stream +
            # per-partition LRU (open_windowed_via_fileref / open_tile).
            # ---------------------------------------------------------------
            if partition.window is None and partition.clip_polygon is None:
                file_size = os.path.getsize(partition.file_path)
                # Deferred whole-file virtual read: the header is NOT opened here,
                # so COG cannot be distinguished from plain GeoTIFF (both are .tif)
                # without a read — report "gtiff". The pixel read opens the file and
                # rasterio applies COG overviews regardless of this metadata string.
                meta = {
                    "sourcePath": partition.file_path,
                    "format": "gtiff",
                    "path_file_size": str(file_size),
                }
                yield (
                    source,
                    _v2_tile_row(
                        _encode.CELLID_FRESH,
                        None,
                        path=partition.file_path,
                        window=None,  # deferred: open_tile resolves to full extent
                        metadata=meta,
                        clip_polygon=None,
                        clip_crs=None,
                    ),
                )
            else:
                # Non-whole-file virtual tile (clip-envelope window, explicit
                # window, or tile_size window): keep the header open.
                # window is already set by the planner; dims are needed for
                # metadata and to confirm the window fits the source extent.
                file_size = os.path.getsize(partition.file_path)
                with rasterio.open(partition.file_path) as ds:
                    meta = {
                        "sourcePath": partition.file_path,
                        "driver": ds.driver,
                        "format": ("cog" if ds.driver == "COG" else "gtiff"),
                        "width": str(ds.width),
                        "height": str(ds.height),
                        "count": str(ds.count),
                        "path_file_size": str(file_size),
                    }
                    win = (
                        partition.window
                    )  # always set for non-whole-file virtual tiles
                yield (
                    source,
                    _v2_tile_row(
                        _encode.CELLID_FRESH,
                        None,
                        path=partition.file_path,
                        window=win,
                        metadata=meta,
                        clip_polygon=partition.clip_polygon,
                        clip_crs=partition.clip_crs,
                    ),
                )
            return

        # ------------------------------------------------------------------
        # Materialized clip tile (Choice 2): stage + encode the envelope window,
        # then pre-clip via _clip.clip_dataset. A None return (all-nodata /
        # non-overlap) means SKIP -> emit nothing. Otherwise emit the clipped
        # GTiff bytes with clip_polygon/clip_crs kept as a reference to what was
        # applied. Runs BEFORE the generic encode branch.
        # ------------------------------------------------------------------
        if partition.clip_polygon is not None:
            from rasterio.io import MemoryFile

            from databricks.labs.gbx.pyrx.core import _clip
            from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

            local_path = _get_or_stage_file(partition.file_path)
            with rasterio.Env(GDAL_CACHEMAX=128):
                with rasterio.open(local_path) as ds:
                    tile_crs = crs_to_canonical(ds.crs)
                    _cellid, win_bytes, meta = _encode.encode_tile(
                        ds,
                        window=partition.window,
                        source_path=partition.file_path,
                        all_parents=partition.all_parents,
                        tile_format="gtiff",
                    )
            with MemoryFile(win_bytes) as mf, mf.open() as wds:
                clipped = _clip.clip_dataset(
                    wds, partition.clip_polygon, partition.clip_crs
                )
            if clipped is None:
                return  # all-nodata / non-overlap -> skip (no tile)
            # Add tile_size metadata (materialized raster byte length).
            meta["tile_byte_size"] = str(len(clipped))
            yield (
                source,
                _v2_tile_row(
                    _encode.CELLID_FRESH,
                    clipped,
                    path=partition.file_path,
                    window=partition.window,
                    metadata=meta,
                    clip_polygon=partition.clip_polygon,
                    clip_crs=partition.clip_crs,
                    crs=tile_crs,
                ),
            )
            return

        # ------------------------------------------------------------------
        # Passthrough tile: emit original file bytes, no decode.
        # ------------------------------------------------------------------
        if partition.is_passthrough:
            from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

            with rasterio.open(partition.file_path) as ds:
                tile_crs = crs_to_canonical(ds.crs)
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
            # Add tile_size metadata (materialized raster byte length).
            meta["tile_byte_size"] = str(len(raster_bytes))
            yield (
                source,
                _v2_tile_row(
                    cellid,
                    raster_bytes,
                    path=partition.file_path,
                    window=(0, 0, width, height),
                    metadata=meta,
                    crs=tile_crs,
                ),
            )
            return

        # ------------------------------------------------------------------
        # Windowed / whole-image encode: stage source once, read one window.
        # ------------------------------------------------------------------
        from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

        local_path = _get_or_stage_file(partition.file_path)

        with rasterio.Env(GDAL_CACHEMAX=128):
            with rasterio.open(local_path) as ds:
                tile_crs = crs_to_canonical(ds.crs)
                cellid, raster_bytes, meta = _encode.encode_tile(
                    ds,
                    window=partition.window,
                    source_path=partition.file_path,
                    all_parents=partition.all_parents,
                    tile_format=partition.emit_fmt,
                    cog_blocksize=partition.cog_blocksize,
                    cog_overview_resampling=partition.cog_overview_resampling,
                )
        # Add tile_size metadata (materialized raster byte length).
        meta["tile_byte_size"] = str(len(raster_bytes))
        yield (
            source,
            _v2_tile_row(
                cellid,
                raster_bytes,
                path=partition.file_path,
                window=partition.window,
                metadata=meta,
                crs=tile_crs,
            ),
        )

    # ------------------------------------------------------------------
    # Legacy path (backward compat with tests that use _FilePartition directly)
    # ------------------------------------------------------------------
    def _read_legacy(self, partition: "_FilePartition") -> Iterator[Tuple]:
        """Reproduce original one-file-yields-all-tiles behaviour for _FilePartition."""
        import rasterio

        source = _listing.to_spark_uri(partition.file_path)

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
                # Add tile_size metadata (materialized raster byte length).
                meta["tile_byte_size"] = str(len(raster_bytes))
                yield (
                    source,
                    _v2_tile_row(
                        cellid,
                        raster_bytes,
                        path=partition.file_path,
                        window=(0, 0, width, height),
                        metadata=meta,
                    ),
                )
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
                    # Add tile_size metadata (materialized raster byte length).
                    meta["tile_byte_size"] = str(len(raster_bytes))
                    yield (
                        source,
                        _v2_tile_row(
                            cellid,
                            raster_bytes,
                            path=partition.file_path,
                            window=(col, row, w, h),
                            metadata=meta,
                        ),
                    )
        finally:
            shutil.rmtree(staged_dir, ignore_errors=True)


class RasterGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "raster_gbx"

    def schema(self) -> StructType:
        return reader_schema_v2()

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
        # Resolve the compression surface.
        # ``compress`` is the canonical option; ``cogCompression`` is a
        # deprecated alias retained for back-compat. When both are supplied,
        # ``compress`` wins.
        _compress = self.options.get("compress")
        _cog_compression_alias = self.options.get("cogCompression")
        if _compress is None and _cog_compression_alias is not None:
            _compress = _cog_compression_alias.lower()
        if _compress is None:
            _compress = "auto"
        _compress_level_raw = self.options.get("compressLevel")
        _compress_level = (
            int(_compress_level_raw) if _compress_level_raw is not None else None
        )
        _predictor_raw = self.options.get("predictor")
        _predictor = int(_predictor_raw) if _predictor_raw is not None else None
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
            compress=_compress,
            compress_level=_compress_level,
            predictor=_predictor,
        )
