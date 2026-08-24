"""cog_gbx writer — master-COG file preparation. Two modes:

DEFAULT (per-partition, ``driverMode=false``):
  Accepts PATH-bearing rows (from file_gbx) and converts each source to ONE
  master COG via cog_convert_file (rasterio.shutil.copy driver="COG") on the
  EXECUTOR. GDAL reads the source natively; the COG is written to a worker-local
  temp then copied bytes-only to the output Volume. Pixels never ride a Spark
  column (accumulation-proof).
  CEILING: a single very large source (~1+ GiB) cannot be converted inside the
  Serverless DS-V2 write task — the worker's tight per-task memory envelope
  (~1 GB per-PySpark-UDF cap) is exhausted by GDAL's overview-build transient
  (confirmed: Python copyfileobj / GDAL-direct read both OOM; WorkspaceClient/
  dbutils cannot even be constructed in a worker). Inherent to the DS-V2 write
  sandbox. So the default mode is for MODERATE files.

driverMode (``driverMode=true``):
  write() on executors gathers only the source path strings (cap-safe, no GDAL,
  no pixels); commit() on the DRIVER runs prepare_cogs over the full list. The
  driver is NOT under the ~1 GB per-UDF cap, so this handles large single files
  and large batches one-at-a-time.

  MEMORY FOOTPRINT (why standard Serverless is enough):
    COG generation streams block-by-block (bounded GDAL cache) and processes one
    file at a time, so peak memory is dominated by GDAL's overview-build transient
    and is essentially FLAT regardless of source size OR batch count — measured
    ~2.0-2.1 GiB RSS for 1.5 GiB, 10x1.5 GiB, and a single 10 GiB source alike.
    That fits comfortably under a STANDARD Serverless driver (~16 GiB), so no
    special compute is needed. A HIGH-MEMORY Serverless driver (~32 GiB) only buys
    extra headroom for much larger single files / more margin — useful, not
    required. Crucially there is NO memory tier that helps the SPARK (worker)
    profile: worker tasks are capped at ~1 GB per PySpark UDF regardless of
    instance size — which is exactly why the distributed paths (this writer's
    default per-partition mode, and a scalar-UDF approach) OOM on large files
    while DRIVER-orchestrated preparation does not.

  ⚠ COMMIT TIMEOUT — Spark Connect channel cancellation:
    commit() runs the conversion INSIDE the ``.save()`` gRPC call. On Databricks
    Serverless (Spark Connect), a commit() that blocks for many minutes can have
    its channel cancelled — surfacing as
    ``UnknownException: (java.nio.channels.CancelledKeyException)`` and a FAILED
    run — even though nothing is wrong with the conversion itself. Risk grows
    with corpus size / per-file convert time (rough throughput ~1 GB/min).
    HOW TO AVOID IT: skip the writer and call the lower-level driver function
    directly from your notebook — it is plain Python on the driver with NO Spark
    RPC, so there is no channel to cancel:

        from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs
        # `sources` = a dir, a file, or a list mixing both (file_gbx not required)
        summary = prepare_cogs(sources, out_dir, blocksize=512, verbose=True)

    prepare_cogs is the robust path for large / long-running preparation; the
    driverMode writer is a convenience wrapper best used for smaller/faster
    batches where the .save() call completes well within the channel timeout.
    prepare_cogs is idempotent (skip_if_exists), so re-running after a timeout
    only fills the gaps.
"""

from __future__ import annotations

import glob
import hashlib
import logging
import math
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _listing
from databricks.labs.gbx.ds.file_gbx import (
    _COG_DRIVER_MAX_BYTES,
    StageTooLargeError,
    _connect_aware_lru_sizing,
    materialize_decision,
)

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mosaic-mode tile grid helpers
# ---------------------------------------------------------------------------

#: Default tile edge length (pixels) when MosaicOptions.tile_size is None.
_MOSAIC_DEFAULT_TILE_SIZE = 1024


def _tile_grid_windows(
    src_width: int, src_height: int, tile_size: int, overlap_pct: float
):
    """Yield ``(row_idx, col_idx, Window)`` for each tile in the native pixel grid.

    Parameters
    ----------
    src_width, src_height:
        Source raster dimensions in pixels.
    tile_size:
        Tile edge length in pixels (square grid cells).
    overlap_pct:
        Tile-edge overlap as a percentage of tile_size (0 = non-overlapping).
        A positive value expands each tile's read window by
        ``ceil(tile_size * overlap_pct / 100)`` pixels on every side (halo),
        clamped to the source bounds.

    Yields
    ------
    (row_idx, col_idx, rasterio.windows.Window)
        Row-major order (row 0 col 0, row 0 col 1, ...).  The Window is
        pixel-aligned to the source and clamped to [0, src_*) on every edge.
    """
    from rasterio.windows import Window

    n_cols = math.ceil(src_width / tile_size)
    n_rows = math.ceil(src_height / tile_size)
    halo = int(math.ceil(tile_size * overlap_pct / 100.0)) if overlap_pct > 0 else 0

    for r in range(n_rows):
        for c in range(n_cols):
            base_col = c * tile_size
            base_row = r * tile_size
            base_w = min(tile_size, src_width - base_col)
            base_h = min(tile_size, src_height - base_row)

            # Expand with halo (clamped to source bounds on every edge).
            exp_col = max(0, base_col - halo)
            exp_row = max(0, base_row - halo)
            exp_right = min(src_width, base_col + base_w + halo)
            exp_bottom = min(src_height, base_row + base_h + halo)
            exp_w = exp_right - exp_col
            exp_h = exp_bottom - exp_row

            yield r, c, Window(exp_col, exp_row, exp_w, exp_h)


def _source_discriminator(src_path: str) -> str:
    """Return a short, stable, filesystem-safe discriminator for *src_path*.

    Mosaic tiles from every source in a write land in the same flat ``out_dir``.
    Naming them ``tile_<row>_<col>.tif`` with no source component means two source
    rasters in one write (the normal multi-file input, or two partitions sharing
    ``out_dir``) collide on identical tile names — ``cog_skip_if_exists`` skips one
    or the other silently overwrites it, losing pixels and leaving a VRT that
    presents one source as the whole mosaic.

    This discriminator namespaces the tile name
    (``tile_<disc>_<row>_<col>.tif``):

    - **Stable / deterministic** — the same source path always yields the same
      discriminator, so an idempotent re-run (``skip_if_exists``) and any partition
      that reprocesses the same source line up on identical names.
    - **Distinct per source** — different source paths (even the same basename in
      different directories) yield different discriminators via the path hash, so
      distinct sources never collide, even across partitions writing one
      ``out_dir``.
    - **Single alnum token** — contains no ``_``, so ``tile_<disc>_<row>_<col>``
      parses unambiguously (row/col are always the final two ``_``-separated
      tokens).

    The human-readable stem prefix is a convenience for eyeballing the output
    directory; correctness rests on the 8-hex sha1 suffix of the full path.
    """
    digest = hashlib.sha1(src_path.encode("utf-8")).hexdigest()[:8]
    stem = os.path.splitext(os.path.basename(src_path))[0]
    safe = re.sub(r"[^0-9A-Za-z]+", "", stem)[:16]
    return f"{safe}{digest}" if safe else digest


def _is_all_nodata(data, nodata) -> bool:
    """Return True when every pixel in *data* matches *nodata*.

    Parameters
    ----------
    data:
        numpy array of pixel values (any shape).
    nodata:
        Source nodata value (may be None, float, or NaN).

    Returns False when nodata is None (no declared nodata → cannot prune).
    """
    import numpy as np

    if nodata is None:
        return False
    if isinstance(nodata, float) and math.isnan(nodata):
        return bool(np.all(np.isnan(data)))
    return bool(np.all(data == nodata))


# ---------------------------------------------------------------------------
# Mosaic-mode option model
# ---------------------------------------------------------------------------

_DGGS_SYSTEMS = frozenset({"quadbin", "bng", "h3"})
_VALID_GRID_SYSTEMS = frozenset({"none"}) | _DGGS_SYSTEMS
_VALID_MERGE_STRATEGIES = frozenset({"none", "min", "max", "avg", "first", "last"})
_VALID_VRT_PATHS = frozenset({"relative", "absolute"})

# ---------------------------------------------------------------------------
# VRT mosaic index builder (pure Python + rasterio — no osgeo dependency)
# ---------------------------------------------------------------------------

#: Mapping from rasterio dtype string to GDAL VRT dataType attribute name.
_RASTERIO_TO_GDAL_DTYPE: Dict[str, str] = {
    "uint8": "Byte",
    "int8": "Int8",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint32": "UInt32",
    "int32": "Int32",
    "float32": "Float32",
    "float64": "Float64",
    "complex64": "CFloat32",
    "complex128": "CFloat64",
}


def _build_mosaic_vrt(
    tile_paths: List[str],
    out_dir: str,
    vrt_paths: str = "relative",
) -> str:
    """Build a GDAL VRT mosaic index at ``<out_dir>/mosaic.vrt``.

    Pure Python + rasterio — no ``osgeo`` / native GDAL Python bindings
    required (light-tier / Serverless safe).

    Algorithm
    ---------
    1. Open each tile to read its affine transform, width, height, CRS, band
       count, dtype, and nodata value.
    2. Compute the mosaic bounding box as the union of all tile extents.
    3. Derive the mosaic ``rasterXSize`` / ``rasterYSize`` and
       ``GeoTransform``.
    4. For each band, emit one ``<SimpleSource>`` per tile.  The
       ``<DstRect>`` offset is computed from the tile's spatial position
       relative to the mosaic origin.
    5. ``vrt_paths="relative"`` (default) writes bare filenames and sets
       ``relativeToVRT="1"`` so the VRT is portable.
       ``vrt_paths="absolute"`` writes full paths with ``relativeToVRT="0"``.

    Assumptions (valid for Phase A native-pixel tiling):
    - All tiles share the same CRS, pixel size, band count, and dtype.
    - The source uses a north-up affine transform (no rotation).

    Parameters
    ----------
    tile_paths:
        Absolute paths to the mini-COG tiles to include in the VRT.
    out_dir:
        Directory where ``mosaic.vrt`` will be written (same directory as
        the tiles, so relative paths are bare filenames).
    vrt_paths:
        ``"relative"`` (default) or ``"absolute"``.

    Returns
    -------
    str
        Absolute path to the written ``mosaic.vrt``.
    """
    import xml.etree.ElementTree as ET

    import rasterio

    vrt_out = os.path.join(out_dir, "mosaic.vrt")

    # ── Step 1: collect tile metadata ─────────────────────────────────────
    metas = []
    for path in tile_paths:
        with rasterio.open(path) as ds:
            metas.append(
                {
                    "path": path,
                    "width": ds.width,
                    "height": ds.height,
                    "transform": ds.transform,
                    "crs": ds.crs,
                    "dtypes": ds.dtypes,  # tuple, one per band
                    "count": ds.count,
                    "nodatavals": ds.nodatavals,  # tuple, one per band (None where unset)
                }
            )

    if not metas:
        _logger.warning("_build_mosaic_vrt: no tile paths supplied; VRT not written.")
        return vrt_out

    # ── Step 2: compute mosaic envelope ───────────────────────────────────
    # Use the first tile as the reference for CRS / pixel size.
    ref = metas[0]
    pixel_x = ref["transform"].a  # positive (east)
    pixel_y = ref["transform"].e  # negative (north-up)
    crs = ref["crs"]
    count = ref["count"]
    dtypes = ref["dtypes"]
    nodatavals = ref["nodatavals"]

    mosaic_left = min(m["transform"].c for m in metas)
    mosaic_top = max(m["transform"].f for m in metas)
    mosaic_right = max(m["transform"].c + m["width"] * pixel_x for m in metas)
    mosaic_bottom = min(m["transform"].f + m["height"] * pixel_y for m in metas)

    mosaic_width = int(round((mosaic_right - mosaic_left) / pixel_x))
    mosaic_height = int(round((mosaic_top - mosaic_bottom) / abs(pixel_y)))

    # ── Step 3: build VRT XML tree ─────────────────────────────────────────
    root = ET.Element(
        "VRTDataset",
        {
            "rasterXSize": str(mosaic_width),
            "rasterYSize": str(mosaic_height),
        },
    )

    # SRS (WKT)
    srs_el = ET.SubElement(root, "SRS")
    srs_el.text = crs.to_wkt() if crs else ""

    # GeoTransform: x_origin, pixel_x, rot_x, y_origin, rot_y, pixel_y
    gt_text = (
        f" {mosaic_left:.15g},"
        f"  {pixel_x:.15g},"
        f"  0,"
        f"  {mosaic_top:.15g},"
        f"  0,"
        f"  {pixel_y:.15g}"
    )
    ET.SubElement(root, "GeoTransform").text = gt_text

    # ── Step 4: per-band VRTRasterBand with one SimpleSource per tile ──────
    for band_idx in range(1, count + 1):
        dtype_name = _RASTERIO_TO_GDAL_DTYPE.get(str(dtypes[band_idx - 1]), "Float32")
        band_el = ET.SubElement(
            root, "VRTRasterBand", {"dataType": dtype_name, "band": str(band_idx)}
        )
        # Per-band NoData: rasterio's ``nodatavals`` is a tuple with one entry
        # per band (None where unset), so differing per-band nodata is honored.
        # Behavior-identical to the old single ``nodata`` for the common uniform
        # case. (rasterio's high-level API cannot WRITE differing per-band nodata
        # without osgeo, which the light tier forbids, so only uniform nodata is
        # unit-tested.)
        band_nodata = nodatavals[band_idx - 1]
        if band_nodata is not None:
            ET.SubElement(band_el, "NoDataValue").text = repr(float(band_nodata))

        for meta in metas:
            t = meta["transform"]
            tile_w = meta["width"]
            tile_h = meta["height"]

            # Pixel offset of this tile within the mosaic coordinate frame.
            dst_x = int(round((t.c - mosaic_left) / pixel_x))
            dst_y = int(round((mosaic_top - t.f) / abs(pixel_y)))

            if vrt_paths == "relative":
                fn_text = os.path.basename(meta["path"])
                rel_attr = "1"
            else:
                fn_text = meta["path"]
                rel_attr = "0"

            # ComplexSource (not SimpleSource): with a <NODATA> child GDAL SKIPS
            # source pixels equal to nodata when compositing. Hex mini-COGs are
            # bbox tiles whose corners (outside the hexagon) are nodata and whose
            # bboxes OVERLAP neighbours; SimpleSource copies those nodata corners
            # over a neighbour's real data → interior holes/seams. ComplexSource
            # makes nodata transparent so the neighbour's data shows through.
            src_el = ET.SubElement(band_el, "ComplexSource")
            fn_el = ET.SubElement(src_el, "SourceFilename", {"relativeToVRT": rel_attr})
            fn_el.text = fn_text
            ET.SubElement(src_el, "SourceBand").text = str(band_idx)
            ET.SubElement(
                src_el,
                "SrcRect",
                {
                    "xOff": "0",
                    "yOff": "0",
                    "xSize": str(tile_w),
                    "ySize": str(tile_h),
                },
            )
            ET.SubElement(
                src_el,
                "DstRect",
                {
                    "xOff": str(dst_x),
                    "yOff": str(dst_y),
                    "xSize": str(tile_w),
                    "ySize": str(tile_h),
                },
            )
            # Skip this tile's nodata pixels when compositing (transparent overlap).
            if band_nodata is not None:
                ET.SubElement(src_el, "NODATA").text = repr(float(band_nodata))

    # ── Step 5: write XML to disk ──────────────────────────────────────────
    tree = ET.ElementTree(root)
    with open(vrt_out, "wb") as fh:
        tree.write(fh, xml_declaration=True, encoding="utf-8")

    _logger.debug("_build_mosaic_vrt: wrote %s (%d members)", vrt_out, len(metas))
    return vrt_out


@dataclass
class MosaicOptions:
    """Validated options for cog_gbx mosaic mode (native mini-COG + VRT tiling).

    Constructed only by :func:`parse_mosaic_options`; never instantiated directly.
    ``None`` returned by that function means single-COG mode (unchanged behaviour).

    Attributes
    ----------
    grid_system:
        ``"none"`` = native pixel-tiling (Phase A).  DGGS systems (``quadbin``,
        ``bng``, ``h3``) are reserved for later phases and are rejected by the
        parser until that support lands.
    tile_size:
        Tile edge length in pixels (native tiling only).  ``None`` = use writer
        default.
    overlap_percent:
        Tile-edge overlap as a percentage of tile_size (native tiling only).
    merge_strategy:
        Pixel-merge rule for overlapping tiles.  ``"none"`` = last-write wins.
    prune_empty:
        When ``True`` (default) skip writing tiles that contain only NoData.
    write_vrt:
        When ``True`` (default) emit a ``.vrt`` mosaic index alongside the tiles.
    vrt_paths:
        Controls tile paths embedded inside the VRT: ``"relative"`` (default) or
        ``"absolute"``.
    grid_min_resolution / grid_max_resolution / grid_step_resolution:
        DGGS-only resolution range options.  Must be ``None`` when
        ``grid_system="none"``.
    """

    grid_system: str = "none"
    tile_size: Optional[int] = None
    overlap_percent: float = 0.0
    merge_strategy: str = "none"
    prune_empty: bool = True
    write_vrt: bool = True
    vrt_paths: str = "relative"
    grid_resolution: Optional[int] = None
    grid_min_resolution: Optional[int] = None
    grid_max_resolution: Optional[int] = None
    grid_step_resolution: Optional[int] = None


def _parse_bool(value, default: bool) -> bool:
    """Coerce *value* (string or bool) to bool.  Strings ``'false'/'0'/'no'`` are
    ``False``; anything else truthy is ``True``.  ``None`` returns *default*."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() not in ("false", "0", "no", "")


def _parse_grid_resolution(grid_system: str, opts: Dict[str, object]) -> Optional[int]:
    """Parse and validate ``gridResolution`` from *opts*.

    - Required for ``gridSystem='quadbin'``; raises if absent.
    - Must be absent for ``gridSystem='none'``; raises if present.
    """
    grid_res_raw = opts.get("gridResolution")
    if grid_system in ("quadbin", "h3"):
        if grid_res_raw is None:
            raise ValueError(
                f"gridResolution is required when gridSystem={grid_system!r}; "
                "supply an integer resolution level."
            )
        return int(grid_res_raw)
    if grid_res_raw is not None:
        raise ValueError(
            "gridResolution is only valid with a DGGS gridSystem "
            f"(quadbin, bng, h3); got gridSystem={grid_system!r}"
        )
    return None


def _check_quadbin_incompatible_opts(
    grid_system: str,
    opts: Dict[str, object],
    grid_min: Optional[int],
    grid_max: Optional[int],
    grid_step: Optional[int],
) -> None:
    """Raise ``ValueError`` for options that are not yet supported with quadbin."""
    if grid_system != "quadbin":
        return
    if opts.get("downsampleFactor") is not None:
        raise ValueError(
            "downsampleFactor is not supported with gridSystem='quadbin'; "
            "quadbin mosaic uses cell-based resolution, not pixel downsampling."
        )
    for _name, _val in (
        ("gridMinResolution", grid_min),
        ("gridMaxResolution", grid_max),
        ("gridStepResolution", grid_step),
    ):
        if _val is not None:
            raise ValueError(
                f"{_name}: DGGS resolution pyramid not yet implemented "
                f"(deferred to a follow-on release)."
            )


def _check_h3_incompatible_opts(
    grid_system: str,
    grid_min: Optional[int],
    grid_max: Optional[int],
    grid_step: Optional[int],
) -> None:
    """Raise ValueError for options that are not supported with h3.

    Unlike quadbin, h3 ALLOWS downsampleFactor (documented in spec).
    Only the resolution pyramid options are deferred.
    """
    if grid_system != "h3":
        return
    for _name, _val in (
        ("gridMinResolution", grid_min),
        ("gridMaxResolution", grid_max),
        ("gridStepResolution", grid_step),
    ):
        if _val is not None:
            raise ValueError(
                f"{_name}: DGGS resolution pyramid not yet implemented "
                f"(deferred to a follow-on release)."
            )


def parse_mosaic_options(opts: Dict[str, object]) -> Optional[MosaicOptions]:
    """Parse and validate mosaic-mode options from a raw options mapping.

    *opts* is typically the ``self.options`` dict from a Spark DataSource V2
    ``DataSource.writer()`` call (all values are strings), but the function also
    accepts Python-typed values for direct unit-test use.

    Returns
    -------
    ``None``
        When mosaic mode is NOT triggered (neither ``vrtMosaic`` nor ``gridSystem``
        is present in *opts*, or ``vrtMosaic`` is explicitly false).
    :class:`MosaicOptions`
        A fully-validated mosaic configuration.

    Raises
    ------
    ValueError
        When a supplied option combination is invalid or unsupported in this
        release (e.g. a DGGS grid system, contradictory ``driverMode``, or a
        native-only option combined with a DGGS system).
    """
    mosaic_raw = opts.get("vrtMosaic")
    grid_system_raw = opts.get("gridSystem")

    # ── mosaic-mode trigger ──────────────────────────────────────────────────
    # Triggered when `vrtMosaic` is truthy OR `gridSystem` is explicitly supplied.
    if mosaic_raw is None and grid_system_raw is None:
        return None  # single-COG mode — existing path unchanged

    # Explicit opt-out: vrtMosaic=false with no gridSystem.
    if grid_system_raw is None and not _parse_bool(mosaic_raw, True):
        return None

    # ── gridSystem ───────────────────────────────────────────────────────────
    grid_system = (
        str(grid_system_raw).lower() if grid_system_raw is not None else "none"
    )
    if grid_system not in _VALID_GRID_SYSTEMS:
        raise ValueError(
            f"gridSystem={grid_system!r} is not a recognised value; "
            f"must be one of {sorted(_VALID_GRID_SYSTEMS)}"
        )

    # quadbin and h3 are supported; bng is deferred to a later release.
    if grid_system in _DGGS_SYSTEMS and grid_system not in ("quadbin", "h3"):
        raise ValueError(
            f"gridSystem={grid_system!r} is not yet supported; "
            f"available values: 'none', 'quadbin', 'h3'."
        )

    is_dggs = grid_system in _DGGS_SYSTEMS

    # ── gridResolution (required for quadbin; invalid for 'none') ────────────
    grid_resolution: Optional[int] = _parse_grid_resolution(grid_system, opts)

    # ── driverMode contradiction ─────────────────────────────────────────────
    # driverMode produces a single driver-side COG; mosaic mode produces many
    # per-tile mini-COGs on executors.  The two are mutually exclusive.
    driver_mode_raw = opts.get("driverMode")
    if driver_mode_raw is not None:
        if _parse_bool(driver_mode_raw, False):
            raise ValueError(
                "driverMode=True and mosaic mode are contradictory: driverMode "
                "produces a single driver-side COG while mosaic mode produces "
                "per-tile mini-COGs on executors. "
                "Use one or the other, not both."
            )

    # ── tileSize (native only) ────────────────────────────────────────────────
    tile_size_raw = opts.get("tileSize")
    tile_size: Optional[int] = int(tile_size_raw) if tile_size_raw is not None else None
    if is_dggs and tile_size is not None:
        raise ValueError(
            f"tileSize is only valid with gridSystem='none' (native tiling); "
            f"got gridSystem={grid_system!r}"
        )

    # ── overlapPercent (native only) ──────────────────────────────────────────
    overlap_raw = opts.get("overlapPercent")
    overlap_percent: float = float(overlap_raw) if overlap_raw is not None else 0.0
    if is_dggs and overlap_raw is not None:
        raise ValueError(
            f"overlapPercent is only valid with gridSystem='none' (native tiling); "
            f"got gridSystem={grid_system!r}"
        )

    # ── mergeStrategy ─────────────────────────────────────────────────────────
    merge_raw = opts.get("mergeStrategy", "none")
    merge_strategy = str(merge_raw).lower()
    if merge_strategy not in _VALID_MERGE_STRATEGIES:
        raise ValueError(
            f"mergeStrategy={merge_raw!r} is not recognised; "
            f"must be one of {sorted(_VALID_MERGE_STRATEGIES)}"
        )

    # ── pruneEmpty / writeVrt / vrtPaths ──────────────────────────────────────
    prune_empty = _parse_bool(opts.get("pruneEmpty"), True)
    write_vrt = _parse_bool(opts.get("writeVrt"), True)

    vrt_paths_raw = opts.get("vrtPaths", "relative")
    vrt_paths = str(vrt_paths_raw).lower()
    if vrt_paths not in _VALID_VRT_PATHS:
        raise ValueError(f"vrtPaths={vrt_paths_raw!r} must be 'relative' or 'absolute'")

    # ── DGGS-only resolution options ──────────────────────────────────────────
    grid_min_raw = opts.get("gridMinResolution")
    grid_max_raw = opts.get("gridMaxResolution")
    grid_step_raw = opts.get("gridStepResolution")

    grid_min: Optional[int] = int(grid_min_raw) if grid_min_raw is not None else None
    grid_max: Optional[int] = int(grid_max_raw) if grid_max_raw is not None else None
    grid_step: Optional[int] = int(grid_step_raw) if grid_step_raw is not None else None

    if not is_dggs:
        if grid_min is not None:
            raise ValueError(
                f"gridMinResolution is only valid with a DGGS gridSystem "
                f"(quadbin, bng, h3); got gridSystem={grid_system!r}"
            )
        if grid_max is not None:
            raise ValueError(
                f"gridMaxResolution is only valid with a DGGS gridSystem "
                f"(quadbin, bng, h3); got gridSystem={grid_system!r}"
            )
        if grid_step is not None:
            raise ValueError(
                f"gridStepResolution is only valid with a DGGS gridSystem "
                f"(quadbin, bng, h3); got gridSystem={grid_system!r}"
            )

    # Resolution pyramid + downsampleFactor are deferred for quadbin.
    _check_quadbin_incompatible_opts(grid_system, opts, grid_min, grid_max, grid_step)
    _check_h3_incompatible_opts(grid_system, grid_min, grid_max, grid_step)

    return MosaicOptions(
        grid_system=grid_system,
        tile_size=tile_size,
        overlap_percent=overlap_percent,
        merge_strategy=merge_strategy,
        prune_empty=prune_empty,
        write_vrt=write_vrt,
        vrt_paths=vrt_paths,
        grid_resolution=grid_resolution,
        grid_min_resolution=grid_min,
        grid_max_resolution=grid_max,
        grid_step_resolution=grid_step,
    )


@dataclass
class CogCommitMessage(WriterCommitMessage):
    paths: List[str]
    # Source paths deferred to driver-side conversion (auto-routed by size gate).
    # These are NOT output paths — never remove them on abort().
    pending_paths: List[str] = field(default_factory=list)


def _is_v2_envelope(schema: StructType) -> bool:
    """True when *schema* is a (source, tile) envelope whose ``tile`` is a struct.

    Accepts either the v1 tile struct (``cellid, raster, metadata``) or the v2
    virtual struct (8 fields). ``_to_virtual_tile`` normalizes both, so the
    writer only needs to know it is receiving a tile envelope (not a top-level
    ``path`` column).
    """
    names = [f.name for f in schema.fields]
    if names != ["source", "tile"]:
        return False
    return isinstance(schema["tile"].dataType, StructType)


def assert_path_schema(schema: StructType) -> None:
    """cog_gbx writer accepts EITHER a top-level 'path' column (file_gbx output)
    OR a (source, tile) v1/v2 envelope (a virtual-tile DataFrame)."""
    names = [f.name for f in schema.fields]
    if "path" in names:
        return
    if _is_v2_envelope(schema):
        return
    raise ValueError(
        "cog_gbx writer requires a top-level 'path' column (file_gbx output) "
        f"or a (source, tile) tile envelope; got {names}"
    )


class CogGbxWriter(DataSourceWriter):
    def __init__(
        self,
        path,
        schema,
        overwrite,
        cog_blocksize=512,
        cog_overview_resampling="AVERAGE",
        # Unified compression surface (Task 5).
        # ``compress`` = "auto" | "zstd" | "deflate" | "lzw" | "none".
        # Deprecated: ``cog_compression`` is the old option; maps to ``compress``.
        # When compress == "auto", resolves to "ZSTD" (the spec ZSTD baseline).
        # If BOTH are given, compress wins.
        compress="auto",
        compress_level=None,
        predictor=None,
        cog_compression=None,
        name_col=None,
        ext="tif",
        cog_subdataset=None,
        cog_skip_if_exists=True,
        driver_mode=False,
        driver_mode_verbose=True,
        cog_bigtiff="YES",
        # Mosaic mode (Phase A: native mini-COG + VRT).  ``None`` = single-COG
        # mode (default, unchanged behaviour).  Set by CogGbxDataSource.writer()
        # after calling parse_mosaic_options(); Tasks 2–3 read self.mosaic_opts.
        mosaic_opts: Optional[MosaicOptions] = None,
    ):
        assert_path_schema(schema)
        self.tile_envelope = _is_v2_envelope(schema)
        self.out_dir = _listing.to_local_path(path)
        self.overwrite = overwrite
        self.cog_blocksize = int(cog_blocksize)
        self.cog_overview_resampling = cog_overview_resampling
        # Resolve compress: explicit compress wins over deprecated cog_compression.
        if compress == "auto" and cog_compression is not None:
            self.compress = cog_compression.lower()
        else:
            self.compress = compress
        self.compress_level = compress_level
        self.predictor = predictor
        self.name_col = name_col
        self.ext = ext
        self.cog_subdataset = cog_subdataset
        self.cog_skip_if_exists = cog_skip_if_exists
        self.driver_mode = driver_mode
        self.driver_mode_verbose = driver_mode_verbose
        self.cog_bigtiff = cog_bigtiff
        # Mosaic mode: None = single-COG (unchanged); set for Tasks 2–3 to read.
        self.mosaic_opts = mosaic_opts
        # Driver-capture the connect-aware materialize cap. __init__ runs on the
        # DRIVER (DataSource V2 contract) where a live session is present; the
        # instance is pickled to workers, so self._cap travels to write(). Resolving
        # the cap inside write() — which runs per-partition on a session-less
        # Serverless worker — would fall back to the 256 MiB classic cap and
        # mis-route a 64-256 MiB source to executor conversion → OOM. Use
        # _resolve_session_for_cap() (getActiveSession → getOrCreate fallback) so
        # that threading contexts where getActiveSession() returns None still resolve
        # to the live Connect session. Mirrors file_ref_arg / rst_fromfile's driver
        # capture (parity with those read-gate surfaces).
        from databricks.labs.gbx.ds.file_gbx import _resolve_session_for_cap

        self._cap = _connect_aware_lru_sizing(_resolve_session_for_cap())[0]
        if overwrite and os.path.isdir(self.out_dir):
            # Sweep stale outputs. Include the mosaic index (*.vrt): a prior
            # mosaic write leaves mosaic.vrt behind, and globbing only *.<ext>
            # would strand it — a reader pointed at the dir/vrt would then see a
            # stale index referencing tiles this overwrite may not reproduce.
            stale_paths = glob.glob(os.path.join(self.out_dir, f"*.{ext}"))
            stale_paths += glob.glob(os.path.join(self.out_dir, "*.vrt"))
            for stale in stale_paths:
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def _resolved_cog_compression(self) -> str:
        """Return the compression string for cog_convert_file / prepare_cogs.

        ``cog_convert_file`` expects a codec name that rio-cogeo recognises (e.g.
        ``"DEFLATE"``, ``"LZW"``, ``"ZSTD"``). When the writer's compress is
        ``"auto"``, resolve to ``"ZSTD"`` — the spec ZSTD baseline applies to
        COG outputs as well as GTiff outputs.  (``cog_convert_file`` routes
        "ZSTD" through the compression authority, which uses the balanced default
        level since decoded_bytes is unavailable in the streaming path.)
        """
        c = str(self.compress).lower()
        if c == "auto":
            return "ZSTD"
        if c == "none":
            return "RAW"
        return c.upper()

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        # Mosaic mode: branch before tile_envelope check so that path-column
        # inputs produce a mini-COG grid instead of a single output file.
        if self.mosaic_opts is not None:
            if self.tile_envelope:
                raise ValueError(
                    "cog_gbx mosaic mode requires a top-level 'path' column "
                    "(file_gbx output); tile envelope input is not yet supported "
                    "with mosaic mode."
                )
            return self._write_mosaic(iterator)

        if self.tile_envelope:
            # v2 (source, tile) envelope. driverMode over v2 tiles is a follow-on
            # (see module docstring / task-5): the path-gather-then-driver-convert
            # path only makes sense for whole-file virtual tiles, and windowed/
            # clipped/materialized tiles would need bytes on the driver. For now
            # v2-tile input runs in DEFAULT (per-partition) mode only.
            if self.driver_mode:
                raise ValueError(
                    "cog_gbx driverMode does not yet support (source, tile) tile "
                    "input; use a top-level 'path' column (file_gbx) with "
                    "driverMode, or DEFAULT mode with a tile DataFrame."
                )
            return self._write_tiles(iterator)

        if self.driver_mode:
            # Gather source path strings only — NO conversion on the executor
            # (cap-safe: no GDAL, no pixels). Conversion happens on driver.
            paths = [str(row["path"]) for row in iterator]
            return CogCommitMessage(paths=paths)

        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

        # Use the DRIVER-captured cap (see __init__); NEVER resolve it here — write()
        # runs on a session-less Serverless worker where the connect-aware detection
        # would wrongly return the 256 MiB classic cap.
        _executor_cap_bytes = self._cap

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []
        # Source paths deferred to driver-side conversion by the size gate.
        pending: List[str] = []
        for row in iterator:
            src_volume = _listing.to_local_path(str(row["path"]))
            # output name: derive from source basename (or name_col if given)
            if self.name_col and row[self.name_col] is not None:
                base = os.path.basename(str(row[self.name_col]))
            else:
                base = os.path.basename(src_volume)
            stem = os.path.splitext(base)[0]
            out_path = os.path.join(self.out_dir, f"{stem}.{self.ext}")

            # Skip when the output already exists (idempotent resume).
            if self.cog_skip_if_exists and os.path.exists(out_path):
                written.append(out_path)
                continue

            # Size gate: check source size before committing to executor conversion.
            # COG overview-build transient can use 2–3× source RAM on the executor;
            # a large source blows the ~1 GiB Serverless per-task cap.
            try:
                src_size: Optional[int] = os.path.getsize(src_volume)
            except OSError:
                src_size = None  # unknown size → "stream" (safe default)

            decision = materialize_decision(src_size, "cog_write", cap_bytes=self._cap)
            if decision == "error":
                size_mib = (
                    f"{src_size // (1024 ** 2)} MiB" if src_size else "unknown size"
                )
                raise StageTooLargeError(
                    f"Source {src_volume!r} ({size_mib}) exceeds the driver "
                    f"COG-conversion budget ({_COG_DRIVER_MAX_BYTES // (1024 ** 3)} GiB). "
                    f"Split with sizeInMB= or use a classic cluster."
                )
            if decision == "driver":
                # Auto-route: defer conversion to driver-side (commit() → prepare_cogs).
                # GDAL COG conversion has a flat ~2 GiB RSS profile on the driver
                # regardless of source size; this avoids the executor per-task cap.
                size_mib = f"{src_size // (1024 ** 2)}" if src_size else "?"
                cap_mib = _executor_cap_bytes // (1024**2)
                _logger.info(
                    "cog_gbx: auto-routing %s (%s MiB) to driver-side COG conversion "
                    "(source exceeds executor cap %d MiB; commit() will run prepare_cogs)",
                    src_volume,
                    size_mib,
                    cap_mib,
                )
                pending.append(src_volume)
                continue

            # "stream": source fits within executor cap — convert on executor (default path).

            # Build a NetCDF subdataset URI when requested.
            conv_src = src_volume
            if self.cog_subdataset:
                conv_src = f'NETCDF:"{src_volume}":{self.cog_subdataset}'

            # Pass the source path directly to cog_convert_file. GDAL (via
            # rasterio.shutil.copy driver="COG") reads the source natively
            # block-by-block — no Python-heap copy of the whole file. Only the
            # COG output (local temp → copyfile) touches the Python heap.
            fd, tmp = tempfile.mkstemp(suffix=f".{self.ext}")
            os.close(fd)
            try:
                cog_convert_file(
                    conv_src,
                    tmp,
                    compression=self._resolved_cog_compression(),
                    blocksize=self.cog_blocksize,
                    overview_resampling=self.cog_overview_resampling,
                    bigtiff=self.cog_bigtiff,
                    compress_level=self.compress_level,
                    predictor=self.predictor,
                )
                shutil.copyfile(tmp, out_path)  # bytes-only → FUSE-safe on /Volumes
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            written.append(out_path)
        return CogCommitMessage(paths=written, pending_paths=pending)

    def _write_mosaic(self, iterator: Iterator) -> WriterCommitMessage:
        """Mosaic-mode write: tile each source into bounded, non-overlapping mini-COGs.

        Dispatches to :meth:`_write_mosaic_quadbin` when ``gridSystem='quadbin'``;
        otherwise runs the native pixel-tiling path (Phase A).

        For each input path row the source is NEVER fully materialised in RAM.
        Instead, the source is opened once with rasterio and each tile window is
        read independently (``src.read(window=…)``).  Each window's decoded size
        is bounded by ``tile_size × tile_size × bands × itemsize`` — small by
        design.  If a window exceeds the connect-aware cap (rare), it falls back
        to ``_windowed_materialize_bytes`` (block-streaming, no full-array in RAM).

        Output (native):
          ``<out_dir>/tile_<row>_<col>.tif`` — a mini-COG (driver=COG, internal
          tiling + overviews) for each non-empty tile cell.

        Output (quadbin):
          ``<out_dir>/cell_<disc>_<cellid>.tif`` — one mini-COG per overlapping
          quadbin cell, reprojected to EPSG:3857 and tagged with
          ``GBX_CELLID=<cellid>`` and ``GBX_GRIDSYSTEM=quadbin``.

        The returned :class:`CogCommitMessage` carries all written paths in
        ``msg.paths``; ``pending_paths`` is always empty (mosaic write never
        defers to driver-side ``prepare_cogs``).

        commit() reads ``msg.paths`` from all partition messages and builds the
        ``.vrt`` mosaic index.
        """
        if self.mosaic_opts.grid_system == "quadbin":
            return self._write_mosaic_quadbin(iterator)
        if self.mosaic_opts.grid_system == "h3":
            return self._write_mosaic_h3(iterator)
        import numpy as np
        import rasterio
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.pyrx.core import compression as _comp

        opts = self.mosaic_opts
        tile_size = (
            opts.tile_size if opts.tile_size is not None else _MOSAIC_DEFAULT_TILE_SIZE
        )
        overlap_pct = opts.overlap_percent
        prune_empty = opts.prune_empty

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []

        for row in iterator:
            src_path = _listing.to_local_path(str(row["path"]))
            # Per-source discriminator: namespaces tile names so two source
            # rasters in one write (or across partitions sharing out_dir) never
            # collide on identical tile_<row>_<col>.tif names (silent data loss).
            srcdisc = _source_discriminator(src_path)

            with rasterio.open(src_path) as src:
                src_width, src_height = src.width, src.height
                count = src.count
                out_dtype = src.dtypes[0]
                src_nodata = src.nodata

                for tile_row, tile_col, window in _tile_grid_windows(
                    src_width, src_height, tile_size, overlap_pct
                ):
                    out_name = f"tile_{srcdisc}_{tile_row}_{tile_col}.tif"
                    out_path = os.path.join(self.out_dir, out_name)

                    if self.cog_skip_if_exists and os.path.exists(out_path):
                        written.append(out_path)
                        continue

                    # Compute decoded window size before reading (cheap, header-only).
                    decoded_size = (
                        count
                        * int(window.width)
                        * int(window.height)
                        * np.dtype(out_dtype).itemsize
                    )

                    if decoded_size > self._cap:
                        # Large tile (rare by design): block-streaming path via
                        # _windowed_materialize_bytes — at most one block in RAM
                        # at a time.  Skip pruneEmpty for large tiles (reading a
                        # down-sampled proxy to check nodata is not worth the
                        # complexity for a case that should never occur in practice).
                        _logger.info(
                            "cog_gbx mosaic: tile %d,%d decoded size %d bytes "
                            "exceeds cap %d bytes; using block-streaming path",
                            tile_row,
                            tile_col,
                            decoded_size,
                            self._cap,
                        )
                        from databricks.labs.gbx.pyrx.core.open_tile import (
                            _windowed_materialize_bytes,
                        )
                        from databricks.labs.gbx.pyrx.core.virtual_tile import (
                            VirtualTile,
                        )

                        vt = VirtualTile(
                            cellid=0,
                            path=src_path,
                            window=(
                                int(window.col_off),
                                int(window.row_off),
                                int(window.width),
                                int(window.height),
                            ),
                        )
                        window_bytes = _windowed_materialize_bytes(vt)
                        self._bytes_to_cog(window_bytes, out_path)
                        written.append(out_path)
                        continue

                    # Normal path: read whole window (bounded ≤ cap), check prune,
                    # build GTiff bytes from already-read data (single read pass).
                    data = src.read(window=window)

                    # pruneEmpty: skip tiles that are entirely nodata.
                    if prune_empty and _is_all_nodata(data, src_nodata):
                        continue

                    # Build GTiff bytes for this window.  Profile mirrors
                    # _window_dataset_bytes / materialize_to_bytes for fidelity:
                    # same driver, height, width, count, transform, compression.
                    profile = src.profile.copy()
                    # A tiled/COG source carries tiled=True + blockxsize/blockysize
                    # (e.g. 512) and possibly interleave. A partial-edge tile whose
                    # dims are smaller than those block dims conflicts when written
                    # as an intermediate GTiff, so drop the source's block layout and
                    # let the encoder pick blocks for the actual tile dims (the final
                    # COG re-tiles via _bytes_to_cog regardless).
                    for _blk in ("tiled", "blockxsize", "blockysize", "interleave"):
                        profile.pop(_blk, None)
                    profile.update(
                        driver="GTiff",
                        height=int(window.height),
                        width=int(window.width),
                        count=count,
                        transform=src.window_transform(window),
                    )
                    profile.update(
                        _comp.creation_opts(
                            out_dtype, decoded_bytes=decoded_size, compress="auto"
                        )
                    )
                    with MemoryFile() as mf:
                        with mf.open(**profile) as dst:
                            dst.write(data)
                        window_bytes = mf.read()

                    self._bytes_to_cog(window_bytes, out_path)
                    written.append(out_path)

        return CogCommitMessage(paths=written)

    def _write_mosaic_quadbin(self, iterator: Iterator) -> WriterCommitMessage:
        """Quadbin mosaic-mode write: one mini-COG per overlapping quadbin cell.

        For each source path row:

        1. Compute the source bounds in EPSG:4326.
        2. Enumerate overlapping quadbin cells via
           :func:`~databricks.labs.gbx.ds._quadbin_grid.quadbin_cells_for_bounds`.
        3. For each cell:

           a. Compute the destination grid (CRS=EPSG:3857) from the cell's
              west/south/east/north extent at the source's native GSD.
           b. Find the intersecting source window; read only those pixels.
           c. Reproject with ``Resampling.average`` (continuous data default).
           d. Route decoded destination size through ``materialize_decision``
              (Serverless per-task cap guard).
           e. Skip if all-nodata (``pruneEmpty``).
           f. Write a mini-COG tagged with ``GBX_CELLID`` and
              ``GBX_GRIDSYSTEM="quadbin"``.

        Output:  ``<out_dir>/cell_<disc>_<cellid>.tif``

        Returns
        -------
        :class:`CogCommitMessage`
            Written paths in ``msg.paths``; ``pending_paths`` is always empty.
        """
        import numpy as np
        import rasterio
        from rasterio.crs import CRS
        from rasterio.io import MemoryFile
        from rasterio.transform import from_bounds as transform_from_bounds
        from rasterio.warp import (
            Resampling,
            calculate_default_transform,
            reproject,
            transform_bounds,
        )
        from rasterio.windows import Window

        from databricks.labs.gbx.ds._quadbin_grid import quadbin_cells_for_bounds
        from databricks.labs.gbx.pyrx.core import compression as _comp

        opts = self.mosaic_opts
        prune_empty = opts.prune_empty
        dst_crs = CRS.from_epsg(3857)

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []

        for row in iterator:
            src_path = _listing.to_local_path(str(row["path"]))
            srcdisc = _source_discriminator(src_path)

            with rasterio.open(src_path) as src:
                count = src.count
                out_dtype = src.dtypes[0]
                src_nodata = src.nodata
                src_full_window = Window(0, 0, src.width, src.height)

                # Source bounds in EPSG:4326 for cell enumeration.
                bounds_4326 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)

                # Native pixel size in EPSG:3857 — used to size all destination
                # grids consistently (one calculate_default_transform per source).
                native_tf, _, _ = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds
                )
                native_px = abs(native_tf.a)  # metres/pixel in 3857

                cells = quadbin_cells_for_bounds(bounds_4326, opts.grid_resolution)

                for cell in cells:
                    out_name = f"cell_{srcdisc}_{cell.cellid}.tif"
                    out_path = os.path.join(self.out_dir, out_name)

                    if self.cog_skip_if_exists and os.path.exists(out_path):
                        written.append(out_path)
                        continue

                    # ── Destination grid (cell-aligned, EPSG:3857) ───────────
                    # Both dimensions use the same native_px: calculate_default_transform
                    # yields square pixels, quadbin cells in EPSG:3857 are square, and
                    # dst_transform is rebuilt from cell bounds — so one pixel size is
                    # correct for both axes.  Do NOT substitute native_tf.e for height:
                    # native_tf.e is negative (south-pointing) and would produce cell_h <= 0.
                    cell_w = max(1, int(round((cell.east - cell.west) / native_px)))
                    cell_h = max(1, int(round((cell.north - cell.south) / native_px)))
                    dst_transform = transform_from_bounds(
                        cell.west, cell.south, cell.east, cell.north, cell_w, cell_h
                    )

                    # ── Serverless cap gate ───────────────────────────────────
                    decoded_size = (
                        count * cell_w * cell_h * np.dtype(out_dtype).itemsize
                    )
                    decision = materialize_decision(
                        decoded_size, "cog_write", cap_bytes=self._cap
                    )
                    if decision in ("error", "driver"):
                        size_mib = decoded_size // (1024**2)
                        raise StageTooLargeError(
                            f"quadbin cell {cell.cellid} at gridResolution="
                            f"{opts.grid_resolution} decodes to {size_mib} MiB, "
                            f"over the per-task memory cap; use a finer gridResolution."
                        )

                    # ── Find source window that overlaps this cell ────────────
                    # Transform cell 3857 bounds to source CRS; read only those
                    # pixels (never the full source — per-cell-window contract).
                    try:
                        src_bounds = transform_bounds(
                            dst_crs,
                            src.crs,
                            cell.west,
                            cell.south,
                            cell.east,
                            cell.north,
                        )
                        src_win = src.window(*src_bounds)
                        src_win = src_win.intersection(src_full_window)
                    except Exception:
                        continue  # degenerate geometry — skip cell

                    if src_win.width <= 0 or src_win.height <= 0:
                        continue  # cell does not overlap source

                    # ── Reproject source window → cell's 3857 extent ─────────
                    src_data = src.read(window=src_win)
                    src_win_transform = src.window_transform(src_win)

                    fill = (
                        src_nodata
                        if src_nodata is not None
                        else (
                            0 if np.issubdtype(np.dtype(out_dtype), np.integer) else 0.0
                        )
                    )
                    dst_data = np.full((count, cell_h, cell_w), fill, dtype=out_dtype)
                    reproject(
                        source=src_data,
                        destination=dst_data,
                        src_transform=src_win_transform,
                        src_crs=src.crs,
                        dst_transform=dst_transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.average,
                        src_nodata=src_nodata,
                        dst_nodata=src_nodata,
                    )

                    # ── pruneEmpty ────────────────────────────────────────────
                    if prune_empty and _is_all_nodata(dst_data, src_nodata):
                        continue

                    # ── Build intermediate GTiff bytes with GBX_CELLID tag ───
                    # GBX_CELLID and GBX_GRIDSYSTEM travel through
                    # rasterio.shutil.copy (cog_convert_file) via GDAL's
                    # GDALCreateCopy metadata copy, so the tag is present on
                    # the final COG without a separate post-write step.
                    profile = {
                        "driver": "GTiff",
                        "height": cell_h,
                        "width": cell_w,
                        "count": count,
                        "dtype": out_dtype,
                        "crs": dst_crs,
                        "transform": dst_transform,
                    }
                    if src_nodata is not None:
                        profile["nodata"] = src_nodata
                    profile.update(
                        _comp.creation_opts(
                            out_dtype, decoded_bytes=decoded_size, compress="auto"
                        )
                    )
                    with MemoryFile() as mf:
                        with mf.open(**profile) as dst_ds:
                            dst_ds.write(dst_data)
                            dst_ds.update_tags(
                                GBX_CELLID=str(cell.cellid),
                                GBX_GRIDSYSTEM="quadbin",
                            )
                        window_bytes = mf.read()

                    self._bytes_to_cog(window_bytes, out_path)
                    written.append(out_path)

        return CogCommitMessage(paths=written)

    def _write_mosaic_h3(self, iterator: Iterator) -> WriterCommitMessage:
        """h3 mosaic-mode write: one mini-COG per overlapping h3 cell.

        For each source path row:
        1. Compute source bounds in EPSG:4326.
        2. Enumerate h3 cells via h3_cells_for_bounds.
        3. Per cell:
           a. Determine destination grid (CRS=EPSG:4326) from the cell's bbox at
              the source's native GSD in EPSG:4326 degrees.
           b. Find intersecting source window; read only those pixels.
           c. Reproject with Resampling.nearest (pixel statistics must not interpolate).
           d. Derive an out_nodata sentinel unconditionally:
              - src.nodata if set; else np.nan for float dtypes; else dtype max for int.
           e. Rasterize the hex boundary via rasterio.features.geometry_mask; set pixels
              outside the hexagon to out_nodata ALWAYS (never gated on src_nodata).
           f. Route decoded size through materialize_decision (cap guard).
           g. Skip if all-nodata (pruneEmpty).
           h. Write a mini-COG tagged GBX_CELLID=<h3index> + GBX_GRIDSYSTEM="h3".

        Output: <out_dir>/cell_<disc>_<h3index>.tif
        """
        import h3
        import numpy as np
        import rasterio
        from rasterio.crs import CRS
        from rasterio.features import geometry_mask
        from rasterio.io import MemoryFile
        from rasterio.transform import from_bounds as transform_from_bounds
        from rasterio.warp import (
            Resampling,
            calculate_default_transform,
            reproject,
            transform_bounds,
        )
        from rasterio.windows import Window

        from databricks.labs.gbx.ds._h3_grid import h3_cells_for_bounds
        from databricks.labs.gbx.pyrx.core import compression as _comp

        opts = self.mosaic_opts
        prune_empty = opts.prune_empty
        dst_crs = CRS.from_epsg(4326)

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []

        for row in iterator:
            src_path = _listing.to_local_path(str(row["path"]))
            srcdisc = _source_discriminator(src_path)

            with rasterio.open(src_path) as src:
                count = src.count
                out_dtype = src.dtypes[0]
                src_nodata = src.nodata
                src_full_window = Window(0, 0, src.width, src.height)

                # Ruling A: derive out_nodata unconditionally — h3 output MUST
                # always carry a nodata value so the hex-clip contract is honoured
                # even when the source has none.
                if src_nodata is not None:
                    out_nodata = src_nodata
                elif np.issubdtype(np.dtype(out_dtype), np.floating):
                    out_nodata = float("nan")
                else:
                    out_nodata = int(np.iinfo(out_dtype).max)

                # Source bounds in EPSG:4326 for cell enumeration.
                bounds_4326 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)

                # Native pixel size in EPSG:4326 degrees — used to size all
                # destination grids consistently (one transform per source).
                native_tf, _, _ = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds
                )
                native_px = abs(native_tf.a)  # degrees/pixel in EPSG:4326

                cells = h3_cells_for_bounds(bounds_4326, opts.grid_resolution)

                for cell in cells:
                    out_name = f"cell_{srcdisc}_{cell.cellid}.tif"
                    out_path = os.path.join(self.out_dir, out_name)

                    if self.cog_skip_if_exists and os.path.exists(out_path):
                        written.append(out_path)
                        continue

                    # ── Destination grid (cell-bbox-aligned, EPSG:4326) ──────
                    cell_w = max(1, int(round((cell.east - cell.west) / native_px)))
                    cell_h = max(1, int(round((cell.north - cell.south) / native_px)))
                    dst_transform = transform_from_bounds(
                        cell.west, cell.south, cell.east, cell.north, cell_w, cell_h
                    )

                    # ── Decoded size for cap gate ────────────────────────────
                    decoded_size = (
                        count * cell_w * cell_h * np.dtype(out_dtype).itemsize
                    )

                    # ── Serverless cap gate ──────────────────────────────────
                    # Both "error" and "driver" raise: per-cell reprojection
                    # cannot be deferred to the driver path, and proceeding on
                    # executor risks Serverless OOM.  Use a finer gridResolution.
                    decision = materialize_decision(
                        decoded_size, "cog_write", cap_bytes=self._cap
                    )
                    if decision in ("error", "driver"):
                        size_mib = decoded_size // (1024**2)
                        raise StageTooLargeError(
                            f"h3 cell {cell.cellid!r} at gridResolution="
                            f"{opts.grid_resolution} decodes to {size_mib} MiB, "
                            f"over the per-task memory cap; use a finer gridResolution."
                        )

                    # ── Find source window overlapping this cell ─────────────
                    try:
                        src_bounds = transform_bounds(
                            dst_crs,
                            src.crs,
                            cell.west,
                            cell.south,
                            cell.east,
                            cell.north,
                        )
                        src_win = src.window(*src_bounds)
                        src_win = src_win.intersection(src_full_window)
                    except Exception:
                        continue  # degenerate geometry — skip cell

                    if src_win.width <= 0 or src_win.height <= 0:
                        continue  # cell does not overlap source

                    # ── Reproject source window → cell's EPSG:4326 bbox ─────
                    src_data = src.read(window=src_win)
                    src_win_transform = src.window_transform(src_win)

                    dst_data = np.full(
                        (count, cell_h, cell_w), out_nodata, dtype=out_dtype
                    )
                    reproject(
                        source=src_data,
                        destination=dst_data,
                        src_transform=src_win_transform,
                        src_crs=src.crs,
                        dst_transform=dst_transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest,
                        src_nodata=src_nodata,
                        dst_nodata=out_nodata,
                    )

                    # ── Hex-clip: ALWAYS set pixels outside the hexagon to
                    # out_nodata (unconditional — Ruling A).
                    # h3.cell_to_boundary returns [(lat, lon), ...]; swap to
                    # (lon, lat) for rasterio (geographic CRS: x=lon, y=lat).
                    boundary = h3.cell_to_boundary(cell.cellid)
                    hex_coords = [(lon, lat) for lat, lon in boundary]
                    # Close the ring (geometry_mask requires a closed polygon).
                    hex_geojson = {
                        "type": "Polygon",
                        "coordinates": [hex_coords + [hex_coords[0]]],
                    }
                    outside_mask = geometry_mask(
                        [hex_geojson],
                        transform=dst_transform,
                        out_shape=(cell_h, cell_w),
                        invert=False,  # True = outside polygon
                        # all_touched: keep every pixel the hexagon TOUCHES (not just
                        # center-inside). Each cell reprojects on its own grid, so with
                        # center-based masking the misaligned grids drop a thin band of
                        # boundary pixels on BOTH sides of a shared edge → honeycomb
                        # nodata seams between abutting hexes. Touch-based masking makes
                        # neighbours overlap ~1px at edges so the VRT composite closes
                        # the seams (the intended full-hex-overlap behaviour).
                        all_touched=True,
                    )
                    for b in range(count):
                        dst_data[b][outside_mask] = out_nodata

                    # ── pruneEmpty ───────────────────────────────────────────
                    if prune_empty and _is_all_nodata(dst_data, out_nodata):
                        continue

                    # ── Build intermediate GTiff bytes with GBX_CELLID tag ───
                    profile = {
                        "driver": "GTiff",
                        "height": cell_h,
                        "width": cell_w,
                        "count": count,
                        "dtype": out_dtype,
                        "crs": dst_crs,
                        "transform": dst_transform,
                        "nodata": out_nodata,  # always set (Ruling A)
                    }
                    profile.update(
                        _comp.creation_opts(
                            out_dtype, decoded_bytes=decoded_size, compress="auto"
                        )
                    )
                    with MemoryFile() as mf:
                        with mf.open(**profile) as dst_ds:
                            dst_ds.write(dst_data)
                            dst_ds.update_tags(
                                GBX_CELLID=cell.cellid,
                                GBX_GRIDSYSTEM="h3",
                            )
                        window_bytes = mf.read()

                    self._bytes_to_cog(window_bytes, out_path)
                    written.append(out_path)

        return CogCommitMessage(paths=written)

    def _out_path_for(self, vt, row) -> str:
        """Output COG path for a v2 tile row (name_col > tile.path > source > cellid)."""
        base = None
        if self.name_col and row[self.name_col] is not None:
            base = os.path.basename(str(row[self.name_col]))
        elif vt.path:
            base = os.path.basename(str(vt.path))
        elif "source" in row and row["source"]:
            base = os.path.basename(str(row["source"]))
        if not base:
            base = str(vt.cellid)
        stem = os.path.splitext(base)[0]
        return os.path.join(self.out_dir, f"{stem}.{self.ext}")

    def _is_whole_file_virtual(self, vt) -> bool:
        """A virtual tile that covers the FULL source extent with no pending
        clip or reprojection.

        Whole-file ⟺ raster is None AND clip_polygon is None AND crs is None
        (a pending warp would be silently dropped by a path-direct convert)
        AND the window is None (implicit whole-file) OR equals (0, 0, srcW, srcH).
        Such a tile can be path-direct converted (no pixels through the Python
        heap). A sub-window, a clip, or a pending warp must be materialized first
        so the output honors it.

        Source dims come from the reader-stamped ``metadata["width"]/["height"]``
        (strings) — NO staging/opening of the source just to read dims. If those
        are absent (a non-reader-produced virtual tile), we conservatively return
        False (materialize) so a windowed tile is never wrongly path-converted.
        """
        if not vt.is_virtual() or vt.clip_polygon is not None or vt.crs is not None:
            return False
        if vt.window is None:
            return True
        col_off, row_off, width, height = vt.window
        if col_off != 0 or row_off != 0:
            return False
        meta = vt.metadata or {}
        try:
            src_w = int(meta["width"])
            src_h = int(meta["height"])
        except (KeyError, TypeError, ValueError):
            # Dims unknown → do NOT path-direct; materialize is always safe.
            return False
        return width == src_w and height == src_h

    def _bytes_to_cog(self, raster_bytes: bytes, out_path: str) -> None:
        """Convert in-memory raster bytes to a COG at *out_path* (FUSE-safe)."""
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

        fd, tmp_src = tempfile.mkstemp(suffix=f".{self.ext}")
        os.close(fd)
        fd2, tmp_out = tempfile.mkstemp(suffix=f".{self.ext}")
        os.close(fd2)
        try:
            with open(tmp_src, "wb") as fh:
                fh.write(raster_bytes)
            cog_convert_file(
                tmp_src,
                tmp_out,
                compression=self._resolved_cog_compression(),
                blocksize=self.cog_blocksize,
                overview_resampling=self.cog_overview_resampling,
                bigtiff=self.cog_bigtiff,
                compress_level=self.compress_level,
                predictor=self.predictor,
            )
            shutil.copyfile(tmp_out, out_path)  # bytes-only → FUSE-safe on /Volumes
        finally:
            for t in (tmp_src, tmp_out):
                if os.path.exists(t):
                    os.remove(t)

    def _write_tiles(self, iterator: Iterator) -> WriterCommitMessage:
        """DEFAULT-mode conversion for a v2 (source, tile) envelope.

        Whole-file virtual tile → PATH-DIRECT convert (cog_convert_file on the
        source path, no bytes round-trip). Windowed/clipped virtual tile →
        materialize the window/clip to bytes, then convert those bytes.
        Materialized tile (raster set) → materialize_to_bytes is a no-op; convert
        its bytes.
        """
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file
        from databricks.labs.gbx.pyrx.core.open_tile import (
            _to_virtual_tile,
            materialize_to_bytes,
        )

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            vt = _to_virtual_tile(row["tile"])
            out_path = self._out_path_for(vt, row)

            if self.cog_skip_if_exists and os.path.exists(out_path):
                written.append(out_path)
                continue

            if self._is_whole_file_virtual(vt):
                # PATH-DIRECT: GDAL reads the source natively block-by-block; no
                # pixels touch the Python heap (same as the file_gbx path).
                src_local = _listing.to_local_path(str(vt.path))
                fd, tmp = tempfile.mkstemp(suffix=f".{self.ext}")
                os.close(fd)
                try:
                    cog_convert_file(
                        src_local,
                        tmp,
                        compression=self._resolved_cog_compression(),
                        blocksize=self.cog_blocksize,
                        overview_resampling=self.cog_overview_resampling,
                        bigtiff=self.cog_bigtiff,
                        compress_level=self.compress_level,
                        predictor=self.predictor,
                    )
                    shutil.copyfile(tmp, out_path)
                finally:
                    if os.path.exists(tmp):
                        os.remove(tmp)
            else:
                # Windowed/clipped virtual → materialize the window/clip; a
                # materialized tile → materialize_to_bytes is a no-op. Convert
                # the resulting bytes to a COG.
                raster_bytes = materialize_to_bytes(vt).raster
                self._bytes_to_cog(raster_bytes, out_path)

            written.append(out_path)
        return CogCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        # NOTE: this runs INSIDE the .save() Spark Connect RPC. A long-blocking
        # commit (large corpus / big files, ~1 GB/min) can have its channel
        # cancelled → java.nio.channels.CancelledKeyException + a FAILED run.
        # If you hit that, bypass the writer and call prepare_cogs directly on
        # the driver (plain Python, no Spark RPC) — see this module's docstring.
        from databricks.labs.gbx.ds._listing import to_local_path
        from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs

        if self.driver_mode:
            # explicit driverMode=True: m.paths holds source paths gathered by write()
            all_pending = []
            for m in messages:
                if isinstance(m, CogCommitMessage):
                    all_pending.extend(to_local_path(p) for p in m.paths)
        else:
            # default mode: m.pending_paths holds sources auto-routed by the size gate
            all_pending = []
            for m in messages:
                if isinstance(m, CogCommitMessage):
                    all_pending.extend(to_local_path(p) for p in m.pending_paths)

        if all_pending:
            prepare_cogs(
                all_pending,
                self.out_dir,
                blocksize=self.cog_blocksize,
                resampling=self.cog_overview_resampling,
                compression=self._resolved_cog_compression(),
                compress_level=self.compress_level,
                predictor=self.predictor,
                subdataset=self.cog_subdataset,
                skip_if_exists=self.cog_skip_if_exists,
                verbose=self.driver_mode_verbose,
                bigtiff=self.cog_bigtiff,
            )

        # ── Task 3: mosaic VRT index ──────────────────────────────────────
        # Collect all mini-COG paths written by _write_mosaic() across all
        # partitions and build a single portable VRT index file.
        # Guarded by write_vrt=True (default); mosaic_opts=None (single-COG
        # mode) skips this block entirely.
        if self.mosaic_opts is not None and self.mosaic_opts.write_vrt:
            all_tile_paths: List[str] = []
            for m in messages:
                if isinstance(m, CogCommitMessage):
                    all_tile_paths.extend(m.paths)
            if all_tile_paths:
                _build_mosaic_vrt(
                    all_tile_paths,
                    self.out_dir,
                    vrt_paths=self.mosaic_opts.vrt_paths,
                )

        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        if self.driver_mode:
            # In driverMode, CogCommitMessage.paths holds SOURCE paths (write()
            # gathered references; commit() does the conversion via prepare_cogs).
            # Removing them would delete user input.  Do nothing on abort —
            # prepare_cogs is idempotent (skip_if_exists) so a re-run is cheap.
            return None
        for msg in messages:
            if isinstance(msg, CogCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
