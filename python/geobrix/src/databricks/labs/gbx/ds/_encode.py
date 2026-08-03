"""Windowed GTiff re-encode + 11-key metadata, matching the heavy reader.

Mirrors RasterDriver.writeToBytes (GTiff on the wire) and WindowedExtract
metadata. tile.raster is NOT raw source bytes.

Default compression is ``"auto"`` which routes through the compression
authority (``pyrx.core.compression``) for ZSTD + dtype-predictor output.
"""

from __future__ import annotations

import os
import tempfile
from typing import Dict, Tuple

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import cog as _cog
from databricks.labs.gbx.pyrx.core import compression as _comp

CELLID_FRESH = -1  # GDAL_Reader.scala:30 writes -1L for un-tessellated tiles


def encode_tile(
    ds: "rasterio.DatasetReader",
    window: Tuple[int, int, int, int],
    source_path: str,
    all_parents: str,
    compression: str = "auto",
    tile_format: str = "gtiff",
    cog_blocksize: int = 512,
    cog_overview_resampling: str = "AVERAGE",
) -> Tuple[int, bytes, Dict[str, str]]:
    """Read one window, re-encode it as an in-memory GTiff or COG, return (cellid, bytes, metadata).

    For the COG path (``tile_format="cog"``) the decoded window is written
    directly via GDAL's ``driver="COG"`` without a GTiff intermediate round-trip.
    This keeps peak RSS at ~2.8× the decoded tile size, well within the
    Databricks Serverless 1 GB Python UDF hard cap.

    Args:
        ds:                    Open rasterio DatasetReader.
        window:                (col_off, row_off, win_w, win_h) pixel window.
        source_path:           Original source file path (for metadata).
        all_parents:           Semicolon-delimited parent chain (for metadata).
        compression:           Compression codec (default ``"auto"`` → size-adaptive
                               ZSTD + dtype-predictor via the compression authority,
                               for both GTiff and COG paths). Explicit values
                               ``"DEFLATE"``, ``"LZW"``, ``"ZSTD"``, ``"NONE"`` /
                               ``"RAW"`` are forwarded through the authority unchanged.
        tile_format:           ``"gtiff"`` (default, plain windowed GTiff) or
                               ``"cog"`` (opt-in COG with overviews, ~2.8× peak).
        cog_blocksize:         Internal tile size for COG output (default 512).
        cog_overview_resampling: Overview resampling algorithm for COG (default
                               ``"AVERAGE"``).
    """
    col_off, row_off, win_w, win_h = window
    rio_window = Window(col_off, row_off, win_w, win_h)
    data = ds.read(window=rio_window)

    win_transform = ds.window_transform(rio_window)

    if str(tile_format).lower() == "cog":
        # Write directly from the decoded array to driver="COG" — no GTiff
        # intermediate so peak = decoded array + COG file bytes (not all three).
        from rio_cogeo.profiles import cog_profiles

        # Resolve "auto" → "ZSTD" for the ZSTD baseline (constraint requirement).
        # Route through the compression authority for size-adaptive levels.
        cog_compression = compression if str(compression).lower() != "auto" else "AUTO"
        compression_str = str(cog_compression).upper()

        # If not AUTO, validate compression name (same set as analysis.cog_convert).
        if compression_str != "AUTO":
            _profile_check = cog_profiles.get(compression_str.lower())
            if _profile_check is None:
                raise ValueError(
                    f"encode_tile: unknown compression '{cog_compression}'; "
                    f"valid: {', '.join(sorted(cog_profiles.keys()))}"
                )

        # Route through the compression authority for proper codec/level/predictor
        # with driver="COG" to ensure LEVEL (not zstd_level) is used.
        out_dtype = ds.dtypes[0]
        decoded_bytes = ds.count * win_w * win_h * np.dtype(out_dtype).itemsize
        _comp_arg = "none" if compression_str == "RAW" else compression_str.lower()
        _comp_opts = _comp.creation_opts(
            str(out_dtype),
            decoded_bytes=decoded_bytes,
            compress=_comp_arg,
            driver="COG",
        )

        profile = ds.profile.copy()
        profile.update(
            driver="COG",
            width=win_w,
            height=win_h,
            transform=win_transform,
            blocksize=cog_blocksize,
            overview_resampling=str(cog_overview_resampling).upper(),
        )
        # Remove stale compression keys from the source profile before merging authority opts.
        for _k in ("compress", "predictor", "zstd_level", "zlevel", "LEVEL"):
            profile.pop(_k, None)
        # Merge authority options (codec + level + predictor)
        profile.update(_comp_opts)

        tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
        tmp.close()
        try:
            with rasterio.open(tmp.name, "w", **profile) as out:
                out.write(data)
            # Free the decoded array before reading bytes back.
            del data
            with open(tmp.name, "rb") as fh:
                raster_bytes = fh.read()
        finally:
            os.unlink(tmp.name)
    else:
        # GTiff path: in-memory encode via the compression authority.
        profile = ds.profile.copy()
        profile.update(
            driver="GTiff",
            width=win_w,
            height=win_h,
            transform=win_transform,
        )
        out_dtype = profile.get("dtype", ds.dtypes[0])
        decoded_bytes = ds.count * win_w * win_h * np.dtype(out_dtype).itemsize
        profile.update(
            _comp.creation_opts(
                out_dtype, decoded_bytes=decoded_bytes, compress=compression
            )
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as out:
                out.write(data)
            raster_bytes = mf.read()

    metadata = {
        "path": f"/vsimem/light_{os.path.basename(source_path)}_{col_off}_{row_off}.tif",
        "sourcePath": source_path,
        "driver": "GTiff",
        "format": "GTiff",
        "last_command": f"windowed_extract -srcwin {col_off} {row_off} {win_w} {win_h}",
        "last_error": "",
        "all_parents": f"{source_path};{all_parents}",
        "size": "-1",
        "compression": compression,
        "isZipped": "false",
        "isSubset": "false",
    }
    metadata = _cog.stamp_format_metadata(raster_bytes, metadata)
    return CELLID_FRESH, raster_bytes, metadata


def passthrough_tile(
    file_path: str,
    width: int,
    height: int,
    source_path: str,
    all_parents: str,
    compression: str = "DEFLATE",
) -> Tuple[int, bytes, Dict[str, str]]:
    """Whole-file GTiff fast path: emit the ORIGINAL file bytes, no decode/re-encode.

    Valid only when one tile spans the whole raster and the source is already a
    GTiff: the decoded pixels are byte-for-byte the source's, so this is identical
    in pixel terms to ``encode_tile`` over the full window but ~80x cheaper
    (profiling: the GTiff/DEFLATE re-encode is ~95% of per-tile cost). Parity is
    decoded-pixel, not byte, so passing source bytes through is contract-safe and
    also preserves colormaps/masks that a re-encode would drop.
    """
    with open(file_path, "rb") as fh:
        raster_bytes = fh.read()

    metadata = {
        "path": f"/vsimem/light_{os.path.basename(source_path)}_0_0.tif",
        "sourcePath": source_path,
        "driver": "GTiff",
        "format": "GTiff",
        "last_command": f"passthrough -srcwin 0 0 {width} {height}",
        "last_error": "",
        "all_parents": f"{source_path};{all_parents}",
        "size": "-1",
        "compression": compression,
        "isZipped": "false",
        "isSubset": "false",
    }
    metadata = _cog.stamp_format_metadata(raster_bytes, metadata)
    return CELLID_FRESH, raster_bytes, metadata
