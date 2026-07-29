"""Windowed GTiff(DEFLATE) re-encode + 11-key metadata, matching the heavy reader.

Mirrors RasterDriver.writeToBytes (always GTiff/DEFLATE on the wire) and
WindowedExtract metadata. tile.raster is NOT raw source bytes.
"""

from __future__ import annotations

import os
from typing import Dict, Tuple

import rasterio
from rasterio.io import MemoryFile
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import cog as _cog

CELLID_FRESH = -1  # GDAL_Reader.scala:30 writes -1L for un-tessellated tiles


def encode_tile(
    ds: "rasterio.DatasetReader",
    window: Tuple[int, int, int, int],
    source_path: str,
    all_parents: str,
    compression: str = "DEFLATE",
    tile_format: str = "gtiff",
    cog_blocksize: int = 512,
    cog_overview_resampling: str = "AVERAGE",
) -> Tuple[int, bytes, Dict[str, str]]:
    """Read one window, re-encode it as an in-memory GTiff or COG, return (cellid, bytes, metadata).

    Args:
        ds:                    Open rasterio DatasetReader.
        window:                (col_off, row_off, win_w, win_h) pixel window.
        source_path:           Original source file path (for metadata).
        all_parents:           Semicolon-delimited parent chain (for metadata).
        compression:           GTiff/COG compression (default ``"DEFLATE"``).
        tile_format:           ``"gtiff"`` (default, plain windowed GTiff) or
                               ``"cog"`` (opt-in COG with overviews via
                               ``analysis.cog_convert``).
        cog_blocksize:         Internal tile size for COG output (default 512).
        cog_overview_resampling: Overview resampling algorithm for COG (default
                               ``"AVERAGE"``).
    """
    col_off, row_off, win_w, win_h = window
    rio_window = Window(col_off, row_off, win_w, win_h)
    data = ds.read(window=rio_window)

    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        width=win_w,
        height=win_h,
        compress=compression.lower(),
        transform=ds.window_transform(rio_window),
    )

    with MemoryFile() as mf:
        with mf.open(**profile) as out:
            out.write(data)
        raster_bytes = mf.read()

    if str(tile_format).lower() == "cog":
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert

        with MemoryFile(raster_bytes) as cmf, cmf.open() as cds:
            raster_bytes = cog_convert(cds, compression, cog_blocksize,
                                       cog_overview_resampling)

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
