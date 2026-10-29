"""Per-tile byte production for the raster writer.

Hybrid, mirroring the heavy gdal writer's intent (encoding from tile.metadata):
- target driver GTiff (the common case; raster_gbx/gtiff_gbx tiles are already
  GTiff) -> pass tile.raster bytes through VERBATIM. Pixel-identical to heavy;
  heavy's specific creation options differ but our contract is decoded-pixel.
- target driver COG -> rasterio re-encode applying metadata-derived
  compression/blocksize/zlevel/zstd, and stamp RASTERX_<key> + RASTERX_CELL. The tile's
  alpha band keeps its label (a 4th data band is not made alpha) and its own mask band
  is written with it.

Only GTiff (verbatim pass-through) and COG (re-encode, GTiff-structured) are
validated end-to-end. A Zarr tile (``driver`` Zarr) names its source store, but its
raster bytes are a GeoTIFF, so it is written as GTiff unless the writer forces a
driver. Other drivers (PNG, PNM, etc.) are passed best-effort to rasterio with the
metadata-derived options and are NOT guaranteed to match heavy (heavy special-cases
dtype coercion for PNG, etc.).

Writer .option()s never carry encoding; only tile.metadata does (like heavy).
"""

from __future__ import annotations

from typing import Dict, Optional

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.pyrx.core.compression import (
    creation_opts as _canonical_creation_opts,
)


def _tile_creation_opts(
    driver: str, meta: Dict[str, str], dtype: str, width: int, height: int
) -> Dict[str, str]:
    """GTiff/COG creation options from tile metadata, mirroring OperatorOptions.appendOptions.

    Compression and predictor are delegated to canonical creation_opts
    (pyrx/core/compression.py), which mirrors heavy OperatorOptions.appendOptions.
    ZSTD default level 9 matches heavy OperatorOptions :77; DEFLATE default 6
    matches heavy :86.  Blocksize stays here: the canonical helper does not
    compute blocksize.
    """
    compress = str(meta.get("compression", "DEFLATE")).lower()
    try:
        level: Optional[int] = (
            int(meta.get("zstd_level", 9))
            if compress == "zstd"
            else int(meta.get("zlevel", 6)) if compress == "deflate" else None
        )
    except (ValueError, TypeError):
        level = None
    opts: Dict[str, str] = dict(
        _canonical_creation_opts(dtype, compress=compress, level=level, driver=driver)
    )
    # Blocksize: tile-dimension-aware clamping (stays at this call site).
    try:
        blk = int(meta.get("blocksize", "512"))
    except ValueError:
        blk = 512
    blk = max(64, (min(blk, min(width, height)) // 16) * 16)
    blk = max(16, blk)  # never 0 for tiny rasters
    if driver.upper() == "COG":
        opts["blocksize"] = str(blk)
    return opts


def tile_to_bytes(
    cellid: int,
    raster_bytes: bytes,
    metadata: Dict[str, str],
    force_driver: Optional[str] = None,
) -> bytes:
    """Return the on-disk bytes for one tile (verbatim GTiff, else re-encode)."""
    driver = force_driver or metadata.get("driver") or metadata.get("format") or "GTiff"
    if not force_driver and driver == "Zarr":
        driver = "GTiff"  # a Zarr tile names its source; the raster bytes are a GeoTIFF
    if str(driver).upper() == "GTIFF":
        return raster_bytes

    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import _nodata
    from databricks.labs.gbx.pyrx.core.compression import internal_mask_env, match_alpha

    with MemoryFile(raster_bytes) as src_mf, open_bytes(src_mf) as src:
        data = src.read()
        profile = src.profile.copy()
        profile["driver"] = driver
        profile.update(
            _tile_creation_opts(driver, metadata, src.dtypes[0], src.width, src.height)
        )
        # The tile's own (per-dataset, non-alpha) mask band is part of the raster: a
        # GTiff-structured target (COG) carries it with the pixels. The other drivers
        # have no mask band to write (PNG, JPEG ... have their own alpha semantics).
        mask = (
            src.read_masks(1)
            if str(driver).upper() == "COG" and _nodata.has_own_mask(src)
            else None
        )
        with internal_mask_env(mask), MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                # GDAL labels band 4 of a 4-band uint8 raster alpha when it creates one:
                # label alpha only where the tile's alpha band is.
                match_alpha(dst, src.colorinterp)
                dst.write(data)
                if mask is not None:
                    dst.write_mask(mask)
                tags = {f"RASTERX_{k}": str(v) for k, v in (metadata or {}).items()}
                tags["RASTERX_CELL"] = str(cellid)
                dst.update_tags(**tags)
            return out_mf.read()
