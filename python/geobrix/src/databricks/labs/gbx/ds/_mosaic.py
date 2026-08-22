"""on-demand (mint) transient VRT builder.

Provides :func:`mint_vrt`, which assembles a TRANSIENT GDAL VRT over a
dynamic tile collection — a filtered query result, an ad-hoc list, or a
directory of COG files — without persisting the index alongside the tiles.

This is the counterpart to the *persisted* ``mosaic.vrt`` that
:func:`~databricks.labs.gbx.ds.cog_writer.CogGbxWriter.commit` writes after a
full mosaic-mode write.  The minted VRT is ephemeral: the caller opens it, uses
it, and may discard it.

Light-tier safe: pure Python + rasterio only — no ``osgeo`` / native GDAL
Python bindings, no Spark session, no ``_jvm`` / ``_jsc``.

Exported names
--------------
:func:`mint_vrt`
    Build a transient VRT over an explicit tile list.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from typing import List, Optional

from databricks.labs.gbx.ds.cog_writer import _build_mosaic_vrt

__all__ = ["mint_vrt"]

_logger = logging.getLogger(__name__)


def mint_vrt(tile_paths: List[str], out: Optional[str] = None) -> str:
    """Build a transient GDAL VRT over *tile_paths*.

    The VRT is NOT placed alongside the tiles; it is written to a temp
    location (or to *out* when given) with **absolute** ``SourceFilename``
    paths so rasterio can resolve each member regardless of where the VRT
    file lives.

    Connect-safe: runs entirely on the driver/notebook Python process with no
    Spark session, no ``_jvm``, and no ``.rdd`` access.  No ``osgeo`` is
    required — the XML is built by the pure-Python
    :func:`~databricks.labs.gbx.ds.cog_writer._build_mosaic_vrt` routine.

    Parameters
    ----------
    tile_paths:
        Absolute paths to the COG tiles to include in the mosaic.  All tiles
        must share the same CRS, pixel size, band count, and dtype (same
        assumptions as :func:`~databricks.labs.gbx.ds.cog_writer._build_mosaic_vrt`).
    out:
        Optional destination path for the VRT.  When omitted the VRT is
        placed in a ``tempfile.mkdtemp()`` directory.  The caller is
        responsible for cleanup when the file is no longer needed.

    Returns
    -------
    str
        Absolute path to the written VRT file.
    """
    if not tile_paths:
        raise ValueError("mint_vrt: tile_paths must not be empty")

    # Normalise to absolute paths so that VRT member references survive a cwd change.
    abs_paths = [os.path.abspath(p) for p in tile_paths]

    if out is not None:
        # Caller supplied a destination — write into a temp dir first, then move.
        tmp_dir = tempfile.mkdtemp(suffix="_gbx_mint_vrt")
        try:
            built = _build_mosaic_vrt(abs_paths, tmp_dir, vrt_paths="absolute")
            dest_dir = os.path.dirname(os.path.abspath(out))
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)
            shutil.move(built, out)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        _logger.debug("mint_vrt: wrote %d-tile VRT to %s", len(abs_paths), out)
        return out

    # Transient: write into a private temp dir.  The VRT references tiles by
    # absolute path, so the temp dir location is irrelevant to rasterio.
    tmp_dir = tempfile.mkdtemp(suffix="_gbx_mint_vrt")
    vrt_path = _build_mosaic_vrt(abs_paths, tmp_dir, vrt_paths="absolute")
    _logger.debug(
        "mint_vrt: wrote %d-tile transient VRT to %s", len(abs_paths), vrt_path
    )
    return vrt_path
