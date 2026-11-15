"""CHM = clamp(align(DSM to DEM) - DEM, min=0). Thin composition; light-only.

Canopy Height Model convenience: aligns the DSM onto the DEM's georeference
(CRS, transform, and pixel dimensions) via nearest-neighbour resampling, then
subtracts, then clamps negatives to 0.  NoData in either input propagates to
the output.  Output sentinel is -9999 (Float32).

``chm_engine`` is the session function's body: the subtraction is the GDAL spec engine's map algebra, so it needs
the engine (no numpy fallback). ``chm`` keeps the NumPy subtraction for the Unity Catalog handler, which runs where
the engine does not.
"""

import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.pyrx.core import warp
from databricks.labs.gbx.pyrx.core.compression import write_gtiff

_NODATA = -9999.0


def chm_working_bytes(width_px: int, height_px: int) -> int:
    """An upper estimate of the memory :func:`chm` holds besides the two rasters it reads: the float64 DSM and DEM
    arrays (16 bytes a pixel), the three validity masks (3), the ``A - D``, ``maximum`` and ``where`` temporaries
    (24), the Float32 result (4) and its copy for the encoder with the encoded bytes (8): 55 bytes a pixel.
    """
    return int(width_px) * int(height_px) * 55


class EngineUnavailable(RuntimeError):
    """The GDAL spec engine cannot run here; the message is the engine's reason, with its Fix."""


def chm_engine(dsm_bytes: bytes, dem_bytes: bytes) -> bytes:
    """Compute the Canopy Height Model: the DSM aligned to the DEM's grid, minus the DEM, no lower than 0.

    The DSM is warped onto the DEM's georeference (nearest neighbour, uncovered pixels NoData), then the engine's
    map algebra computes ``max(A - B, 0)`` on the two rasters: a pixel is NoData in the output when either input is
    NoData there. Returns single-band Float32 GTiff bytes (NoData -9999), or ``None`` when either input is
    missing or empty. Raises :class:`EngineUnavailable` where the engine cannot run, and ``ValueError`` with
    the engine's reason for a raster it cannot compute on.
    """
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.engine import run

    why = run.unavailable_reason()
    if why:
        raise EngineUnavailable(why)
    aligned_dsm = warp.align_to_tiles(dsm_bytes, dem_bytes, dst_nodata=_NODATA)
    if aligned_dsm is None:
        return None
    res = run.run(
        manifest.load().functions["gbx_rst_mapalgebra"],
        [bytes(aligned_dsm), bytes(dem_bytes)],
        {"expression": "max(A - B, 0)"},
    )
    if res is None or res.raster is None:
        raise ValueError(res.error if res is not None else "rst_chm: no result")
    return res.raster


def chm(dsm_bytes: bytes, dem_bytes: bytes) -> bytes:
    """Compute Canopy Height Model: clamp(align(DSM->DEM) - DEM, min=0).

    DSM bytes are warped onto the DEM's georeference (CRS, transform, width,
    height) via nearest-neighbour resampling (mirrors RST_AlignTo / gdalwarp
    ``-r near``).  After alignment the DEM is subtracted pixel-by-pixel;
    negative differences are clamped to 0.  A pixel is NoData in the output
    when EITHER input is NoData at that location.

    Args:
        dsm_bytes: Digital Surface Model as GTiff bytes.
        dem_bytes:  Digital Elevation Model as GTiff bytes (defines the output
                    grid: CRS, transform, width, height).

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.  Returns ``None`` if
        either input is missing/empty (mirrors ``warp.align_to_tiles`` behaviour).
    """
    # Force uncovered (extent-padded) pixels to the NoData sentinel: a DSM with
    # nodata=None would otherwise leave them at GDAL's fill of 0 and be read as
    # a valid 0 m surface, inventing canopy wherever the DEM sits below datum.
    aligned_dsm = warp.align_to_tiles(dsm_bytes, dem_bytes, dst_nodata=_NODATA)
    if aligned_dsm is None:
        return None

    with MemoryFile(bytes(aligned_dsm)) as dsm_mf:
        with open_bytes(dsm_mf) as dsm_ds:
            A = dsm_ds.read(1).astype("float64")
            nod_a = dsm_ds.nodata

    with MemoryFile(bytes(dem_bytes)) as dem_mf:
        with open_bytes(dem_mf) as dem_ds:
            D = dem_ds.read(1).astype("float64")
            nod_d = dem_ds.nodata
            profile = dem_ds.profile.copy()

    valid_a = (A != nod_a) if nod_a is not None else np.ones(A.shape, dtype=bool)
    valid_d = (D != nod_d) if nod_d is not None else np.ones(D.shape, dtype=bool)
    valid = valid_a & valid_d

    diff = np.where(valid, np.maximum(A - D, 0.0), _NODATA).astype("float32")

    profile.update(driver="GTiff", count=1, dtype="float32", nodata=_NODATA)
    return write_gtiff(profile, diff[np.newaxis, ...])
