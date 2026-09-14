"""CHM = clamp(align(DSM to DEM) - DEM, min=0). Thin composition; light-only.

Canopy Height Model convenience: aligns the DSM onto the DEM's georeference
(CRS, transform, and pixel dimensions) via nearest-neighbour resampling, then
subtracts, then clamps negatives to 0.  NoData in either input propagates to
the output.  Output sentinel is -9999 (Float32).
"""

import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import agg

_NODATA = -9999.0


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
        either input is missing/empty (mirrors ``agg.align_to_tiles`` behaviour).
    """
    aligned_dsm = agg.align_to_tiles(dsm_bytes, dem_bytes)
    if aligned_dsm is None:
        return None

    with MemoryFile(bytes(aligned_dsm)) as dsm_mf:
        with dsm_mf.open() as dsm_ds:
            A = dsm_ds.read(1).astype("float64")
            nod_a = dsm_ds.nodata

    with MemoryFile(bytes(dem_bytes)) as dem_mf:
        with dem_mf.open() as dem_ds:
            D = dem_ds.read(1).astype("float64")
            nod_d = dem_ds.nodata
            profile = dem_ds.profile.copy()

    valid_a = (A != nod_a) if nod_a is not None else np.ones(A.shape, dtype=bool)
    valid_d = (D != nod_d) if nod_d is not None else np.ones(D.shape, dtype=bool)
    valid = valid_a & valid_d

    diff = np.where(valid, np.maximum(A - D, 0.0), _NODATA).astype("float32")

    profile.update(driver="GTiff", count=1, dtype="float32", nodata=_NODATA)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(diff, 1)
        return mf.read()
