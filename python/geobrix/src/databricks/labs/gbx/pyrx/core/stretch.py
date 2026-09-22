"""Percentile-based contrast stretch for raster tiles (pure NumPy/rasterio).

Per-band stretch to uint8 [0,255] via percentile clipping and linear rescale.
"""

import numpy as np

from databricks.labs.gbx.pyrx.core._nodata import emit, read_masked


def percentile_stretch(ds, lo_pct: float, hi_pct: float) -> bytes:
    """Per-band percentile contrast stretch to uint8 [0,255].

    For each band:
    1. Compute lo_pct and hi_pct percentiles over valid (non-NoData) pixels
    2. Clip to [lo_pct_val, hi_pct_val]
    3. Linearly rescale to [0, 255]
    4. Preserve NoData as 0

    Args:
        ds:     Open rasterio DatasetReader.
        lo_pct: Lower percentile (0-100), e.g., 2.0 for 2nd percentile.
        hi_pct: Upper percentile (0-100), e.g., 98.0 for 98th percentile.

    Returns:
        Single-band uint8 GTiff bytes (NoData = 0).
    """
    lo_pct = float(lo_pct)
    hi_pct = float(hi_pct)

    if lo_pct < 0 or lo_pct > 100:
        raise ValueError(f"lo_pct must be in [0, 100], got {lo_pct}")
    if hi_pct < 0 or hi_pct > 100:
        raise ValueError(f"hi_pct must be in [0, 100], got {hi_pct}")
    if lo_pct >= hi_pct:
        raise ValueError(f"lo_pct must be < hi_pct, got lo_pct={lo_pct}, hi_pct={hi_pct}")

    # For multi-band, we'll process the first band and stack the result
    # (If multiple bands, loop and stack; for now assume single-band or process all)
    count = ds.count
    invalid_combined = None

    # If multi-band, read and stretch each band, then stack
    if count == 1:
        data, valid = read_masked(ds, 1)
        lo_val = np.percentile(data[valid], lo_pct)
        hi_val = np.percentile(data[valid], hi_pct)

        # Clip and rescale
        clipped = np.clip(data, lo_val, hi_val)
        if hi_val > lo_val:
            rescaled = (clipped - lo_val) / (hi_val - lo_val) * 255.0
        else:
            rescaled = np.zeros_like(clipped)

        invalid_combined = ~valid
        result = rescaled
    else:
        # Multi-band: process each band and stack as separate bands
        bands = []
        for band_idx in range(1, count + 1):
            data, valid = read_masked(ds, band_idx)
            lo_val = np.percentile(data[valid], lo_pct)
            hi_val = np.percentile(data[valid], hi_pct)

            clipped = np.clip(data, lo_val, hi_val)
            if hi_val > lo_val:
                rescaled = (clipped - lo_val) / (hi_val - lo_val) * 255.0
            else:
                rescaled = np.zeros_like(clipped)

            bands.append(rescaled)
            if invalid_combined is None:
                invalid_combined = ~valid
            else:
                invalid_combined = invalid_combined & ~valid

        result = np.stack(bands, axis=0)

    # Emit as uint8 GTiff (single-band for now; multi-band stacked above)
    if count == 1:
        return emit(ds, result, 0, invalid_combined, "uint8")
    else:
        # For multi-band, we need to write each band to the output
        from rasterio.io import MemoryFile

        profile = ds.profile.copy()
        profile.update(driver="GTiff", count=count, dtype="uint8", nodata=0)
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                for band_idx, band_data in enumerate(bands, start=1):
                    out = np.where(invalid_combined, 0, band_data).astype("uint8")
                    dst.write(out, band_idx)
            return mf.read()
