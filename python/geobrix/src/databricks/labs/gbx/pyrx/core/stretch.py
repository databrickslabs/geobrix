"""Percentile-based contrast stretch for raster tiles (pure NumPy/rasterio).

Per-band stretch to uint8 [0,255] via percentile clipping and linear rescale.
Memory-lean so it fits a Serverless worker (~1 GB/task): one working float32
band at a time, a single percentile call per band, no full-array float64 copies.
"""

import numpy as np

# Percentiles are estimated from at most this many valid samples per band. The
# 2nd/98th percentiles of a large raster are stable under decimation, so this
# bounds np.percentile's (float64) working set regardless of raster size —
# the difference from the exact percentile is well below one grey level.
_PERCENTILE_SAMPLE_CAP = 500_000


def percentile_stretch(ds, lo_pct: float, hi_pct: float) -> bytes:
    """Per-band percentile contrast stretch to uint8 [0,255].

    For each band: compute the lo_pct/hi_pct percentiles over valid (non-NoData)
    pixels, clip to that range, linearly rescale to [0, 255]. Pixels that are
    NoData in *every* band are preserved as 0.

    Args:
        ds:     Open rasterio DatasetReader.
        lo_pct: Lower percentile (0-100), e.g. 2.0.
        hi_pct: Upper percentile (0-100), e.g. 98.0.

    Returns:
        uint8 GTiff bytes (NoData = 0), same band count / georeferencing as ``ds``.
    """
    from rasterio.io import MemoryFile

    lo_pct = float(lo_pct)
    hi_pct = float(hi_pct)
    if not (0 <= lo_pct <= 100) or not (0 <= hi_pct <= 100):
        raise ValueError(
            f"percentiles must be in [0, 100], got lo={lo_pct}, hi={hi_pct}"
        )
    if lo_pct >= hi_pct:
        raise ValueError(
            f"lo_pct must be < hi_pct, got lo_pct={lo_pct}, hi_pct={hi_pct}"
        )

    count = ds.count

    # Pass 1: combined NoData mask = pixels invalid in EVERY band. Read each band
    # in its NATIVE dtype (not float64) so uint8/uint16 rasters stay cheap and take
    # the LUT path below; the memory hog is float rescale buffers, bounded in pass 2.
    datas, valids = [], []
    invalid_all = None
    for b in range(1, count + 1):
        data = ds.read(b)
        valid = ds.read_masks(b) != 0
        datas.append(data)
        valids.append(valid)
        inv = ~valid
        invalid_all = inv if invalid_all is None else (invalid_all & inv)

    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=count, dtype="uint8", nodata=0)

    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for i in range(count):
                data, valid = datas[i], valids[i]
                vv = data[valid]
                if vv.size:
                    # decimate to a bounded sample before np.percentile (its
                    # float64 upcast + partition is the memory hog on large bands)
                    if vv.size > _PERCENTILE_SAMPLE_CAP:
                        vv = vv[:: vv.size // _PERCENTILE_SAMPLE_CAP + 1]
                    lo_val, hi_val = np.percentile(vv, (lo_pct, hi_pct))
                else:
                    lo_val = hi_val = 0.0
                del vv
                if hi_val <= lo_val:
                    out = np.zeros(data.shape, dtype="uint8")
                elif (
                    np.issubdtype(data.dtype, np.unsignedinteger)
                    and data.dtype.itemsize <= 2
                ):
                    # uint8/uint16 raster: map each possible value once
                    # via a LUT, then a single uint8 gather — no full-array float
                    # temp. clip((v - lo) / (hi - lo) * 255, 0, 255).
                    n = int(np.iinfo(data.dtype).max) + 1
                    vals = np.arange(n, dtype="float32")
                    vals -= lo_val
                    vals *= 255.0 / (hi_val - lo_val)
                    lut = np.clip(vals, 0, 255).astype("uint8")
                    out = lut[data]
                else:
                    # float / wide-integer raster: rescale in float32 in place
                    out = data.astype("float32")
                    np.clip(out, lo_val, hi_val, out=out)
                    out -= lo_val
                    out *= 255.0 / (hi_val - lo_val)
                    out = out.astype("uint8")
                out[invalid_all] = 0
                dst.write(out, i + 1)
                # free this band's buffers before the next
                datas[i] = None
                valids[i] = None
                del out, data, valid
        return mf.read()
