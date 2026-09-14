"""Spark-free point->cell binning (the DSM engine).

Bins (x, y, z) arrays into a ``width_px x height_px`` grid over the extent
``[xmin, xmax] x [ymin, ymax]`` and reduces z values per cell by a statistic.
Empty cells (no points) are filled with NoData (-9999.0). Returns single-band
Float32 GTiff bytes.

This module is intentionally Spark-free: no pyspark import. Task 6 wraps
``bin_points`` in UDFs (``gbx_rst_binpoints`` / ``_agg``).
"""

from __future__ import annotations

import numpy as np
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

_NODATA: float = -9999.0


def _cell_indices(
    x, y, xmin: float, ymin: float, xmax: float, ymax: float, w: int, h: int
):
    """Map coordinate arrays to integer (row, col) cell indices.

    Raster convention: row 0 is the TOP (max y), so the row index flips y.

    Half-open interval: a point at exactly x==xmax (or y==ymax) computes
    col==w (or row==h) and is DROPPED — not clamped into the last cell. This
    matches the brief's reference code and prevents cross-tile double-counting
    when DSMs are tiled on shared boundaries.

    Returns
    -------
    row : int64 ndarray (only in-bounds points)
    col : int64 ndarray (only in-bounds points)
    inb : bool ndarray  (same length as input x/y)
    """
    xa = np.asarray(x, dtype="float64")
    ya = np.asarray(y, dtype="float64")

    # Fractional position then multiply by grid dimensions.
    # Flip y: raster row 0 = top (max y).
    col = np.floor((xa - xmin) / (xmax - xmin) * w).astype("int64")
    row = np.floor((ymax - ya) / (ymax - ymin) * h).astype("int64")

    # Strict half-open interval: col in [0, w), row in [0, h).
    # A point exactly on xmax yields col==w and is excluded.
    # A point exactly on ymax yields row==h and is excluded.
    inb = (col >= 0) & (col < w) & (row >= 0) & (row < h)
    return row[inb], col[inb], inb


def bin_points(
    x,
    y,
    z,
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    width_px: int,
    height_px: int,
    srid: int,
    statistic: str = "max",
) -> bytes:
    """Bin (x, y, z) point clouds into a raster cell statistic.

    Parameters
    ----------
    x, y, z:
        Coordinate and value arrays (list or ndarray). All same length.
    xmin, ymin, xmax, ymax:
        Spatial extent of the output raster (in the CRS of ``srid``).
    width_px, height_px:
        Output raster dimensions in pixels.
    srid:
        EPSG code for the output CRS.
    statistic:
        One of ``"max"`` (default), ``"min"``, ``"mean"``, ``"median"``,
        ``"count"``, or ``"percentile:<p>"`` (e.g. ``"percentile:90"``).

    Returns
    -------
    bytes
        Single-band Float32 GTiff. Empty cells carry NoData ``-9999.0``.
    """
    w, h = int(width_px), int(height_px)
    z = np.asarray(z, dtype="float64")

    row, col, inb = _cell_indices(x, y, xmin, ymin, xmax, ymax, w, h)
    z = z[inb]
    flat = row * w + col  # flat cell index into a (h*w,) array

    stat = str(statistic).lower()

    if stat in ("max", "min"):
        # Seed with NaN so untouched cells remain NaN -> NoData.
        # np.fmax.at / np.fmin.at skip NaN operands — when the accumulator
        # starts at NaN and a real value arrives, the result is the real value.
        # When no value arrives the cell stays NaN. (numpy >=1.12; confirmed
        # on numpy 2.x used in geobrix light envs.)
        out = np.full(w * h, np.nan, dtype="float64")
        if stat == "max":
            np.fmax.at(out, flat, z)
        else:
            np.fmin.at(out, flat, z)

    elif stat == "count":
        out = np.zeros(w * h, dtype="float64")
        np.add.at(out, flat, 1.0)
        out[out == 0] = np.nan  # zero-count cells -> NoData

    elif stat == "mean":
        s = np.zeros(w * h, dtype="float64")
        c = np.zeros(w * h, dtype="float64")
        np.add.at(s, flat, z)
        np.add.at(c, flat, 1.0)
        with np.errstate(invalid="ignore"):
            out = np.where(c > 0, s / c, np.nan)

    elif stat == "median" or stat.startswith("percentile:"):
        p = 50.0 if stat == "median" else float(stat.split(":", 1)[1])
        out = np.full(w * h, np.nan, dtype="float64")
        if len(flat):
            # Group by flat cell index using sorted order — avoids a Python
            # dict and is cache-friendly for large arrays.
            order = np.argsort(flat, kind="stable")
            fs = flat[order]
            zs = z[order]
            uniq, starts = np.unique(fs, return_index=True)
            ends = np.append(starts[1:], len(fs))
            for u, a, b in zip(uniq, starts, ends):
                out[int(u)] = np.percentile(zs[a:b], p)

    else:
        raise ValueError(
            f"unknown statistic {statistic!r}; expected one of: "
            "max, min, mean, median, count, percentile:<p>"
        )

    # Replace NaN sentinels with the NoData value and reshape to (h, w).
    band = np.where(np.isnan(out), _NODATA, out).astype("float32").reshape(h, w)

    transform = from_bounds(xmin, ymin, xmax, ymax, w, h)
    profile = dict(
        driver="GTiff",
        height=h,
        width=w,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(int(srid)),
        transform=transform,
        nodata=_NODATA,
    )

    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(band, 1)
        return mf.read()
