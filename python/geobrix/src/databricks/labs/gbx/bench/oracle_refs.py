"""Independent numpy references for the oracle rows of the DEM and band-math functions (no GDAL): what each
algorithm computes, written from its definition, with GDAL's no-edges rule (a pixel whose 3 x 3 window holds
NoData or crosses the raster's border is NoData). Each returns ``(values float64, mask bool)``, the mask True
where the output is NoData."""

from __future__ import annotations

import numpy as np


def _windows(z: np.ndarray, nodata):
    """(the 8 neighbours stacked (8, h-2, w-2), the centres, the output mask (h, w)) of a 2-D array."""
    h, w = z.shape
    bad = ~np.isfinite(z) if nodata is None else (z == nodata) | ~np.isfinite(z)
    mask = np.ones((h, w), bool)
    win_bad = np.zeros((h - 2, w - 2), bool)
    nbrs = []
    for dy in (0, 1, 2):
        for dx in (0, 1, 2):
            win_bad |= bad[dy : dy + h - 2, dx : dx + w - 2]
            if (dy, dx) != (1, 1):
                nbrs.append(z[dy : dy + h - 2, dx : dx + w - 2])
    mask[1:-1, 1:-1] = win_bad
    return np.stack(nbrs).astype("float64"), z[1:-1, 1:-1].astype("float64"), mask


def _place(inner: np.ndarray, mask: np.ndarray) -> np.ndarray:
    out = np.zeros(mask.shape, "float64")
    out[1:-1, 1:-1] = inner
    return out


def riley_tri(z, nodata):
    """Riley's Terrain Ruggedness Index: the root of the summed squared differences to the 8 neighbours."""
    n, c, mask = _windows(z, nodata)
    return _place(np.sqrt(((n - c) ** 2).sum(axis=0)), mask), mask


def tpi(z, nodata):
    """Topographic Position Index: the centre minus the mean of its 8 neighbours."""
    n, c, mask = _windows(z, nodata)
    return _place(c - n.mean(axis=0), mask), mask


def roughness(z, nodata):
    """Roughness: the largest minus the smallest value of the 3 x 3 window, centre included."""
    n, c, mask = _windows(z, nodata)
    allv = np.concatenate([n, c[np.newaxis]])
    return _place(allv.max(axis=0) - allv.min(axis=0), mask), mask


def horn(z, nodata, xres, yres, xscale=1.0, yscale=1.0):
    """Horn's gradients (rise per unit run, along x to the east and along y to the south) and the NoData mask."""
    _, _, mask = _windows(z, nodata)
    z = z.astype("float64")
    a, b, c = z[:-2, :-2], z[:-2, 1:-1], z[:-2, 2:]
    d, f = z[1:-1, :-2], z[1:-1, 2:]
    g, h, i = z[2:, :-2], z[2:, 1:-1], z[2:, 2:]
    dzdx = ((c + 2 * f + i) - (a + 2 * d + g)) / (8 * xres * xscale)
    dzdy = ((g + 2 * h + i) - (a + 2 * b + c)) / (8 * yres * yscale)
    return _place(dzdx, mask), _place(dzdy, mask), mask


def aspect(z, nodata, xres, yres, trigonometric=False, zero_for_flat=False):
    """The direction of steepest descent, in degrees: clockwise from north, or counter-clockwise from east
    (both in [0, 360)). A flat cell is NoData, or 0 with ``zero_for_flat``."""
    dzdx, dzdy, mask = horn(z, nodata, xres, yres)
    deg = np.degrees(np.arctan2(dzdy, -dzdx))
    out = deg % 360.0 if trigonometric else (90.0 - deg) % 360.0
    flat = (dzdx == 0) & (dzdy == 0) & ~mask
    if zero_for_flat:
        out = np.where(flat, 0.0, out)
    else:
        mask = mask | flat
    return out, mask


def hillshade(
    z,
    nodata,
    xres,
    yres,
    azimuth=315.0,
    altitude=45.0,
    z_factor=1.0,
    xscale=1.0,
    yscale=1.0,
):
    """The cosine of the sun's incidence angle on the surface (the standard shaded-relief formula), as GDAL's
    byte scale: 1 + 254 * max(cos, 0)."""
    dzdx, dzdy, mask = horn(z, nodata, xres, yres, xscale, yscale)
    slope = np.arctan(z_factor * np.hypot(dzdx, dzdy))
    asp = np.arctan2(dzdy, -dzdx)
    zenith = np.radians(90.0 - altitude)
    sun = np.radians((360.0 - azimuth + 90.0) % 360.0)
    cang = np.cos(zenith) * np.cos(slope) + np.sin(zenith) * np.sin(slope) * np.cos(
        sun - asp
    )
    return 1.0 + 254.0 * np.maximum(cang, 0.0), mask
