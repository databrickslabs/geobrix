"""Shared Spark-free helpers for pyrx core ops."""

import math

import numpy as np
from rasterio.enums import Resampling

# The resampling words of gdalwarp -r that the functions accept, as the heavy tier's RST_ResampleHelper allows them
# (plus ``nearest``, GDAL's ``near``), each with the rasterio member that does the same work.
_GDAL_WORDS = {
    "near": "nearest",
    "nearest": "nearest",
    "bilinear": "bilinear",
    "cubic": "cubic",
    "cubicspline": "cubic_spline",
    "lanczos": "lanczos",
    "average": "average",
    "mode": "mode",
    "max": "max",
    "min": "min",
    "med": "med",
    "q1": "q1",
    "q3": "q3",
}


def resampling_enum(name: str) -> Resampling:
    """Map a gdalwarp-style resampling name to rasterio's Resampling enum.

    The word is case-insensitive and is one of the words the heavy tier accepts: ``near`` (or ``nearest``),
    ``bilinear``, ``cubic``, ``cubicspline``, ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``
    and ``q3``. Any other word, rasterio's ``gauss`` or ``sum`` among them, raises a ``ValueError`` that names it and
    lists the ones that work.
    """
    word = str(name).strip().lower()
    key = _GDAL_WORDS.get(word)
    if key is None:
        raise ValueError(
            f"unknown resampling algorithm {name!r}. "
            f"Fix: use one of {', '.join(sorted(_GDAL_WORDS))}"
        )
    return Resampling[key]


def snap_bounds(bxmin, bymin, bxmax, bymax, pixel_size, anchor=(0.0, 0.0), tol=0.0):
    """Snap a bounding box outward to the nearest pixel_size lattice.

    Returns (xmin, ymin, xmax, ymax, width, height) where xmin and ymax are
    lattice lines (integer multiples of pixel_size, or of it from ``anchor``), and the grid is at
    least 1x1. Independent grids built with the same pixel_size and these snapped origins will
    align.
    Note: pixel_size is NOT included in the return tuple; callers must thread
    it separately (e.g. compute_gridspec inserts it into the returned gridspec).

    ``pixel_size`` is one number or an ``(xres, yres)`` pair. ``anchor`` is the ``(left, top)``
    corner of a pixel of the lattice (the default is the coordinate origin). ``tol`` is a
    fraction of a pixel by which an edge may lie inside a lattice line and still count as on it,
    so float noise on an aligned box does not add a pixel (the default 0 is exact).
    """
    rx, ry = (pixel_size, pixel_size) if np.isscalar(pixel_size) else pixel_size
    ax, ay = anchor
    xmin = ax + math.floor((bxmin - ax) / rx + tol) * rx
    ymax = ay + math.ceil((bymax - ay) / ry - tol) * ry
    width = max(1, int(math.ceil((bxmax - xmin) / rx - tol)))
    height = max(1, int(math.ceil((ymax - bymin) / ry - tol)))
    xmax = xmin + width * rx
    ymin = ymax - height * ry
    return xmin, ymin, xmax, ymax, width, height
