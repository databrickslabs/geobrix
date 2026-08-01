"""Clip-safe AOI windowing for raster reads.

window_for_bbox computes the pixel Window of a dataset covering a bbox and CLIPS it
to the dataset BEFORE returning it. Callers do ds.read(win) + ds.window_transform(win)
on an in-bounds window, so pixels and georeference always agree -- the clip-vs-
window_transform footgun (read clips to the dataset, but window_transform used the
unclipped window's origin -> raster shifted by the overhang) cannot occur.
"""

from __future__ import annotations

from typing import Optional, Tuple

from rasterio.windows import Window
from rasterio.windows import from_bounds as _from_bounds


def window_for_geom(
    src,
    geom,
    geom_crs: Optional[str] = None,
) -> Optional[Window]:
    """Clipped, integer, in-bounds Window of ``src`` covering ``geom``'s envelope.

    ``geom`` is a shapely geometry in ``geom_crs`` (or ``src.crs`` if None). The
    geometry's bounds are reprojected to ``src.crs`` and turned into a whole-pixel
    window clipped to the dataset; returns None if it does not overlap. Same
    clip-safe contract as ``window_for_bbox`` (window and window_transform agree).
    """
    minx, miny, maxx, maxy = geom.bounds
    return window_for_bbox(src, (minx, miny, maxx, maxy), geom_crs)


def window_for_bbox(
    src,
    bbox: Tuple[float, float, float, float],
    bbox_crs: Optional[str] = None,
) -> Optional[Window]:
    """Clipped, integer, in-bounds Window of ``src`` covering ``bbox``.

    bbox is (minx, miny, maxx, maxy). bbox_crs (e.g. "EPSG:4326") declares the bbox
    CRS; None means the bbox is already in src.crs. Returns None if the bbox does not
    overlap the dataset.
    """
    minx, miny, maxx, maxy = bbox
    if bbox_crs is not None and str(bbox_crs) != str(src.crs):
        from rasterio.warp import transform_bounds as _transform_bounds

        minx, miny, maxx, maxy = _transform_bounds(
            bbox_crs, src.crs, minx, miny, maxx, maxy
        )
    win = _from_bounds(minx, miny, maxx, maxy, transform=src.transform)
    # Whole-pixel coverage of the bbox, then clip to the dataset extent.
    win = win.round_offsets(op="floor").round_lengths(op="ceil")
    try:
        win = win.intersection(Window(0, 0, src.width, src.height))
    except Exception:  # rasterio.errors.WindowError when disjoint
        return None
    if win.width < 1 or win.height < 1:
        return None
    return Window(int(win.col_off), int(win.row_off), int(win.width), int(win.height))
