"""Pure-Python h3 cell-grid computation for a source 4326 bbox.

Returns the h3 cells overlapping a bounding box (in EPSG:4326) with their
EPSG:4326 extents and h3index strings — no Spark, no GDAL dataset.

Used by the h3 mosaic branch of ``_write_mosaic`` in cog_writer.py.
"""

from collections import namedtuple

import h3

# H3 maximum resolution (v4 API supports 0..15).
_H3_MAX_RES = 15

# A single h3 cell with its EPSG:4326 bounding box.
# - cellid: the h3index string (canonical h3 cell identifier)
# - west, south, east, north: the cell hexagon's bounding box in EPSG:4326 degrees
H3Cell = namedtuple("H3Cell", "cellid west south east north")


def h3_cells_for_bounds(bounds_4326, resolution: int) -> list:
    """Return the h3 cells covering *bounds_4326* at *resolution*.

    Args:
        bounds_4326: ``(minlon, minlat, maxlon, maxlat)`` in EPSG:4326.
        resolution:  H3 resolution in ``[0, 15]``.

    Returns:
        List of :class:`H3Cell` namedtuples, one per covering cell.
        Each cell carries the h3index string and its hexagon bounding box
        in EPSG:4326 degrees (``west, south, east, north``).

    Note:
        Uses ``h3.polygon_to_cells_experimental(contain="overlap")`` (v4 API)
        over the bbox polygon so that cells whose hexagon body overlaps the bbox
        are included even when their centroid falls outside (e.g. when the bbox
        is smaller than a single cell at the chosen resolution).
        ``h3.LatLngPoly`` takes ``(lat, lon)`` tuples; ``h3.cell_to_boundary``
        returns ``[(lat, lon), ...]`` — lats and lons are swapped internally.
    """
    minlon, minlat, maxlon, maxlat = bounds_4326
    res = int(resolution)
    if res < 0 or res > _H3_MAX_RES:
        raise ValueError(
            f"h3_cells_for_bounds: resolution must be in [0, {_H3_MAX_RES}]; got {res}"
        )

    # Build the bbox polygon in h3's native (lat, lon) order.
    bbox_poly = h3.LatLngPoly(
        [(minlat, minlon), (minlat, maxlon), (maxlat, maxlon), (maxlat, minlon)]
    )
    # Use contain="overlap" so fringe cells whose centroid falls outside the bbox
    # (but whose hexagon body intersects it) are still included.  This matches
    # iter_tessellate_h3 in pyrx/core/tessellate.py and avoids returning an empty
    # set when the bbox is smaller than a single cell at the chosen resolution.
    cell_strs = h3.polygon_to_cells_experimental(bbox_poly, res, contain="overlap")

    cells = []
    for cellid_str in cell_strs:
        # cell_to_boundary returns [(lat, lon), ...] — swap to extract lon/lat.
        boundary = h3.cell_to_boundary(cellid_str)
        lats = [pt[0] for pt in boundary]
        lons = [pt[1] for pt in boundary]
        cells.append(H3Cell(cellid_str, min(lons), min(lats), max(lons), max(lats)))
    return cells
