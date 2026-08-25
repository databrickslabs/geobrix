"""Pure-Python quadbin cell-grid computation for a source 4326 bbox.

Returns the quadbin cells overlapping a bounding box (in EPSG:4326) with
their EPSG:3857 extents and integer cell ids — no Spark, no GDAL dataset.

Used by the quadbin mosaic branch of ``_write_mosaic`` to enumerate the
cell grid for a source file's extent before writing each mini-COG.
"""

from collections import namedtuple

import quadbin
from rasterio.warp import transform_bounds

from databricks.labs.gbx.pygx import _quadbin as _qb

# A single quadbin cell with its EPSG:3857 bounding box.
# - cellid: the quadbin 64-bit unsigned int (as returned by quadbin.tile_to_cell)
# - west, south, east, north: the cell extent in EPSG:3857 metres
QuadbinCell = namedtuple("QuadbinCell", "cellid west south east north")

_WGS84 = "EPSG:4326"
_WEBMERC = "EPSG:3857"


def quadbin_cells_for_bounds(bounds_4326, resolution: int) -> list[QuadbinCell]:
    """Return the quadbin cells overlapping *bounds_4326* at *resolution*.

    Args:
        bounds_4326: ``(minlon, minlat, maxlon, maxlat)`` in EPSG:4326.
        resolution:  Quadbin resolution in ``[0, 20]`` (mirrors polyfill
                     limit in ``pygx._quadbin.polyfill``).

    Returns:
        List of :class:`QuadbinCell` namedtuples, one per overlapping cell.
        Each cell carries the quadbin int cell id and its extent in
        EPSG:3857 metres (``west, south, east, north``).

    Note:
        Cell enumeration mirrors ``pygx._quadbin.polyfill`` (bbox corner tiles
        → tile-range walk → ``quadbin.tile_to_cell``), so this function and
        ``rst_quadbin_tessellate`` agree on the cell set for a given
        extent+resolution.  A shared helper is a future DRY opportunity.
    """
    minlon, minlat, maxlon, maxlat = bounds_4326
    z = int(resolution)
    if z < 0 or z > _qb._MAX_POLYFILL_RES:
        raise ValueError(
            f"quadbin_cells_for_bounds: resolution must be in "
            f"[0, {_qb._MAX_POLYFILL_RES}]; got {z}"
        )

    # Derive the tile range the same way polyfill does (mirrors Quadbin.scala
    # polyfillBbox): upper-left from (w, n), lower-right from (e, s).
    x0, y0 = _qb._lonlat_to_tile(minlon, maxlat, z)  # upper-left (north)
    x1, y1 = _qb._lonlat_to_tile(maxlon, minlat, z)  # lower-right (south)
    x_lo, x_hi = min(x0, x1), max(x0, x1)
    y_lo, y_hi = min(y0, y1), max(y0, y1)

    count = (x_hi - x_lo + 1) * (y_hi - y_lo + 1)
    if count > 1_000_000:
        raise ValueError(
            f"quadbin_cells_for_bounds would produce {count} cells "
            f"(max=1000000); use a lower resolution"
        )

    cells = []
    for x in range(x_lo, x_hi + 1):
        for y in range(y_lo, y_hi + 1):
            cell = quadbin.tile_to_cell((x, y, z))
            # Cell 4326 bbox → 3857
            w4326, s4326, e4326, n4326 = quadbin.cell_to_bounding_box(cell)
            w3857, s3857, e3857, n3857 = transform_bounds(
                _WGS84, _WEBMERC, w4326, s4326, e4326, n4326
            )
            cells.append(QuadbinCell(cell, w3857, s3857, e3857, n3857))
    return cells
