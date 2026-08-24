"""BNG (British National Grid, EPSG:27700) cell enumeration for the mini-COG mosaic.

Mirrors ds/_quadbin_grid.py and ds/_h3_grid.py. Enumerates OVERLAP coverage — every BNG
cell whose EPSG:27700 extent intersects the source bounds — by direct grid iteration
(BNG is a regular square grid). Cellids are bit-identical to rst_bng_tessellate: they
come from pygx._bng.point_to_cell_id + format.
"""
import math
from collections import namedtuple

from databricks.labs.gbx.pygx import _bng

BngCell = namedtuple("BngCell", "cellid west south east north")


def bng_cells_for_bounds(bounds_27700, resolution: int) -> list[BngCell]:
    """BNG cells covering *bounds_27700* = (west, south, east, north) in EPSG:27700 metres.

    *resolution* is a BNG int index (already resolved via _bng.get_resolution). Returns
    every cell overlapping the bbox (edges included); a source smaller than one cell
    returns its single covering cell.
    """
    west, south, east, north = (float(v) for v in bounds_27700)
    size = _bng.get_edge_size(resolution)  # metres per cell edge (incl. quadrant sizes)
    # Snap the SW corner down to the grid; step across every overlapping cell. Sample
    # each cell at its centre so point_to_cell_id lands squarely inside it.
    x0 = math.floor(west / size) * size
    y0 = math.floor(south / size) * size
    seen: set = set()
    cells: list = []
    y = y0
    while y <= north:
        x = x0
        while x <= east:
            cid = _bng.point_to_cell_id(x + size / 2.0, y + size / 2.0, resolution)
            if cid not in seen:
                seen.add(cid)
                if _bng.is_valid(cid):
                    geom = _bng.cell_id_to_geometry(cid)  # EPSG:27700 square polygon
                    cw, cs, ce, cn = geom.bounds
                    cells.append(BngCell(_bng.format(cid), cw, cs, ce, cn))
            x += size
        y += size
    return cells
