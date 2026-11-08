"""What the four grids say, from the grid libraries and not from the raster code under test.

Shared by the tessellate and grid-aggregate tables: the cell a point is in, the polygon of a cell, the cells next to a
cell, the cells that share positive area with a raster extent, and a small GeoTIFF builder. A pixel belongs to the cell
its centre is in; a raster overlaps a cell when the two share positive area (an edge-only touch is not an overlap).
"""

import h3
import numpy as np
import quadbin
import rasterio
from rasterio.io import MemoryFile
from shapely.geometry import Polygon, box

from databricks.labs.gbx.pygx import _bng, _custom
from databricks.labs.gbx.pygx._custom import CustomGridConf

NODATA = -9999.0
GRIDS = ["h3", "quadbin", "bng", "custom"]

# bounds 0..200 on both axes, root cells of 200 m split in two a resolution: four 100 m cells at resolution 1
CUSTOM_CONF = CustomGridConf(
    bound_x_min=0,
    bound_x_max=200,
    bound_y_min=0,
    bound_y_max=200,
    cell_splits=2,
    root_cell_size_x=200,
    root_cell_size_y=200,
    srid=-1,
)


def gtiff(crs, bounds, width, height=None, bands=1, holes=(), dtype="float32"):
    """A north-up GeoTIFF of ``bounds`` (minx, miny, maxx, maxy). Band b holds ``1 + pixel index`` scaled by b (band 1
    is 1, 2, 3, ...); ``holes`` is a list of (band, row, col) set to NoData."""
    height = height or width
    minx, miny, maxx, maxy = bounds
    base = np.arange(1, width * height + 1, dtype="float64").reshape(height, width)
    data = np.stack([base * b for b in range(1, bands + 1)]).astype(dtype)
    for b, r, c in holes:
        data[b - 1, r, c] = NODATA
    prof = dict(
        driver="GTiff",
        height=height,
        width=width,
        count=bands,
        dtype=dtype,
        crs=crs,
        transform=rasterio.transform.from_origin(
            minx, maxy, (maxx - minx) / width, (maxy - miny) / height
        ),
        nodata=NODATA,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data)
        return mf.read()


def cell_of(grid, res, x, y):
    """The cell holding the point (x, y) in the grid's own coordinates (lon/lat for h3 and quadbin)."""
    if grid == "h3":
        return h3.str_to_int(h3.latlng_to_cell(y, x, res))
    if grid == "quadbin":
        return int(quadbin.point_to_cell(x, y, res))
    if grid == "bng":
        return _bng.format(_bng.point_to_cell_id(x, y, res))
    return int(_custom.point_to_cell_id(CUSTOM_CONF, x, y, res))


def polygon_of(grid, res, key):
    if grid == "h3":
        return Polygon(
            [(lng, lat) for lat, lng in h3.cell_to_boundary(h3.int_to_str(key))]
        )
    if grid == "quadbin":
        return box(*quadbin.cell_to_bounding_box(key))
    if grid == "bng":
        return _bng.cell_id_to_geometry(_bng.parse(key))
    return _custom.cell_id_to_polygon(CUSTOM_CONF, key)


def _near(grid, res, key):
    if grid == "h3":
        return {h3.str_to_int(c) for c in h3.grid_disk(h3.int_to_str(key), 1)}
    if grid == "quadbin":
        return {int(c) for c in quadbin.k_ring(key, 1)}
    if grid == "bng":
        cx, cy = polygon_of(grid, res, key).centroid.coords[0]
        edge = _bng.get_edge_size(res)
        return {
            cell_of(grid, res, cx + i * edge, cy + j * edge)
            for i in (-1, 0, 1)
            for j in (-1, 0, 1)
        }
    return {int(c) for c in _custom.k_ring(CUSTOM_CONF, key, 1)}


def overlapping_cells(grid, res, bounds, step):
    """The cells with positive-area overlap with ``bounds``: the cells under a lattice of ``step`` over the extent and
    their neighbours (a sliver the lattice misses lies next to a cell it finds), each judged by its polygon.
    """
    minx, miny, maxx, maxy = bounds
    xs = np.append(np.arange(minx, maxx, step), maxx - step / 100)
    ys = np.append(np.arange(miny, maxy, step), maxy - step / 100)
    found = {cell_of(grid, res, x, y) for x in xs for y in ys}
    found |= {n for c in list(found) for n in _near(grid, res, c)}
    extent = box(*bounds)
    return {c for c in found if extent.intersection(polygon_of(grid, res, c)).area > 0}


def pixel_values_by_cell(grid, res, data, bounds, band=0):
    """{cell: [valid values of the band whose pixel centre is in it]}."""
    minx, miny, maxx, maxy = bounds
    _, height, width = data.shape
    out = {}
    for r in range(height):
        for c in range(width):
            v = float(data[band, r, c])
            if v == NODATA:
                continue
            x = minx + (c + 0.5) * (maxx - minx) / width
            y = maxy - (r + 0.5) * (maxy - miny) / height
            out.setdefault(cell_of(grid, res, x, y), []).append(v)
    return out
