"""Spark-free H3 raster tessellation (mirrors heavyweight ``RST_H3_Tessellate``
/ ``RasterTessellate``).

For every H3 cell overlapping the raster's bounding box at the requested
resolution, the raster is clipped to that cell's hexagon geometry and one tile
is yielded per cell, carrying the H3 cell id as its ``cellid``. A cell is
skipped only when its hexagon does not geometrically overlap the raster at all;
a cell that overlaps but clips to entirely NoData is still emitted (its value
reducers then return NULL), matching the heavyweight tier.
"""

from collections import defaultdict

import h3
import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds, transform_geom
from shapely.geometry import Polygon, mapping, shape

from databricks.labs.gbx.pyrx.core import edit

H3_MAX_RES = 15

_WGS84 = "EPSG:4326"
_VALID_MODES = {"covering", "centroid"}
_DEFAULT_NODATA = -9999.0


def _cell_polygon_lonlat(cell: str) -> Polygon:
    """H3 cell hexagon as a shapely Polygon in (lon, lat) order.

    ``h3.cell_to_boundary`` returns (lat, lng) tuples; shapely expects
    (lon, lat), so the coordinates are flipped.
    """
    boundary = h3.cell_to_boundary(cell)  # list of (lat, lng)
    return Polygon([(lng, lat) for lat, lng in boundary])


def _h3_str_to_signed_int64(cell: str) -> int:
    """Convert an H3 cell string id to a signed int64 (matching Spark LongType)."""
    cellid = h3.str_to_int(cell)
    if cellid >= 2**63:
        cellid -= 2**64
    return cellid


def _centroid_chips(ds, resolution: int):
    """Centroid-partition mode: every valid pixel goes to exactly one H3 cell.

    Pixel centroid (lon, lat) → ``h3.latlng_to_cell`` → group pixels by cell.
    Each cell's chip is the full-tile raster with all out-of-cell pixels set to
    nodata.  This guarantees a strict partition: every valid pixel appears in
    exactly one chip.

    Yields ``(cellid_int, gtiff_bytes)`` pairs.
    """
    # Reproject pixel coords to WGS84 if needed.
    dst_epsg = ds.crs.to_epsg() if ds.crs else None
    need_reproject = dst_epsg != 4326

    # Build pixel-center coordinate arrays (row, col) → (lon, lat).
    rows, cols = np.mgrid[0 : ds.height, 0 : ds.width]
    # rasterio xy() returns (x, y) = (lon_or_easting, lat_or_northing) for each pixel.
    xs, ys = ds.xy(rows.ravel(), cols.ravel())
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    if need_reproject:
        from rasterio.warp import transform as warp_transform

        lons, lats = warp_transform(ds.crs, _WGS84, xs.tolist(), ys.tolist())
        lons = np.asarray(lons, dtype="float64")
        lats = np.asarray(lats, dtype="float64")
    else:
        lons, lats = xs, ys

    # Read all bands + determine the nodata value.
    data = ds.read()  # shape (bands, height, width)
    nodata = ds.nodata

    # Valid pixel mask: a pixel is valid if it is not nodata in ANY band.
    if nodata is not None:
        valid_flat = ~np.all(data.reshape(ds.count, -1).T == nodata, axis=1)
    else:
        valid_flat = np.ones(ds.height * ds.width, dtype=bool)

    # Map each valid pixel (flat index) → signed-int64 H3 cell id string.
    cell_pixels = defaultdict(list)  # cell_str → [flat_idx, ...]
    for flat_idx in np.where(valid_flat)[0]:
        lat = float(lats[flat_idx])
        lon = float(lons[flat_idx])
        cell_str = h3.latlng_to_cell(lat, lon, resolution)
        cell_pixels[cell_str].append(int(flat_idx))

    # Build the output profile (full-tile extent, same CRS/transform).
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    # Ensure a nodata value is set so masked pixels are well-defined.
    if nodata is None:
        nd = _DEFAULT_NODATA
        profile["nodata"] = nd
    else:
        nd = nodata

    for cell_str, flat_indices in cell_pixels.items():
        # Start with a full-nodata copy of all bands.
        chip = np.full_like(data, nd)
        # Write only this cell's pixel values.
        row_indices, col_indices = np.unravel_index(flat_indices, (ds.height, ds.width))
        chip[:, row_indices, col_indices] = data[:, row_indices, col_indices]

        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(chip)
            raster_bytes = mf.read()

        yield (_h3_str_to_signed_int64(cell_str), raster_bytes)


def iter_tessellate_h3(ds, resolution: int, mode: str = "covering"):
    """Streaming variant of :func:`tessellate_h3`.

    Yields ``(cellid_int, gtiff_bytes)`` one cell at a time — never buffers the
    full cell list (large-fan-out OOM guard).

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: H3 resolution in ``[0, 15]``.
        mode:       ``"covering"`` (default) — clip each overlapping hexagon;
                    ``"centroid"`` — strict pixel partition: each valid pixel
                    assigned to exactly one cell by its centroid.

    Yields:
        ``(cellid, raster_bytes)`` tuples, one per H3 cell with valid pixels.
        ``cellid`` is the signed int64 H3 cell id.
    """
    resolution = int(resolution)
    if resolution < 0 or resolution > H3_MAX_RES:
        raise ValueError(
            f"rst_h3_tessellate: resolution must be in [0, {H3_MAX_RES}]; "
            f"got {resolution}"
        )
    if mode not in _VALID_MODES:
        raise ValueError(
            f"rst_h3_tessellate: mode must be one of covering, centroid; got '{mode}'"
        )

    if mode == "centroid":
        yield from _centroid_chips(ds, resolution)
        return

    # Raster bbox in WGS84 lon/lat.
    west, south, east, north = transform_bounds(ds.crs, _WGS84, *ds.bounds)
    # True overlapping cell set via h3-py 4.4.2 native primitive — no ring
    # expansion or post-hoc prune needed (polygon_to_cells_experimental with
    # contain="overlap" returns exactly the cells whose hexagon intersects the
    # bbox polygon).
    bbox_poly = h3.LatLngPoly(
        [(south, west), (north, west), (north, east), (south, east)]
    )
    covered = h3.polygon_to_cells_experimental(bbox_poly, resolution, contain="overlap")

    dst_epsg = ds.crs.to_epsg() if ds.crs else None
    reproject = dst_epsg != 4326

    for cell in covered:
        cell_poly = _cell_polygon_lonlat(cell)
        if reproject:
            geom = transform_geom(_WGS84, ds.crs, mapping(cell_poly))
            cell_poly = shape(geom)
        try:
            # all_touched=True: boundary pixels touched by the hexagon edge are
            # included in the chip, consistent with the covering selection intent.
            clipped = edit.clip_to_geom(
                ds, shapely.wkb.dumps(cell_poly), all_touched=True
            )
        except ValueError:
            # rasterio.mask raises ValueError when the shape does not overlap.
            continue
        # clip_to_geom returns None ONLY on true geometric non-overlap (the
        # rasterio "Input shapes do not overlap raster" case, edit.py). A cell
        # that overlaps the BBOX but clips to entirely NoData clips successfully
        # to a nodata-filled chip and IS emitted -- its value reducers then
        # return NULL. So this skip drops only non-overlapping cells; it does
        # NOT skip all-nodata cells (matches the heavyweight tier's covering
        # keep-test, which is purely geometric).
        if clipped is None:
            continue
        yield (_h3_str_to_signed_int64(cell), clipped)


def tessellate_h3(ds, resolution: int) -> list:
    """Tessellate a raster into H3 cells; return ``[(cellid_int, gtiff_bytes)]``.

    List-materializing wrapper around :func:`iter_tessellate_h3` (kept for the
    Spark-free core API and bench/parity callers).
    """
    return list(iter_tessellate_h3(ds, resolution))
