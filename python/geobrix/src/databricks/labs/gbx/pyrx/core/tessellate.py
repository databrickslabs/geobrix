"""Spark-free H3 and quadbin raster tessellation.

Mirrors heavyweight ``RST_H3_Tessellate`` / ``RST_Quadbin_Tessellate``
(``RasterTessellate``).

For every cell overlapping the raster's bounding box at the requested
resolution, the raster is clipped to that cell's geometry and one tile is
yielded per cell, carrying the cell id as its ``cellid``. A cell is skipped
only when its geometry does not geometrically overlap the raster at all; a cell
that overlaps but clips to entirely NoData is still emitted (its value reducers
then return NULL), matching the heavyweight tier.
"""

from collections import defaultdict
from contextlib import contextmanager

import h3
import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds, transform_geom
from shapely.geometry import Polygon, box, mapping, shape

from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx.core import edit, warp

H3_MAX_RES = 15
QUADBIN_MAX_RES = _quadbin._MAX_POLYFILL_RES

_WGS84 = "EPSG:4326"
_BNG_EPSG = 27700
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


# ---------------------------------------------------------------------------
# Quadbin tessellate
# ---------------------------------------------------------------------------


def _quadbin_uint64_to_signed_int64(cell: int) -> int:
    """Convert an unsigned quadbin cell id to a signed int64 (Spark LongType)."""
    if cell >= 2**63:
        return cell - 2**64
    return int(cell)


def _centroid_chips_quadbin(ds, resolution: int):
    """Centroid-partition mode for quadbin: each valid pixel → exactly one cell.

    Pixel centroid (lon, lat) → ``pygx._quadbin.point_as_cell`` → group pixels
    by cell.  Each cell's chip is the full-tile raster with all out-of-cell
    pixels set to nodata.  This guarantees a strict partition: every valid pixel
    appears in exactly one chip.

    Quadbin is 4326-native — no reprojection is performed (the input raster must
    be in EPSG:4326 or the pixel coordinates are treated as lon/lat directly,
    matching H3 centroid behaviour).

    Yields ``(cellid_int, gtiff_bytes)`` pairs.
    """
    # Build pixel-center coordinate arrays.
    rows, cols = np.mgrid[0 : ds.height, 0 : ds.width]
    xs, ys = ds.xy(rows.ravel(), cols.ravel())
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    dst_epsg = ds.crs.to_epsg() if ds.crs else None
    need_reproject = dst_epsg != 4326
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

    # Valid pixel mask.
    if nodata is not None:
        valid_flat = ~np.all(data.reshape(ds.count, -1).T == nodata, axis=1)
    else:
        valid_flat = np.ones(ds.height * ds.width, dtype=bool)

    # Map each valid pixel → quadbin cell id.
    cell_pixels = defaultdict(list)  # cell_int → [flat_idx, ...]
    for flat_idx in np.where(valid_flat)[0]:
        lon = float(lons[flat_idx])
        lat = float(lats[flat_idx])
        cell_int = _quadbin.point_as_cell(lon, lat, resolution)
        cell_pixels[cell_int].append(int(flat_idx))

    # Build output profile.
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    if nodata is None:
        nd = _DEFAULT_NODATA
        profile["nodata"] = nd
    else:
        nd = nodata

    for cell_int, flat_indices in cell_pixels.items():
        chip = np.full_like(data, nd)
        row_indices, col_indices = np.unravel_index(flat_indices, (ds.height, ds.width))
        chip[:, row_indices, col_indices] = data[:, row_indices, col_indices]

        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(chip)
            raster_bytes = mf.read()

        yield (_quadbin_uint64_to_signed_int64(cell_int), raster_bytes)


def iter_tessellate_quadbin(ds, resolution: int, mode: str = "covering"):
    """Streaming quadbin tessellate: yield ``(cellid_int, gtiff_bytes)`` per cell.

    For every quadbin cell overlapping the raster's bounding box at the
    requested resolution, the raster is clipped to that cell's bounding-box
    polygon and one tile is yielded carrying the quadbin cell id as its
    ``cellid``.

    Quadbin cells are axis-aligned rectangular tiles on the Web Mercator grid,
    so the clip geometry is a simple box derived from
    ``pygx._quadbin.as_wkb(cell)`` (EWKB, SRID 4326).

    Quadbin is 4326-native — no reprojection of cell geometries is performed
    when the raster is already in EPSG:4326; for other CRS the cell box is
    reprojected to the raster CRS before clipping.

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: Quadbin resolution in ``[0, 26]`` (polyfill limited to
                    ``[0, 20]`` — see :data:`QUADBIN_MAX_RES`).
        mode:       ``"covering"`` (default) — clip each overlapping cell bbox;
                    ``"centroid"`` — strict pixel partition: each valid pixel
                    assigned to exactly one cell by its centroid.

    Yields:
        ``(cellid, raster_bytes)`` tuples, one per quadbin cell with valid
        pixels.  ``cellid`` is a signed int64 quadbin cell id.
    """
    resolution = int(resolution)
    if resolution < 0 or resolution > QUADBIN_MAX_RES:
        raise ValueError(
            f"rst_quadbin_tessellate: resolution must be in [0, {QUADBIN_MAX_RES}]; "
            f"got {resolution}"
        )
    if mode not in _VALID_MODES:
        raise ValueError(
            f"rst_quadbin_tessellate: mode must be one of covering, centroid; "
            f"got '{mode}'"
        )

    if mode == "centroid":
        yield from _centroid_chips_quadbin(ds, resolution)
        return

    # Raster bbox in WGS84 lon/lat.
    west, south, east, north = transform_bounds(ds.crs, _WGS84, *ds.bounds)
    bbox_poly = box(west, south, east, north)

    # polyfill: quadbin cells covering the bbox envelope.
    covered = _quadbin.polyfill(bbox_poly, resolution)

    dst_epsg = ds.crs.to_epsg() if ds.crs else None
    reproject = dst_epsg != 4326

    for cell in covered:
        # as_wkb returns EWKB with SRID=4326; shapely.wkb.loads handles EWKB
        # transparently (reads the geometry; SRID embedded in the EWKB is not
        # used here because reprojection is handled by the `reproject` branch
        # below, mirroring the iter_tessellate_h3 pattern).
        cell_poly = shapely.wkb.loads(_quadbin.as_wkb(cell))
        if reproject:
            geom = transform_geom(_WGS84, ds.crs, mapping(cell_poly))
            cell_poly = shape(geom)
        try:
            clipped = edit.clip_to_geom(ds, cell_poly, all_touched=True)
        except ValueError:
            continue
        if clipped is None:
            continue
        yield (_quadbin_uint64_to_signed_int64(cell), clipped)


# ---------------------------------------------------------------------------
# BNG tessellate
# ---------------------------------------------------------------------------


@contextmanager
def _as_bng_dataset(ds):
    """Yield ``ds`` (or a 27700-warped copy) as an open EPSG:27700 dataset.

    BNG has no lon/lat input path — the raster must be in EPSG:27700 before cell
    geometry (``pygx._bng.cell_id_to_geometry``) and the geometric keep-test can
    be applied.  When ``ds`` is already 27700 it is yielded unchanged; otherwise
    it is reprojected (nearest) via :func:`warp.reproject_to_srid` and the warped
    dataset is yielded and cleaned up.  Mirrors heavy ``warpToBng``.
    """
    src_epsg = ds.crs.to_epsg() if ds.crs else None
    if src_epsg == _BNG_EPSG:
        yield ds
        return
    warped_bytes = warp.reproject_to_srid(ds, _BNG_EPSG, resampling="nearest")
    with MemoryFile(warped_bytes) as mf:
        with mf.open() as work_ds:
            yield work_ds


def _centroid_chips_bng(ds, resolution: int):
    """Centroid-partition mode for BNG: each valid pixel → exactly one cell.

    The raster is warped to EPSG:27700 first; pixel centroids are then read
    directly as eastings/northings (no per-pixel reprojection) and mapped via
    ``pygx._bng.point_to_cell_id``.  Out-of-GB pixels are dropped via
    ``pygx._bng.is_valid`` (mirrors heavy ``tessellateBngCentroidIter``).  Each
    cell's chip is the full-tile raster with all out-of-cell pixels set to
    nodata, guaranteeing a strict partition.

    Yields ``(cellid_str, gtiff_bytes)`` pairs — ``cellid_str`` is a BNG String
    id via ``pygx._bng.format``.
    """
    with _as_bng_dataset(ds) as work_ds:
        # Pixel-center coords are already in EPSG:27700 (eastings, northings).
        rows, cols = np.mgrid[0 : work_ds.height, 0 : work_ds.width]
        eastings, northings = work_ds.xy(rows.ravel(), cols.ravel())
        eastings = np.asarray(eastings, dtype="float64")
        northings = np.asarray(northings, dtype="float64")

        data = work_ds.read()  # (bands, height, width)
        nodata = work_ds.nodata

        if nodata is not None:
            valid_flat = ~np.all(data.reshape(work_ds.count, -1).T == nodata, axis=1)
        else:
            valid_flat = np.ones(work_ds.height * work_ds.width, dtype=bool)

        # Map each valid pixel → BNG Long cell id; drop out-of-GB via is_valid.
        cell_pixels = defaultdict(list)  # cell_int → [flat_idx, ...]
        for flat_idx in np.where(valid_flat)[0]:
            e = float(eastings[flat_idx])
            n = float(northings[flat_idx])
            cell_int = _bng.point_to_cell_id(e, n, resolution)
            if not _bng.is_valid(cell_int):
                continue
            cell_pixels[cell_int].append(int(flat_idx))

        profile = work_ds.profile.copy()
        profile.update(driver="GTiff")
        if nodata is None:
            nd = _DEFAULT_NODATA
            profile["nodata"] = nd
        else:
            nd = nodata

        for cell_int, flat_indices in cell_pixels.items():
            chip = np.full_like(data, nd)
            row_indices, col_indices = np.unravel_index(
                flat_indices, (work_ds.height, work_ds.width)
            )
            chip[:, row_indices, col_indices] = data[:, row_indices, col_indices]

            with MemoryFile() as chip_mf:
                with chip_mf.open(**profile) as dst:
                    dst.write(chip)
                raster_bytes = chip_mf.read()

            yield (_bng.format(cell_int), raster_bytes)


def iter_tessellate_bng(ds, resolution, mode: str = "covering"):
    """Streaming BNG tessellate: yield ``(cellid_str, gtiff_bytes)`` per cell.

    The raster is reprojected to EPSG:27700 first (skipped if already 27700).
    Cells are enumerated ONLY via ``pygx._bng.polyfill`` over the (buffered)
    raster bbox polygon and geometrised via ``pygx._bng.cell_id_to_geometry`` —
    the vector ``bng_tessellate`` codepath is never touched.  Out-of-GB cells are
    dropped via ``pygx._bng.is_valid``.  Mirrors heavy ``tessellateBngIter``.

    covering enumeration is BOUNDARY-COMPLETE: ``BNG.polyfill`` is a centroid
    flood-fill, so a cell whose square overlaps the raster but whose centroid
    sits just outside the bbox would be missed.  The bbox is therefore BUFFERED
    by the cell half-diagonal (``pygx._bng.get_buffer_radius``) before polyfill to
    pull those fringe centroids in; the ``intersects`` keep-test against the
    UNBUFFERED bbox then filters any buffered-but-non-overlapping cell.

    Args:
        ds:         Open rasterio ``DatasetReader`` (any CRS; warped to 27700).
        resolution: BNG resolution — an Int index (±1..±6) or a resolutionMap
                    string key (e.g. ``"1km"``, ``"100m"``); resolved via
                    ``pygx._bng.get_resolution``.
        mode:       ``"covering"`` (default) — clip each overlapping cell square;
                    ``"centroid"`` — strict pixel partition: each valid pixel
                    assigned to exactly one cell by its centroid.

    Yields:
        ``(cellid, raster_bytes)`` tuples, one per BNG cell with valid pixels.
        ``cellid`` is a BNG String id (e.g. ``"TQ38"``).
    """
    if mode not in _VALID_MODES:
        raise ValueError(
            f"rst_bng_tessellate: mode must be one of covering, centroid; "
            f"got '{mode}'"
        )
    # Resolve the BNG resolution (Int index ±1..±6 or resolutionMap string key).
    resolution = _bng.get_resolution(resolution)

    if mode == "centroid":
        yield from _centroid_chips_bng(ds, resolution)
        return

    with _as_bng_dataset(ds) as work_ds:
        # Raster bbox in EPSG:27700 (same CRS as BNG.cell_id_to_geometry) — the
        # geometric keep-test lives in 27700; NO WGS84 hop.
        west, south, east, north = work_ds.bounds
        bbox_poly = box(west, south, east, north)

        # Buffer the bbox by the cell half-diagonal before polyfill so boundary
        # cells (centroid just outside the bbox) are not dropped by the centroid
        # flood-fill.  The intersects keep-test below runs on the UNBUFFERED bbox.
        buf_radius = _bng.get_buffer_radius(resolution)
        covered = _bng.polyfill(bbox_poly.buffer(buf_radius), resolution)

        for cell in covered:
            if not _bng.is_valid(cell):
                continue
            cell_poly = _bng.cell_id_to_geometry(cell)  # 27700 shapely polygon
            # Standard covering keep-test: cell square must overlap the raster
            # bbox (unbuffered).  Filters buffered-but-non-overlapping fringe.
            if not cell_poly.intersects(bbox_poly):
                continue
            try:
                clipped = edit.clip_to_geom(work_ds, cell_poly, all_touched=True)
            except ValueError:
                continue
            if clipped is None:
                continue
            yield (_bng.format(cell), clipped)
