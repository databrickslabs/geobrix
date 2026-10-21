"""Spark-free H3, quadbin, and BNG raster tessellation.

Mirrors heavyweight ``RST_H3_Tessellate`` / ``RST_Quadbin_Tessellate`` /
``RST_BNG_Tessellate`` (``RasterTessellate``).

The single generic entry point is :func:`iter_tessellate`, which dispatches on
``grid`` for the ~40% divergent cell math (CRS setup, polyfill, cell geometry,
id encoding, validity filter) while sharing the ~60% common body (mode
validation, centroid dispatch, positive-area keep-test, clip, yield).

The three per-grid wrappers (:func:`iter_tessellate_h3`,
:func:`iter_tessellate_quadbin`, :func:`iter_tessellate_bng`) are kept as thin
delegates for backward compatibility with bench callers and existing tests.
"""

import math
from collections import defaultdict
from contextlib import contextmanager, nullcontext

import h3
import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.warp import transform_geom
from rasterio.windows import Window
from shapely.geometry import Polygon, mapping, shape
from shapely.geometry.polygon import orient

from databricks.labs.gbx.core.crs import authority_srid_of, get_transformer
from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx.core import edit, warp
from databricks.labs.gbx.pyrx.core._nodata import fallback_nodata
from databricks.labs.gbx.pyrx.core.compression import write_gtiff

H3_MAX_RES = 15
QUADBIN_MAX_RES = _quadbin._MAX_POLYFILL_RES

_WGS84 = "EPSG:4326"
_BNG_EPSG = 27700
_VALID_MODES = {"covering", "centroid"}
_VALID_COVERAGES = {"sparse", "complete"}
_VALID_GRIDS = frozenset({"h3", "quadbin", "bng", "custom"})


def _has_positive_area_overlap(cell_poly, bbox_poly) -> bool:
    """Covering keep-test: cell is emitted iff it has POSITIVE-AREA overlap with the raster footprint.

    Mere boundary touch (a shared edge line or corner point) is NOT enough. On a grid-aligned
    tile (raster edges land exactly on cell boundaries) a fringe cell just outside the data
    shares only a 1-D boundary with the raster: ``cell_poly.intersects(bbox) is True`` but
    ``cell_poly.intersection(bbox).area == 0.0`` and it holds ZERO source pixels, so the clip
    would (via rasterio "shapes do not overlap") drop it anyway. This guard makes that intent
    EXPLICIT and consistent with the heavyweight tier's positive-area keep-test.

    KEEPS cells with real areal overlap even if they clip to all-NoData (cloud hole, the raster's
    own NoData) — those have ``area > 0`` and must still be emitted (covering mode fills their
    position; the clip yields a real chip with NoData pixels, not a gap in the mosaic).

    ``cell_poly`` and ``bbox_poly`` MUST be in the same CRS.
    """
    if not cell_poly.intersects(bbox_poly):
        return False
    return cell_poly.intersection(bbox_poly).area > 0.0


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


def _quadbin_uint64_to_signed_int64(cell: int) -> int:
    """Convert an unsigned quadbin cell id to a signed int64 (Spark LongType)."""
    if cell >= 2**63:
        return cell - 2**64
    return int(cell)


# ---------------------------------------------------------------------------
# BNG context manager (shared by covering and centroid)
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
    src_epsg = authority_srid_of(ds.crs)
    if src_epsg == _BNG_EPSG:
        yield ds
        return
    warped_bytes = warp.reproject_to_srid(ds, _BNG_EPSG, resampling="nearest")
    with MemoryFile(warped_bytes) as mf:
        with mf.open() as work_ds:
            yield work_ds


# ---------------------------------------------------------------------------
# Per-grid oracle helpers (the divergent ~40%)
# ---------------------------------------------------------------------------


def _resolve_resolution(resolution, grid: str):
    """Validate and normalise resolution for the given grid.

    H3 and quadbin accept integer indices; BNG additionally accepts string keys
    (e.g. ``"1km"``, ``"100m"``); custom accepts integer indices (validated
    against conf.max_resolution at call time, not here).
    """
    if grid == "h3":
        resolution = int(resolution)
        if resolution < 0 or resolution > H3_MAX_RES:
            raise ValueError(
                f"rst_h3_tessellate: resolution must be in [0, {H3_MAX_RES}]; "
                f"got {resolution}"
            )
    elif grid == "quadbin":
        resolution = int(resolution)
        if resolution < 0 or resolution > QUADBIN_MAX_RES:
            raise ValueError(
                f"rst_quadbin_tessellate: resolution must be in [0, {QUADBIN_MAX_RES}]; "
                f"got {resolution}"
            )
    elif grid == "custom":
        resolution = int(resolution)  # conf.max_resolution validated at use time
    else:  # bng
        resolution = _bng.get_resolution(resolution)
    return resolution


def _polyfill_cells(bbox_poly, resolution, grid: str, conf=None):
    """Return the iterable of raw cell ids covering *bbox_poly* for *grid*.

    ``bbox_poly`` must be a shapely geometry in the work CRS for the grid:
    WGS84 for h3/quadbin, EPSG:27700 for bng, grid-native for custom. It may be
    the raster footprint quadrilateral (:func:`_footprint_polygon`): h3 and
    quadbin enumerate its bounding box, bng and custom (whose footprint is an
    axis-aligned rectangle) polyfill it directly.
    BNG and custom both apply the buffer-before-polyfill fix internally (custom
    via ``_custom.covering_candidate_cells``, mirroring heavy
    ``CustomGridSystem.coveringCandidateCells``) so a sub-cell pixel/bbox still
    enumerates its containing cell; h3 uses ``contain="overlap"``.
    ``conf`` is required when ``grid="custom"`` (a ``CustomGridConf`` instance).
    """
    if grid == "h3":
        west, south, east, north = bbox_poly.bounds
        h3_bbox = h3.LatLngPoly(
            [(south, west), (north, west), (north, east), (south, east)]
        )
        return h3.polygon_to_cells_experimental(h3_bbox, resolution, contain="overlap")
    elif grid == "quadbin":
        return _quadbin.polyfill(bbox_poly, resolution)
    elif grid == "custom":
        from databricks.labs.gbx.pygx import _custom as _custom_mod

        # Covering enumeration must mirror heavy CustomGridSystem.coveringCandidateCells:
        # buffer by one cell dimension so a sub-cell pixel/bbox (which contains no cell
        # centre under centroid-containment polyfill) still enumerates its containing
        # cell. Un-buffered polyfill here dropped sub-cell pixels (lost mass). The
        # downstream inter.area>0 / positive-area keep-test removes the extra buffered
        # candidates. (bng buffers here too; h3 uses contain="overlap".)
        return _custom_mod.covering_candidate_cells(conf, bbox_poly, resolution)
    else:  # bng — buffer so centroid-BFS doesn't miss boundary cells
        buf_radius = _bng.get_buffer_radius(resolution)
        return _bng.polyfill(bbox_poly.buffer(buf_radius), resolution)


def _cell_geom(cell, grid: str, conf=None):
    """Return the cell polygon in the work CRS (WGS84 for h3/quadbin; 27700 for bng;
    grid-native for custom).

    ``conf`` is required when ``grid="custom"`` (a ``CustomGridConf`` instance).
    """
    if grid == "h3":
        return _cell_polygon_lonlat(cell)
    elif grid == "quadbin":
        # as_wkb returns EWKB with SRID=4326; shapely.wkb.loads handles EWKB
        # transparently (reads the geometry; SRID is not used here because
        # reprojection is handled separately by the need_reproject branch).
        return shapely.wkb.loads(_quadbin.as_wkb(cell))
    elif grid == "custom":
        from databricks.labs.gbx.pygx import _custom as _custom_mod

        return _custom_mod.cell_id_to_polygon(conf, cell)
    else:  # bng
        return _bng.cell_id_to_geometry(cell)  # already EPSG:27700


def _cell_is_valid(cell, grid: str, conf=None) -> bool:
    """Return ``True`` unless the cell is structurally invalid (BNG only).

    ``conf`` is unused but accepted for API uniformity with the other oracles.
    """
    if grid == "bng":
        return bool(_bng.is_valid(cell))
    return True  # h3, quadbin, custom: always structurally valid


def _encode_cellid(cell, grid: str, conf=None):
    """Encode the raw cell id to the Python type yielded to callers.

    H3 and quadbin yield signed int64; BNG yields a String id (e.g. ``"TQ38"``);
    custom yields a plain int (cell IDs fit in signed int64 by construction).
    ``conf`` is accepted for API uniformity but unused.
    """
    if grid == "h3":
        return _h3_str_to_signed_int64(cell)
    elif grid == "quadbin":
        return _quadbin_uint64_to_signed_int64(cell)
    elif grid == "custom":
        return int(cell)  # already int64; positive and fits in signed int64
    else:  # bng
        return _bng.format(cell)


def _footprint_polygon(ds, work_ds, grid: str) -> Polygon:
    """Raster footprint in the grid's work CRS: the region a covered cell must overlap.

    Mirrors heavy ``BoundingBox.windowBBox``: the four raster corners
    ``(c + x*a, f + y*e)`` for ``(x, y)`` in ``(0,0), (W,0), (W,H), (0,H)``, moved
    into the work CRS and joined as a quadrilateral. The rotation terms of the
    geotransform (``b``/``d``, gt2/gt4) are ignored, exactly as heavy ignores them.
    Under a projected raster CRS the reprojected corners form a slanted quadrilateral,
    NOT the axis-aligned lon/lat box of ``transform_bounds``, so a cell that overlaps
    only the box corner and not the raster is correctly rejected.

    Work CRS per grid: h3/quadbin WGS84 (corners reprojected unless the raster is
    already EPSG:4326 or has no CRS, in which case they are used as-is); bng the
    EPSG:27700 *work_ds* corners; custom the grid-native *ds* corners. The result is
    oriented counter-clockwise, as heavy's is.
    """
    src = work_ds if grid == "bng" else ds
    t = src.transform
    width, height = src.width, src.height
    xs = [t.c + x * t.a for x in (0, width, width, 0)]
    ys = [t.f + y * t.e for y in (0, 0, height, height)]
    if (
        grid in ("h3", "quadbin")
        and ds.crs is not None
        and authority_srid_of(ds.crs) != 4326
    ):
        xs, ys = get_transformer(ds.crs, _WGS84).transform(xs, ys)
        if not (np.isfinite(xs).all() and np.isfinite(ys).all()):
            raise ValueError(
                "tessellate: raster corners cannot be transformed to EPSG:4326"
            )
    return orient(Polygon(zip(xs, ys)), 1.0)


def _chip_is_all_nodata(raster_bytes: bytes) -> bool:
    """Return True when no pixel of the GTiff chip is valid in any band.

    Used by covering+sparse to drop chips whose pixels are entirely masked.
    Validity comes from the mask bands, so a NaN NoData is recognised (``== nan``
    never is). A chip with no declared nodata has all-valid masks and is kept.
    """
    with MemoryFile(raster_bytes) as mf:
        with mf.open() as chip_ds:
            return not (chip_ds.read_masks() != 0).any()


def _chip_nodata(work_ds) -> float:
    """NoData every centroid chip carries: the source's, else the dtype fallback."""
    if work_ds.nodata is not None:
        return work_ds.nodata
    return fallback_nodata(work_ds.dtypes[0])


def _empty_chip_builder(work_ds, grid: str, conf=None):
    """Return ``build(cell) -> bytes`` making the 1×1 empty chip of a covered cell.

    Everything that depends only on *work_ds* is resolved once here: identifying
    the raster CRS (a PROJ ``FindMatches`` per call, so too slow to repeat per
    cell), the grid-to-raster transformer, the inverse geotransform, the chip
    profile and its all-NoData pixel. ``build`` then does only the per-cell work.
    See :func:`_build_empty_chip` for the placement rule.
    """
    to_work = None
    if (
        grid in ("h3", "quadbin")
        and work_ds.crs is not None
        and authority_srid_of(work_ds.crs) != 4326
    ):
        to_work = get_transformer(4326, work_ds.crs).transform
    inv = ~work_ds.transform
    last_col, last_row = work_ds.width - 1, work_ds.height - 1
    nd = _chip_nodata(work_ds)
    profile = work_ds.profile.copy()
    profile.update(width=1, height=1, nodata=nd)
    chip = np.full((work_ds.count, 1, 1), nd, dtype=work_ds.dtypes[0])

    def build(cell) -> bytes:
        centroid = _cell_geom(cell, grid, conf=conf).centroid
        x, y = centroid.x, centroid.y
        if to_work is not None:
            x, y = to_work(x, y)
        col_f, row_f = inv * (x, y)
        col = min(max(math.floor(col_f), 0), last_col)
        row = min(max(math.floor(row_f), 0), last_row)
        window_tf = work_ds.window_transform(Window(col, row, 1, 1))
        return write_gtiff(dict(profile, transform=window_tf), chip)

    return build


def _build_empty_chip(work_ds, cell, grid: str, conf=None) -> bytes:
    """Build the 1×1 all-NoData chip of a covered cell that received no pixel.

    Used by centroid+complete to emit a synthetic covered-but-empty tile. The chip
    has the source's band count, dtype and CRS, and sits on the source pixel that
    holds the cell's centroid: the planar centroid of the cell polygon (grid CRS),
    moved into the raster CRS for h3/quadbin when the raster is not EPSG:4326
    (bng and custom already share the work CRS), floored to a pixel and clamped
    into the raster. ``conf`` is a ``CustomGridConf`` when ``grid="custom"``.

    For many cells of one raster, build once with :func:`_empty_chip_builder`.
    """
    return _empty_chip_builder(work_ds, grid, conf=conf)(cell)


# ---------------------------------------------------------------------------
# Consolidated centroid implementation
# ---------------------------------------------------------------------------


def _centroid_chips_inner(work_ds, resolution, grid: str, conf=None):
    """Inner centroid-partition loop for an already-prepared dataset.

    For h3/quadbin ``work_ds`` is the original dataset (pixel coords are
    reprojected to WGS84 if needed; CRS-less -> use coords directly).
    For BNG ``work_ds`` is already in EPSG:27700 and pixel coords are used
    directly as eastings/northings.  For custom the coords are grid-native.

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.

    Yields ``(cellid, gtiff_bytes)`` pairs in first-seen pixel order (row-major).
    Each chip is cropped to the bounding pixel window of its cell's pixels and
    carries the source NoData (else the dtype fallback, ``fallback_nodata``).
    """
    rows, cols = np.mgrid[0 : work_ds.height, 0 : work_ds.width]
    xs, ys = work_ds.xy(rows.ravel(), cols.ravel())
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    if grid in ("h3", "quadbin"):
        dst_epsg = authority_srid_of(work_ds.crs)
        # Reproject when the raster HAS a CRS and it isn't already WGS84.
        # Gate on CRS PRESENCE (work_ds.crs is not None), not EPSG-authority
        # presence (dst_epsg is not None): authority_srid_of returns None for valid
        # non-EPSG/non-ESRI CRSes (WKT/PROJ4-only) which still need reprojection.
        # CF3: crs=None (grid-native) → skip correctly.
        if work_ds.crs is not None and dst_epsg != 4326:
            from rasterio.warp import transform as warp_transform

            coord_x, coord_y = warp_transform(
                work_ds.crs, _WGS84, xs.tolist(), ys.tolist()
            )
            coord_x = np.asarray(coord_x, dtype="float64")
            coord_y = np.asarray(coord_y, dtype="float64")
        else:
            coord_x, coord_y = xs, ys  # already WGS84 or CRS-less (grid-native)
    else:  # bng or custom: use native coords directly
        coord_x, coord_y = xs, ys

    # Pre-import custom module once outside the pixel loop.
    # _custom_mod is used in the `elif grid == "custom"` branch below.
    _custom_mod = None
    if grid == "custom":
        from databricks.labs.gbx.pygx import _custom as _custom_mod

    data = work_ds.read()  # shape (bands, height, width)
    # A pixel is valid when ANY band's mask marks it valid (heavy anyValid). The mask
    # bands recognise a NaN NoData, which an ``== nodata`` comparison never matches.
    masks = work_ds.read_masks()  # shape (bands, height, width); 0 = invalid
    valid_flat = (masks != 0).any(axis=0).ravel()
    del masks

    cell_pixels: dict = defaultdict(list)
    for flat_idx in np.where(valid_flat)[0]:
        cx = float(coord_x[flat_idx])
        cy = float(coord_y[flat_idx])
        if grid == "h3":
            cell = h3.latlng_to_cell(cy, cx, resolution)  # latlng_to_cell(lat, lon)
        elif grid == "quadbin":
            cell = _quadbin.point_as_cell(cx, cy, resolution)  # (lon, lat)
        elif grid == "custom":
            cell = _custom_mod.point_to_cell_id_or_none(conf, cx, cy, resolution)
            if cell is None:
                continue  # pixel outside grid bounds — skip
        else:  # bng
            cell = _bng.point_to_cell_id(cx, cy, resolution)  # (easting, northing)
            if not _bng.is_valid(cell):
                continue
        cell_pixels[cell].append(int(flat_idx))

    nd = _chip_nodata(work_ds)
    profile = work_ds.profile.copy()
    profile["nodata"] = nd

    # Each chip covers only the bounding pixel window of its cell's pixels, on the
    # source grid (window_transform keeps rotated and south-up geotransforms intact);
    # pixels of that window assigned to other cells are NoData.
    for cell, flat_indices in cell_pixels.items():
        row_idx, col_idx = np.unravel_index(
            flat_indices, (work_ds.height, work_ds.width)
        )
        r0, c0 = int(row_idx.min()), int(col_idx.min())
        h = int(row_idx.max()) + 1 - r0
        w = int(col_idx.max()) + 1 - c0
        chip = np.full((work_ds.count, h, w), nd, dtype=data.dtype)
        chip[:, row_idx - r0, col_idx - c0] = data[:, row_idx, col_idx]
        profile.update(
            width=w,
            height=h,
            transform=work_ds.window_transform(Window(c0, r0, w, h)),
        )
        yield (_encode_cellid(cell, grid, conf=conf), write_gtiff(profile, chip))


def _centroid_chips_generic(ds, resolution, grid: str, conf=None):
    """Consolidated centroid-partition dispatcher for all grids.

    BNG warps the dataset to EPSG:27700 first (via :func:`_as_bng_dataset`).
    H3 and quadbin use the original dataset, reprojecting pixel coords to WGS84
    inside :func:`_centroid_chips_inner` if needed (CRS-less -> grid-native).
    Custom uses the original dataset directly (grid-native coordinates).

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.

    Yields ``(cellid, gtiff_bytes)`` pairs.
    """
    if grid == "bng":
        with _as_bng_dataset(ds) as work_ds:
            yield from _centroid_chips_inner(work_ds, resolution, grid)
    else:
        yield from _centroid_chips_inner(ds, resolution, grid, conf=conf)


def _centroid_complete(ds, resolution, grid: str, conf=None):
    """centroid + complete coverage: yield centroid chips, then synthetic empty
    chips for covered cells that received no centroid pixels (each a 1×1 chip on
    the cell's centroid pixel; see :func:`_build_empty_chip`).

    Extracted from :func:`iter_tessellate` to stay within C901 complexity limit.

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.

    Covered cells are kept against the raster footprint of
    :func:`_footprint_polygon` (heavy's reprojected-corner quadrilateral), not
    the axis-aligned lon/lat box of the raster. CF3 guard: a CRS-less raster
    (``ds.crs is None``) uses its corners as grid-native coordinates.
    """
    centroid_ids = set()
    for cellid, raster_bytes in _centroid_chips_generic(
        ds, resolution, grid, conf=conf
    ):
        centroid_ids.add(cellid)
        yield (cellid, raster_bytes)
    # Compute covering set to find covered-but-empty cells.
    _ctx = _as_bng_dataset(ds) if grid == "bng" else nullcontext(ds)
    with _ctx as work_ds:
        footprint = _footprint_polygon(ds, work_ds, grid)
        covered = _polyfill_cells(footprint, resolution, grid, conf=conf)
        build_empty = _empty_chip_builder(work_ds, grid, conf=conf)
        for cell in covered:
            if not _cell_is_valid(cell, grid, conf=conf):
                continue
            cell_poly = _cell_geom(cell, grid, conf=conf)
            if not _has_positive_area_overlap(cell_poly, footprint):
                continue
            encoded = _encode_cellid(cell, grid, conf=conf)
            if encoded not in centroid_ids:
                yield (encoded, build_empty(cell))


# ---------------------------------------------------------------------------
# Generic tessellate (the consolidated public entry point)
# ---------------------------------------------------------------------------


def iter_tessellate(
    ds,
    resolution,
    grid: str,
    mode: str = "covering",
    coverage: str = None,
    conf=None,
):
    """Generic streaming tessellate: yield ``(cellid, gtiff_bytes)`` per overlapping cell.

    Dispatches on *grid* for the ~40% divergent cell math (CRS setup, polyfill,
    cell geometry, id encoding, validity filter) while sharing the ~60% common
    body (mode validation, centroid dispatch, positive-area keep-test, clip,
    yield).

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: Grid-appropriate resolution:

                    - H3: int in ``[0, 15]``
                    - Quadbin: int in ``[0, 26]`` (polyfill limited to
                      ``[0, 20]``; see :data:`QUADBIN_MAX_RES`)
                    - BNG: int index ``±1..±6`` or string key (e.g.
                      ``"1km"``, ``"100m"``); resolved via
                      ``pygx._bng.get_resolution``
                    - Custom: int in ``[0, conf.max_resolution]``

        grid:       One of ``"h3"``, ``"quadbin"``, ``"bng"``, ``"custom"``.
        mode:       ``"covering"`` (default) — clip each overlapping cell
                    boundary; ``"centroid"`` — strict pixel partition: each
                    valid pixel assigned to exactly one cell by its centroid.
        coverage:   Controls whether covered-but-empty (all-NoData) cells are
                    emitted.  ``None`` (default) uses the mode-appropriate
                    legacy default: ``"complete"`` for covering (preserves the
                    pre-Task-8 behaviour of emitting all-NoData chips) and
                    ``"sparse"`` for centroid (preserves the pre-Task-8
                    behaviour of emitting only cells with valid pixels).
                    Pass explicitly to override.
        conf:       ``CustomGridConf`` instance.  Required when
                    ``grid="custom"``; ignored for all other grids.

    Yields:
        ``(cellid, raster_bytes)`` tuples, one per overlapping cell.
        ``cellid`` is a signed int64 for h3, quadbin, and custom; a BNG String
        id (e.g. ``"TQ38"``) for bng.

    Covering and centroid+complete keep a covered cell only if it overlaps the
    raster footprint of :func:`_footprint_polygon` (heavy's reprojected-corner
    quadrilateral). CF3 guard: when ``ds.crs is None`` the raster corners are used
    directly as grid-native coordinates.
    """
    if grid not in _VALID_GRIDS:
        raise ValueError(
            f"iter_tessellate: grid must be one of {sorted(_VALID_GRIDS)}; "
            f"got {grid!r}"
        )
    if mode not in _VALID_MODES:
        raise ValueError(
            f"rst_{grid}_tessellate: mode must be one of covering, centroid; "
            f"got '{mode}'"
        )
    # Apply mode-appropriate legacy default when coverage is not specified.
    if coverage is None:
        coverage = "complete" if mode == "covering" else "sparse"
    if coverage not in _VALID_COVERAGES:
        raise ValueError(
            f"rst_{grid}_tessellate: coverage must be one of sparse, complete; "
            f"got '{coverage}'"
        )

    resolution = _resolve_resolution(resolution, grid)

    if mode == "centroid":
        if coverage == "sparse":
            # Default centroid behaviour: only cells with valid pixels are emitted.
            yield from _centroid_chips_generic(ds, resolution, grid, conf=conf)
        else:
            yield from _centroid_complete(ds, resolution, grid, conf=conf)
        return

    # ---- covering mode -------------------------------------------------------
    # BNG: warp the dataset to EPSG:27700 and operate in that CRS throughout.
    # H3 / quadbin: use the original dataset; footprint/polyfill/keep-test live in
    # WGS84; cell polys are reprojected to ds.crs only for the rasterio clip.
    # Custom: use the original dataset in its grid-native CRS.
    _ctx = _as_bng_dataset(ds) if grid == "bng" else nullcontext(ds)

    with _ctx as work_ds:
        footprint = _footprint_polygon(ds, work_ds, grid)

        covered = _polyfill_cells(footprint, resolution, grid, conf=conf)

        dst_epsg = authority_srid_of(work_ds.crs)
        # Custom and BNG never need cell-polygon reprojection (both are grid-native).
        # For h3/quadbin, reproject when the raster HAS a CRS and it isn't WGS84.
        # Gate on CRS PRESENCE (work_ds.crs is not None), not EPSG-authority presence:
        # authority_srid_of returns None for authority-less CRSes (WKT/PROJ4-only)
        # which still need reprojection. CF3: crs=None → work_ds.crs is None → False → skip.
        need_reproject = (
            grid not in ("bng", "custom")
            and work_ds.crs is not None
            and dst_epsg != 4326
        )

        for cell in covered:
            if not _cell_is_valid(cell, grid, conf=conf):
                continue
            cell_poly = _cell_geom(cell, grid, conf=conf)
            # Positive-area covering keep-test: drop edge-only-touching cells
            # (zero pixel overlap on grid-aligned tiles); keep real areal overlap
            # including all-NoData-but-overlapping cells.  The clip below remains
            # the pixel-level safety net.
            if not _has_positive_area_overlap(cell_poly, footprint):
                continue
            if need_reproject:
                geom = transform_geom(_WGS84, work_ds.crs, mapping(cell_poly))
                cell_poly = shape(geom)
            try:
                # all_touched=True: boundary pixels touched by the cell edge are
                # included in the chip, consistent with the covering selection.
                clipped = edit.clip_to_geom(work_ds, cell_poly, all_touched=True)
            except ValueError:
                # rasterio.mask raises ValueError when the shape does not overlap.
                continue
            # clip_to_geom returns None only on true geometric non-overlap (the
            # rasterio "Input shapes do not overlap raster" case).  A cell that
            # overlaps the bbox but clips to entirely NoData is still emitted in
            # complete mode; sparse mode drops it.
            if clipped is None:
                continue
            if coverage == "sparse" and _chip_is_all_nodata(clipped):
                continue
            yield (_encode_cellid(cell, grid, conf=conf), clipped)


# ---------------------------------------------------------------------------
# Per-grid thin delegates (kept for backward compat with bench + existing tests)
# ---------------------------------------------------------------------------


def iter_tessellate_h3(ds, resolution: int, mode: str = "covering"):
    """Streaming H3 tessellate — delegates to :func:`iter_tessellate`.

    Kept for backward compatibility with bench callers and existing tests.

    Yields ``(cellid_int, gtiff_bytes)`` one cell at a time — never buffers the
    full cell list (large-fan-out OOM guard).

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: H3 resolution in ``[0, 15]``.
        mode:       ``"covering"`` (default) or ``"centroid"``.

    Yields:
        ``(cellid, raster_bytes)`` tuples.  ``cellid`` is the signed int64 H3
        cell id.
    """
    yield from iter_tessellate(ds, resolution, "h3", mode)


def tessellate_h3(ds, resolution: int) -> list:
    """Tessellate a raster into H3 cells; return ``[(cellid_int, gtiff_bytes)]``.

    List-materializing wrapper around :func:`iter_tessellate_h3` (kept for the
    Spark-free core API and bench/parity callers).
    """
    return list(iter_tessellate_h3(ds, resolution))


def iter_tessellate_quadbin(ds, resolution: int, mode: str = "covering"):
    """Streaming quadbin tessellate — delegates to :func:`iter_tessellate`.

    Kept for backward compatibility with bench callers and existing tests.

    Yields ``(cellid_int, gtiff_bytes)`` one cell at a time.

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: Quadbin resolution in ``[0, 26]`` (polyfill limited to
                    ``[0, 20]``; see :data:`QUADBIN_MAX_RES`).
        mode:       ``"covering"`` (default) or ``"centroid"``.

    Yields:
        ``(cellid, raster_bytes)`` tuples.  ``cellid`` is a signed int64
        quadbin cell id.
    """
    yield from iter_tessellate(ds, resolution, "quadbin", mode)


def iter_tessellate_bng(ds, resolution, mode: str = "covering"):
    """Streaming BNG tessellate — delegates to :func:`iter_tessellate`.

    Kept for backward compatibility with bench callers and existing tests.

    The raster is reprojected to EPSG:27700 first (skipped if already 27700).
    Out-of-GB cells are dropped.  Enumeration is BOUNDARY-COMPLETE (bbox is
    buffered before polyfill so boundary cells are not dropped by the centroid
    flood-fill).

    Args:
        ds:         Open rasterio ``DatasetReader`` (any CRS; warped to 27700).
        resolution: BNG resolution — an Int index (±1..±6) or a resolutionMap
                    string key (e.g. ``"1km"``, ``"100m"``).
        mode:       ``"covering"`` (default) or ``"centroid"``.

    Yields:
        ``(cellid, raster_bytes)`` tuples.  ``cellid`` is a BNG String id
        (e.g. ``"TQ38"``).
    """
    yield from iter_tessellate(ds, resolution, "bng", mode)
