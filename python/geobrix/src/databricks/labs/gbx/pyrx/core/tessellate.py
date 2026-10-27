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
from contextlib import contextmanager, nullcontext

import h3
import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.warp import transform_geom
from rasterio.windows import Window
from shapely.geometry import Polygon, mapping, shape
from shapely.geometry.polygon import orient

from databricks.labs.gbx.core.crs import (
    authority_srid_of,
    crs_to_canonical,
    get_transformer,
)
from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx.core import edit, warp
from databricks.labs.gbx.pyrx.core._nodata import fallback_nodata, has_alpha, valid_mask
from databricks.labs.gbx.pyrx.core.compression import write_gtiff
from databricks.labs.gbx.pyrx.core.gridagg import _bng_cells, _h3_cells

H3_MAX_RES = 15
QUADBIN_MAX_RES = _quadbin._MAX_POLYFILL_RES

_WGS84 = "EPSG:4326"
_BNG_EPSG = 27700
_VALID_MODES = {"covering", "centroid"}
_VALID_COVERAGES = {"sparse", "complete"}
_VALID_GRIDS = frozenset({"h3", "quadbin", "bng", "custom"})
# Points per raster edge, corners included, walked when reprojecting the footprint (rasterio transform_bounds default).
_FOOTPRINT_EDGE_POINTS = 21
# Pixels per centroid-assignment strip (1 M): its coordinate/cell temporaries, ~80 B/px
# with h3, stay ~80 MB per strip, so assignment fits a Serverless task.
_STRIP_PIXELS = 1_048_576
# Initial number of per-label pixel windows; doubled as cells are found.
_LABEL_CAPACITY = 1024


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

    A source that declares no NoData is warped with the dtype fallback as its
    NoData, so the pixels the warp fills outside the source footprint are NoData
    rather than valid zeros that would be tessellated into the edge cells.
    """
    src_epsg = authority_srid_of(ds.crs)
    if src_epsg == _BNG_EPSG:
        yield ds
        return
    warped_bytes = warp.reproject_to_srid(
        ds, _BNG_EPSG, resampling="nearest", mark_fill_nodata=True
    )
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
    the raster footprint polygon (:func:`_footprint_polygon`): h3 and quadbin
    enumerate its bounding box, bng and custom (whose footprint is an
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

    The footprint is the raster's pixel-grid rectangle ``(c + x*a, f + y*e)`` for
    ``x`` in ``0..W`` and ``y`` in ``0..H``; the rotation terms of the geotransform
    (``b``/``d``, gt2/gt4) are ignored, exactly as heavy ``BoundingBox.windowBBox``
    ignores them.

    When the corners must be reprojected (h3/quadbin on a raster whose CRS is not
    EPSG:4326) each edge is walked with :data:`_FOOTPRINT_EDGE_POINTS` points,
    corners included, before the transform, so the ring follows the curved image of
    the edge in lon/lat. A 4-corner chord would cut inside that curve and lose real
    edge pixels; the lon/lat box of ``transform_bounds`` would add cells outside the
    raster. With no transform (EPSG:4326, CRS-less, bng, custom) the edge is
    straight, so the four corners are the footprint.

    Work CRS per grid: h3/quadbin WGS84; bng the EPSG:27700 *work_ds*; custom the
    grid-native *ds*. The result is oriented counter-clockwise, as heavy's is.

    Raises ``ValueError`` when the reprojected ring cannot be a lon/lat polygon: a
    corner does not transform, or the footprint crosses the antimeridian or encloses
    a pole (longitudes wrap, so the ring jumps by more than 180 degrees or is
    invalid). Cell enumeration and overlap tests on such a ring are meaningless, so
    the raster must be reprojected or split first.
    """
    src = work_ds if grid == "bng" else ds
    t = src.transform
    width, height = src.width, src.height
    reproject = (
        grid in ("h3", "quadbin")
        and ds.crs is not None
        and authority_srid_of(ds.crs) != 4326
    )
    # Walk the four edges corner to corner; each edge's last point is the next edge's first.
    n = _FOOTPRINT_EDGE_POINTS - 1 if reproject else 1
    frac = np.arange(n) / n
    cols = np.concatenate(
        [frac * width, np.full(n, width), (1 - frac) * width, np.zeros(n)]
    )
    rows = np.concatenate(
        [np.zeros(n), frac * height, np.full(n, height), (1 - frac) * height]
    )
    xs = t.c + cols * t.a
    ys = t.f + rows * t.e
    if not reproject:
        return orient(Polygon(zip(xs, ys)), 1.0)
    xs, ys = get_transformer(ds.crs, _WGS84).transform(xs, ys)
    if not (np.isfinite(xs).all() and np.isfinite(ys).all()):
        raise ValueError(
            f"rst_{grid}_tessellate: the raster corners cannot be transformed "
            f"from {crs_to_canonical(ds.crs)} to EPSG:4326"
        )
    ring = Polygon(zip(xs, ys))
    wraps = (np.abs(np.diff(np.append(xs, xs[0]))) > 180).any()
    if wraps or not ring.is_valid:
        raise ValueError(
            f"rst_{grid}_tessellate: the raster footprint crosses the antimeridian "
            f"or encloses a pole in {crs_to_canonical(ds.crs)}; reproject it "
            "(e.g. to EPSG:4326) or split it before tessellating"
        )
    return orient(ring, 1.0)


def _chip_is_all_nodata(raster_bytes: bytes) -> bool:
    """Return True when no pixel of the GTiff chip is valid.

    Used by covering+sparse to drop chips whose pixels are entirely masked.
    Validity is :func:`valid_mask`: a pixel is valid when some data band is valid (mask
    bands, so a NaN NoData is recognised, where ``== nan`` never is) and, for a chip with an
    alpha band, its alpha is above 0. A chip with no declared nodata and no alpha band has
    all-valid masks and is kept.
    """
    with MemoryFile(raster_bytes) as mf:
        with mf.open() as chip_ds:
            return not valid_mask(chip_ds).any()


def _chip_nodata(work_ds):
    """NoData every chip carries: the source's, else the dtype fallback, else none.

    A source with an alpha band and no NoData gets none: the chip keeps the alpha band,
    which marks the transparent pixels and the pixels outside the cell (written as 0, so
    alpha 0), and a NoData declared over it would shadow the alpha band and turn those
    pixels valid.
    """
    if work_ds.nodata is not None:
        return work_ds.nodata
    if has_alpha(work_ds):
        return None
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
    chip = np.full(
        (work_ds.count, 1, 1), 0 if nd is None else nd, dtype=work_ds.dtypes[0]
    )

    def build(cell) -> bytes:
        centroid = _cell_geom(cell, grid, conf=conf).centroid
        x, y = centroid.x, centroid.y
        if to_work is not None:
            x, y = to_work(x, y)
        col_f, row_f = inv * (x, y)
        col = min(max(math.floor(col_f), 0), last_col)
        row = min(max(math.floor(row_f), 0), last_row)
        window_tf = work_ds.window_transform(Window(col, row, 1, 1))
        return write_gtiff(
            dict(profile, transform=window_tf), chip, alpha_like=work_ds.colorinterp
        )

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


def _pixel_cells(xs, ys, resolution, grid: str, conf=None) -> np.ndarray:
    """Integer cell key of each pixel centre ``(xs[i], ys[i])`` in the grid's work CRS.

    h3: the uint64 H3 index; quadbin: the cell as int64 (``point_as_cell_vec``, which
    is bit-identical to the scalar ``point_as_cell`` heavy matches, and is also the
    rule ``gridagg.raster_to_grid`` uses); bng: the Long id (validity is checked per
    unique cell by :func:`_raw_cell`); custom: the id, or ``-1`` where the centre
    falls outside the grid. h3 and custom encode one point at a time (no array API).
    """
    if grid == "h3":
        return _h3_cells(xs, ys, resolution)
    if grid == "quadbin":
        return _quadbin.point_as_cell_vec(xs, ys, resolution)
    if grid == "bng":
        return _bng_cells(xs, ys, resolution)
    from databricks.labs.gbx.pygx import _custom as _custom_mod

    to_cell = _custom_mod.point_to_cell_id_or_none
    return np.fromiter(
        (
            (
                -1
                if (cell := to_cell(conf, float(x), float(y), resolution)) is None
                else cell
            )
            for x, y in zip(xs, ys)  # vectorscan: ok (no array API for custom)
        ),
        dtype=np.int64,
        count=len(xs),
    )


def _raw_cell(key: int, grid: str):
    """The raw cell of a :func:`_pixel_cells` key, as the grid helpers take it.

    ``None`` drops the key's pixels: a bng cell outside the grid (``_bng.is_valid``)
    or a pixel outside a custom grid's bounds.
    """
    if grid == "h3":
        return h3.int_to_str(key)
    if grid == "bng":
        return key if _bng.is_valid(key) else None
    if grid == "custom":
        return None if key < 0 else key
    return key  # quadbin


def _grow_windows(windows: np.ndarray, n: int) -> np.ndarray:
    """Widen the ``(4, capacity)`` per-label window array to hold label ``n - 1``.

    Rows are ``r0, r1, c0, c1``. The capacity doubles (from at least
    :data:`_LABEL_CAPACITY`); new labels start empty (``r0``/``c0`` at the int32
    maximum, ``r1``/``c1`` at -1) so min/max can fold in.
    """
    cap = max(windows.shape[1], _LABEL_CAPACITY)
    while cap < n:
        cap *= 2
    out = np.empty((4, cap), dtype=np.int32)
    out[:, : windows.shape[1]] = windows
    fresh = out[:, windows.shape[1] :]
    fresh[0::2] = np.iinfo(np.int32).max
    fresh[1::2] = -1
    return out


def _strip_rows(work_ds) -> int:
    """Rows per centroid-assignment strip: at most :data:`_STRIP_PIXELS` pixels.

    One row if a single row is wider than that. The cap holds for any width and
    for a source whose block is the whole image (a single-strip GTiff). When the
    source blocks are shorter than the cap, the strip is a whole number of blocks
    (at least one), so a block is decoded for one strip only.
    """
    rows = max(1, min(work_ds.height, _STRIP_PIXELS // work_ds.width))
    block_rows = work_ds.block_shapes[0][0]
    if block_rows < rows:
        rows -= rows % block_rows
    return rows


def _assign_pixels(work_ds, resolution, grid: str, conf=None):
    """Assign every valid pixel to the cell holding its centre, one row strip at a time.

    Returns ``(labels, windows, order)``: an int32 ``(height, width)`` label raster
    (``-1`` = unassigned), the ``(4, n)`` int32 ``r0, r1, c0, c1`` pixel window of each
    label, and the raw cell of each label. Labels are numbered in first-seen pixel
    order (row-major), so chips come out in that order.

    A pixel is valid by :func:`valid_mask` (heavy ``ValidMask``): some data band's mask
    marks it valid (heavy anyValid; the mask bands recognise a NaN NoData) and, when the
    source has an alpha band, its alpha is not 0, so a transparent pixel belongs to no
    cell. Pixel centres go through the full geotransform;
    h3/quadbin move them to EPSG:4326 when the raster has a CRS that is not 4326
    (gated on CRS presence: an authority-less CRS still has ``authority_srid_of``
    None and needs it; a CRS-less raster is grid-native).

    Memory is the label raster (4 B/px) plus the coordinate and cell arrays of one
    strip of at most :data:`_STRIP_PIXELS` pixels (:func:`_strip_rows`); no
    per-pixel Python except the scalar h3/custom encoders.
    """
    height, width = work_ds.height, work_ds.width
    strip_rows = _strip_rows(work_ds)
    to_lonlat = None
    if (
        grid in ("h3", "quadbin")
        and work_ds.crs is not None
        and authority_srid_of(work_ds.crs) != 4326
    ):
        to_lonlat = get_transformer(work_ds.crs, 4326).transform

    labels = np.full((height, width), -1, dtype=np.int32)
    windows = _grow_windows(np.empty((4, 0), dtype=np.int32), 0)
    order = []  # raw cell of each label
    index = {}  # cell key -> label (-1: dropped)
    for row0 in range(0, height, strip_rows):  # vectorscan: ok (per row strip)
        nrows = min(strip_rows, height - row0)
        valid = valid_mask(work_ds, window=Window(0, row0, width, nrows))
        rr, cc = np.nonzero(valid)
        del valid
        if rr.size == 0:
            continue
        rr = (rr + row0).astype(np.int32)
        cc = cc.astype(np.int32)
        xs, ys = work_ds.transform * (cc + 0.5, rr + 0.5)
        if to_lonlat is not None:
            xs, ys = to_lonlat(xs, ys)
        cells = _pixel_cells(xs, ys, resolution, grid, conf=conf)
        del xs, ys
        uniq, first, inv = np.unique(cells, return_index=True, return_inverse=True)
        del cells
        # Visit this strip's cells in first-seen order; a new cell takes the next label.
        cell_label = np.empty(uniq.size, dtype=np.int32)
        for j in np.argsort(first):
            key = int(uniq[j])
            label = index.get(key)
            if label is None:
                raw = _raw_cell(key, grid)
                label = -1 if raw is None else len(order)
                if raw is not None:
                    order.append(raw)
                index[key] = label
            cell_label[j] = label
        lab = cell_label[inv]
        del inv
        keep = lab >= 0
        if not keep.all():
            rr, cc, lab = rr[keep], cc[keep], lab[keep]
        labels[rr, cc] = lab
        if len(order) > windows.shape[1]:
            windows = _grow_windows(windows, len(order))
        np.minimum.at(windows[0], lab, rr)
        np.maximum.at(windows[1], lab, rr)
        np.minimum.at(windows[2], lab, cc)
        np.maximum.at(windows[3], lab, cc)
    return labels, windows[:, : len(order)], order


def _centroid_chips_inner(work_ds, resolution, grid: str, conf=None):
    """Inner centroid-partition loop for an already-prepared dataset.

    For h3/quadbin ``work_ds`` is the original dataset (pixel coords are
    reprojected to WGS84 if needed; CRS-less -> use coords directly).
    For BNG ``work_ds`` is already in EPSG:27700 and pixel coords are used
    directly as eastings/northings.  For custom the coords are grid-native.

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.

    Yields ``(cellid, gtiff_bytes)`` pairs in first-seen pixel order (row-major).
    Each chip is cropped to the bounding pixel window of its cell's pixels, read
    from ``work_ds`` by that window (no second whole-raster copy is made), and
    carries the source NoData (else the dtype fallback, ``fallback_nodata``; a source with
    an alpha band keeps it in the chips and declares none, see :func:`_chip_nodata`).
    Assignment is :func:`_assign_pixels`.
    """
    labels, windows, order = _assign_pixels(work_ds, resolution, grid, conf=conf)
    nd = _chip_nodata(work_ds)
    profile = work_ds.profile.copy()
    profile["nodata"] = nd
    blank = 0 if nd is None else nd  # pixels of the window assigned to other cells

    # Each chip covers only the bounding pixel window of its cell's pixels, on the
    # source grid (window_transform keeps rotated and south-up geotransforms intact);
    # pixels of that window assigned to other cells are NoData.
    for label, cell in enumerate(order):
        r0, r1, c0, c1 = (int(v) for v in windows[:, label])
        h, w = r1 + 1 - r0, c1 + 1 - c0
        win = Window(c0, r0, w, h)
        data = work_ds.read(window=win)
        chip = np.full(data.shape, blank, dtype=data.dtype)
        np.copyto(chip, data, where=(labels[r0 : r1 + 1, c0 : c1 + 1] == label)[None])
        profile.update(width=w, height=h, transform=work_ds.window_transform(win))
        yield (
            _encode_cellid(cell, grid, conf=conf),
            write_gtiff(profile, chip, alpha_like=work_ds.colorinterp),
        )


def _work_dataset(ds, grid: str):
    """Context yielding the dataset a grid works on: bng's EPSG:27700 copy, else ``ds``."""
    return _as_bng_dataset(ds) if grid == "bng" else nullcontext(ds)


def _centroid_chips_generic(work_ds, resolution, grid: str, conf=None):
    """Consolidated centroid-partition dispatcher for all grids.

    ``work_ds`` is already prepared by :func:`_work_dataset`: for BNG the EPSG:27700
    dataset of :func:`_as_bng_dataset`, so a caller that also needs it afterwards
    (centroid+complete) warps once. H3 and quadbin use the original dataset,
    reprojecting pixel coords to WGS84 inside :func:`_centroid_chips_inner` if
    needed (CRS-less -> grid-native). Custom uses the original dataset directly
    (grid-native coordinates).

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.

    Yields ``(cellid, gtiff_bytes)`` pairs.
    """
    yield from _centroid_chips_inner(work_ds, resolution, grid, conf=conf)


def _centroid_complete(ds, resolution, grid: str, conf=None):
    """centroid + complete coverage: yield centroid chips, then synthetic empty
    chips for covered cells that received no centroid pixels (each a 1×1 chip on
    the cell's centroid pixel; see :func:`_build_empty_chip`).

    Extracted from :func:`iter_tessellate` to stay within C901 complexity limit.

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.

    Covered cells are kept against the raster footprint of
    :func:`_footprint_polygon` (the reprojected, densified raster edge), not
    the axis-aligned lon/lat box of the raster. CF3 guard: a CRS-less raster
    (``ds.crs is None``) uses its corners as grid-native coordinates. BNG warps
    the raster once, for both the chips and the covered cells.
    """
    with _work_dataset(ds, grid) as work_ds:
        # An unusable footprint must fail before any chip is produced.
        footprint = _footprint_polygon(ds, work_ds, grid)
        centroid_ids = set()
        for cellid, raster_bytes in _centroid_chips_generic(
            work_ds, resolution, grid, conf=conf
        ):
            centroid_ids.add(cellid)
            yield (cellid, raster_bytes)
        # Compute covering set to find covered-but-empty cells.
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
    raster footprint of :func:`_footprint_polygon` (the reprojected, densified
    raster edge). CF3 guard: when ``ds.crs is None`` the raster corners are used
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
            with _work_dataset(ds, grid) as work_ds:
                yield from _centroid_chips_generic(work_ds, resolution, grid, conf=conf)
        else:
            yield from _centroid_complete(ds, resolution, grid, conf=conf)
        return

    # ---- covering mode -------------------------------------------------------
    # BNG: warp the dataset to EPSG:27700 and operate in that CRS throughout.
    # H3 / quadbin: use the original dataset; footprint/polyfill/keep-test live in
    # WGS84; cell polys are reprojected to ds.crs only for the rasterio clip.
    # Custom: use the original dataset in its grid-native CRS.
    with _work_dataset(ds, grid) as work_ds:
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
        # A source without NoData gets the dtype fallback, so the clip fill outside
        # each cell reads as NoData rather than as valid zero pixels; with an alpha band it
        # gets none, and the clip's 0 fill is transparent (see _chip_nodata).
        fill = _chip_nodata(work_ds)

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
                clipped = edit.clip_to_geom(
                    work_ds, cell_poly, all_touched=True, nodata=fill
                )
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
