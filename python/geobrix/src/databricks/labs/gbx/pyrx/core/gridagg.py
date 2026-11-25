"""Spark-free raster->discrete-global-grid aggregation (H3 + quadbin + BNG).

* Per band, every valid pixel is mapped to a grid cell id by
  the pixel-centroid world coordinate (0.5-pixel offset through the
  geotransform). H3/quadbin cells are in EPSG:4326 lon/lat, BNG cells in EPSG:27700;
  a raster with no CRS (and no ``crs`` override) is taken as in the grid's CRS
  already, and pixels outside GB are dropped via ``pygx._bng.is_valid``.
* A raster in another CRS is gridded on its own pixels, in its own CRS (the native
  frame, ``_Frame`` / ``_grid_native``): ``centroid`` moves each pixel centre to the
  grid's CRS; ``covering`` moves each cell into the raster's CRS (``_native_rings``,
  each edge walked to within ``_NATIVE_EDGE_TOLERANCE``) and shares the native pixels
  by overlap area, so ``count`` is in source pixels. Only a raster the frame cannot
  hold (a cell across its CRS's own seam, or a point that does not transform) is
  warped to the grid's CRS (nearest) first, as all were before.
  A pixel is valid for a band when the band's own mask says so (NoData, the raster's
  own mask band) and, for a raster with an alpha band, its alpha is above 0
  (``_nodata.band_valid_mask``): every band is judged alone, and a transparent pixel
  counts for none of them, however many bands the raster has.
* Pixel values are accumulated per cell, then reduced by the chosen aggregate.
* BNG cell math is the single source of truth in ``pygx._bng`` (no numpy codec);
  H3/quadbin ids are ``int`` (Long) but BNG ids are rendered to ``str`` via
  ``pygx._bng.format`` at the output boundary.

Two parameters shape the result:

* ``coverage``: ``"sparse"`` (emit only cells with at least one valid pixel) or
  ``"complete"`` (default, matches heavy) -- also emit covering-but-empty cells
  carrying ``None`` (or ``0.0`` for ``count``).
* ``assignment``: ``"centroid"`` (default) -- bin each valid pixel to the cell
  containing its centroid (the original behaviour); ``"covering"`` -- distribute
  each pixel's value across every cell that overlaps its rectangular footprint,
  weighted by the intersection-area fraction. Weighted reducers mirror the heavy
  tier exactly (see ``_weighted_reduce``).

count is now Double/float in all modes (centroid count = n as float; covering
count = Sigma-w; covered-but-empty count = 0.0).

Backed by the best-in-class lib ``h3`` (v4); quadbin and BNG cell math is the
shared ``pygx`` core.

Hot path (centroid) is vectorized: valid pixels are gathered once, mapped to
cell ids, and grouped/reduced with numpy. The quadbin encoder is
``pygx._quadbin.point_as_cell_vec`` -- the same floor-and-clamp point->cell rule
as heavy ``Quadbin.pointToCellID`` and light tessellate, so a pixel centre at
longitude 180 or on a tile edge lands in the same cell in every function and
tier. The H3 encoder stays a scalar comprehension over valid pixels because
``h3.latlng_to_cell`` is C-backed and exposes no array API.

The covering path iterates per pixel for quadbin, BNG and custom grids (with an interior fast path, see
``_COVERING_FAST_PATH_EXACT``) and for H3 cells larger than the pixels. For H3 cells smaller than the pixels it is
cell-major and vectorised (``_covering_band_h3_vectorised``): each fine cell overlaps only a few pixels, and
``_box_overlap_area`` computes every (cell, pixel) overlap area of a chunk in numpy, exactly (each pixel is an
axis-aligned box in lon/lat). Both give the same cells, and states within 1e-9 relative or 1e-10 absolute: the loop's
GEOS rounds intersection points at the longitude's scale (a weight off by up to ~1e-11 of a pixel), which shows
beyond 1e-9 relative only in a cell that a sliver of overlap dominates. The vectorised pass keeps a (cell, pixel)
pair only when its weight is above ``_COVER_WEIGHT_FLOOR`` (1e-14 of a pixel). A pair with no overlap beyond the
concave part of a distorted cell (an H3 cell across an icosahedron face edge) integrates to rounding noise, at most
~1e-15 of a pixel (measured over 40 res-3 distorted cells); kept, it would add a cell, or pull a neighbouring pixel's
value into a cell's min / max (both weight-agnostic). The floor sits 10x above that noise and below the smallest real
overlaps seen on HLS tiles (2.8e-13 of a pixel); GEOS on lon/lat cannot resolve slivers below ~1e-11 of a pixel.
"""

import math

import numpy as np

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pyrx.core._nodata import band_valid_mask

H3_MAX_RES = 15
QUADBIN_MAX_RES = 20

# Grids whose point→cell partition exactly equals the cell geometry (analytic
# square / rectangular cells).  For these grids, a pixel whose four corners all
# encode to the SAME cell is unambiguously interior: its area fraction is exactly
# 1.0 and we can skip shapely polygon construction + polyfill + intersection.
# H3 is deliberately excluded: its geoToH3 Voronoi partition diverges from its
# chord-polygon geometry by up to ~1e-4 fractional area (measured at res-3), far
# above the 1e-9 result-neutrality bar.  Any grid NOT listed here falls back to
# the existing polyfill+intersection path (safe default for future grids).
_COVERING_FAST_PATH_EXACT = frozenset({"custom", "quadbin", "bng"})
# The vectorised H3 covering path: read at call time, so tests can set it False to force the per-pixel loop.
_H3_COVERING_VECTORISED = True
# Cells per vectorised chunk: bounds the (cell, pixel) pair arrays to a few hundred thousand entries.
_H3_VEC_CHUNK_CELLS = 20000
# The vectorised H3 covering pass keeps a (cell, pixel) pair only when its weight (overlap / pixel area) is above this:
# beyond a reflex part of a distorted cell a pair with no overlap integrates to rounding noise up to ~1e-15 of a pixel
# (see _box_overlap_area and the module docstring). A real overlap this small carries no measurable mass.
# The noise bound holds where the pass runs (cells no larger than the pixel, the _use_h3_vectorised gate): the noise
# grows with the cell / pixel size ratio, so a caller with pixels much finer than the cell must not reuse this floor.
_COVER_WEIGHT_FLOOR = 1e-14

_AGGS = ("avg", "count", "min", "max", "median", "sum", "variance", "stddev")
# The statistics that split: each is a plain function of one cell's partial state (see ``measure_of``), so ONE pass over the
# pixels gives all seven. The median does not split (it needs the values), and keeps its own per-cell pass.
PARTIAL_STATS = tuple(a for a in _AGGS if a != "median")
_PARTIAL = "partial"  # the internal agg word: the reducers return the partial state tuple instead of one measure
# The partial state of one cell, in this order: the number of valid pixels (the sum of the weights when pixels are
# shared out), the sum of the values, M2 (the sum of the squared deviations from the cell's mean), the minimum, the maximum.
PARTIAL_FIELDS = ("count", "sum", "m2", "min", "max")
_EMPTY_STATE = (0.0, None, None, None, None)  # a covered cell that holds no valid pixel
_COVERAGES = ("sparse", "complete")
_ASSIGNMENTS = ("centroid", "covering")


def _validate_resolution(resolution: int, grid: str) -> None:
    if grid == "h3":
        if resolution < 0 or resolution > H3_MAX_RES:
            raise ValueError(
                f"H3 resolution has to be between 0 and {H3_MAX_RES}; "
                f"found {resolution}"
            )
    elif grid == "quadbin":
        if resolution < 0 or resolution > QUADBIN_MAX_RES:
            raise ValueError(
                f"raster->quadbin: resolution must be in [0, {QUADBIN_MAX_RES}]; "
                f"got {resolution}"
            )
    elif grid == "bng":
        # Delegated to pygx._bng.get_resolution: only integer indices +/-1..+/-6
        # (or resolutionMap string keys, normalized upstream) are valid; raises
        # on metres-as-Int and out-of-range indices.
        _bng.get_resolution(resolution)
    elif grid == "custom":
        # Resolution validated against conf.max_resolution in _raster_to_custom
        # after the conf is decoded from the grid struct.
        pass
    else:
        raise ValueError(
            f"unknown grid {grid!r}; expected 'h3', 'quadbin', 'bng' or 'custom'"
        )


def _h3_cells(lon: np.ndarray, lat: np.ndarray, resolution: int) -> np.ndarray:
    """Per-valid-pixel H3 cell ids (uint64). Scalar lib call; no array API."""
    import h3  # lazy: stats_partial and the BNG paths run without h3 installed

    cells = [
        h3.str_to_int(h3.latlng_to_cell(float(la), float(lo), resolution))
        for lo, la in zip(lon, lat)  # vectorscan: ok (h3 no array API)
    ]
    return np.array(cells, dtype="uint64")


def _bng_cells(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray:
    """Per-valid-pixel BNG cell ids (Long int64), fully vectorized.

    ``e``/``n`` are EPSG:27700 eastings/northings (pixel centroids, of a 27700 raster or
    moved there from the raster's own CRS). Delegates to ``pygx._bng.point_to_cell_id_vec`` -- the SAME shared
    numpy core the scalar ``point_to_cell_id`` wraps, so cell ids are identical.
    """
    return _bng.point_to_cell_id_vec(e, n, resolution)


def _grouped_measures(
    cids: np.ndarray, vals: np.ndarray, agg: str, weights: np.ndarray = None
):
    """Group ``vals`` by ``cids`` and reduce -> (unique_cids, measures).

    Loops at most over CELLS (median only), never over pixels. ``count`` yields
    Python floats (changed from int for heavy-tier parity: centroid count = n as
    float; covering count = Sigma-w); every other agg yields Python floats --
    matching the original per-cell ``_reduce`` types and values exactly.

    ``weights`` (one per value, optional) are covering weights (intersection area / pixel area): the count is then
    Sigma w, the sum Sigma v*w, the mean and M2 weighted, min / max weight-agnostic, and the median the weighted median
    of ``_weighted_reduce`` -- ``_weighted_reduce``'s formulas with numpy group sums. None means every weight is 1
    (the centroid path), and takes the same operations as before the argument existed.
    """
    uniq, inv = np.unique(cids, return_inverse=True)
    inv = inv.astype(np.intp)
    n = uniq.size
    w = None if weights is None else np.asarray(weights, dtype="float64")
    if w is None:
        counts = np.bincount(inv, minlength=n)
    else:
        counts = np.bincount(inv, weights=w, minlength=n)

    def _gsum(x):
        """Per-cell sum of ``x``, each term times its weight when there are weights."""
        return np.bincount(inv, weights=x if w is None else x * w, minlength=n)

    if agg == _PARTIAL:
        vals = np.asarray(vals, dtype="float64")
        sums = _gsum(vals)
        # the two-pass M2 of the variance below: the mean first, then the squared deviations from it
        m2 = _gsum((vals - (sums / counts)[inv]) ** 2)
        lo = np.full(n, np.inf)
        np.minimum.at(lo, inv, vals)
        hi = np.full(n, -np.inf)
        np.maximum.at(hi, inv, vals)
        cols = (counts, sums, m2, lo, hi)
        return uniq, [
            (float(c), float(s), float(m), float(a), float(b))
            for c, s, m, a, b in zip(*cols)  # vectorscan: ok (per-cell)
        ]

    if agg == "count":
        measures = [float(c) for c in counts]  # float for heavy-tier parity
        return uniq, measures

    vals = np.asarray(vals, dtype="float64")
    if agg == "avg":
        out = _gsum(vals) / counts
    elif agg == "sum":
        # Total of valid pixel values per cell -- the avg numerator (bincount
        # weighted sum) WITHOUT dividing by counts. A cell is only emitted with
        # >=1 valid pixel (sec 2.6), so sum is always well-defined.
        out = _gsum(vals)
    elif agg in ("variance", "stddev"):
        # Population (ddof=0), TWO-PASS (numerically stable; NOT E[x^2]-E[x]^2):
        # pass 1 per-cell mean; pass 2 mean of squared deviations. A 1-pixel
        # cell gives variance 0 (clean). stddev = sqrt(variance) so it tracks
        # variance's cross-tier parity exactly.
        means = _gsum(vals) / counts
        var = _gsum((vals - means[inv]) ** 2) / counts
        out = var if agg == "variance" else np.sqrt(var)
    elif agg == "min":
        out = np.full(n, np.inf)
        np.minimum.at(out, inv, vals)
    elif agg == "max":
        out = np.full(n, -np.inf)
        np.maximum.at(out, inv, vals)
    elif agg == "median":
        order = np.argsort(inv, kind="stable")
        bounds = np.cumsum(np.bincount(inv, minlength=n))[:-1]
        segments = np.split(vals[order], bounds)
        if w is None:
            out = np.array([np.median(seg) for seg in segments], dtype="float64")
        else:
            ws = np.split(w[order], bounds)
            out = np.array(
                [
                    _weighted_reduce(list(zip(v.tolist(), x.tolist())), "median")
                    for v, x in zip(segments, ws)  # vectorscan: ok (per-cell)
                ],
                dtype="float64",
            )
    else:
        raise ValueError(f"unknown agg {agg!r}; expected one of {_AGGS}")

    return uniq, [float(m) for m in out]


def _weighted_reduce(pairs: list, agg: str) -> float:
    """Covering weighted reducer: mirrors the heavy tier's ``fAggW`` exactly.

    ``pairs`` is a list of ``(value, weight)`` tuples where ``weight`` is the
    intersection-area fraction of the pixel that falls in the cell
    (``inter.area / pixel.area``).  All reducers assume ``len(pairs) >= 1``
    (a cell with no pixels is never passed here -- it is materialised as
    ``emptyValue`` upstream).

    Heavy-tier equivalents (confirmed from Scala sources):
      sum:      Sigma(v*w)
      count:    Sigma(w)   -- fractional, mass-conserving
      avg:      Sigma(v*w) / Sigma(w)
      min/max:  min/max(v)  -- weight-agnostic
      variance: Sigma(w*(v-mean)^2)/Sigma(w)  mean=Sigma(v*w)/Sigma(w)
      stddev:   sqrt(variance)
      median:   cumulative-weight interpolated weighted median
    """
    sw = sum(p[1] for p in pairs)
    if agg == _PARTIAL:
        sv = sum(v * w for v, w in pairs)
        mean = sv / sw
        return (
            float(sw),
            float(sv),
            float(sum(w * (v - mean) ** 2 for v, w in pairs)),
            float(min(p[0] for p in pairs)),
            float(max(p[0] for p in pairs)),
        )
    if agg == "count":
        return float(sw)
    if agg == "sum":
        return float(sum(v * w for v, w in pairs))
    if agg == "avg":
        return float(sum(v * w for v, w in pairs) / sw)
    if agg == "min":
        return float(min(p[0] for p in pairs))
    if agg == "max":
        return float(max(p[0] for p in pairs))
    if agg == "variance":
        mean = sum(v * w for v, w in pairs) / sw
        return float(sum(w * (v - mean) ** 2 for v, w in pairs) / sw)
    if agg == "stddev":
        mean = sum(v * w for v, w in pairs) / sw
        var = sum(w * (v - mean) ** 2 for v, w in pairs) / sw
        return float(var**0.5)
    if agg == "median":
        # Sort by value; accumulate weight until Sigma_w / 2 is reached.
        # Mirrors heavy RST_H3_RasterToGridMedian.fAggW exactly.
        sorted_pairs = sorted(pairs, key=lambda p: p[0])
        half = sw / 2.0
        cum = 0.0
        idx = 0
        while idx < len(sorted_pairs) and cum + sorted_pairs[idx][1] < half:
            cum += sorted_pairs[idx][1]
            idx += 1
        # Boundary interpolation: if cumulative weight lands exactly on a value
        # boundary, average the two straddling values.
        if idx < len(sorted_pairs) - 1 and (cum + sorted_pairs[idx][1] == half):
            return float((sorted_pairs[idx][0] + sorted_pairs[idx + 1][0]) / 2.0)
        return float(sorted_pairs[idx][0])
    raise ValueError(f"unknown agg {agg!r}; expected one of {_AGGS}")


def _covering_band(
    work_ds,
    bi: int,
    band: np.ndarray,
    mask: np.ndarray,
    resolution,
    grid: str,
    agg: str,
    gt: tuple,
    conf=None,
) -> list:
    """Per-band area-weighted aggregation: distribute each valid pixel's value
    across every overlapping cell, weighted by (pixel_inter_cell / pixel).

    Reuses the tessellate oracles (``_polyfill_cells``, ``_cell_geom``,
    ``_cell_is_valid``, ``_encode_cellid``) so coverage enumeration is identical
    to the tessellate covering path.  The loop is per pixel, as the heavy tier's
    O(pixels) loop is, for quadbin, BNG and custom grids and for H3 cells larger
    than the pixels.

    H3 gate: when ``_use_h3_vectorised`` holds (the ``_H3_COVERING_VECTORISED``
    flag is on, the geotransform is north-up, and the average H3 cell is smaller
    than the centre pixel), the band goes to ``_covering_band_h3_vectorised``,
    which is cell-major and returns the same cells and states (within 1e-9, or
    the loop's GEOS rounding: see the module docstring).

    Interior fast-path (O(boundary) optimisation, mirrors the heavy tier):
    For exact-geometry grids (``_COVERING_FAST_PATH_EXACT``: custom, quadbin,
    bng), a pixel whose four corners all encode to the SAME cell is unambiguously
    interior -- its intersection area equals the pixel area, so the weight is
    exactly 1.0.  We accumulate ``(value, 1.0)`` directly and skip the shapely
    polygon, polyfill, and intersection.  Boundary pixels (corners spanning two
    or more cells) always use the existing path unchanged.  H3 is excluded from
    the fast-path set (see ``_COVERING_FAST_PATH_EXACT``).

    ``work_ds`` MUST already be in the grid's native CRS:
      * WGS84 (EPSG:4326) for h3 / quadbin
      * EPSG:27700         for bng
      * grid-native        for custom

    ``conf`` is a ``CustomGridConf`` instance when ``grid="custom"``.
    """
    if grid == "h3" and _use_h3_vectorised(work_ds, resolution, gt):
        return _covering_band_h3_vectorised(band, mask, int(resolution), agg, gt)
    # Lazy imports: tessellate oracles + shapely (avoid heavy deps at import time)
    from shapely.geometry import Polygon as _Polygon

    from databricks.labs.gbx.pyrx.core.tessellate import (
        _cell_geom,
        _cell_is_valid,
        _encode_cellid,
        _polyfill_cells,
    )

    # Set up the interior fast-path point→cell encoder for exact-geometry grids.
    # Returns the RAW cell id (same type as polyfill candidates) or None when the
    # point is outside the grid bounds.  _fast_cell=None disables the fast-path
    # (h3 and any grid not in _COVERING_FAST_PATH_EXACT use the old path).
    # The outer gate reads _COVERING_FAST_PATH_EXACT at call time so tests can
    # monkeypatch it to frozenset() to force the old path on all grids.
    if grid in _COVERING_FAST_PATH_EXACT:
        if grid == "custom":
            from databricks.labs.gbx.pygx import _custom as _custom_fp

            def _fast_cell(cx, cy):  # noqa: F811
                return _custom_fp.point_to_cell_id_or_none(conf, cx, cy, resolution)

        elif grid == "quadbin":
            from databricks.labs.gbx.pygx import _quadbin as _qb_fp

            def _fast_cell(cx, cy):  # noqa: F811
                return _qb_fp.point_as_cell(cx, cy, resolution)

        else:  # bng

            def _fast_cell(cx, cy):  # noqa: F811
                cid = _bng.point_to_cell_id(cx, cy, resolution)
                return cid if _bng.is_valid(cid) else None

    else:
        _fast_cell = None  # h3 and any grid not in the exact-geometry set

    ys, xs = np.nonzero(mask)
    if ys.size == 0:
        return []

    acc: dict = {}  # raw_cell_id -> [(value, weight)]

    for y_i, x_i in zip(ys, xs):  # vectorscan: ok (covering intentionally per-pixel)
        y, x = int(y_i), int(x_i)
        val = float(band[y, x])

        # Pixel rectangle corners from the affine geotransform.
        # (Mirrors heavy RasterToGridGeneric.executeOnCovering verbatim.)
        x0 = gt[0] + x * gt[1] + y * gt[2]
        y0 = gt[3] + x * gt[4] + y * gt[5]
        x1 = gt[0] + (x + 1) * gt[1] + y * gt[2]
        y1 = gt[3] + (x + 1) * gt[4] + y * gt[5]
        x2 = gt[0] + (x + 1) * gt[1] + (y + 1) * gt[2]
        y2 = gt[3] + (x + 1) * gt[4] + (y + 1) * gt[5]
        x3 = gt[0] + x * gt[1] + (y + 1) * gt[2]
        y3 = gt[3] + x * gt[4] + (y + 1) * gt[5]

        # Interior fast-path: if all 4 corners encode to the SAME cell, the pixel
        # is fully contained — weight is 1.0, no shapely work needed.
        if _fast_cell is not None:
            c0 = _fast_cell(x0, y0)
            if c0 is not None:
                c1 = _fast_cell(x1, y1)
                c2 = _fast_cell(x2, y2)
                c3 = _fast_cell(x3, y3)
                if c0 == c1 == c2 == c3:
                    acc.setdefault(c0, []).append((val, 1.0))
                    continue  # skip polyfill + intersection

        # Fallback: existing buffered-polyfill + shapely-intersection path.
        # Used for boundary pixels (corners span multiple cells) and for h3.
        pixel_poly = _Polygon([(x0, y0), (x1, y1), (x2, y2), (x3, y3)])
        px_area = pixel_poly.area
        if px_area == 0.0:
            continue

        for cell in _polyfill_cells(pixel_poly, resolution, grid, conf=conf):
            if not _cell_is_valid(cell, grid, conf=conf):
                continue
            cell_poly = _cell_geom(cell, grid, conf=conf)
            inter = cell_poly.intersection(pixel_poly)
            if inter is not None and inter.area > 0:
                w = inter.area / px_area
                if cell not in acc:
                    acc[cell] = []
                acc[cell].append((val, w))

    if not acc:
        return []

    return [
        {
            "cellID": _encode_cellid(cell, grid, conf=conf),
            "measure": _weighted_reduce(pairs, agg),
        }
        for cell, pairs in acc.items()  # vectorscan: ok (per-cell)
    ]


def _use_h3_vectorised(work_ds, resolution, gt, area=None) -> bool:
    """True when the H3 covering pass takes the vectorised path: the flag is on, the grid is north-up (no rotation
    terms) and the average cell is smaller than the centre pixel (``h3fit``, the recommender's own pixel area; or
    ``area``, the centre pixel's area in m2 when the caller knows the raster's CRS better than ``work_ds`` does).

    A resolution outside 0..15 raises (``_validate_resolution``). A pixel area that is not a positive finite number
    (a degenerate geotransform) is not applicable and keeps the per-pixel loop; any other error surfaces.
    """
    if not _H3_COVERING_VECTORISED or gt[2] != 0.0 or gt[4] != 0.0:
        return False
    from databricks.labs.gbx.pyrx.core.h3fit import H3_AVG_CELL_AREA_M2, pixel_area_m2

    resolution = int(resolution)
    _validate_resolution(resolution, "h3")
    if area is None:
        area = pixel_area_m2(work_ds)
    if not (math.isfinite(area) and area > 0.0):
        return False
    return H3_AVG_CELL_AREA_M2[resolution] < area


def _h3_candidate_cells(gt: tuple, rows: tuple, cols: tuple, resolution: int) -> list:
    """Every H3 cell (int id) that may overlap a pixel of the window ``rows`` x ``cols`` (``(first, last)``,
    inclusive) of a north-up grid -- the window that holds the valid pixels: one overlap polyfill of the window's bbox,
    padded by one pixel so a cell whose overlap test sits on the bbox edge is still a candidate (a superset; empty
    overlaps drop).
    """
    from h3.api import basic_int as h3i

    xa, xb = gt[0] + cols[0] * gt[1], gt[0] + (cols[1] + 1) * gt[1]
    ya, yb = gt[3] + rows[0] * gt[5], gt[3] + (rows[1] + 1) * gt[5]
    west, east = min(xa, xb) - abs(gt[1]), max(xa, xb) + abs(gt[1])
    south, north = min(ya, yb) - abs(gt[5]), max(ya, yb) + abs(gt[5])
    box = h3i.LatLngPoly([(south, west), (north, west), (north, east), (south, east)])
    return list(h3i.polygon_to_cells_experimental(box, resolution, contain="overlap"))


def _h3_hexagons(cells: list) -> tuple:
    """``(lon, lat)`` vertex arrays, each cells x M, of H3 cells (int ids) -- the vertices of
    ``tessellate._cell_polygon_lonlat``, in ``h3.cell_to_boundary`` order. M is the largest vertex count among the cells
    (6, or up to 10 for a distorted cell); a shorter ring repeats its last vertex, a zero-length edge that adds no area.
    """
    from itertools import chain

    from h3.api import basic_int as h3i

    rings = [h3i.cell_to_boundary(c) for c in cells]  # vectorscan: ok (h3 no array API)
    n = np.fromiter(map(len, rings), dtype=np.intp, count=len(rings))
    m = int(n.max())
    if int(n.min()) != m:
        # vectorscan: ok (pads the rare ring with fewer vertices, per cell)
        rings = [r if len(r) == m else r + (r[-1],) * (m - len(r)) for r in rings]
    flat = np.fromiter(
        chain.from_iterable(chain.from_iterable(rings)),
        dtype="float64",
        count=2 * m * len(rings),
    ).reshape(len(rings), m, 2)
    return np.ascontiguousarray(flat[:, :, 1]), np.ascontiguousarray(flat[:, :, 0])


def _box_overlap_area(
    vx: np.ndarray,
    vy: np.ndarray,
    cell: np.ndarray,
    xl: np.ndarray,
    yb: np.ndarray,
    xr: np.ndarray,
    yt: np.ndarray,
) -> np.ndarray:
    """Planar overlap area of the polygon ``cell[i]`` (vertex rows ``vx``, ``vy``: cells x M, a ring in either
    orientation, padded by repeating a vertex) and the axis-aligned box ``[xl, xr] x [yb, yt]``, for every pair ``i``,
    accurate to ~1e-15 of the box's area.

    The area is the contour integral of x dy over the polygon's boundary pushed into the box by clamping each
    coordinate to the box's range: the clamped curve winds once around every point of the box that the polygon holds
    and never around one it does not, so the integral is the overlap's area (signed by the ring's orientation). Along
    one edge, y is clamped outside the parameter interval where the edge is inside the box's y range (there dy = 0),
    and the clamped x is affine on each of the at most three pieces the box's x range cuts that interval into, so the
    midpoint rule integrates each piece exactly. Coordinates are taken relative to the box's south-west corner first,
    so the area is accurate to the pixel's scale, not the longitude's. The integral of a pair with no overlap is
    rounding noise around 0; such a pair is set to exactly 0 when the pair is separated along an axis or by the line of
    an edge that has its whole polygon on its inner side (a separating axis). Every edge of a convex ring is such an
    edge, so a convex ring's non-overlapping and touching pairs are exactly 0. Beyond a reflex part of a non-convex
    ring (an H3 cell that crosses an icosahedron face edge, 7-10 vertices, is slightly concave in lon/lat) the noise
    can stay, so a caller keeps a pair only above a floor (``_COVER_WEIGHT_FLOOR``).
    """
    x = vx[cell] - xl[:, None]
    y = vy[cell] - yb[:, None]
    w = (xr - xl)[:, None]
    h = (yt - yb)[:, None]
    nxt = np.roll(np.arange(vx.shape[1]), -1)
    dx = x[:, nxt] - x
    dy = y[:, nxt] - y
    with np.errstate(divide="ignore", invalid="ignore"):
        # the edge's parameter interval [lo, hi] with 0 <= y <= h
        ta = -y / dy
        tb = (h - y) / dy
        lo = np.clip(np.fmin(ta, tb), 0.0, 1.0)
        hi = np.clip(np.fmax(ta, tb), 0.0, 1.0)
        # [a, b]: where 0 <= x <= w, inside [lo, hi]; a vertical edge (dx == 0) gives +-inf, so a = lo and b = hi
        # inside the x range, and a = b at the end whose clamped value (x < 0: 0, x > w: w) the pieces below use
        sa = -x / dx
        sb = (w - x) / dx
        a = np.clip(np.fmin(sa, sb), lo, hi)
        b = np.clip(np.fmax(sa, sb), lo, hi)
    # the clamped x before a is w when x decreases (else 0), after b w when x increases or is constant (else 0)
    before = np.where(dx < 0, w, 0.0)
    after = np.where(dx >= 0, w, 0.0)
    middle = np.clip(x + dx * ((a + b) * 0.5), 0.0, w)
    integral = (a - lo) * before + (b - a) * middle + (hi - b) * after
    area = np.abs(np.einsum("ij,ij->i", dy, integral))
    # exact 0 for a pair without overlap: separated along an axis, or by a supporting edge's line
    sep = (x.max(axis=1) <= 0.0) | (x.min(axis=1) >= w[:, 0])
    sep |= (y.max(axis=1) <= 0.0) | (y.min(axis=1) >= h[:, 0])
    orient, support = _ring_supporting_edges(vx, vy)
    # the box corner deepest on the inner side of each edge: cross(edge, corner - start) at its maximum
    o = orient[cell][:, None]
    reach = o * (
        dx * (np.where(o * dx > 0, h, 0.0) - y)
        - dy * (np.where(o * dy < 0, w, 0.0) - x)
    )
    sep |= ((reach <= 0.0) & support[cell]).any(axis=1)
    area[sep] = 0.0
    return area


def _ring_supporting_edges(vx: np.ndarray, vy: np.ndarray) -> tuple:
    """Per ring of ``vx``, ``vy`` (cells x M): its orientation (+1 counter-clockwise, -1 clockwise) and, per edge,
    whether the edge has nonzero length and every vertex of the ring on its inner side (true of every edge of a
    convex ring) -- the edges whose lines may separate a box from the ring."""
    nxt = np.roll(np.arange(vx.shape[1]), -1)
    # vertices relative to the first, so the products are of small numbers (accurate to the ring's scale)
    x = vx - vx[:, :1]
    y = vy - vy[:, :1]
    ex, ey = x[:, nxt] - x, y[:, nxt] - y
    orient = np.where((x * y[:, nxt] - x[:, nxt] * y).sum(axis=1) < 0, -1.0, 1.0)
    # cross(edge k, vertex j - start of edge k) for every k, j: cells x M x M
    cross = ex[:, :, None] * (y[:, None, :] - y[:, :, None]) - ey[:, :, None] * (
        x[:, None, :] - x[:, :, None]
    )
    inner = (orient[:, None, None] * cross >= 0.0).all(axis=2)
    return orient, inner & ((ex != 0.0) | (ey != 0.0))


def _cell_pixel_pairs(bounds: np.ndarray, gt: tuple, height: int, width: int):
    """(cell index, row, col) of every pixel in each cell's bounding window, as flat arrays (inverse geotransform of
    the cell bounds, cells x ``(xmin, ymin, xmax, ymax)``; north-up grid)."""
    b = bounds
    ca, cb = (b[:, 0] - gt[0]) / gt[1], (b[:, 2] - gt[0]) / gt[1]
    ra, rb = (b[:, 1] - gt[3]) / gt[5], (b[:, 3] - gt[3]) / gt[5]
    c0 = np.clip(np.floor(np.minimum(ca, cb)), 0, width - 1).astype(np.int64)
    c1 = np.clip(np.floor(np.maximum(ca, cb)), -1, width - 1).astype(np.int64)
    r0 = np.clip(np.floor(np.minimum(ra, rb)), 0, height - 1).astype(np.int64)
    r1 = np.clip(np.floor(np.maximum(ra, rb)), -1, height - 1).astype(np.int64)
    ncol, nrow = np.maximum(c1 - c0 + 1, 0), np.maximum(r1 - r0 + 1, 0)
    n = ncol * nrow
    cell = np.repeat(np.arange(b.shape[0]), n)
    off = np.arange(int(n.sum())) - np.repeat(np.cumsum(n) - n, n)
    cols = np.repeat(c0, n) + off % np.repeat(np.maximum(ncol, 1), n)
    rows = np.repeat(r0, n) + off // np.repeat(np.maximum(ncol, 1), n)
    return cell, rows, cols


def _covering_band_h3_vectorised(
    band: np.ndarray, mask: np.ndarray, resolution: int, agg: str, gt: tuple
) -> list:
    """``_covering_band`` for H3 cells smaller than the pixels of a north-up EPSG:4326 grid, cell-major: the same
    cells and states as the per-pixel loop (weight = intersection area / pixel area, kept when > 0). Each pixel is an
    axis-aligned box in (lon, lat), so the areas come from ``_box_overlap_area`` over every (cell, pixel) pair of a
    chunk at once -- the planar overlap GEOS computes, without a geometry per pair.
    """
    valid_rows = np.flatnonzero(mask.any(axis=1))
    if valid_rows.size == 0:
        return []  # no valid pixel: no cell, and no candidate built
    valid_cols = np.flatnonzero(mask.any(axis=0))
    px_area = abs(gt[1] * gt[5])
    window = (
        (int(valid_rows[0]), int(valid_rows[-1])),
        (int(valid_cols[0]), int(valid_cols[-1])),
    )
    cells = _h3_candidate_cells(gt, *window, resolution)
    out = []
    step = _H3_VEC_CHUNK_CELLS
    for start in range(0, len(cells), step):  # vectorscan: ok (chunks bound pairs)
        chunk = cells[start : start + step]
        vx, vy = _h3_hexagons(chunk)
        out.extend(_box_pairs_reduce(vx, vy, chunk, band, mask, gt, px_area, agg))
    return out


def _box_pairs_reduce(vx, vy, ids, band, mask, gt, px_area, agg) -> list:
    """``{"cellID", "measure"}`` of the cells ``ids`` (rings ``vx``, ``vy``: cells x M, in the frame of the north-up
    geotransform ``gt``) over the valid pixels of ``band``: every (cell, pixel) pair of the cells' bounding windows,
    weighted by ``_box_overlap_area`` / ``px_area`` and kept above ``_COVER_WEIGHT_FLOOR``. An H3 id is below 2**63,
    so an int id is already the signed Long ``tessellate._encode_cellid`` gives."""
    height, width = mask.shape
    bounds = np.stack([vx.min(1), vy.min(1), vx.max(1), vy.max(1)], axis=1)
    cell, rows, cols = _cell_pixel_pairs(bounds, gt, height, width)
    ok = mask[rows, cols]
    cell, rows, cols = cell[ok], rows[ok], cols[ok]
    if cell.size == 0:
        return []
    xl, xr = gt[0] + cols * gt[1], gt[0] + (cols + 1) * gt[1]
    yt, yb = gt[3] + rows * gt[5], gt[3] + (rows + 1) * gt[5]
    area = _box_overlap_area(
        vx,
        vy,
        cell,
        np.minimum(xl, xr),
        np.minimum(yt, yb),
        np.maximum(xl, xr),
        np.maximum(yt, yb),
    )
    w = area / px_area
    keep = w > _COVER_WEIGHT_FLOOR
    cell, w, vals = (
        cell[keep],
        w[keep],
        band[rows[keep], cols[keep]].astype("float64"),
    )
    if cell.size == 0:
        return []
    uniq, measures = _grouped_measures(cell, vals, agg, weights=w)
    return [
        {"cellID": ids[c], "measure": m}
        for c, m in zip(uniq.tolist(), measures)  # vectorscan: ok (per-cell)
    ]


def _complete_coverage_pass(
    band_result: list, work_ds, resolution, grid: str, agg: str, conf=None
) -> list:
    """Add covered-but-empty cells to ``band_result`` (complete coverage mode).

    For each covering candidate cell that
      (a) is not already in the sparse result,
      (b) passes the cell validity guard, and
      (c) has positive-area overlap with the raster bbox,
    emits ``{cellID, measure: emptyValue}`` where ``emptyValue`` is:
      * ``0.0``  for ``count``  (mass-conserving)
      * ``None`` for all other aggregates

    Mirrors ``RasterToGridGeneric.applyCompleteCoverage`` exactly.

    ``work_ds`` MUST be in the grid's native CRS (same requirement as
    ``_covering_band``).  ``conf`` is a ``CustomGridConf`` instance when
    ``grid="custom"``.
    """
    from shapely.geometry import box as _box

    from databricks.labs.gbx.pyrx.core.tessellate import (
        _cell_geom,
        _cell_is_valid,
        _encode_cellid,
        _has_positive_area_overlap,
        _polyfill_cells,
    )

    # Raster bbox in the work CRS (already native for BNG/custom; WGS84 for h3/quadbin).
    west, south, east, north = work_ds.bounds
    bbox_poly = _box(west, south, east, north)

    empty_value = _EMPTY_STATE if agg == _PARTIAL else (0.0 if agg == "count" else None)
    existing_ids = {r["cellID"] for r in band_result}

    extra = []
    for cell in _polyfill_cells(bbox_poly, resolution, grid, conf=conf):
        if not _cell_is_valid(cell, grid, conf=conf):
            continue
        encoded = _encode_cellid(cell, grid, conf=conf)
        if encoded in existing_ids:
            continue
        cell_poly = _cell_geom(cell, grid, conf=conf)
        if _has_positive_area_overlap(cell_poly, bbox_poly):
            extra.append({"cellID": encoded, "measure": empty_value})

    return band_result + extra


# ---------------------------------------------------------------------------
# The native frame: a raster in another CRS than the grid's, gridded on its own pixels (no warp)
# ---------------------------------------------------------------------------

# A cell edge is straight in the grid's CRS (lon/lat for h3 and quadbin, EPSG:27700 for BNG) and bends once moved into
# the raster's CRS. Each edge is walked with as many points as keep the chord between two of them within this fraction
# of the shorter of the edge and the pixel side, so a covering weight is within about this fraction of the bent cell's.
_NATIVE_EDGE_TOLERANCE = 1e-4
# An edge that needs more points than this does not bend smoothly in the raster's CRS: it crosses the CRS's own seam
# (where its longitudes wrap), so the cell has no outline there and the raster takes the warp instead.
_NATIVE_EDGE_MAX_POINTS = 4096
# Cells x vertices x vertices one vectorised chunk may hold (``_ring_supporting_edges``): about 20 MB per array.
_NATIVE_RING_BUDGET = 2_500_000
# Pixels between two points of a window outline that is moved to the grid's CRS (at least 20 points per side).
_OUTLINE_STEP_PIXELS = 16


class _NeedsWarp(Exception):
    """The native frame cannot hold the raster: a point does not transform, or a cell crosses its CRS's seam."""


def _move(transform, x, y):
    """``transform`` (a ``pyproj`` transform) of the arrays ``x``, ``y``, shapes kept; raises ``_NeedsWarp`` for a
    point that does not transform."""
    xs, ys = transform(np.ravel(x), np.ravel(y))
    xs = np.asarray(xs, dtype="float64").reshape(np.shape(x))
    ys = np.asarray(ys, dtype="float64").reshape(np.shape(y))
    if not (np.isfinite(xs).all() and np.isfinite(ys).all()):
        raise _NeedsWarp
    return xs, ys


def _outline_boxes(x, y, pole: int = 0, geographic: bool = True) -> list:
    """``(w, s, e, n)`` boxes of the grid's CRS that hold the closed outline through the points ``x``, ``y`` (in
    order). In lon/lat (``geographic``) an outline that crosses the antimeridian gives one box on each side, and one
    that holds a pole (``pole``: 1 north, -1 south) the band of latitudes from its farthest point to the pole, in six
    boxes of 60 degrees (a box must span less than 180 degrees of longitude)."""
    x, y = np.asarray(x, dtype="float64"), np.asarray(y, dtype="float64")
    if not geographic:
        return [(x.min(), y.min(), x.max(), y.max())]
    if pole:
        s, n = (float(y.min()), 90.0) if pole > 0 else (-90.0, float(y.max()))
        return [(float(w), s, float(w) + 60.0, n) for w in range(-180, 180, 60)]
    # longitudes along the outline, each step taken the short way round
    step = (np.diff(x) + 180.0) % 360.0 - 180.0
    lon = x[0] + np.concatenate([[0.0], np.cumsum(step)])
    shift = 360.0 * math.floor((lon.min() + 180.0) / 360.0)
    w, e = float(lon.min() - shift), float(lon.max() - shift)
    s, n = float(y.min()), float(y.max())
    if e <= 180.0:
        return [(w, s, e, n)]
    return [(w, s, 180.0, n), (-180.0, s, e - 360.0, n)]


def _moved_outlines(frame, cs, rs) -> tuple:
    """K closed outlines in pixel coordinates (``cs``, ``rs``: K x P, in order) moved to the grid's CRS, with what
    ``_padded_boxes`` needs to hold each: ``(x, y, pad, pole)``. ``x``, ``y`` (K x 2P) are the moved points and the
    moved midpoints between them; ``pad`` (K x 2) is the largest distance, per axis, of a moved midpoint from the
    chord of its moved ends (how far a moved side bends out: past the midpoint it bends out by about a quarter of that
    again, so the pad holds it); ``pole`` is the pole within two pixels of the outline (1, -1, or 0), where a side
    near the pole sweeps longitudes too fast for a pad."""
    k, p = cs.shape
    nxt = np.roll(np.arange(p), -1)
    mc, mr = 0.5 * (cs + cs[:, nxt]), 0.5 * (rs + rs[:, nxt])
    gx, gy = _move(
        frame.to_grid,
        *frame.native_xy(
            np.concatenate([cs, mc], axis=1), np.concatenate([rs, mr], axis=1)
        ),
    )
    ax, ay, bx, by = gx[:, :p], gy[:, :p], gx[:, p:], gy[:, p:]
    step = ax[:, nxt] - ax
    if frame.geographic:  # a longitude step taken the short way round
        step = (step + 180.0) % 360.0 - 180.0
    dev_x = bx - (ax + 0.5 * step)
    if frame.geographic:
        dev_x = (dev_x + 180.0) % 360.0 - 180.0
    dev_y = by - 0.5 * (ay + ay[:, nxt])
    x, y = np.empty((k, 2 * p)), np.empty((k, 2 * p))
    x[:, 0::2], x[:, 1::2], y[:, 0::2], y[:, 1::2] = ax, bx, ay, by
    pad = np.stack([np.abs(dev_x).max(axis=1), np.abs(dev_y).max(axis=1)], axis=1)
    pole = np.zeros(k, dtype=np.intp)
    for sign, (c, r) in frame.poles.items():
        near = (cs.min(axis=1) - 2 <= c) & (c <= cs.max(axis=1) + 2)
        near &= (rs.min(axis=1) - 2 <= r) & (r <= rs.max(axis=1) + 2)
        pole[near] = sign
    return x, y, pad, pole


def _padded_boxes(x, y, pad, pole, geographic: bool) -> list:
    """``_outline_boxes`` of one moved outline of ``_moved_outlines``, each box grown by ``pad`` and 1% of its size
    (in lon/lat kept within the globe: the other side of the antimeridian has its own box).
    """
    out = []
    for w, s, e, n in _outline_boxes(x, y, int(pole), geographic):
        pw = pad[0] + 0.01 * (e - w) + 1e-12
        ph = pad[1] + 0.01 * (n - s) + 1e-12
        w, s, e, n = w - pw, s - ph, e + pw, n + ph
        if geographic:
            w, s, e, n = max(w, -180.0), max(s, -90.0), min(e, 180.0), min(n, 90.0)
        out.append((w, s, e, n))
    return out


_EARTH_RADIUS_M = 6371007.2


def _lon_spans(w: float, e: float) -> list:
    """The longitudes ``[w, e]`` (degrees, possibly past +-180) as spans within [-180, 180], each at most 90 wide."""
    if e - w >= 360.0:
        w, e = -180.0, 180.0
    shift = 360.0 * math.floor((w + 180.0) / 360.0)
    w, e = w - shift, e - shift
    pieces = [(w, min(e, 180.0))] + ([(-180.0, e - 360.0)] if e > 180.0 else [])
    spans = []
    for a, b in pieces:  # vectorscan: ok (at most two pieces)
        k = max(1, math.ceil((b - a) / 90.0))
        spans.extend((a + (b - a) * i / k, a + (b - a) * (i + 1) / k) for i in range(k))
    return spans


def _h3_true_cell_boxes(boxes, resolution: int) -> list:
    """The lon/lat ``boxes`` grown so that h3's overlap test (on its own cells, whose edges are great-circle arcs) finds
    every cell whose lon/lat ring (straight edges, ``_h3_hexagons``) overlaps a box. An arc of length L at latitude
    phi bows from the straight lon/lat edge by about L**2 tan(phi) / (8 R); the boxes grow by twice that, for L twice
    the resolution's average edge, and never by more than that L (the two share their vertices).
    """
    from h3.api import basic_int as h3i

    edge = 2.0 * h3i.average_hexagon_edge_length(resolution, "m")
    out = []
    for w, s, e, n in boxes:  # vectorscan: ok (per box)
        lat = math.radians(min(max(abs(s), abs(n)), 90.0))
        bow = (
            edge
            if lat > math.radians(89.0)
            else min(edge, edge * edge * math.tan(lat) / (4.0 * _EARTH_RADIUS_M))
        )
        dlat = math.degrees(bow / _EARTH_RADIUS_M)
        cos_lat = math.cos(min(lat + math.radians(dlat), math.pi / 2.0))
        dlon = (
            360.0 if cos_lat < 1e-9 else math.degrees(bow / (_EARTH_RADIUS_M * cos_lat))
        )
        s, n = max(s - dlat, -90.0), min(n + dlat, 90.0)
        out.extend((a, s, b, n) for a, b in _lon_spans(w - dlon, e + dlon))
    return out


def _h3_box_cells(w: float, s: float, e: float, n: float, resolution: int) -> list:
    """Every H3 cell (int id) that overlaps the lon/lat box (a superset).

    * A box smaller than half an edge: the cell of its centre and that cell's neighbours (a point within an edge of a
      cell is in it or in a neighbour).
    * Else ``polygon_to_cells_experimental`` in overlap mode, with the centre's cell added, except within a degree
      and three cells of the antimeridian. That call can miss cells: measured, a box within half a degree of 180W
      missed one in up to a quarter of cases, and a 30 m box inside a res-9 cell in Iowa returned only a neighbour.
    * Near the antimeridian the box is sampled at the least inner radius of a cell, and the cells of the samples and
      their neighbours are taken: a cell that overlaps the box holds a sample or is next to a cell that does.
    """
    from h3.api import basic_int as h3i

    edge = h3i.average_hexagon_edge_length(resolution, "m")
    lat_c, lon_c = 0.5 * (s + n), (0.5 * (w + e) + 180.0) % 360.0 - 180.0
    centre = h3i.latlng_to_cell(lat_c, lon_c, resolution)
    lat_low = (
        0.0 if s * n <= 0.0 else min(abs(s), abs(n))
    )  # the box's latitude nearest the equator
    diag = _EARTH_RADIUS_M * math.hypot(
        math.radians(n - s), math.radians(e - w) * math.cos(math.radians(lat_low))
    )
    if diag < 0.5 * edge:
        return list(h3i.grid_disk(centre, 1))
    cos_lat = max(math.cos(math.radians(min(max(abs(s), abs(n)), 90.0))), 1e-9)
    margin = 1.0 + math.degrees(3.0 * 2.0 * edge / (_EARTH_RADIUS_M * cos_lat))
    near_w, near_e = max(w, 180.0 - margin), min(e, -180.0 + margin)
    out = [centre]
    mid = (
        min(e, 180.0 - margin),
        max(w, -180.0 + margin),
    )  # the part away from the antimeridian
    if mid[0] > mid[1]:
        out.extend(
            h3i.polygon_to_cells_experimental(
                h3i.LatLngPoly([(s, mid[1]), (n, mid[1]), (n, mid[0]), (s, mid[0])]),
                resolution,
                contain="overlap",
            )
        )
    # the least inner radius: half the average edge (a distorted cell's shortest), times sqrt(3) / 2
    step = math.degrees(0.5 * edge * math.sqrt(3.0) / 2.0 / _EARTH_RADIUS_M)
    lats = np.append(np.arange(s, n, step), n)
    for a, b in ((near_w, e), (w, near_e)):  # vectorscan: ok (the two strips)
        if a > b:
            continue
        hit = set()
        for (
            lat
        ) in lats:  # vectorscan: ok (per row: the longitude step widens with latitude)
            row_cos = math.cos(math.radians(min(abs(lat) + step, 90.0)))
            k = min(int((b - a) * row_cos / step) + 1, 1_000_000)
            hit.update(
                h3i.latlng_to_cell(float(lat), float(x), resolution)
                for x in np.linspace(a, b, k + 1)  # vectorscan: ok (h3 no array API)
            )
        out.extend(
            d for c in hit for d in h3i.grid_disk(c, 1)
        )  # vectorscan: ok (per cell)
    return out


def _box_cells(boxes, resolution, grid: str) -> list:
    """The cells of ``grid`` that may overlap one of ``boxes`` (a superset; empty overlaps drop), each once, in
    first-seen order: int ids for h3 (``polygon_to_cells_experimental`` in overlap mode), the ``_polyfill_cells`` ids
    for quadbin and BNG."""
    from itertools import chain

    from shapely.geometry import box as _box

    from databricks.labs.gbx.pyrx.core.tessellate import _polyfill_cells

    if grid == "h3":
        parts = (
            _h3_box_cells(w, s, e, n, resolution)
            for w, s, e, n in _h3_true_cell_boxes(
                boxes, resolution
            )  # vectorscan: ok (per box)
        )
    else:
        parts = (
            _polyfill_cells(_box(w, s, e, n), resolution, grid) for w, s, e, n in boxes
        )  # vectorscan: ok (per box)
    return list(dict.fromkeys(chain.from_iterable(parts)))


def _grid_rings(cells, grid: str) -> tuple:
    """``(x, y)`` vertex arrays, each cells x M, of the cells in the grid's CRS: the H3 hexagons of ``_h3_hexagons``
    (lon, lat), the lon/lat box of a quadbin cell, the EPSG:27700 square of a BNG cell.
    """
    if grid == "h3":
        return _h3_hexagons(cells)
    if grid == "quadbin":
        import quadbin

        b = np.array(
            [
                quadbin.cell_to_bounding_box(int(c)) for c in cells
            ],  # vectorscan: ok (no array API)
            dtype="float64",
        ).reshape(-1, 4)
    else:
        b = np.array(
            [
                _bng.cell_id_to_geometry(int(c)).bounds for c in cells
            ],  # vectorscan: ok (no array API)
            dtype="float64",
        ).reshape(-1, 4)
    w, s, e, n = b.T
    return np.stack([w, e, e, w], axis=1), np.stack([s, s, n, n], axis=1)


class _Frame:
    """A raster in its own CRS as a grid sees it: the transforms between the two CRSs and the raster's pixel geometry.

    ``gt`` is the raster's geotransform (rotation terms included), ``px_area`` / ``px_side`` its pixel's area and side
    in the raster's units, ``poles`` the ``(col, row)`` in the raster of each pole that transforms (h3 / quadbin).
    """

    def __init__(self, ds, src_crs, grid: str):
        from databricks.labs.gbx.core.crs import get_transformer

        self.ds, self.grid = ds, grid
        self.geographic = grid != "bng"
        grid_crs = 27700 if grid == "bng" else 4326
        self.to_grid = get_transformer(src_crs, grid_crs).transform
        self.to_native = get_transformer(grid_crs, src_crs).transform
        self.gt = gt = ds.transform.to_gdal()
        self.px_area = abs(gt[1] * gt[5] - gt[2] * gt[4])
        self.px_side = math.sqrt(self.px_area)
        self.poles = {}
        if self.geographic:
            inv = ~ds.transform
            for sign in (1, -1):
                x, y = self.to_native(0.0, 90.0 * sign)
                if math.isfinite(x) and math.isfinite(y):
                    self.poles[sign] = inv * (x, y)

    def native_xy(self, cols, rows):
        """Raster CRS coordinates of the pixel coordinates ``cols``, ``rows`` (the full geotransform)."""
        gt = self.gt
        return gt[0] + cols * gt[1] + rows * gt[2], gt[3] + cols * gt[4] + rows * gt[5]

    def window_boxes(self, rows, cols, pad: int = 1) -> list:
        """``_padded_boxes`` of the pixel window ``rows`` x ``cols`` (``(first, last)``, inclusive) padded by ``pad``
        pixels: its outline walked every ``_OUTLINE_STEP_PIXELS`` pixels (at least 20 per side) and moved to the grid's
        CRS."""
        r0, r1 = rows[0] - pad, rows[1] + 1 + pad
        c0, c1 = cols[0] - pad, cols[1] + 1 + pad
        n = max(20, math.ceil(max(r1 - r0, c1 - c0) / _OUTLINE_STEP_PIXELS))
        frac = np.arange(n) / n
        cs = np.concatenate(
            [
                c0 + frac * (c1 - c0),
                np.full(n, c1),
                c1 - frac * (c1 - c0),
                np.full(n, c0),
            ]
        )
        rs = np.concatenate(
            [
                np.full(n, r0),
                r0 + frac * (r1 - r0),
                np.full(n, r1),
                r1 - frac * (r1 - r0),
            ]
        )
        x, y, gpad, pole = _moved_outlines(self, cs[None, :], rs[None, :])
        return _padded_boxes(x[0], y[0], gpad[0], pole[0], self.geographic)

    def pixel_area_m2(self) -> float:
        """Geodesic area in m2 of the centre pixel (as ``h3fit.pixel_area_m2``, through this frame's CRS)."""
        from shapely.geometry import Polygon

        from databricks.labs.gbx.core.crs import area_m2

        col, row = self.ds.width // 2, self.ds.height // 2
        xs, ys = self.native_xy(
            np.array([col, col + 1, col + 1, col], dtype="float64"),
            np.array([row, row, row + 1, row + 1], dtype="float64"),
        )
        try:
            lon, lat = _move(self.to_grid, xs, ys)
        except _NeedsWarp:
            return math.nan
        lon = lon[0] + (lon - lon[0] + 180.0) % 360.0 - 180.0
        return float(area_m2(Polygon(zip(lon, lat)), "EPSG:4326"))


def _native_rings(frame: _Frame, cells) -> tuple:
    """``(x, y)`` vertex arrays, each cells x M, of the cells in the raster's CRS: every edge of ``_grid_rings`` (in
    lon/lat taken the short way round, across the antimeridian or around a pole) walked with ``n`` points, ``n`` the
    least that keeps the chord within ``_NATIVE_EDGE_TOLERANCE`` of the shorter of the edge and the pixel side (the
    chord's distance from the bent edge falls as ``1 / n**2``). ``n`` depends on the edge alone, so two cells walk
    their shared edge with the same points and still tile. A shorter ring repeats its last vertex.

    Raises ``_NeedsWarp`` for a vertex that does not transform, or an edge that needs more than
    ``_NATIVE_EDGE_MAX_POINTS`` (a cell across the raster CRS's seam).
    """
    gx, gy = _grid_rings(cells, frame.grid)
    k = gx.shape[1]
    nxt = np.roll(np.arange(k), -1)
    dx, dy = gx[:, nxt] - gx, gy[:, nxt] - gy
    if frame.geographic:
        dx = (dx + 180.0) % 360.0 - 180.0
    xs, ys = _move(frame.to_native, gx, gy)
    mx, my = _move(frame.to_native, gx + 0.5 * dx, gy + 0.5 * dy)
    nx_, ny_ = xs[:, nxt], ys[:, nxt]
    length = np.hypot(nx_ - xs, ny_ - ys)
    dev = np.hypot(mx - 0.5 * (xs + nx_), my - 0.5 * (ys + ny_))
    tol = _NATIVE_EDGE_TOLERANCE * np.minimum(length, frame.px_side)
    with np.errstate(divide="ignore", invalid="ignore"):
        need = np.where(length > 0.0, np.ceil(np.sqrt(dev / tol)), 1.0)
    need = np.maximum(need, 1.0)
    if not (need <= _NATIVE_EDGE_MAX_POINTS).all():
        raise _NeedsWarp
    if (need == 1.0).all():
        return xs, ys
    per_edge = need.astype(np.intp).ravel()
    total = int(per_edge.sum())
    edge = np.repeat(np.arange(per_edge.size), per_edge)
    j = np.arange(total) - np.repeat(np.cumsum(per_edge) - per_edge, per_edge)
    px, py = xs.ravel()[edge], ys.ravel()[edge]
    inner = j > 0
    e_in = edge[inner]
    t = j[inner] / per_edge[e_in]
    px[inner], py[inner] = _move(
        frame.to_native,
        gx.ravel()[e_in] + t * dx.ravel()[e_in],
        gy.ravel()[e_in] + t * dy.ravel()[e_in],
    )
    per_cell = need.astype(np.intp).sum(axis=1)
    m = int(per_cell.max())
    pos = np.arange(total) - np.repeat(np.cumsum(per_cell) - per_cell, per_cell)
    vx, vy = np.empty((len(cells), m)), np.empty((len(cells), m))
    vx[edge // k, pos], vy[edge // k, pos] = px, py
    col = np.minimum(np.arange(m)[None, :], per_cell[:, None] - 1)
    row = np.arange(len(cells))[:, None]
    return vx[row, col], vy[row, col]


def _native_id(cell, grid: str):
    """The output id of a raw cell: the Long of an h3 / quadbin id, the formatted BNG reference."""
    return _bng.format(int(cell)) if grid == "bng" else int(cell)


def _corner_cells(gx, gy, resolution, grid: str) -> np.ndarray:
    """The quadbin / BNG cell (int64) of each grid-CRS point, -1 for a point outside BNG."""
    if grid == "quadbin":
        from databricks.labs.gbx.pygx import _quadbin

        return _quadbin.point_as_cell_vec(gx, gy, resolution)
    cells = _bng_cells(gx, gy, resolution)
    return np.where(_bng.is_valid_vec(cells, resolution), cells, -1)


def _covering_band_h3_native(
    frame: _Frame, band, mask, resolution: int, agg: str
) -> list:
    """``_covering_band_h3_vectorised`` for a north-up raster in another CRS: the candidate cells come from the moved
    outline of the window of valid pixels (padded by one pixel), and each ring is moved into the raster's CRS
    (``_native_rings``), where its pixels are axis-aligned boxes."""
    valid_rows = np.flatnonzero(mask.any(axis=1))
    if valid_rows.size == 0:
        return []
    valid_cols = np.flatnonzero(mask.any(axis=0))
    boxes = frame.window_boxes(
        (int(valid_rows[0]), int(valid_rows[-1])),
        (int(valid_cols[0]), int(valid_cols[-1])),
    )
    cells = _box_cells(boxes, resolution, "h3")
    out = []
    step = _H3_VEC_CHUNK_CELLS
    for start in range(0, len(cells), step):  # vectorscan: ok (chunks bound pairs)
        chunk = cells[start : start + step]
        vx, vy = _native_rings(frame, chunk)
        sub = max(1, _NATIVE_RING_BUDGET // vx.shape[1] ** 2)
        for s0 in range(
            0, len(chunk), sub
        ):  # vectorscan: ok (sub-chunks bound the rings)
            out.extend(
                _box_pairs_reduce(
                    vx[s0 : s0 + sub],
                    vy[s0 : s0 + sub],
                    chunk[s0 : s0 + sub],
                    band,
                    mask,
                    frame.gt,
                    frame.px_area,
                    agg,
                )
            )
    return out


def _native_polygons(frame: _Frame, cells, ox: float, oy: float):
    """Shapely polygons of the cells in the raster's CRS, relative to ``(ox, oy)`` (so GEOS rounds at the raster's
    scale, not the coordinates')."""
    import shapely

    polys = []
    step = _H3_VEC_CHUNK_CELLS
    for start in range(0, len(cells), step):  # vectorscan: ok (chunks bound the rings)
        vx, vy = _native_rings(frame, cells[start : start + step])
        polys.append(shapely.polygons(np.stack([vx - ox, vy - oy], axis=-1)))
    return np.concatenate(polys) if polys else np.empty(0, dtype=object)


def _covering_band_native_loop(
    frame: _Frame, band, mask, resolution, grid: str, agg: str
) -> list:
    """``_covering_band``'s per-pixel pass for a raster in another CRS (any geotransform): each valid pixel's
    candidate cells come from its outline moved to the grid's CRS (``_moved_outlines``, ``_padded_boxes``), and the
    weight is the GEOS overlap of the pixel quad and the cell moved into the raster's CRS
    (``_native_rings``), over the pixel's area. quadbin and BNG keep the interior fast path: a pixel whose four
    corners fall in one cell has weight 1 (the cell's moved edge can bend into the pixel only by its chord tolerance).
    """
    import shapely

    from databricks.labs.gbx.pyrx.core.tessellate import _cell_is_valid

    ys, xs = np.nonzero(mask)
    if ys.size == 0:
        return []
    ox, oy = frame.gt[0], frame.gt[3]
    cols = xs[:, None] + np.array([0, 1, 1, 0])
    rows = ys[:, None] + np.array([0, 0, 1, 1])
    nx_, ny_ = frame.native_xy(cols, rows)
    gx, gy = _move(frame.to_grid, nx_, ny_)
    vals = band[ys, xs]
    index: dict = {}  # raw cell -> its index in the pairs
    pair_cell, pair_pix = [], []
    interior = np.zeros(ys.size, dtype=bool)
    fixed_cell = np.empty(0, dtype=np.intp)
    if grid in _COVERING_FAST_PATH_EXACT:
        corner = _corner_cells(gx.ravel(), gy.ravel(), resolution, grid).reshape(-1, 4)
        interior = (corner == corner[:, :1]).all(axis=1) & (corner[:, 0] >= 0)
        fixed_cell = np.fromiter(
            (index.setdefault(int(c), len(index)) for c in corner[interior, 0]),
            dtype=np.intp,
            count=int(interior.sum()),
        )  # vectorscan: ok (per interior pixel, a dict lookup)
    edge = np.flatnonzero(~interior)
    out_x, out_y, out_pad, out_pole = _moved_outlines(
        frame, cols[edge].astype("float64"), rows[edge].astype("float64")
    )
    for k, i in enumerate(
        edge
    ):  # vectorscan: ok (covering per-pixel, as _covering_band)
        boxes = _padded_boxes(
            out_x[k], out_y[k], out_pad[k], out_pole[k], frame.geographic
        )
        for cell in _box_cells(boxes, resolution, grid):
            if _cell_is_valid(cell, grid):
                pair_cell.append(index.setdefault(cell, len(index)))
                pair_pix.append(i)
    raw = list(index)
    pair_cell = np.asarray(pair_cell, dtype=np.intp)
    pair_pix = np.asarray(pair_pix, dtype=np.intp)
    w = np.empty(0)
    if pair_cell.size:
        cell_polys = _native_polygons(frame, raw, ox, oy)
        pixels = shapely.polygons(np.stack([nx_ - ox, ny_ - oy], axis=-1))
        area = shapely.area(
            shapely.intersection(cell_polys[pair_cell], pixels[pair_pix])
        )
        w = area / frame.px_area
        keep = w > 0.0
        pair_cell, pair_pix, w = pair_cell[keep], pair_pix[keep], w[keep]
    cell = np.concatenate([fixed_cell, pair_cell])
    if cell.size == 0:
        return []
    weights = np.concatenate([np.ones(fixed_cell.size), w])
    values = np.concatenate([vals[interior], vals[pair_pix]])
    uniq, measures = _grouped_measures(cell, values, agg, weights=weights)
    return [
        {"cellID": _native_id(raw[c], grid), "measure": m}
        for c, m in zip(uniq.tolist(), measures)  # vectorscan: ok (per-cell)
    ]


def _covering_band_native(frame: _Frame, band, mask, resolution, grid: str, agg: str):
    """Covering in the raster's own CRS: the vectorised H3 pass where ``_use_h3_vectorised`` holds, else the
    per-pixel pass."""
    if grid == "h3" and _use_h3_vectorised(
        frame.ds, resolution, frame.gt, area=frame.pixel_area_m2()
    ):
        return _covering_band_h3_native(frame, band, mask, int(resolution), agg)
    return _covering_band_native_loop(frame, band, mask, resolution, grid, agg)


def _centroid_band_native(frame: _Frame, band, mask, resolution, grid: str, agg: str):
    """Centroid assignment in the raster's own CRS: each valid pixel's centre (the full geotransform) is moved to the
    grid's CRS and binned there, as ``tessellate._assign_pixels`` does; a BNG centre outside the grid drops.
    """
    ys, xs = np.nonzero(mask)
    if ys.size == 0:
        return []
    gx, gy = _move(frame.to_grid, *frame.native_xy(xs + 0.5, ys + 0.5))
    vals = band[ys, xs]
    if grid == "h3":
        cids = _h3_cells(gx, gy, resolution)
    elif grid == "quadbin":
        from databricks.labs.gbx.pygx import _quadbin

        cids = _quadbin.point_as_cell_vec(gx, gy, resolution)
    else:
        cids = _bng_cells(gx, gy, resolution)
        keep = _bng.is_valid_vec(cids, resolution)
        cids, vals = cids[keep], vals[keep]
        if cids.size == 0:
            return []
    uniq, measures = _grouped_measures(cids, vals, agg)
    return [
        {"cellID": _native_id(c, grid), "measure": m}
        for c, m in zip(uniq.tolist(), measures)  # vectorscan: ok (per-cell)
    ]


def _complete_coverage_native(
    band_result: list, frame: _Frame, resolution, grid: str, agg: str
):
    """``_complete_coverage_pass`` in the raster's own CRS: a candidate cell (from the moved outline of the raster,
    padded by one pixel) that is valid and not in ``band_result`` is added, empty, when the cell moved into the
    raster's CRS has a positive-area overlap with the raster's outline there."""
    import shapely
    from shapely.geometry import Polygon

    from databricks.labs.gbx.pyrx.core.tessellate import _cell_is_valid

    ds = frame.ds
    existing = {r["cellID"] for r in band_result}
    todo = [
        c
        for c in _box_cells(
            frame.window_boxes((0, ds.height - 1), (0, ds.width - 1)), resolution, grid
        )
        if _cell_is_valid(c, grid) and _native_id(c, grid) not in existing
    ]  # vectorscan: ok (per cell)
    if not todo:
        return band_result
    ox, oy = frame.gt[0], frame.gt[3]
    cx, cy = frame.native_xy(
        np.array([0, ds.width, ds.width, 0], dtype="float64"),
        np.array([0, 0, ds.height, ds.height], dtype="float64"),
    )
    outline = Polygon(zip(cx - ox, cy - oy))
    polys = _native_polygons(frame, todo, ox, oy)
    hit = shapely.intersects(polys, outline)
    hit[hit] = shapely.area(shapely.intersection(polys[hit], outline)) > 0.0
    empty_value = _EMPTY_STATE if agg == _PARTIAL else (0.0 if agg == "count" else None)
    return band_result + [
        {"cellID": _native_id(c, grid), "measure": empty_value}
        for c, h in zip(todo, hit)  # vectorscan: ok (per cell)
        if h
    ]


def _grid_native(
    ds, src_crs, resolution, grid: str, agg: str, coverage: str, assignment: str
):
    """Grid ``ds`` (its CRS ``src_crs``, not the grid's) on its own pixels: one list per band, as ``_run`` gives, or
    None when the native frame cannot hold the raster (``_NeedsWarp``) and the caller warps it instead.
    """
    frame = _Frame(ds, src_crs, grid)
    if not (math.isfinite(frame.px_area) and frame.px_area > 0.0):
        return None
    try:
        out = []
        for bi in range(1, ds.count + 1):
            band = ds.read(bi).astype("float64")
            mask = band_valid_mask(ds, bi)  # False = NoData, masked or transparent
            if assignment == "centroid":
                result = _centroid_band_native(frame, band, mask, resolution, grid, agg)
            else:
                result = _covering_band_native(frame, band, mask, resolution, grid, agg)
            if coverage == "complete":
                result = _complete_coverage_native(result, frame, resolution, grid, agg)
            out.append(result)
        return out
    except _NeedsWarp:
        return None


def _foreign_crs(ds, crs):
    """The source CRS of an h3 / quadbin raster when it is not EPSG:4326 -- the raster's own, else the ``crs``
    override of a CRS-less raster -- or None (grid-native: 4326, or CRS-less with no override). The same test as
    ``_warp_to_4326_if_needed``."""
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    wgs84 = resolve_crs(4326)
    if ds.crs is not None:
        return None if ds.crs == wgs84 else ds.crs
    if crs is None:
        return None
    override = resolve_crs(crs)
    return None if override == wgs84 else override


def _stamp_crs_bytes(ds, target_crs) -> bytes:
    """Return GTiff bytes identical to ``ds`` but with ``target_crs`` stamped —
    used to give a CRS-less-but-known raster its source CRS before warping. A
    source's own (per-dataset, non-alpha) mask band is written with it, so the
    warp still masks its pixels."""
    import rasterio
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core._nodata import has_own_mask
    from databricks.labs.gbx.pyrx.core.compression import match_alpha, source_profile

    own_mask = has_own_mask(ds)
    profile = source_profile(ds.profile, unless_jpeg=True)
    profile.update(driver="GTiff", crs=target_crs)
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                match_alpha(dst, ds.colorinterp)
                dst.write(ds.read())
                if own_mask:
                    dst.write_mask(ds.read_masks(1))
            return mf.read()


def _warp_to_4326_if_needed(ds, crs):
    """Return GTiff bytes reprojected to EPSG:4326, or ``None`` when no warp is
    needed (already 4326, or CRS-less with no override -> assume grid-native).

    The embedded raster CRS wins as the source; else the ``crs`` override (int
    SRID or CRS string) for a CRS-less raster; else None (never errors). The
    warp fill is NoData, so the reducers never aggregate it, and a source's own
    mask band still masks (``warp.reproject_to_crs(..., mark_fill_nodata=True)``)."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    _WGS84 = resolve_crs(4326)

    if ds.crs is not None:
        if ds.crs == _WGS84:
            return None  # already grid-native
        return warp.reproject_to_crs(
            ds, "EPSG:4326", resampling="nearest", mark_fill_nodata=True
        )

    # CRS-less raster: use the override if given, else assume grid-native (4326).
    if crs is None:
        return None
    override = resolve_crs(crs)
    if override == _WGS84:
        return None  # override says it is already 4326
    stamped = _stamp_crs_bytes(ds, override)
    with MemoryFile(stamped) as mf, open_bytes(mf) as stamped_ds:
        return warp.reproject_to_crs(
            stamped_ds, "EPSG:4326", resampling="nearest", mark_fill_nodata=True
        )


def check_arguments(
    resolution, grid: str, agg: str, coverage: str, assignment: str, grid_conf=None
) -> None:
    """Raise, naming the value, for an argument ``raster_to_grid`` cannot take. A function calls this before it
    opens the tile, so a parameter error never hides behind a tile that is empty or cannot be read.
    """
    _validate_resolution(resolution, grid)
    if agg not in _AGGS:
        raise ValueError(f"unknown agg {agg!r}; expected one of {_AGGS}")
    if coverage not in _COVERAGES:
        raise ValueError(f"coverage must be one of {_COVERAGES}; got {coverage!r}")
    if assignment not in _ASSIGNMENTS:
        raise ValueError(
            f"assignment must be one of {_ASSIGNMENTS}; got {assignment!r}"
        )
    if grid == "custom":
        from databricks.labs.gbx.pygx import _custom

        max_res = _custom.conf_from_row(grid_conf).max_resolution
        if resolution < 0 or resolution > max_res:
            raise ValueError(
                f"raster->custom: resolution ({resolution}) is outside "
                f"[0, {max_res}] for this grid conf"
            )


def measure_of(stat: str, state: tuple):
    """The measure of ONE cell for ``stat``, from its partial state ``(count, sum, m2, min, max)``: the sum over the
    count, M2 over the count (the population variance), its square root, or a field as it is. A covered cell that holds no
    valid pixel (a ``None`` sum) is NULL for every statistic but ``count``, which is 0.0. The SQL projection of the
    Unity Catalog table functions (the manifest's ``tvf_select``) is this function, and a test holds them equal.
    """
    n, s, m2, lo, hi = state
    if stat == "count":
        return n
    if s is None:
        return None
    if stat == "avg":
        return s / n
    if stat == "sum":
        return s
    if stat == "variance":
        return m2 / n
    if stat == "stddev":
        return math.sqrt(m2 / n)
    if stat == "min":
        return lo
    if stat == "max":
        return hi
    raise ValueError(f"{stat!r} does not split; expected one of {PARTIAL_STATS}")


def raster_to_grid(
    ds,
    resolution: int,
    grid: str,
    agg: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    crs=None,
    grid_conf=None,
) -> list:
    """Aggregate raster pixel values into discrete-global-grid cells, per band: :func:`raster_to_grid_partial`'s pass
    with the statistic taken from each cell's state (:func:`measure_of`), or, for the median, its own pass. The rest of
    this note is :func:`_raster_to_grid`'s.

    Returns one list per band of ``{"cellID", "measure"}``.
    """
    check_arguments(resolution, grid, agg, coverage, assignment, grid_conf)
    if agg == "median":
        return _raster_to_grid(
            ds, resolution, grid, agg, coverage, assignment, crs, grid_conf
        )
    return [
        [{"cellID": c["cellID"], "measure": measure_of(agg, c["state"])} for c in band]
        for band in _partial_states(
            ds, resolution, grid, coverage, assignment, crs, grid_conf
        )
    ]


def raster_to_grid_single(
    ds,
    resolution: int,
    grid: str,
    stat: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    crs=None,
    grid_conf=None,
) -> list:
    """ONE statistic of every cell, per band, computed on its own: the pass of ``stat`` alone, with no count, sum, M2, minimum
    or maximum made for the others. It returns what :func:`raster_to_grid` returns (a test holds the two equal, bit for bit,
    for every statistic, coverage and assignment); the Unity Catalog table functions call this one, because a statistic read
    from the partial state costs the whole state, about twice a sum or a count.
    """
    check_arguments(resolution, grid, stat, coverage, assignment, grid_conf)
    return _raster_to_grid(
        ds, resolution, grid, stat, coverage, assignment, crs, grid_conf
    )


def _partial_states(ds, resolution, grid, coverage, assignment, crs, grid_conf):
    """One list per band of ``{"cellID", "state"}`` (the state is the tuple of :data:`PARTIAL_FIELDS`)."""
    return [
        [{"cellID": c["cellID"], "state": c["measure"]} for c in band]
        for band in _raster_to_grid(
            ds, resolution, grid, _PARTIAL, coverage, assignment, crs, grid_conf
        )
    ]


def raster_to_grid_partial(
    ds,
    resolution: int,
    grid: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    crs=None,
    grid_conf=None,
) -> list:
    """The mergeable state of every cell, per band, in one pass over the pixels: one list per band of ``{"cellID",
    "count", "sum", "m2", "min", "max"}`` (see :data:`PARTIAL_FIELDS`). The arguments are those of
    :func:`raster_to_grid`. Two tiles of one grid merge by summing ``count`` and ``sum``, taking the least ``min`` and
    greatest ``max``, and adding to the ``m2`` of each tile the count times the squared distance of its mean from the merged
    mean (Chan's parallel update), so a statistic over many tiles needs no pixel.
    """
    check_arguments(resolution, grid, "count", coverage, assignment, grid_conf)
    return [
        [{"cellID": c["cellID"], **dict(zip(PARTIAL_FIELDS, c["state"]))} for c in band]
        for band in _partial_states(
            ds, resolution, grid, coverage, assignment, crs, grid_conf
        )
    ]


def _raster_to_grid(
    ds,
    resolution: int,
    grid: str,
    agg: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    crs=None,
    grid_conf=None,
) -> list:
    """Aggregate raster pixel values into discrete-global-grid cells, per band.

    H3 and quadbin cells are in EPSG:4326 lon/lat. A raster that carries another CRS
    is gridded on its own pixels (``_grid_native``), never with its easting/northing
    read as lon/lat; only one the native frame cannot hold is warped to EPSG:4326
    (nearest) first.

    Never errors on an absent CRS: a raster with no CRS (and no ``crs`` override)
    is assumed already-in-grid-native (4326) and processed unchanged.

    Args:
        ds:         An open rasterio ``DatasetReader``.
        resolution: Grid resolution (H3 0..15; quadbin 0..20; BNG index
                    +/-1..+/-6 or a resolutionMap string key e.g. ``"1km"``;
                    custom int in ``[0, conf.max_resolution]``).
        grid:       ``"h3"``, ``"quadbin"``, ``"bng"`` or ``"custom"``.
        agg:        One of ``"avg"``, ``"count"``, ``"min"``, ``"max"``,
                    ``"median"``, ``"sum"``, ``"variance"``, ``"stddev"``.
        coverage:   ``"complete"`` (default) -- also emit covered-but-empty
                    cells (measure=None, or 0.0 for count); ``"sparse"`` -- emit
                    only cells with at least one valid pixel (the original
                    behaviour).
        assignment: ``"centroid"`` (default) -- bin each valid pixel to the cell
                    containing its centroid; ``"covering"`` -- distribute each
                    pixel's value across all overlapping cells, weighted by
                    intersection-area fraction.
        crs:        Optional source-CRS override (int SRID or CRS string) for a
                    CRS-less-but-known raster. Ignored when the raster already
                    carries a CRS. When neither is set, grid-native is assumed.
                    Not used for ``grid="custom"`` (always grid-native).
        grid_conf:  Grid spec struct (dict or Row) for ``grid="custom"``.
                    Required when ``grid="custom"``; ignored otherwise.

    Returns:
        One list per band; each is a list of ``{"cellID": id, "measure":
        float|None}`` (``None`` only for covered-but-empty cells in ``complete``
        mode for non-count aggregates). ``cellID`` is an ``int`` (Long) for
        H3/quadbin/custom and a formatted BNG ``str`` (e.g. ``"TQ3080"``) for
        BNG.
    """
    if grid == "custom":
        return _raster_to_custom(
            ds,
            int(resolution),
            agg,
            coverage=coverage,
            assignment=assignment,
            grid_conf=grid_conf,
        )

    if grid == "bng":
        return _raster_to_bng(
            ds,
            _bng.get_resolution(resolution),
            agg,
            coverage=coverage,
            assignment=assignment,
            crs=crs,
        )

    resolution = int(resolution)
    if grid == "h3":
        encode = _h3_cells
    else:
        from databricks.labs.gbx.pygx import (
            _quadbin,  # lazy: only the quadbin grid needs it
        )

        encode = _quadbin.point_as_cell_vec

    def _run(work_ds):
        gt = work_ds.transform.to_gdal()  # (c, a, b, f, d, e) GDAL geotransform
        out = []
        for bi in range(1, work_ds.count + 1):
            band = work_ds.read(bi).astype("float64")
            mask = band_valid_mask(work_ds, bi)  # False = NoData, masked or transparent

            if assignment == "centroid":
                ys, xs = np.nonzero(mask)  # valid pixels only (matches mask==0 skip)
                if ys.size == 0:
                    band_result = []
                else:
                    x_off = xs + 0.5
                    y_off = ys + 0.5
                    lon = gt[0] + x_off * gt[1] + y_off * gt[2]
                    lat = gt[3] + x_off * gt[4] + y_off * gt[5]
                    vals = band[ys, xs]

                    cids = encode(lon, lat, resolution)
                    uniq, measures = _grouped_measures(cids, vals, agg)
                    band_result = [
                        # vectorscan: ok (per-cell)
                        {"cellID": int(cid), "measure": m}
                        for cid, m in zip(uniq.tolist(), measures)
                    ]
            else:  # covering
                band_result = _covering_band(
                    work_ds, bi, band, mask, resolution, grid, agg, gt
                )

            if coverage == "complete":
                band_result = _complete_coverage_pass(
                    band_result, work_ds, resolution, grid, agg
                )

            out.append(band_result)
        return out

    # A raster in another CRS is gridded on its own pixels. The embedded raster CRS
    # wins; else the `crs` override (for a CRS-less-but-known raster); else None ->
    # assume grid-native 4326 and process unchanged (never errors).
    src_crs = _foreign_crs(ds, crs)
    if src_crs is None:
        return _run(ds)
    native = _grid_native(ds, src_crs, resolution, grid, agg, coverage, assignment)
    if native is not None:
        return native
    # The native frame cannot hold it (a cell across the CRS's seam, or a point that
    # does not transform): warp to 4326 (nearest), as before.
    warped_bytes = _warp_to_4326_if_needed(ds, crs)
    from rasterio.io import MemoryFile

    with MemoryFile(warped_bytes) as mf, open_bytes(mf) as work_ds:
        return _run(work_ds)


def _raster_to_bng(
    ds,
    resolution: int,
    agg: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    crs=None,
) -> list:
    """BNG raster->grid: bin per pixel in EPSG:27700, render String ids.

    A raster in another CRS is gridded on its own pixels (``_grid_native``); only one
    the native frame cannot hold, or one with no CRS at all, is warped to EPSG:27700
    (nearest) first. Pixel centroids are mapped to BNG cell ids via ``pygx._bng.point_to_cell_id``;
    pixels whose cell falls outside GB are dropped via ``pygx._bng.is_valid``;
    ids are rendered to the user-facing BNG ``str`` via ``pygx._bng.format`` at
    the output boundary. Grouping stays Long-keyed (reuses ``_grouped_measures``).

    ``coverage``/``assignment`` behave identically to the h3/quadbin paths:
      * ``complete`` adds covered-but-empty cells (None / 0.0 for count)
      * ``covering`` distributes pixel values by area fraction

    ``crs`` is a source-CRS override for a CRS-less-but-known raster (stamped
    first, so a georeferenced-but-unlabeled raster grids in that CRS); ignored when
    the raster already carries a CRS.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp

    # A CRS-less raster whose true CRS is given via override -> stamp it first so
    # the warp to 27700 has a source CRS to project from.
    if ds.crs is None and crs is not None:
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs

        stamped = _stamp_crs_bytes(ds, resolve_crs(crs))
        with MemoryFile(stamped) as _mf, open_bytes(_mf) as _stamped_ds:
            return _raster_to_bng(
                _stamped_ds,
                resolution,
                agg,
                coverage=coverage,
                assignment=assignment,
                crs=None,
            )

    # Reproject to EPSG:27700 (nearest) unless already there. epsg may be None
    # for an undefined CRS -> warp (rasterio treats an unset src crs as an error
    # anyway; matching heavy, we assume a georeferenced source).
    from databricks.labs.gbx.core.crs import authority_srid_of

    already_bng = ds.crs is not None and authority_srid_of(ds.crs) == 27700

    def _run(work_ds):
        gt = work_ds.transform.to_gdal()
        out = []
        for bi in range(1, work_ds.count + 1):
            band = work_ds.read(bi).astype("float64")
            mask = band_valid_mask(work_ds, bi)  # False = NoData, masked or transparent

            if assignment == "centroid":
                ys, xs = np.nonzero(mask)  # valid pixels only
                if ys.size == 0:
                    band_result = []
                else:
                    x_off = xs + 0.5
                    y_off = ys + 0.5
                    e = gt[0] + x_off * gt[1] + y_off * gt[2]
                    n = gt[3] + x_off * gt[4] + y_off * gt[5]
                    vals = band[ys, xs]

                    cids = _bng_cells(e, n, resolution)
                    # Drop out-of-GB pixels (is_valid) BEFORE grouping so a cell is
                    # only emitted for >=1 valid, in-GB pixel (sec 2.6). Vectorized.
                    keep = _bng.is_valid_vec(cids, resolution)
                    cids = cids[keep]
                    vals = vals[keep]
                    if cids.size == 0:
                        band_result = []
                    else:
                        uniq, measures = _grouped_measures(cids, vals, agg)
                        band_result = [
                            {"cellID": _bng.format(int(cid)), "measure": m}
                            for cid, m in zip(
                                uniq.tolist(), measures
                            )  # vectorscan: ok (per-cell)
                        ]
            else:  # covering
                band_result = _covering_band(
                    work_ds, bi, band, mask, resolution, "bng", agg, gt
                )

            if coverage == "complete":
                band_result = _complete_coverage_pass(
                    band_result, work_ds, resolution, "bng", agg
                )

            out.append(band_result)
        return out

    if already_bng:
        return _run(ds)
    if ds.crs is not None:
        # Gridded on its own pixels (the override's CRS was stamped above).
        native = _grid_native(ds, ds.crs, resolution, "bng", agg, coverage, assignment)
        if native is not None:
            return native
    # The native frame cannot hold it, or the raster has no CRS at all: warp to
    # 27700 (nearest). The warp fill is NoData and a source's own mask still masks.
    warped_bytes = warp.reproject_to_srid(
        ds, 27700, resampling="nearest", mark_fill_nodata=True
    )
    with MemoryFile(warped_bytes) as mf, open_bytes(mf) as work_ds:
        return _run(work_ds)


def _raster_to_custom(
    ds,
    resolution: int,
    agg: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    grid_conf=None,
) -> list:
    """Custom-grid raster->grid: operate in grid-native coords, emit Long cell ids.

    Custom grids have user-defined extents and cell sizes (via ``CustomGridConf``).
    The raster is assumed to already be in the grid's native CRS — no reprojection
    is performed (mirrors how heavy ``RST_Custom_RasterToGrid`` operates when the
    raster CRS matches the grid SRID, which is the common parity-test scenario).

    Custom cell IDs are BIGINT (positive signed int64 by construction:
    ``cell_pos | (resolution << 56)``; max is well below 2^63).

    ``coverage``/``assignment`` behave identically to the h3/quadbin/bng paths.
    """
    from databricks.labs.gbx.pygx import _custom

    conf = _custom.conf_from_row(grid_conf)

    # Validate resolution against the conf-derived maximum.
    if resolution < 0 or resolution > conf.max_resolution:
        raise ValueError(
            f"raster->custom: resolution ({resolution}) is outside "
            f"[0, {conf.max_resolution}] for this grid conf"
        )

    def _run(work_ds):
        gt = work_ds.transform.to_gdal()  # (c, a, b, f, d, e) GDAL geotransform
        out = []
        for bi in range(1, work_ds.count + 1):
            band = work_ds.read(bi).astype("float64")
            mask = band_valid_mask(work_ds, bi)  # False = NoData, masked or transparent

            if assignment == "centroid":
                ys, xs = np.nonzero(mask)
                if ys.size == 0:
                    band_result = []
                else:
                    x_off = xs + 0.5
                    y_off = ys + 0.5
                    px = gt[0] + x_off * gt[1] + y_off * gt[2]
                    py = gt[3] + x_off * gt[4] + y_off * gt[5]
                    vals = band[ys, xs]

                    # Scalar loop (like BNG): map each valid pixel centroid to a
                    # custom cell id; -1 sentinel for out-of-bounds pixels.
                    # MUST use explicit `is None` check — cell id 0 is valid at
                    # resolution 0 and `0 or -1` would incorrectly map it to -1.
                    cids_raw = np.array(
                        [
                            (  # vectorscan: ok (no array API for custom)
                                c
                                if (
                                    c := _custom.point_to_cell_id_or_none(
                                        conf, float(x), float(y), resolution
                                    )
                                )
                                is not None
                                else -1
                            )
                            for x, y in zip(px, py)
                        ],
                        dtype="int64",
                    )
                    keep = cids_raw != -1
                    cids = cids_raw[keep]
                    vals = vals[keep]

                    if cids.size == 0:
                        band_result = []
                    else:
                        uniq, measures = _grouped_measures(cids, vals, agg)
                        band_result = [
                            {"cellID": int(cid), "measure": m}
                            for cid, m in zip(uniq.tolist(), measures)
                            # vectorscan: ok (per-cell)
                        ]
            else:  # covering
                band_result = _covering_band(
                    work_ds, bi, band, mask, resolution, "custom", agg, gt, conf=conf
                )

            if coverage == "complete":
                band_result = _complete_coverage_pass(
                    band_result, work_ds, resolution, "custom", agg, conf=conf
                )

            out.append(band_result)
        return out

    # No CRS reprojection for custom: the raster is expected in grid-native coords.
    return _run(ds)
