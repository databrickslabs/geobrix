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
  by overlap area, so ``count`` is in source pixels. A cell across the CRS's own seam
  is cut there. A lon/lat CRS has no seam: its longitude runs on by turns, so a moved
  cell is unwrapped, closed over a pole it holds, and repeated by turns over the
  raster (``_periodic_rings``); a cylindrical CRS the raster reaches past the seam of
  runs on the same way. An EPSG:4326 raster that is global, crosses 180 or holds
  longitudes outside -180..180 takes this frame too (``_lonlat_needs_frame``); a
  regional one keeps the planar lon/lat pass. Only a raster with a point that does not
  transform is warped to the grid's CRS (nearest) first.
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
from functools import partial

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


def _bng_inside(e, n, cids, resolution) -> np.ndarray:
    """Whether each EPSG:27700 point (``e``, ``n``) with its cell id ``cids`` is inside BNG: a valid id, and no
    negative easting or northing (the encoder truncates a point up to 100 km west or south of the grid's origin into
    the first column or row, a cell it is not in)."""
    e, n = np.asarray(e, dtype="float64"), np.asarray(n, dtype="float64")
    return _bng.is_valid_vec(cids, resolution) & (e >= 0.0) & (n >= 0.0)


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


def _fast_cell_encoder(grid: str, resolution, conf=None):
    """The interior fast-path point->cell encoder of ``_covering_band`` for exact-geometry grids: the RAW cell id
    (same type as polyfill candidates) of a point, or None when the point is outside the grid; None (no fast path)
    for h3 and any grid not in ``_COVERING_FAST_PATH_EXACT``. That set is read at call time, so tests can
    monkeypatch it to frozenset() to force the overlap path on all grids."""
    if grid not in _COVERING_FAST_PATH_EXACT:
        return None
    if grid == "custom":
        from databricks.labs.gbx.pygx import _custom as _custom_fp

        def _fast_cell(cx, cy):
            return _custom_fp.point_to_cell_id_or_none(conf, cx, cy, resolution)

    elif grid == "quadbin":
        from databricks.labs.gbx.pygx import _quadbin as _qb_fp

        def _fast_cell(cx, cy):
            # a corner beyond quadbin's latitudes is in no cell (point_as_cell would clamp it into the
            # first or last row): the pixel takes the overlap path, which counts only its part inside
            if not _qb_fp._LAT_MIN <= cy <= _qb_fp._LAT_MAX:
                return None
            return _qb_fp.point_as_cell(cx, cy, resolution)

    else:  # bng

        def _fast_cell(cx, cy):
            if cx < 0.0 or cy < 0.0:
                return None  # west or south of the grid (the encoder would truncate it into a cell)
            cid = _bng.point_to_cell_id(cx, cy, resolution)
            return cid if _bng.is_valid(cid) else None

    return _fast_cell


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

    _fast_cell = _fast_cell_encoder(grid, resolution, conf)

    ys, xs = np.nonzero(mask)
    if ys.size == 0:
        return []

    acc: dict = {}  # raw_cell_id -> [(value, weight)]
    geoms: dict = {}  # raw_cell_id -> its polygon, built once per band

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

        if grid in ("h3", "bng"):
            cands = _pixel_candidates(pixel_poly, resolution, grid)
        else:
            cands = _polyfill_cells(pixel_poly, resolution, grid, conf=conf)
        for cell in cands:
            if not _cell_is_valid(cell, grid, conf=conf):
                continue
            cell_poly = geoms.get(cell)
            if cell_poly is None:
                cell_poly = geoms[cell] = _cell_geom(cell, grid, conf=conf)
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


def _pixel_candidates(pixel_poly, resolution, grid: str) -> list:
    """The cells (the raw ids ``_cell_geom`` takes) that may overlap one pixel of the planar pass: the polyfill's,
    then those it missed from ``_box_cells`` (h3: the cells around the pixel's box; BNG: the lattice squares under
    it). Both polyfills can miss a cell: h3's overlap mode returned only a neighbour of a 30 m pixel inside a res-9
    cell, and BNG's flood fill missed 30 of the 240 res-4 cells under a 2 km box. The polyfill's cells keep their
    order, so a pixel's pairs come in the order they did."""
    from databricks.labs.gbx.pyrx.core.tessellate import _polyfill_cells

    found = list(_polyfill_cells(pixel_poly, resolution, grid))
    seen = set(found)
    if grid == "h3":
        import h3

        extra = (
            h3.int_to_str(c) for c in _box_cells([pixel_poly.bounds], resolution, grid)
        )
    else:
        extra = _bng_box_cells(pixel_poly.bounds, resolution)
    for cell in extra:  # vectorscan: ok (a pixel's few cells)
        if cell not in seen:
            seen.add(cell)
            found.append(cell)
    return found


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
    geotransform ``gt``) over the valid pixels of ``band``: the pairs of ``_box_pairs``, grouped by cell. An H3 id is
    below 2**63, so an int id is already the signed Long ``tessellate._encode_cellid`` gives.
    """
    cell, w, vals = _box_pairs(vx, vy, band, mask, gt, px_area)
    if cell.size == 0:
        return []
    uniq, measures = _grouped_measures(cell, vals, agg, weights=w)
    return [
        {"cellID": ids[c], "measure": m}
        for c, m in zip(uniq.tolist(), measures)  # vectorscan: ok (per-cell)
    ]


def _box_pairs(vx, vy, band, mask, gt, px_area) -> tuple:
    """``(ring, weight, value)`` of every (ring, valid pixel) pair of the rings' bounding windows (rings ``vx``, ``vy``:
    rings x M, in the frame of the north-up geotransform ``gt``), weighted by ``_box_overlap_area`` / ``px_area`` and
    kept above ``_COVER_WEIGHT_FLOOR``."""
    height, width = mask.shape
    bounds = np.stack([vx.min(1), vy.min(1), vx.max(1), vy.max(1)], axis=1)
    cell, rows, cols = _cell_pixel_pairs(bounds, gt, height, width)
    ok = mask[rows, cols]
    cell, rows, cols = cell[ok], rows[ok], cols[ok]
    if cell.size == 0:
        return cell, np.empty(0), np.empty(0)
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
    return (
        cell[keep],
        w[keep],
        band[rows[keep], cols[keep]].astype("float64"),
    )


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
# The most points an edge is walked with (a bend this sharp at this pixel size is left at the cap).
_NATIVE_EDGE_MAX_POINTS = 4096
# A moved edge whose midpoint is more than this fraction of its chord off the chord may jump across the raster CRS's
# seam (about 0.5 there); ``_jumping`` then decides, as a sharp bend near a pole can pass it too. Pixel size plays no
# part.
_SEAM_DEVIATION = 0.25
# Halvings of a nominated edge that tell a jump from a sharp bend or a stretch (``_jumping``).
_SEAM_BISECTIONS = 50
# Degrees of the raster CRS's own longitude kept clear of the seam when a cell is cut on a traced seam
# (``_traced_pieces``): above the bow of a straight step between two traced points; ~0.1 m.
_SEAM_GAP_TRACED_DEG = 1e-6
# Lines across a cell along which its seam is traced, at first and at most (``_trace_cut``).
_SEAM_TRACE_LINES = 257
_SEAM_TRACE_MAX_LINES = 4097
# Degrees kept clear on each side of the seam meridian where a cell is cut.
_SEAM_GAP_DEG = 1e-9
# The latitude that closes a cell around a pole before it is cut, for a CRS in which the pole is not a point.
_POLE_CAP_LAT = 89.99999
# Valid pixels the per-pixel pass moves at a time, and the most (cell, pixel) pairs it sends to GEOS in one batch.
_NATIVE_LOOP_PIXELS = 16_384
_NATIVE_LOOP_PAIRS = 100_000
# Cells x vertices x vertices one vectorised chunk may hold (``_ring_supporting_edges``): about 20 MB per array.
_NATIVE_RING_BUDGET = 2_500_000
# Pixels between two points of a window outline that is moved to the grid's CRS (at least 20 points per side).
_OUTLINE_STEP_PIXELS = 16


def _wrap(d, period):
    """``d`` written within half a ``period`` of 0 (unchanged when ``period`` is None)."""
    if not period:
        return d
    return (d + 0.5 * period) % period - 0.5 * period


class _NeedsWarp(Exception):
    """The native frame cannot hold the raster: a point does not transform."""


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
    first-seen order: int ids for h3 (``_h3_box_cells``), the ``_polyfill_cells`` ids for quadbin, the lattice cells of
    BNG (``_bng_box_cells``)."""
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
    elif grid == "bng":
        parts = (
            _bng_box_cells(b, resolution) for b in boxes
        )  # vectorscan: ok (per box)
    else:
        parts = (
            _polyfill_cells(_box(w, s, e, n), resolution, grid) for w, s, e, n in boxes
        )  # vectorscan: ok (per box)
    return list(dict.fromkeys(chain.from_iterable(parts)))


def _bng_box_cells(box, resolution) -> list:
    """Every BNG cell (Long id) that a box of EPSG:27700 touches: the cells are a lattice of the resolution's edge from
    the grid's origin, so the box's lattice squares are listed, by their centres, and the ones outside the grid drop.
    (``_bng.polyfill``, a flood fill of the cells whose centres a buffered box holds, misses some: measured, 30 of the
    240 res-4 cells under a 2 km pixel's box.)"""
    edge = float(_bng.get_edge_size(resolution))
    w, s, e, n = box
    # the lattice indices, clamped to the grid's: the last column and row start at easting 700000 / northing 1300000
    i0, i1 = max(math.floor(w / edge), 0), min(
        math.floor(e / edge), int(700000 // edge)
    )
    j0, j1 = max(math.floor(s / edge), 0), min(
        math.floor(n / edge), int(1300000 // edge)
    )
    if i0 > i1 or j0 > j1:
        return []
    xs = (np.arange(i0, i1 + 1) + 0.5) * edge
    ys = (np.arange(j0, j1 + 1) + 0.5) * edge
    cx, cy = np.meshgrid(xs, ys)
    ids = _bng.point_to_cell_id_vec(cx.ravel(), cy.ravel(), resolution)
    return [
        int(c) for c in ids[_bng.is_valid_vec(ids, resolution)]
    ]  # vectorscan: ok (per cell)


def _grid_rings(cells, grid: str) -> tuple:
    """``(x, y)`` vertex arrays, each cells x M, of the cells in the grid's CRS: the H3 hexagons of ``_h3_hexagons``
    (lon, lat), the lon/lat box of a quadbin cell (its east-west edges split under 90 degrees), the EPSG:27700 square
    of a BNG cell.
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
    k = (
        1
        if grid == "bng" or not b.size
        else max(1, math.ceil(float((e - w).max()) / 90.0))
    )
    if k == 1:
        return np.stack([w, e, e, w], axis=1), np.stack([s, s, n, n], axis=1)
    # a quadbin cell 90 degrees wide or more (z0, z1): its east-west edges are split into steps under 90 degrees, so
    # each runs east as it should (a step taken the short way round would turn back at 180)
    f = (np.arange(k) / k)[None, :]
    width = (e - w)[:, None]
    xs = np.concatenate(
        [w[:, None] + f * width, e[:, None], e[:, None] - f * width, w[:, None]], axis=1
    )
    ys = np.concatenate(
        [np.repeat(s[:, None], k + 1, axis=1), np.repeat(n[:, None], k + 1, axis=1)],
        axis=1,
    )
    return xs, ys


class _Frame:
    """A raster in its own CRS as a grid sees it: the transforms between the two CRSs and the raster's pixel geometry.

    ``gt`` is the raster's geotransform (rotation terms included), ``px_area`` / ``px_side`` its pixel's area and side
    in the raster's units, ``poles`` the ``(col, row)`` in the raster of each pole that transforms (h3 / quadbin).
    """

    def __init__(self, ds, src_crs, grid: str):
        from databricks.labs.gbx.core.crs import (
            get_operation_transformer,
            get_transformer,
            to_pyproj_crs,
        )

        self.ds, self.grid, self.src_crs = ds, grid, src_crs
        self._pcrs = pcrs = to_pyproj_crs(src_crs)
        self.seam = (
            None  # the seam's longitude, found when a cell crosses it (``_jumping``)
        )
        self.geographic = grid != "bng"
        grid_crs = 27700 if grid == "bng" else 4326
        # cells move into the raster's CRS by the inverse of the operation that moves pixels to the grid's: PROJ can
        # pick different operations for the two directions (measured: 122 m apart between EPSG:27700 and a PROJ-string
        # Mercator on +datum=WGS84), and a cell and its pixels must agree
        self.gt = gt = ds.transform.to_gdal()
        cols, rows = self._probe(ds)
        xs = gt[0] + cols[:4] * gt[1] + rows[:4] * gt[2]
        self.x_range = (float(xs.min()), float(xs.max()))
        forward = get_transformer(src_crs, grid_crs)
        # a lon/lat raster CRS has no seam: its longitude runs on by whole turns (``period``, in its own unit), so a
        # moved cell is unwrapped and repeated by turns over the raster's x range (``_periodic_rings``); a projected
        # raster that reaches past its CRS's seam by a constant shift (a cylindrical CRS) repeats its cells the same way
        self.lonlat_src = bool(pcrs.is_geographic)
        if self.lonlat_src:
            unit = pcrs.axis_info[0].unit_conversion_factor if pcrs.axis_info else 0.0
            self.period = 2.0 * math.pi / unit if unit else 360.0
        else:
            self.period = self._seam_period(forward, ds, gt)
        self.pinned = not self._round_trips(forward, ds, gt, pcrs)
        if self.pinned:
            forward = get_operation_transformer(
                src_crs, grid_crs, self._lonlat_area(ds)
            )
        self.to_grid = forward.transform
        self.to_native = partial(forward.transform, direction="INVERSE")
        # the seam's longitude in the raster CRS's own lon/lat (``_jumping``); that lon/lat and the meridian-cut
        # choice are built on the first cell across the seam (``to_src_lonlat``, ``meridian_cut``)
        self.seam_src, self._src_lonlat, self._meridian_cut = None, False, None
        self.fn = f"gbx_rst_{grid}_rastertogrid"  # the function, for a message (``_grid_native`` names it)
        self.px_area = abs(gt[1] * gt[5] - gt[2] * gt[4])
        self.px_side = math.sqrt(self.px_area)
        self.poles = {}
        # in a lon/lat raster CRS a pole is a row, not a point: an outline never winds around it
        if self.geographic and not self.lonlat_src:
            inv = ~ds.transform
            for sign in (1, -1):
                x, y = self.to_native(0.0, 90.0 * sign)
                if math.isfinite(x) and math.isfinite(y):
                    self.poles[sign] = inv * (x, y)

    @property
    def to_src_lonlat(self):
        """The raster CRS's own lon/lat of its points (the inverse projection to its geodetic CRS: no datum shift),
        where its seam is a meridian (``_traced_pieces``); None for a CRS with no geodetic CRS.
        """
        if self._src_lonlat is False:
            from databricks.labs.gbx.core.crs import get_transformer, resolve_crs

            geo = self._pcrs.geodetic_crs
            self._src_lonlat = (
                None
                if geo is None
                else get_transformer(self.src_crs, resolve_crs(geo.to_wkt())).transform
            )
        return self._src_lonlat

    @property
    def meridian_cut(self) -> bool:
        """Whether a lon/lat grid's longitude is the raster CRS's own (sampled at 9 points; a sphere or a WGS 84
        datum, however the CRS is written): then its seam is a meridian of the grid too, cut exactly
        (``_seam_pieces``). Else (another datum, or a planar grid) the seam is traced (``_traced_pieces``).
        """
        if self._meridian_cut is None:
            self._meridian_cut = False
            if self.geographic and self.to_src_lonlat is not None:
                lon = np.array([-150.0, -30.0, 90.0] * 3)
                lat = np.repeat([-60.0, 0.0, 60.0], 3)
                try:
                    x, y = _move(self.to_native, lon, lat)
                    back = np.asarray(self.to_src_lonlat(x, y)[0], dtype="float64")
                except _NeedsWarp:
                    back = None
                if back is not None and np.isfinite(back).all():
                    turn = (back - lon + 180.0) % 360.0 - 180.0
                    self._meridian_cut = bool((np.abs(turn) < 1e-9).all())
        return self._meridian_cut

    @property
    def cap_lat(self) -> float:
        """The latitude that closes a traced cell around a pole (``_traced_pieces``): halfway from the raster's
        farthest latitude to the pole, at most ``_POLE_CAP_LAT``. Near the pole a datum shift of metres swings the
        seam's meridian through degrees of the grid's longitude, past tracing; no pixel lies above the cap.
        """
        if getattr(self, "_cap_lat", None) is None:
            # the raster's farthest latitude is on its outline (unpadded: a padded box, grown for candidate cells, can
            # reach the pole and leave no room for the cap), unless it holds a pole
            ds = self.ds
            far = 90.0
            inside = any(
                0.0 <= c <= ds.width and 0.0 <= r <= ds.height
                for c, r in self.poles.values()
            )
            if not inside:
                try:
                    _, y = self._outline_points(
                        (0, ds.height - 1), (0, ds.width - 1), 0
                    )
                    far = float(np.abs(y).max())
                except _NeedsWarp:
                    pass
            self._cap_lat = min(_POLE_CAP_LAT, 0.5 * (far + 90.0))
        return self._cap_lat

    @staticmethod
    def _probe(ds):
        """Pixel coordinates of the raster's corners and centre."""
        return np.array([0.0, ds.width, ds.width, 0.0, ds.width / 2]), np.array(
            [0.0, 0.0, ds.height, ds.height, ds.height / 2]
        )

    def _round_trips(self, forward, ds, gt, pcrs) -> bool:
        """Whether ``forward``'s inverse brings the raster's corners and centre back within 1e-3 of a pixel, and never
        nearer than 1 cm (1e-7 degrees for a geographic CRS): millimetres of round-off in one Helmert shift are not two
        operations, and must not change the operation with the pixel size."""
        cols, rows = self._probe(ds)
        x, y = gt[0] + cols * gt[1] + rows * gt[2], gt[3] + cols * gt[4] + rows * gt[5]
        with np.errstate(all="ignore"):
            gx, gy = forward.transform(x, y)
            bx, by = forward.transform(gx, gy, direction="INVERSE")
            # a point that comes back whole turns away is the same point (``period``); in a lon/lat CRS a longitude
            # step counts by the cosine of the latitude (at a pole every longitude is the same point)
            dx = _wrap(np.asarray(bx) - x, self.period)
            if self.lonlat_src:
                dx = dx * np.cos(
                    np.clip(y * (math.tau / self.period), -math.pi, math.pi)
                )
            miss = np.hypot(dx, np.asarray(by) - y)
        side = math.sqrt(abs(gt[1] * gt[5] - gt[2] * gt[4]))
        if pcrs.is_geographic:
            floor = 1e-7
        else:
            metres = pcrs.axis_info[0].unit_conversion_factor if pcrs.axis_info else 1.0
            floor = 0.01 / (metres or 1.0)
        finite = np.isfinite(miss)
        return bool(not finite.any() or (miss[finite] <= max(1e-3 * side, floor)).all())

    @staticmethod
    def _seam_period(forward, ds, gt):
        """The x shift by which a projected raster's points past its CRS's seam come back from the grid's CRS (the
        CRS's width, e.g. 2 pi R for EPSG:3857), when every such point comes back by the same shift and on its own
        row: then the CRS runs on by that period (a cylindrical CRS). None when no point of the raster is past the
        seam, or the shift changes from row to row (a pseudo-cylindrical CRS, whose outside is off the globe). Pixel
        centres are sampled, never the raster's edge: an edge on the seam comes back on either side of it.
        """
        k = 9
        cols = np.tile((np.arange(k) + 0.5) * ds.width / k, k)
        rows = np.repeat((np.arange(k) + 0.5) * ds.height / k, k)
        cols = np.concatenate([cols, np.full(k, 0.5), np.full(k, ds.width - 0.5)])
        rows = np.concatenate([rows, np.tile((np.arange(k) + 0.5) * ds.height / k, 2)])
        x, y = gt[0] + cols * gt[1] + rows * gt[2], gt[3] + cols * gt[4] + rows * gt[5]
        with np.errstate(all="ignore"):
            gx, gy = forward.transform(x, y)
            bx, by = forward.transform(gx, gy, direction="INVERSE")
            d, dy = x - np.asarray(bx), np.asarray(by) - y
        side = math.sqrt(abs(gt[1] * gt[5] - gt[2] * gt[4]))
        ok = np.isfinite(d) & np.isfinite(dy)
        off = ok & (np.abs(d) > max(1e-3 * side, 1e-6))
        if not off.any():
            return None
        shift = np.abs(d[off])
        if (np.abs(shift - shift[0]) > 1e-9 * shift[0]).any() or (
            np.abs(dy[off]) > max(1e-3 * side, 1e-6)
        ).any():
            return None
        return float(shift[0])

    def _lonlat_area(self, ds):
        """The raster's lon/lat box (corners and centre), for picking one operation; None when it does not move."""
        from databricks.labs.gbx.core.crs import get_transformer

        cols, rows = self._probe(ds)
        t = ds.transform
        lon, lat = get_transformer(self.src_crs, 4326).transform(*(t * (cols, rows)))
        lon, lat = np.asarray(lon), np.asarray(lat)
        if not (np.isfinite(lon).all() and np.isfinite(lat).all()):
            return None
        return (float(lon.min()), float(lat.min()), float(lon.max()), float(lat.max()))

    def native_xy(self, cols, rows):
        """Raster CRS coordinates of the pixel coordinates ``cols``, ``rows`` (the full geotransform)."""
        gt = self.gt
        return gt[0] + cols * gt[1] + rows * gt[2], gt[3] + cols * gt[4] + rows * gt[5]

    def window_boxes(self, rows, cols, pad: int = 1) -> list:
        """``_padded_boxes`` of the pixel window ``rows`` x ``cols`` (``(first, last)``, inclusive) padded by ``pad``
        pixels: its outline walked every ``_OUTLINE_STEP_PIXELS`` pixels (at least 20 per side) and moved to the grid's
        CRS. A padded outline that leaves the CRS's domain (a raster whose edge is on the CRS's seam, as for
        ``+proj=robin +lon_0=180``) is taken unpadded: the boxes grow by the outline's bend and 1% all the same.
        """
        try:
            return self._window_boxes(rows, cols, pad)
        except _NeedsWarp:
            if not pad:
                raise
            return self._window_boxes(rows, cols, 0)

    def _window_boxes(self, rows, cols, pad: int) -> list:
        x, y, gpad, pole = self._outline_points(rows, cols, pad, full=True)
        return _padded_boxes(x[0], y[0], gpad[0], pole[0], self.geographic)

    def _outline_points(self, rows, cols, pad: int, full: bool = False):
        """The outline of the pixel window, padded by ``pad`` pixels, walked and moved to the grid's CRS: its points
        (``_moved_outlines``; all of what it returns with ``full``)."""
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
        out = _moved_outlines(self, cs[None, :], rs[None, :])
        return out if full else (out[0][0], out[1][0])

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


def _move_rings(frame: _Frame, gx, gy, wrap: bool = True) -> tuple:
    """``(x, y, jumps)``: rings of the grid's CRS (``gx``, ``gy``: rings x K, a shorter ring repeating its last vertex)
    moved into the raster's CRS. Every edge (in lon/lat taken the short way round, across the antimeridian or around a
    pole, unless ``wrap`` is off: rings already unwrapped, as the pieces of a cut) is walked with ``n`` points, ``n`` the
    least that keeps the chord within ``_NATIVE_EDGE_TOLERANCE`` of the
    shorter of the edge and the pixel side (the chord's distance from the bent edge falls as ``1 / n**2``; at most
    ``_NATIVE_EDGE_MAX_POINTS``). ``n`` depends on the edge alone, so two cells walk their shared edge with the same
    points and still tile. ``jumps`` marks a ring with an edge that jumps (``_jumping``, whatever the pixel size): the
    edge crosses the raster CRS's seam, and the ring's moved points mean nothing (``_native_rings`` cuts it). Raises ``_NeedsWarp`` for a point that does not transform.
    """
    k = gx.shape[1]
    nxt = np.roll(np.arange(k), -1)
    dx, dy = gx[:, nxt] - gx, gy[:, nxt] - gy
    if frame.geographic and wrap:
        dx = (dx + 180.0) % 360.0 - 180.0
    xs, ys = _move(frame.to_native, gx, gy)
    mx, my = _move(frame.to_native, gx + 0.5 * dx, gy + 0.5 * dy)
    nx_, ny_ = xs[:, nxt], ys[:, nxt]
    # a lon/lat raster CRS has no seam: each moved point is taken within half a turn of its edge's start
    period = frame.period if frame.lonlat_src else None
    if period:
        nx_ = xs + _wrap(nx_ - xs, period)
        mx = xs + _wrap(mx - xs, period)
    length = np.hypot(nx_ - xs, ny_ - ys)
    offset = np.hypot(mx - 0.5 * (xs + nx_), my - 0.5 * (ys + ny_))
    if period:
        jumps = np.zeros(gx.shape[0], dtype=bool)
    else:
        jumps = _jumping(frame, gx, gy, dx, dy, offset > _SEAM_DEVIATION * length).any(
            axis=1
        )
    # the bend: the moved midpoint's distance from the chord's midpoint, which bounds it also where the moved edge runs
    # unevenly along its chord; an edge whose moved midpoint is on the chord's line (within a quarter of the
    # tolerance) is straight, however unevenly it runs (a Mercator meridian), and needs no points
    tol = _NATIVE_EDGE_TOLERANCE * np.minimum(length, frame.px_side)
    with np.errstate(divide="ignore", invalid="ignore"):
        perp = np.where(
            length > 0.0,
            np.abs((nx_ - xs) * (my - ys) - (ny_ - ys) * (mx - xs)) / length,
            0.0,
        )
    dev = np.where(perp <= 0.25 * tol, perp, offset)
    with np.errstate(divide="ignore", invalid="ignore"):
        need = np.where(length > 0.0, np.ceil(np.sqrt(dev / tol)), 1.0)
    need = np.clip(need, 1.0, _NATIVE_EDGE_MAX_POINTS)
    need[jumps] = 1.0
    if (need == 1.0).all():
        return _unwrap_rings(xs, period), ys, jumps
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
    per_ring = need.astype(np.intp).sum(axis=1)
    m = int(per_ring.max())
    pos = np.arange(total) - np.repeat(np.cumsum(per_ring) - per_ring, per_ring)
    vx, vy = np.empty((gx.shape[0], m)), np.empty((gx.shape[0], m))
    vx[edge // k, pos], vy[edge // k, pos] = px, py
    col = np.minimum(np.arange(m)[None, :], per_ring[:, None] - 1)
    row = np.arange(gx.shape[0])[:, None]
    return _unwrap_rings(vx[row, col], period), vy[row, col], jumps


def _unwrap_rings(x, period):
    """The rings ``x`` (rings x M) with each step taken within half a ``period`` (unchanged when None): continuous
    from each ring's first vertex."""
    if not period:
        return x
    step = _wrap(np.diff(x, axis=1), period)
    return x[:, :1] + np.concatenate(
        [np.zeros((x.shape[0], 1)), np.cumsum(step, axis=1)], axis=1
    )


def _jumping(frame: _Frame, gx, gy, dx, dy, nominated) -> np.ndarray:
    """``nominated`` (rings x K edges) kept only where the moved edge truly jumps: each is bisected
    ``_SEAM_BISECTIONS`` times toward its larger moved half; across a seam the last step stays a large part of the
    moved chord, while a continuous edge, however stretched (Mercator near a pole), shrinks to nothing. Scale-free: the
    pixel size plays no part. The first jump found gives the seam's longitude (``frame.seam``, in lon/lat for every
    grid).
    """
    out = np.zeros(nominated.shape, dtype=bool)
    r, e = np.nonzero(nominated)
    if r.size == 0:
        return out
    ax, ay, bx, by = gx[r, e], gy[r, e], dx[r, e], dy[r, e]
    x, y = _move(
        frame.to_native, np.stack([ax, ax + bx], 1), np.stack([ay, ay + by], 1)
    )
    chord = np.hypot(x[:, 1] - x[:, 0], y[:, 1] - y[:, 0])
    lo, hi = np.zeros(r.size), np.ones(r.size)
    for _ in range(
        _SEAM_BISECTIONS
    ):  # vectorscan: ok (a bisection, over the nominated edges)
        mid = 0.5 * (lo + hi)
        t = np.stack([lo, mid, hi], 1)
        x, y = _move(
            frame.to_native,
            ax[:, None] + t * bx[:, None],
            ay[:, None] + t * by[:, None],
        )
        left = np.hypot(x[:, 1] - x[:, 0], y[:, 1] - y[:, 0])
        right = np.hypot(x[:, 2] - x[:, 1], y[:, 2] - y[:, 1])
        hi = np.where(left > right, mid, hi)
        lo = np.where(left > right, lo, mid)
    x, y = _move(
        frame.to_native,
        ax[:, None] + np.stack([lo, hi], 1) * bx[:, None],
        ay[:, None] + np.stack([lo, hi], 1) * by[:, None],
    )
    # a seam's last step is the CRS's width; on an edge of micrometres float noise passes 1% of the chord
    jump = np.hypot(x[:, 1] - x[:, 0], y[:, 1] - y[:, 0]) > np.maximum(
        0.01 * chord, 1e-3 * frame.px_side
    )
    out[r, e] = jump
    if jump.any() and frame.seam is None:
        k = int(np.flatnonzero(jump)[0])
        t = 0.5 * (lo[k] + hi[k])
        lon = ax[k] + t * bx[k]
        frame.seam = (
            180.0 - (180.0 - lon) % 360.0
        )  # in (-180, 180]; a grid longitude (lon/lat grids)
    if jump.any() and frame.seam_src is None and frame.to_src_lonlat is not None:
        # the seam in the raster CRS's own lon/lat: its two sides there are a turn apart (just west of the seam
        # meridian, and just east of it, written a turn back)
        k = int(np.flatnonzero(jump)[0])
        ends = np.array([lo[k], hi[k]])
        x, y = _move(frame.to_native, ax[k] + ends * bx[k], ay[k] + ends * by[k])
        lon = np.asarray(frame.to_src_lonlat(x, y)[0], dtype="float64")
        mid = 0.5 * (
            lon.max() + lon.min() + (360.0 if lon.max() - lon.min() > 180.0 else 0.0)
        )
        frame.seam_src = 180.0 - (180.0 - mid) % 360.0
    return out


def _pad_rings(x, m: int):
    """``x`` (rings x K) widened to ``m`` columns by repeating each ring's last vertex."""
    if x.shape[1] >= m:
        return x
    return np.concatenate([x, np.repeat(x[:, -1:], m - x.shape[1], axis=1)], axis=1)


def _native_rings(frame: _Frame, cells) -> tuple:
    """``(x, y, owner)``: the rings of the cells in the raster's CRS (``_move_rings``), rings x M, and the index into
    ``cells`` of each ring. A cell across the raster CRS's seam gives one ring per side (``_seam_pieces``), so ``owner``
    can repeat an index; a piece that still jumps raises ``ValueError`` with a Fix."""
    gx, gy = _grid_rings(cells, frame.grid)
    vx, vy, jumps = _move_rings(frame, gx, gy)
    owner = np.arange(len(cells))
    if not jumps.any():
        return _periodic_rings(frame, vx, vy, owner)
    pieces, piece_owner = [], []
    cut = _seam_pieces if frame.meridian_cut else _traced_pieces
    for i in np.flatnonzero(jumps):  # vectorscan: ok (per cell across the seam, a few)
        cell_pieces = cut(frame, gx[i], gy[i])
        if cell_pieces is None:
            raise ValueError(_seam_refusal(frame))
        for px, py in cell_pieces:
            pieces.append((px, py))
            piece_owner.append(i)
    keep = np.flatnonzero(~jumps)
    if not pieces:
        return _periodic_rings(frame, vx[keep], vy[keep], owner[keep])
    m = max(len(px) for px, _ in pieces)
    qx = np.array([np.pad(px, (0, m - len(px)), mode="edge") for px, _ in pieces])
    qy = np.array([np.pad(py, (0, m - len(py)), mode="edge") for _, py in pieces])
    qx, qy, still = _move_rings(frame, qx, qy, wrap=False)
    if still.any():
        raise ValueError(_seam_refusal(frame))
    m = max(vx.shape[1], qx.shape[1])
    return _periodic_rings(
        frame,
        np.concatenate([_pad_rings(vx[keep], m), _pad_rings(qx, m)]),
        np.concatenate([_pad_rings(vy[keep], m), _pad_rings(qy, m)]),
        np.concatenate([owner[keep], np.asarray(piece_owner, dtype=owner.dtype)]),
    )


def _periodic_rings(frame: _Frame, x, y, owner) -> tuple:
    """``(x, y, owner)`` of moved rings (rings x M) in a raster CRS that runs on by ``frame.period``: a lon/lat
    raster CRS, or a cylindrical one the raster reaches past the seam of. Unchanged for any other.

    * In a lon/lat raster CRS (rings unwrapped, ``_move_rings``) a ring that winds once around a pole (a cell that
      holds it) is closed over that pole: its pole is the row at latitude +-90 there, so the ring runs on to its first
      vertex a turn on, then along the pole and back.
    * Each ring is then repeated by whole turns over the raster's x range (``frame.x_range``), so a cell across the
      antimeridian, a 0..360 raster and a raster wider than the grid's -180..180 all meet their pixels. A ring that
      meets none keeps the turn nearest the raster (it overlaps no pixel either way).
    """
    period = frame.period
    if not period:
        return x, y, owner
    if frame.lonlat_src:
        close = x[:, 0] + _wrap(x[:, 0] - x[:, -1], period) + x[:, -1] - x[:, 0]
        winding = close - x[:, 0]  # 0, or a turn for a ring around a pole
        around = np.abs(winding) > 0.5 * period
        if around.any():
            quarter = 0.25 * period  # 90 degrees in the CRS's unit
            north = np.where(y[around].mean(axis=1) > 0.0, quarter, -quarter)
            x0, y0, xe = x[around, 0], y[around, 0], x[around, 0] + winding[around]
            tail_x = np.stack([xe, xe, x0], axis=1)
            tail_y = np.stack([y0, north, north], axis=1)
            m = x.shape[1] + 3
            x, y = _pad_rings(x, m), _pad_rings(y, m)
            x[around] = np.concatenate([x[around, :-3], tail_x], axis=1)
            y[around] = np.concatenate([y[around, :-3], tail_y], axis=1)
    lo, hi = frame.x_range
    a, b = x.min(axis=1), x.max(axis=1)
    k0 = np.ceil((lo - b) / period)
    k1 = np.floor((hi - a) / period)
    none = k0 > k1
    near = np.round((0.5 * (lo + hi) - 0.5 * (a + b)) / period)
    k0, k1 = np.where(none, near, k0), np.where(none, near, k1)
    n = (k1 - k0 + 1).astype(np.intp)
    if (n == 1).all() and (k0 == 0).all():
        return x, y, owner
    ring = np.repeat(np.arange(x.shape[0]), n)
    turn = np.repeat(k0, n) + (np.arange(int(n.sum())) - np.repeat(np.cumsum(n) - n, n))
    return x[ring] + (turn * period)[:, None], y[ring], owner[ring]


def _seam_refusal(frame: _Frame) -> str:
    """The message for a cell that crosses the raster CRS's seam where it cannot be cut along it."""
    return (
        f"{frame.fn}: a cell crosses the seam of the raster's CRS ({_crs_label(frame.src_crs)}) where it cannot be "
        "cut along it. Fix: reproject the raster (for example to EPSG:4326 with gbx_rst_transform), or crop it "
        "away from the seam."
    )


def _crs_label(crs) -> str:
    """A short name of ``crs`` for a message: its authority code, else its name and projection method (no WKT; for a
    CRS bound to a datum shift, its projection's)."""
    from databricks.labs.gbx.core.crs import crs_to_canonical, to_pyproj_crs

    canon = crs_to_canonical(crs)
    if len(canon) <= 40:
        return canon
    pcrs = to_pyproj_crs(crs)
    if pcrs.is_bound and pcrs.source_crs is not None:
        pcrs = pcrs.source_crs
    op = getattr(pcrs, "coordinate_operation", None)
    method = getattr(op, "method_name", "") if op is not None else ""
    label = pcrs.name if not method else f"{pcrs.name}, {method}"
    return label[:80]


def _seam_side(frame: _Frame, x, y):
    """Longitude, in the raster CRS's own lon/lat, of the grid-CRS points ``x``, ``y`` (moved by the frame's own
    operation, then the raster CRS's inverse projection), written within half a turn of the seam's: continuous
    across the seam, equal to ``frame.seam_src`` on it."""
    nx_, ny_ = _move(frame.to_native, x, y)
    lon = np.asarray(frame.to_src_lonlat(nx_, ny_)[0], dtype="float64")
    return frame.seam_src + (lon - frame.seam_src + 180.0) % 360.0 - 180.0


def _traced_pieces(frame: _Frame, gx, gy):
    """The rings (grid CRS) of one cell across the raster CRS's seam, cut on it where the seam is: a meridian of the
    raster CRS's own lon/lat (``frame.seam_src``), which for a planar grid (BNG) or another datum than WGS 84 is a
    curve in the grid's CRS. On a lon/lat grid the cell's longitudes are unwrapped, a cell around a pole is closed at
    ``_POLE_CAP_LAT`` (as ``_seam_pieces`` does), the cell is clipped to ``frame.cap_lat`` (above the raster: nearer
    the pole the seam cannot be traced), and a part wider than 90 degrees is split into 90-degree strips, so each
    keeps within half a turn of the seam; a part the seam crosses is cut by ``_trace_cut``. None when the seam cannot
    be traced: the caller refuses the cell.
    """
    from shapely.geometry import Polygon
    from shapely.geometry import box as _box

    if frame.seam_src is None:
        return None
    x, y = np.asarray(gx, dtype="float64"), np.asarray(gy, dtype="float64")
    if not frame.geographic:
        cell = Polygon(zip(x, y))
        return _trace_cut(frame, cell if cell.is_valid else cell.buffer(0))
    step = (np.diff(np.append(x, x[0])) + 180.0) % 360.0 - 180.0
    x = x[0] + np.concatenate([[0.0], np.cumsum(step[:-1])])
    pts = list(zip(x, y))
    winding = float(step.sum())
    if abs(winding) > 180.0:  # around a pole: close over it
        cap = math.copysign(_POLE_CAP_LAT, float(np.mean(y)))
        pts += [(x[0] + winding, y[0]), (x[0] + winding, cap), (x[0], cap)]
    cell = Polygon(pts)
    if not cell.is_valid:
        cell = cell.buffer(0)
    area = cell.area
    clipped = cell.intersection(_box(-1e9, -frame.cap_lat, 1e9, frame.cap_lat))
    out = []
    for whole in getattr(
        clipped, "geoms", [clipped]
    ):  # vectorscan: ok (the parts of one clip)
        if whole.geom_type != "Polygon" or whole.area <= 1e-12 * area:
            continue
        west, south, east, north = whole.bounds
        strips = [whole]
        if east - west > 90.0:
            strips = []
            for k in range(
                math.ceil((east - west) / 90.0)
            ):  # vectorscan: ok (at most 5 strips)
                part = whole.intersection(
                    _box(
                        west + 90.0 * k,
                        south - 1.0,
                        min(west + 90.0 * (k + 1), east),
                        north + 1.0,
                    )
                )
                strips.extend(getattr(part, "geoms", [part]))
        for p in strips:  # vectorscan: ok (the parts of one cell, at most a few)
            if p.geom_type != "Polygon" or p.area <= 1e-12 * area:
                continue  # a sliver where a strip's edge meets a vertex
            xy = np.asarray(p.exterior.coords)[:-1]
            g = _seam_side(frame, xy[:, 0], xy[:, 1])
            # only a part across the seam is cut (a part of a pole cell, clipped at the cap, may lie wholly on one
            # side); across the seam, not its antipode (where g wraps a turn but the CRS runs on): a part at most
            # 90 degrees wide cannot hold both
            if g.max() - g.min() < 180.0 and g.min() < frame.seam_src < g.max():
                cut = _trace_cut(frame, p)
                if cut is None:
                    return None
                out.extend(cut)
            else:
                out.append((xy[:, 0], xy[:, 1]))
    return out


def _trace_cut(frame: _Frame, cell):
    """``_trace_once`` with ``_SEAM_TRACE_LINES`` lines, then 4 times as many (up to ``_SEAM_TRACE_MAX_LINES``) while
    the trace fails (between two lines the seam left a line's ends on one side) or a piece still reaches across the
    seam between two lines (near a pole a datum shift bends the seam sharply in the grid's longitude): a piece's
    vertices and edge midpoints must all keep to one side (``_seam_side``).
    """
    n = _SEAM_TRACE_LINES
    while n <= _SEAM_TRACE_MAX_LINES:  # vectorscan: ok (at most 3 tries)
        out = _trace_once(frame, cell, n)
        if out is not None and all(
            _one_side(frame, px, py) for px, py in out
        ):  # vectorscan: ok (per piece)
            return out
        n = 4 * (n - 1) + 1
    return None


def _one_side(frame: _Frame, px, py) -> bool:
    """Whether a piece's vertices and edge midpoints all keep to one side of the seam."""
    nxt = np.roll(np.arange(px.size), -1)
    g = _seam_side(
        frame,
        np.concatenate([px, 0.5 * (px + px[nxt])]),
        np.concatenate([py, 0.5 * (py + py[nxt])]),
    )
    return bool((g < frame.seam_src).all() or (g > frame.seam_src).all())


def _trace_once(frame: _Frame, cell, lines_n: int):
    """The pieces (rings) of the polygon ``cell`` (grid CRS) on each side of the raster CRS's seam: the cut
    boundaries, ``_SEAM_GAP_TRACED_DEG`` on each side of it, are traced by bisection along ``lines_n`` lines across
    the cell (``_seam_side``), and the pieces keep the cell's own edges. None when the seam does not cross every line
    the same way. The two regions and the strip between the boundaries cover the lines' span by construction, so the
    pieces hold the whole cell but that strip; a piece that a seam crossing a line more than once leaves across it
    is caught by ``_trace_cut``'s side test."""
    from shapely.geometry import Polygon

    x0, y0, x1, y1 = cell.bounds
    mx, my = 0.5 * (x1 - x0) + 1e-9, 0.01 * (y1 - y0) + 1e-12
    lo_y, hi_y = y0 - my, y1 + my
    if frame.geographic:  # no line past a pole cell's cap
        lo_y, hi_y = max(lo_y, -frame.cap_lat), min(hi_y, frame.cap_lat)
    lines = np.linspace(lo_y, hi_y, lines_n)
    west, east = np.full(lines.size, x0 - mx), np.full(lines.size, x1 + mx)
    g_w, g_e = _seam_side(frame, west, lines), _seam_side(frame, east, lines)
    rising_lines = g_w < g_e
    if not (rising_lines.all() or not rising_lines.any()):
        return None
    rising = bool(rising_lines.all())
    out = []
    for target in (
        frame.seam_src - _SEAM_GAP_TRACED_DEG,
        frame.seam_src + _SEAM_GAP_TRACED_DEG,
    ):  # vectorscan: ok
        lo_ok = (g_w < target) == rising
        hi_ok = (g_e > target) == rising
        if not (lo_ok.all() and hi_ok.all()):
            return None
        lo, hi = west.copy(), east.copy()
        for _ in range(
            _SEAM_BISECTIONS
        ):  # vectorscan: ok (a bisection, over the lines)
            mid = 0.5 * (lo + hi)
            below = (_seam_side(frame, mid, lines) < target) == rising
            lo, hi = np.where(below, mid, lo), np.where(below, hi, mid)
        xs = 0.5 * (lo + hi)
        # the side of this boundary away from the seam: west of the west one, east of the east one (in g)
        side_x = west if (target < frame.seam_src) == rising else east
        region = Polygon(list(zip(xs, lines)) + list(zip(side_x[::-1], lines[::-1])))
        if not region.is_valid:
            region = region.buffer(0)
        part = cell.intersection(region)
        for p in getattr(
            part, "geoms", [part]
        ):  # vectorscan: ok (the parts of one cut)
            if p.geom_type == "Polygon" and p.area > 1e-12 * cell.area:
                xy = np.asarray(p.exterior.coords)[:-1]
                out.append((xy[:, 0], xy[:, 1]))
    return out


def _seam_pieces(frame: _Frame, gx, gy) -> list:
    """The lon/lat rings (``(x, y)`` arrays) of one cell across the raster CRS's seam, cut on the seam meridian
    (``frame.seam``, from ``_jumping``), each piece shifted by whole turns to the seam's west side, where the CRS is continuous. A cell
    around a pole (its longitudes wind once) is closed at ``_POLE_CAP_LAT`` first, for a CRS in which the pole is not a
    point (Mercator). The cut keeps ``_SEAM_GAP_DEG`` clear of the seam on each side."""
    from shapely.geometry import Polygon
    from shapely.geometry import box as _box

    step = (np.diff(np.append(gx, gx[0])) + 180.0) % 360.0 - 180.0
    lon = gx[0] + np.concatenate([[0.0], np.cumsum(step[:-1])])
    pts = list(zip(lon, gy))
    winding = float(step.sum())
    if (
        abs(winding) > 180.0
    ):  # the ring's closing edge ends a turn away; close over the pole
        cap = math.copysign(_POLE_CAP_LAT, float(np.mean(gy)))
        pts += [(lon[0] + winding, gy[0]), (lon[0] + winding, cap), (lon[0], cap)]
    poly = Polygon(pts)
    if not poly.is_valid:
        poly = poly.buffer(0)
    seam = frame.seam  # set by ``_jumping`` when it found this cell's jump
    west, _, east, _ = poly.bounds
    out = []
    for k in range(
        math.floor((west - seam) / 360.0), math.floor((east - seam) / 360.0) + 1
    ):  # vectorscan: ok (one or two turns)
        left = seam + 360.0 * k
        part = poly.intersection(
            _box(left + _SEAM_GAP_DEG, -90.0, left + 360.0 - _SEAM_GAP_DEG, 90.0)
        )
        for p in getattr(
            part, "geoms", [part]
        ):  # vectorscan: ok (the parts of one cut)
            if p.geom_type == "Polygon" and p.area > 0.0:
                xy = np.asarray(p.exterior.coords)[:-1]
                out.append((xy[:, 0] - 360.0 * (k + 1), xy[:, 1]))
    return out


def _native_id(cell, grid: str):
    """The output id of a raw cell: the Long of an h3 / quadbin id, the formatted BNG reference."""
    return _bng.format(int(cell)) if grid == "bng" else int(cell)


def _in_lon_range(lon):
    """Longitudes written in -180..180: one outside (a 0..360 raster, or past the antimeridian) is taken a turn back;
    one in the range, 180 included, is kept as it is."""
    lon = np.asarray(lon, dtype="float64")
    out = (lon < -180.0) | (lon > 180.0)
    return np.where(out, (lon + 180.0) % 360.0 - 180.0, lon) if out.any() else lon


def _in_quadbin_lat(lat) -> np.ndarray:
    """Whether each latitude is inside quadbin's -85.05..85.05: a point beyond has no cell (``point_as_cell`` would
    clamp it into the first or last row)."""
    from databricks.labs.gbx.pygx import _quadbin

    lat = np.asarray(lat, dtype="float64")
    return (lat >= _quadbin._LAT_MIN) & (lat <= _quadbin._LAT_MAX)


def _corner_cells(gx, gy, resolution, grid: str) -> np.ndarray:
    """The quadbin / BNG cell (int64) of each grid-CRS point, -1 for a point outside BNG or beyond quadbin's
    latitudes."""
    if grid == "quadbin":
        from databricks.labs.gbx.pygx import _quadbin

        cells = _quadbin.point_as_cell_vec(_in_lon_range(gx), gy, resolution)
        return np.where(_in_quadbin_lat(gy), cells, -1)
    cells = _bng_cells(gx, gy, resolution)
    return np.where(_bng_inside(gx, gy, cells, resolution), cells, -1)


def _covering_band_h3_native(
    frame: _Frame, band, mask, resolution: int, agg: str
) -> list:
    """``_covering_band_h3_vectorised`` for a north-up raster in another CRS: the candidate cells come from the moved
    outline of the window of valid pixels (padded by one pixel), and each ring is moved into the raster's CRS
    (``_native_rings``), where its pixels are axis-aligned boxes. The pairs of a chunk are grouped by cell, so the two
    pieces of a cell across the seam give one state."""
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
        vx, vy, owner = _native_rings(frame, chunk)
        sub = max(1, _NATIVE_RING_BUDGET // vx.shape[1] ** 2)
        parts = []
        for s0 in range(
            0, len(owner), sub
        ):  # vectorscan: ok (sub-chunks bound the rings)
            ring, w, vals = _box_pairs(
                vx[s0 : s0 + sub],
                vy[s0 : s0 + sub],
                band,
                mask,
                frame.gt,
                frame.px_area,
            )
            parts.append((owner[ring + s0], w, vals))
        cell = np.concatenate([p[0] for p in parts])
        if cell.size == 0:
            continue
        uniq, measures = _grouped_measures(
            cell,
            np.concatenate([p[2] for p in parts]),
            agg,
            weights=np.concatenate([p[1] for p in parts]),
        )
        out.extend(
            {"cellID": chunk[c], "measure": m}
            for c, m in zip(uniq.tolist(), measures)  # vectorscan: ok (per-cell)
        )
    return out


def _native_polygons(frame: _Frame, cells, ox: float, oy: float):
    """One shapely geometry per cell in the raster's CRS, relative to ``(ox, oy)`` (so GEOS rounds at the raster's
    scale, not the coordinates'): a polygon, or the union of the pieces of a cell cut at the seam.
    """
    import shapely

    geoms = []
    step = _H3_VEC_CHUNK_CELLS
    for start in range(0, len(cells), step):  # vectorscan: ok (chunks bound the rings)
        chunk = cells[start : start + step]
        vx, vy, owner = _native_rings(frame, chunk)
        polys = shapely.polygons(np.stack([vx - ox, vy - oy], axis=-1))
        if owner.size == len(chunk) and (owner == np.arange(len(chunk))).all():
            geoms.append(polys)
            continue
        merged = np.full(len(chunk), shapely.Polygon(), dtype=object)
        counts = np.bincount(owner, minlength=len(chunk))
        single = counts[owner] == 1
        merged[owner[single]] = polys[single]
        # a cut cell's pieces, unioned: strips of one side touch (a multipolygon of touching parts is invalid, and
        # GEOS gives it no overlap); the two sides of the seam stay apart
        for o in np.flatnonzero(counts > 1):  # vectorscan: ok (per cut cell, a few)
            merged[o] = shapely.union_all(polys[owner == o])
        geoms.append(merged)
    return np.concatenate(geoms) if geoms else np.empty(0, dtype=object)


def _native_pair_weights(frame: _Frame, cells, pair_cell, pair_pix, qx, qy, ox, oy):
    """GEOS overlap of each pair's cell (``cells[pair_cell]``, moved into the raster's CRS) and pixel quad
    (``qx``, ``qy``: pixels x 4, rows ``pair_pix``), over the pixel's area: one batch of the per-pixel pass.
    """
    import shapely

    polys = _native_polygons(frame, cells, ox, oy)
    pixels = shapely.polygons(np.stack([qx - ox, qy - oy], axis=-1))
    area = shapely.area(shapely.intersection(polys[pair_cell], pixels[pair_pix]))
    return area / frame.px_area


def _covering_band_native_loop(
    frame: _Frame, band, mask, resolution, grid: str, agg: str
) -> list:
    """``_covering_band``'s per-pixel pass for a raster in another CRS (any geotransform): each valid pixel's
    candidate cells come from its outline moved to the grid's CRS (``_moved_outlines``, ``_padded_boxes``), and the
    weight is the GEOS overlap of the pixel quad and the cell moved into the raster's CRS (``_native_pair_weights``),
    over the pixel's area. quadbin and BNG keep the interior fast path: a pixel whose four corners fall in one cell has
    weight 1 (the cell's moved edge can bend into the pixel only by its chord tolerance).

    Memory is bounded: pixels are moved ``_NATIVE_LOOP_PIXELS`` at a time, and their candidate pairs go to GEOS in
    batches of at most ``_NATIVE_LOOP_PAIRS`` (plus one pixel's); only the kept pairs' cell, pixel and weight stay.
    """
    from databricks.labs.gbx.pyrx.core.tessellate import _cell_is_valid

    ys, xs = np.nonzero(mask)
    if ys.size == 0:
        return []
    ox, oy = frame.gt[0], frame.gt[3]
    index: dict = {}  # raw cell -> its index in the output
    kept_cell, kept_pix, kept_w = [], [], []
    dc, dr = np.array([0, 1, 1, 0]), np.array([0, 0, 1, 1])
    for b0 in range(
        0, ys.size, _NATIVE_LOOP_PIXELS
    ):  # vectorscan: ok (blocks bound memory)
        cols = xs[b0 : b0 + _NATIVE_LOOP_PIXELS, None] + dc
        rows = ys[b0 : b0 + _NATIVE_LOOP_PIXELS, None] + dr
        qx, qy = frame.native_xy(cols, rows)
        interior = np.zeros(cols.shape[0], dtype=bool)
        if grid in _COVERING_FAST_PATH_EXACT:
            gx, gy = _move(frame.to_grid, qx, qy)
            corner = _corner_cells(gx.ravel(), gy.ravel(), resolution, grid).reshape(
                -1, 4
            )
            interior = (corner == corner[:, :1]).all(axis=1) & (corner[:, 0] >= 0)
            kept_cell.append(
                np.fromiter(
                    (index.setdefault(int(c), len(index)) for c in corner[interior, 0]),
                    dtype=np.intp,
                    count=int(interior.sum()),
                )  # vectorscan: ok (per interior pixel, a dict lookup)
            )
            kept_pix.append(b0 + np.flatnonzero(interior))
            kept_w.append(np.ones(int(interior.sum())))
        edge = np.flatnonzero(~interior)
        out_x, out_y, out_pad, out_pole = _moved_outlines(
            frame, cols[edge].astype("float64"), rows[edge].astype("float64")
        )
        pending: dict = {}  # raw cell -> its index in this batch
        pair_cell, pair_pix = [], []

        def flush():
            if not pair_cell:
                return
            raw = list(pending)
            w = _native_pair_weights(
                frame, raw, np.asarray(pair_cell), np.asarray(pair_pix), qx, qy, ox, oy
            )
            keep = w > 0.0
            to_out = np.fromiter(
                (index.setdefault(c, len(index)) for c in raw),
                dtype=np.intp,
                count=len(raw),
            )  # vectorscan: ok (per cell, a dict lookup)
            kept_cell.append(to_out[np.asarray(pair_cell)[keep]])
            kept_pix.append(b0 + np.asarray(pair_pix)[keep])
            kept_w.append(w[keep])
            pending.clear()
            pair_cell.clear()
            pair_pix.clear()

        for k, i in enumerate(
            edge
        ):  # vectorscan: ok (covering per-pixel, as _covering_band)
            boxes = _padded_boxes(
                out_x[k], out_y[k], out_pad[k], out_pole[k], frame.geographic
            )
            for cell in _box_cells(boxes, resolution, grid):
                if _cell_is_valid(cell, grid):
                    pair_cell.append(pending.setdefault(cell, len(pending)))
                    pair_pix.append(i)
            if len(pair_cell) >= _NATIVE_LOOP_PAIRS:
                flush()
        flush()
    cell = np.concatenate(kept_cell) if kept_cell else np.empty(0, dtype=np.intp)
    if cell.size == 0:
        return []
    pix = np.concatenate(kept_pix)
    uniq, measures = _grouped_measures(
        cell, band[ys[pix], xs[pix]], agg, weights=np.concatenate(kept_w)
    )
    raw = list(index)
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

        keep = _in_quadbin_lat(gy)  # a centre beyond quadbin's latitudes is in no cell
        cids = _quadbin.point_as_cell_vec(_in_lon_range(gx[keep]), gy[keep], resolution)
        vals = vals[keep]
        if cids.size == 0:
            return []
    else:
        cids = _bng_cells(gx, gy, resolution)
        keep = _bng_inside(gx, gy, cids, resolution)
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
    raster's CRS has a positive-area overlap with the raster's outline there (in chunks of cells).
    """
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
    empty_value = _EMPTY_STATE if agg == _PARTIAL else (0.0 if agg == "count" else None)
    extra = []
    step = _H3_VEC_CHUNK_CELLS
    for start in range(0, len(todo), step):  # vectorscan: ok (chunks bound the rings)
        chunk = todo[start : start + step]
        polys = _native_polygons(frame, chunk, ox, oy)
        hit = shapely.intersects(polys, outline)
        hit[hit] = shapely.area(shapely.intersection(polys[hit], outline)) > 0.0
        extra.extend(
            {"cellID": _native_id(c, grid), "measure": empty_value}
            for c, h in zip(chunk, hit)  # vectorscan: ok (per cell)
            if h
        )
    return band_result + extra


def _grid_native(
    ds, src_crs, resolution, grid: str, agg: str, coverage: str, assignment: str
):
    """Grid ``ds`` (its CRS ``src_crs``, not the grid's) on its own pixels: one list per band, as ``_run`` gives, or
    None when the native frame cannot hold the raster (``_NeedsWarp``) and the caller warps it instead.
    """
    frame = _Frame(ds, src_crs, grid)
    frame.fn += "_partial" if agg == _PARTIAL else agg
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
        if not _any_pixel_transforms(frame):
            raise ValueError(
                f"{frame.fn}: no pixel of the raster lies inside its CRS's valid area "
                f"({_crs_label(src_crs)}), so it has no place on the grid. Fix: check the raster's CRS and "
                "geotransform (gbx_rst_georeference shows them; gbx_rst_setsrid or gbx_rst_setcrs relabels a wrong CRS)."
            ) from None
        return None


def _any_pixel_transforms(frame: _Frame) -> bool:
    """Whether any pixel centre of the frame's raster (sampled on a grid of at most 65 x 65) moves to the grid's
    CRS: a raster none of whose pixels does is outside its CRS's valid area (off the globe), and a warp of it fails
    with GDAL's "too many points failed to transform"."""
    ds = frame.ds
    cols = (np.linspace(0.0, ds.width - 1, min(ds.width, 65)) + 0.5)[None, :]
    rows = (np.linspace(0.0, ds.height - 1, min(ds.height, 65)) + 0.5)[:, None]
    x, y = frame.native_xy(
        np.broadcast_to(cols, (rows.size, cols.size)),
        np.broadcast_to(rows, (rows.size, cols.size)),
    )
    with np.errstate(all="ignore"):
        gx, gy = frame.to_grid(np.ravel(x), np.ravel(y))
    return bool((np.isfinite(np.asarray(gx)) & np.isfinite(np.asarray(gy))).any())


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
                    if grid == "quadbin":
                        # a centre beyond quadbin's latitudes is in no cell (as a BNG centre outside the grid)
                        keep = _in_quadbin_lat(lat)
                        if not keep.all():
                            lon, lat, vals = lon[keep], lat[keep], vals[keep]

                    if vals.size == 0:
                        band_result = []
                    else:
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

    def _run_lonlat(work_ds):
        # a lon/lat raster in the grid's CRS: planar in lon/lat, unless a cell it meets crosses the antimeridian or
        # holds a pole, or its longitudes leave -180..180; then the native frame, whose lon/lat runs on by turns
        if _lonlat_needs_frame(work_ds, grid, resolution):
            from databricks.labs.gbx.pyrx.core.crs import resolve_crs

            native = _grid_native(
                work_ds,
                resolve_crs(4326),
                resolution,
                grid,
                agg,
                coverage,
                assignment,
            )
            if native is not None:
                return native
        return _run(work_ds)

    # A raster in another CRS is gridded on its own pixels. The embedded raster CRS
    # wins; else the `crs` override (for a CRS-less-but-known raster); else None ->
    # assume grid-native 4326 and process unchanged (never errors).
    src_crs = _foreign_crs(ds, crs)
    if src_crs is None:
        return _run_lonlat(ds)
    native = _grid_native(ds, src_crs, resolution, grid, agg, coverage, assignment)
    if native is not None:
        return native
    # The native frame cannot hold it (a point that does not transform): warp to
    # 4326 (nearest), as before.
    warped_bytes = _warp_to_4326_if_needed(ds, crs)
    from rasterio.io import MemoryFile

    with MemoryFile(warped_bytes) as mf, open_bytes(mf) as work_ds:
        return _run_lonlat(work_ds)


def _lonlat_needs_frame(ds, grid: str, resolution) -> bool:
    """Whether a raster in EPSG:4326 (or CRS-less, taken as 4326) needs the native frame rather than the planar
    lon/lat pass: its pixels reach outside longitudes -180..180 (a 0..360 raster), or, for H3, a cell that may meet
    them crosses the antimeridian or holds a pole (the planar pass draws such a cell the wrong way round). The margin
    is three average cell edges, plus a pixel. A regional raster away from both keeps the planar pass, unchanged.
    """
    t = ds.transform
    xs, ys = t * (
        np.array([0.0, ds.width, ds.width, 0.0]),
        np.array([0.0, 0.0, ds.height, ds.height]),
    )
    west, east = float(np.min(xs)), float(np.max(xs))
    if west < -180.0 or east > 180.0:
        return True
    if grid != "h3":
        return False
    from h3.api import basic_int as h3i

    side = max(abs(t.a), abs(t.b), abs(t.d), abs(t.e))
    dlat = side + math.degrees(
        3.0 * h3i.average_hexagon_edge_length(int(resolution), "m") / _EARTH_RADIUS_M
    )
    far = float(np.max(np.abs(ys))) + dlat
    if far >= 90.0:
        return True
    dlon = dlat / math.cos(math.radians(far))
    return west - dlon <= -180.0 or east + dlon >= 180.0


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
                    keep = _bng_inside(e, n, cids, resolution)
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
