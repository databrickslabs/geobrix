"""Spark-free raster->discrete-global-grid aggregation (H3 + quadbin + BNG).

Mirrors the heavyweight ``RST_{H3,Quadbin,BNG}_RasterToGrid`` families exactly:

* Per band, every valid pixel (non-zero mask) is mapped to a grid cell id by
  the pixel-centroid world coordinate (0.5-pixel offset through the
  geotransform). For H3/quadbin the raster is interpreted as EPSG:4326 lon/lat
  -- no reprojection (callers reproject upstream via ``rst_transform``). BNG has
  no lon/lat path, so the raster is warped to EPSG:27700 (nearest) up front and
  pixels outside GB are dropped via ``pygx._bng.is_valid``.
* Pixel values are accumulated per cell, then reduced by the chosen aggregate.
* BNG cell math is the single source of truth in ``pygx._bng`` (no numpy codec);
  H3/quadbin ids are ``int`` (Long) but BNG ids are rendered to ``str`` via
  ``pygx._bng.format`` at the output boundary.

Two new parameters (Stage 2, Task 7):

* ``coverage``: ``"sparse"`` (emit only cells with at least one valid pixel) or
  ``"complete"`` (default, matches heavy) -- also emit covering-but-empty cells
  carrying ``None`` (or ``0.0`` for ``count``).
* ``assignment``: ``"centroid"`` (default) -- bin each valid pixel to the cell
  containing its centroid (pre-Stage-2 behaviour); ``"covering"`` -- distribute
  each pixel's value across every cell that overlaps its rectangular footprint,
  weighted by the intersection-area fraction. Weighted reducers mirror the heavy
  tier exactly (see ``_weighted_reduce``).

count is now Double/float in all modes (centroid count = n as float; covering
count = Sigma-w; covered-but-empty count = 0.0).

Backed by the best-in-class libs ``h3`` (v4) and ``quadbin`` (CARTO v0).

Hot path (centroid) is vectorized: valid pixels are gathered once, mapped to
cell ids, and grouped/reduced with numpy. The quadbin encoder ``_quadbin_cells``
is a numpy reimplementation of ``quadbin.point_to_cell`` that is BIT-EXACT with
the upstream lib (the lib's per-call Python is otherwise ~20x slower than H3's
C-backed encoder); the H3 encoder stays a scalar comprehension over valid pixels
because ``h3.latlng_to_cell`` is C-backed and exposes no array API.

The covering path is intentionally NOT vectorized (it iterates per-pixel,
matching the heavy tier's O(pixels) design).  Use small/coarse rasters; this
path is not intended for large tiles.
"""

import h3
import numpy as np

from databricks.labs.gbx.pygx import _bng

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

_AGGS = ("avg", "count", "min", "max", "median", "sum", "variance", "stddev")
_COVERAGES = ("sparse", "complete")
_ASSIGNMENTS = ("centroid", "covering")

# quadbin 64-bit cell layout constants (see quadbin.main / quadbin.utils).
_QB_HEADER = np.uint64(0x4000000000000000)
_QB_FOOTER = np.uint64(0xFFFFFFFFFFFFF)
_QB_MODE = np.uint64(1) << np.uint64(59)
_QB_MAX_LAT = 89.0  # web-mercator clip bound used by clip_latitude


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
    cells = [
        h3.str_to_int(h3.latlng_to_cell(float(la), float(lo), resolution))
        for lo, la in zip(lon, lat)  # vectorscan: ok (h3 no array API)
    ]
    return np.array(cells, dtype="uint64")


def _bng_cells(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray:
    """Per-valid-pixel BNG cell ids (Long int64), fully vectorized.

    ``e``/``n`` are EPSG:27700 eastings/northings (pixel centroids of the WARPED
    raster). Delegates to ``pygx._bng.point_to_cell_id_vec`` -- the SAME shared
    numpy core the scalar ``point_to_cell_id`` wraps, so cell ids are identical.
    """
    return _bng.point_to_cell_id_vec(e, n, resolution)


def _quadbin_cells(lon: np.ndarray, lat: np.ndarray, resolution: int) -> np.ndarray:
    """Vectorized ``quadbin.point_to_cell`` -- bit-exact with the upstream lib.

    Reproduces, in numpy: longitude/latitude clipping, the web-mercator
    ``point_to_tile_fraction`` (+ floor), the tile-x wrap, and ``tile_to_cell``
    (the 32-bit shift, Morton bit-interleave, header/mode/zoom assembly, footer).
    """
    z = int(resolution)
    lon = np.asarray(lon, dtype="float64")
    lat = np.asarray(lat, dtype="float64")

    # clip_longitude / clip_latitude
    lon = np.clip(lon, -180.0, 180.0)
    lat = np.clip(lat, -_QB_MAX_LAT, _QB_MAX_LAT)

    z2 = float(1 << z)
    sinlat = np.sin(lat * np.pi / 180.0)
    xf = z2 * (lon / 360.0 + 0.5)
    yfraction = 0.5 - 0.25 * np.log((1.0 + sinlat) / (1.0 - sinlat)) / np.pi
    yf = np.clip(z2 * yfraction, 0.0, z2 - 1.0)

    # Wrap tile x: x %= z2; x += z2 if x < 0 (Python % on the float, then floor).
    xf = np.mod(xf, z2)
    xf = np.where(xf < 0.0, xf + z2, xf)

    x = np.floor(xf).astype("uint64")
    y = np.floor(yf).astype("uint64")

    shift = np.uint64(32 - z)
    x = x << shift
    y = y << shift

    # Morton spread (matches tile_to_cell's interleave masks/shifts exactly).
    b = [
        np.uint64(0x5555555555555555),
        np.uint64(0x3333333333333333),
        np.uint64(0x0F0F0F0F0F0F0F0F),
        np.uint64(0x00FF00FF00FF00FF),
        np.uint64(0x0000FFFF0000FFFF),
        np.uint64(0x00000000FFFFFFFF),
    ]
    s = [np.uint64(1), np.uint64(2), np.uint64(4), np.uint64(8), np.uint64(16)]

    x = (x | (x << s[4])) & b[4]
    y = (y | (y << s[4])) & b[4]
    x = (x | (x << s[3])) & b[3]
    y = (y | (y << s[3])) & b[3]
    x = (x | (x << s[2])) & b[2]
    y = (y | (y << s[2])) & b[2]
    x = (x | (x << s[1])) & b[1]
    y = (y | (y << s[1])) & b[1]
    x = (x | (x << s[0])) & b[0]
    y = (y | (y << s[0])) & b[0]

    interleaved = (x | (y << np.uint64(1))) >> np.uint64(12)
    footer = _QB_FOOTER >> np.uint64(z * 2)
    return (
        _QB_HEADER | _QB_MODE | (np.uint64(z) << np.uint64(52)) | interleaved | footer
    )


def _grouped_measures(cids: np.ndarray, vals: np.ndarray, agg: str):
    """Group ``vals`` by ``cids`` and reduce -> (unique_cids, measures).

    Loops at most over CELLS (median only), never over pixels. ``count`` yields
    Python floats (changed from int for heavy-tier parity: centroid count = n as
    float; covering count = Sigma-w); every other agg yields Python floats --
    matching the original per-cell ``_reduce`` types and values exactly.
    """
    uniq, inv = np.unique(cids, return_inverse=True)
    inv = inv.astype(np.intp)
    counts = np.bincount(inv, minlength=uniq.size)

    if agg == "count":
        measures = [float(c) for c in counts]  # float for heavy-tier parity
        return uniq, measures

    vals = np.asarray(vals, dtype="float64")
    if agg == "avg":
        sums = np.bincount(inv, weights=vals, minlength=uniq.size)
        out = sums / counts
    elif agg == "sum":
        # Total of valid pixel values per cell -- the avg numerator (bincount
        # weighted sum) WITHOUT dividing by counts. A cell is only emitted with
        # >=1 valid pixel (sec 2.6), so sum is always well-defined.
        out = np.bincount(inv, weights=vals, minlength=uniq.size)
    elif agg in ("variance", "stddev"):
        # Population (ddof=0), TWO-PASS (numerically stable; NOT E[x^2]-E[x]^2):
        # pass 1 per-cell mean; pass 2 mean of squared deviations. A 1-pixel
        # cell gives variance 0 (clean). stddev = sqrt(variance) so it tracks
        # variance's cross-tier parity exactly.
        means = np.bincount(inv, weights=vals, minlength=uniq.size) / counts
        sq = (vals - means[inv]) ** 2
        var = np.bincount(inv, weights=sq, minlength=uniq.size) / counts
        out = var if agg == "variance" else np.sqrt(var)
    elif agg == "min":
        out = np.full(uniq.size, np.inf)
        np.minimum.at(out, inv, vals)
    elif agg == "max":
        out = np.full(uniq.size, -np.inf)
        np.maximum.at(out, inv, vals)
    elif agg == "median":
        order = np.argsort(inv, kind="stable")
        sorted_vals = vals[order]
        bounds = np.cumsum(counts)[:-1]
        segments = np.split(sorted_vals, bounds)
        out = np.array([np.median(seg) for seg in segments], dtype="float64")
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
    to the tessellate covering path.  Intentionally NOT vectorized -- this
    matches the heavy tier's O(pixels) loop and is suitable for small/coarse
    tiles only.

    Interior fast-path (O(boundary) optimisation, mirrors heavy Task 15):
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

    empty_value = 0.0 if agg == "count" else None
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


def _stamp_crs_bytes(ds, target_crs) -> bytes:
    """Return GTiff bytes identical to ``ds`` but with ``target_crs`` stamped —
    used to give a CRS-less-but-known raster its source CRS before warping."""
    from rasterio.io import MemoryFile

    profile = ds.profile.copy()
    profile.update(driver="GTiff", crs=target_crs)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ds.read())
        return mf.read()


def _warp_to_4326_if_needed(ds, crs):
    """Return GTiff bytes reprojected to EPSG:4326, or ``None`` when no warp is
    needed (already 4326, or CRS-less with no override -> assume grid-native).

    The embedded raster CRS wins as the source; else the ``crs`` override (int
    SRID or CRS string) for a CRS-less raster; else None (never errors)."""
    from rasterio.crs import CRS as _RioCRS
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    _WGS84 = resolve_crs(4326)

    if ds.crs is not None:
        if ds.crs == _WGS84:
            return None  # already grid-native
        return warp.reproject_to_crs(ds, "EPSG:4326", resampling="nearest")

    # CRS-less raster: use the override if given, else assume grid-native (4326).
    if crs is None:
        return None
    override = resolve_crs(crs)
    if override == _WGS84:
        return None  # override says it is already 4326
    stamped = _stamp_crs_bytes(ds, override)
    with MemoryFile(stamped) as mf, mf.open() as stamped_ds:
        return warp.reproject_to_crs(stamped_ds, "EPSG:4326", resampling="nearest")


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
    """Aggregate raster pixel values into discrete-global-grid cells, per band.

    H3 and quadbin operate in EPSG:4326 lon/lat. The raster is auto-reprojected
    to EPSG:4326 (nearest-neighbour, so pixel statistics are never interpolated)
    when it carries a CRS that differs — mirroring what BNG already does for
    EPSG:27700. This closes a prior silent-wrong-answer footgun where a non-4326
    (e.g. UTM) raster had its easting/northing read directly as lon/lat.

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
                    only cells with at least one valid pixel (pre-Stage-2
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
    encode = _h3_cells if grid == "h3" else _quadbin_cells

    def _run(work_ds):
        gt = work_ds.transform.to_gdal()  # (c, a, b, f, d, e) GDAL geotransform
        out = []
        for bi in range(1, work_ds.count + 1):
            band = work_ds.read(bi).astype("float64")
            mask = work_ds.read_masks(bi)  # 0 = invalid (nodata-derived)

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

    # Auto-reproject to 4326 when a source CRS is known and differs. The embedded
    # raster CRS wins; else the `crs` override (for a CRS-less-but-known raster);
    # else None -> assume grid-native 4326 and process unchanged (never errors).
    warped_bytes = _warp_to_4326_if_needed(ds, crs)
    if warped_bytes is None:
        return _run(ds)
    from rasterio.io import MemoryFile

    with MemoryFile(warped_bytes) as mf, mf.open() as work_ds:
        return _run(work_ds)


def _raster_to_bng(
    ds,
    resolution: int,
    agg: str,
    coverage: str = "complete",
    assignment: str = "centroid",
    crs=None,
) -> list:
    """BNG raster->grid: warp to EPSG:27700, bin per pixel, render String ids.

    Mirrors heavy ``RST_BNG_RasterToGrid``: BNG has no lon/lat input path, so the
    raster is reprojected to EPSG:27700 (nearest-neighbour, so no interpolated
    values corrupt the pixel statistics) unless it is already there; pixel
    centroids are mapped to BNG cell ids via ``pygx._bng.point_to_cell_id``;
    pixels whose cell falls outside GB are dropped via ``pygx._bng.is_valid``;
    ids are rendered to the user-facing BNG ``str`` via ``pygx._bng.format`` at
    the output boundary. Grouping stays Long-keyed (reuses ``_grouped_measures``).

    ``coverage``/``assignment`` behave identically to the h3/quadbin paths:
      * ``complete`` adds covered-but-empty cells (None / 0.0 for count)
      * ``covering`` distributes pixel values by area fraction

    ``crs`` is a source-CRS override for a CRS-less-but-known raster (stamped
    before the warp so a georeferenced-but-unlabeled raster reprojects correctly);
    ignored when the raster already carries a CRS.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import warp

    # A CRS-less raster whose true CRS is given via override -> stamp it first so
    # the warp to 27700 has a source CRS to project from.
    if ds.crs is None and crs is not None:
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs

        stamped = _stamp_crs_bytes(ds, resolve_crs(crs))
        with MemoryFile(stamped) as _mf, _mf.open() as _stamped_ds:
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
            mask = work_ds.read_masks(bi)  # 0 = invalid (nodata-derived)

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
    warped_bytes = warp.reproject_to_srid(ds, 27700, resampling="nearest")
    with MemoryFile(warped_bytes) as mf, mf.open() as work_ds:
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
    if resolution > conf.max_resolution:
        raise ValueError(
            f"raster->custom: resolution ({resolution}) exceeds max "
            f"{conf.max_resolution} for this grid conf"
        )

    def _run(work_ds):
        gt = work_ds.transform.to_gdal()  # (c, a, b, f, d, e) GDAL geotransform
        out = []
        for bi in range(1, work_ds.count + 1):
            band = work_ds.read(bi).astype("float64")
            mask = work_ds.read_masks(bi)  # 0 = invalid (nodata-derived)

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
