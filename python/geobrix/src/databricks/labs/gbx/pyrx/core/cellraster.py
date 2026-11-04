"""Rasterize a set of discrete-global-grid cells onto a regular grid (pixel-centroid burn).

The inverse of core.gridagg.raster_to_grid: there each pixel centroid is indexed
to a grid cell; here each output pixel takes the value of the cell containing its
centroid. Pure functions (no Spark); rasterio + numpy + pyproj, plus the grid
cell math (h3 for ``grid="h3"``, ``pygx._quadbin`` for ``grid="quadbin"``).

Grid dispatch: ``compute_gridspec`` / ``cell_bbox`` / ``cells_to_raster`` take a
``grid`` param and delegate the cell-specific calls to a per-grid adapter
(``_ADAPTERS``). The H3 adapter binds the original ``h3.*`` calls verbatim, so
the H3 path is behaviorally identical to the pre-adapter code (its existing
tests are the regression gate). The quadbin adapter binds ``pygx._quadbin``
exclusively -- quadbin cell math has a single source of truth. The BNG adapter
binds ``pygx._bng`` exclusively and is EPSG:27700-NATIVE (``src_crs`` 27700, no
WGS84 hop): its cell ids are STRING on the public surface (``parse``/``format``
round-trip a Long digit-id internally).
"""

import math
import sys

import h3
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import Affine

from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core._util import snap_bounds  # noqa: F401  (re-exported)

_NODATA = -9999.0
_U64 = 0xFFFFFFFFFFFFFFFF


def _h3_str(cellid) -> str:
    """Canonical h3 string for a (possibly signed) Spark Long cell id."""
    return h3.int_to_str(int(cellid) & _U64)


def _resolution(cell_strs) -> int:
    """H3 resolution of a set of h3 STRING ids; error on mixed.

    Retained (H3-string signature) for callers in ``functions.py`` that pass
    ``[_h3_str(c) for c in cells]``. Grid-generic resolution lives on the
    adapter (``resolution``), which operates on native cell keys.
    """
    res = h3.get_resolution(next(iter(cell_strs)))
    for c in cell_strs:
        if h3.get_resolution(c) != res:
            raise ValueError("H3 cell set has mixed resolutions")
    return res


def _reproject(xs, ys, src, dst):
    # src/dst may be an int SRID, int-like/CRS string, or a CRS object. Route
    # through the shared resolver (int/int-like classified EPSG-vs-ESRI via the
    # authoritative rule — a bare int like 54008 is ESRI, NOT EPSG:54008 which
    # pyproj's lenient from_user_input would reject). The identity short-circuit
    # survives across spellings (4326 == "EPSG:4326" == CRS(4326)).
    from databricks.labs.gbx.core.crs import get_transformer

    s = _norm_out_crs(src)
    d = _norm_out_crs(dst)
    if s == d:
        return np.asarray(xs, dtype="float64"), np.asarray(ys, dtype="float64")
    tr = get_transformer(s, d)
    x2, y2 = tr.transform(np.asarray(xs), np.asarray(ys))
    return np.asarray(x2, dtype="float64"), np.asarray(y2, dtype="float64")


def _norm_out_crs(srid):
    """Resolve an output-CRS spec (int SRID, int-like/CRS string, or CRS object) to
    a rasterio CRS via the shared resolver — the single place grid rasterize output
    CRS is built. Ints/int-like are classified EPSG-vs-ESRI (54008 -> ESRI:54008)."""
    from rasterio.crs import CRS as _RioCRS

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    if isinstance(srid, _RioCRS):
        return srid
    return resolve_crs(srid)


def _is_geographic(srid) -> bool:
    """Whether an output-CRS spec is geographic (lon/lat degrees). Replaces the old
    ``srid == 4326`` check so ANY geographic CRS gets the degrees pixel-size path,
    not only EPSG:4326."""
    try:
        return bool(_norm_out_crs(srid).is_geographic)
    except Exception:
        return False


# --- per-grid adapters --------------------------------------------------------
# Each adapter works in the grid's *native key space* (h3 -> h3 string, quadbin
# -> unsigned int cell id). `to_key` maps a raw (possibly signed Spark Long) id
# into that space; `pixel_key` maps a WGS84 pixel centroid to the same space;
# `cell_center`/`cell_boundary` return (lon, lat) samples in the adapter's
# `src_crs`. This keeps `compute_gridspec`/`cells_to_raster` grid-agnostic.


class _H3Adapter:
    """Binds the original ``h3.*`` calls verbatim (refactor-only, no behavior change)."""

    src_crs = 4326

    def to_key(self, cellid):
        return _h3_str(cellid)

    def resolution(self, keys) -> int:
        return _resolution(list(keys))

    def k_ring(self, key, k):
        return h3.grid_disk(key, k)

    def cell_center(self, key):
        lat, lon = h3.cell_to_latlng(key)  # h3 returns (lat, lon)
        return lon, lat

    def cell_boundary(self, key):
        return [(lo, la) for la, lo in h3.cell_to_boundary(key)]  # -> (lon, lat)

    def pixel_key(self, lon, lat, res):
        return h3.latlng_to_cell(float(lat), float(lon), res)

    def default_pixel_size(self, keys, res, srid, bymin, bymax):
        edge_m = h3.average_hexagon_edge_length(res, unit="m")
        if _is_geographic(srid):
            midlat = (bymin + bymax) / 2.0
            return edge_m / (111320.0 * max(math.cos(math.radians(midlat)), 1e-6))
        return edge_m


class _QuadbinAdapter:
    """Binds ``pygx._quadbin`` -- the single source of truth for quadbin cell math."""

    src_crs = 4326

    def __init__(self):
        from shapely import from_wkb

        from databricks.labs.gbx.pygx import _quadbin as _qb

        self._qb = _qb
        self._from_wkb = from_wkb

    def to_key(self, cellid):
        return int(cellid) & _U64

    def resolution(self, keys) -> int:
        keys = list(keys)
        res = self._qb.resolution(keys[0])
        for k in keys[1:]:
            if self._qb.resolution(k) != res:
                raise ValueError("quadbin cell set has mixed resolutions")
        return res

    def k_ring(self, key, k):
        return [self.to_key(c) for c in self._qb.k_ring(key, k)]

    def cell_center(self, key):
        # pygx._quadbin.centroid -> EWKB Point (corner-mean, matches heavy).
        pt = self._from_wkb(self._qb.centroid(key))
        return pt.x, pt.y

    def cell_boundary(self, key):
        # pygx._quadbin.as_wkb -> EWKB box polygon; exterior ring corners.
        poly = self._from_wkb(self._qb.as_wkb(key))
        return list(poly.exterior.coords)  # already (lon, lat)

    def pixel_key(self, lon, lat, res):
        return self._qb.point_as_cell(float(lon), float(lat), res) & _U64

    def default_pixel_size(self, keys, res, srid, bymin, bymax):
        # Mirror RST_Quadbin_RasterizeAgg.computeGridspec: native edge = a sample
        # cell's bbox lon-width in degrees. In WGS84 that IS the pixel size; for a
        # projected srid approximate the metre edge at the extent's mid-latitude.
        # Route through pygx._quadbin (SSoT) via as_wkb -> shapely bounds.
        sample_key = next(iter(keys))
        poly = self._from_wkb(self._qb.as_wkb(sample_key))
        minx, _miny, maxx, _maxy = poly.bounds
        edge_deg = abs(maxx - minx)
        if _is_geographic(srid):
            return edge_deg
        midlat = (bymin + bymax) / 2.0
        return edge_deg * 111320.0 * max(math.cos(math.radians(midlat)), 1e-6)


class _BngAdapter:
    """Binds ``pygx._bng`` -- the single source of truth for BNG cell math.

    BNG is EPSG:27700-NATIVE: unlike the H3/quadbin adapters (which sample in
    WGS84 lon/lat and let ``compute_gridspec``/``cells_to_raster`` reproject to
    the output srid), ``src_crs`` here is 27700. ``cell_center``/``cell_boundary``
    return 27700 eastings/northings directly from ``pygx._bng.cell_id_to_geometry``,
    so with a 27700 output srid ``_reproject`` is an identity no-op -- there is NO
    WGS84 hop anywhere in the BNG path. Public BNG ids are STRING; the internal
    key space is the Long digit-id (``parse``/``format`` round-trip).
    """

    src_crs = 27700

    def __init__(self):
        from databricks.labs.gbx.pygx import _bng as _b

        self._b = _b

    def to_key(self, cellid):
        # Public BNG ids are STRING; parse to the internal Long digit-id. Tolerate
        # an already-parsed Long (idempotent for internal callers).
        if isinstance(cellid, str):
            return self._b.parse(cellid)
        return int(cellid)

    def resolution(self, keys) -> int:
        keys = list(keys)
        res = self._b.get_resolution_from_digits(self._b.cell_digits(keys[0]))
        for k in keys[1:]:
            if self._b.get_resolution_from_digits(self._b.cell_digits(k)) != res:
                raise ValueError("BNG cell set has mixed resolutions")
        return res

    def k_ring(self, key, k):
        return self._b.k_ring(key, k)

    def cell_center(self, key):
        # 27700 eastings/northings directly (NO WGS84 reproject).
        c = self._b.cell_id_to_geometry(key).centroid
        return c.x, c.y

    def cell_boundary(self, key):
        # 27700 boundary ring (NO WGS84 reproject).
        return list(self._b.cell_id_to_geometry(key).exterior.coords)

    def pixel_key(self, x, y, res):
        # x, y are 27700 eastings/northings (src_crs == output srid == 27700).
        return self._b.point_to_cell_id(float(x), float(y), res)

    def pixel_keys(self, x, y, res):
        """`pixel_key` of arrays: the same encoder core, so the two cannot drift. Raises, as the scalar does, for a
        coordinate that is not finite."""
        x, y = np.asarray(x, dtype="float64"), np.asarray(y, dtype="float64")
        if not (np.isfinite(x).all() and np.isfinite(y).all()):
            raise ValueError(
                "Non-finite (NaN or infinite) coordinates are not supported."
            )
        return self._b.point_to_cell_id_vec(x, y, res)

    def default_pixel_size(self, keys, res, srid, bymin, bymax):
        # BNG has a metric edge (metres); 27700 is metric, so the metre edge IS
        # the pixel size directly -- NO degree conversion (the whole BNG path is
        # 27700-native). ``srid`` is forced 27700 by the UDF, so no 4326 branch.
        return float(self._b.get_edge_size(res))


class _CustomAdapter:
    """Binds ``pygx._custom`` -- the single source of truth for custom-grid cell math.

    Unlike the H3/quadbin/BNG adapters, this is NOT a singleton: it must be
    instantiated with the ``CustomGridConf`` for each unique grid definition.
    ``src_crs`` is the grid's native CRS (``conf.srid``); when the grid has no
    CRS (``srid == -1``) a caller-supplied ``fallback_srid`` is used so that
    ``_reproject`` remains a valid identity no-op when ``src_crs == out_srid``.
    """

    def __init__(self, conf, fallback_srid=None):
        from databricks.labs.gbx.pygx import _custom as _c

        self._c = _c
        self._conf = conf
        # srid==-1 means the grid has no CRS; use fallback (typically out_srid)
        # so _reproject(src=src_crs, dst=out_srid) is the identity no-op.
        self.src_crs = conf.srid if conf.srid != -1 else (fallback_srid or 4326)

    def to_key(self, cellid):
        # Custom cell IDs are BIGINT ints; store as-is.
        return int(cellid)

    def resolution(self, keys) -> int:
        keys = list(keys)
        res = self._c.get_cell_resolution(keys[0])
        for k in keys[1:]:
            if self._c.get_cell_resolution(k) != res:
                raise ValueError("custom cell set has mixed resolutions")
        return res

    def k_ring(self, key, k):
        return self._c.k_ring(self._conf, key, k)

    def cell_center(self, key):
        # Returns (x, y) in the grid's native CRS (conf.srid).
        centroid = self._c.cell_id_to_centroid(self._conf, key)
        return centroid.x, centroid.y

    def cell_boundary(self, key):
        poly = self._c.cell_id_to_polygon(self._conf, key)
        return list(poly.exterior.coords)

    def pixel_key(self, x, y, res):
        # x, y are in the grid's native CRS (src_crs == conf.srid usually).
        # Use point_to_cell_id_or_none so pixels that fall outside the custom
        # grid's declared bounds return None (-> NoData) instead of raising
        # ValueError (the snap_bounds gridspec can extend slightly past the
        # k-ring boundary, producing a few off-grid pixel centroids).
        return self._c.point_to_cell_id_or_none(self._conf, float(x), float(y), res)

    def default_pixel_size(self, keys, res, srid, bymin, bymax):
        # Default pixel = min(cell_width, cell_height) at this resolution.
        # Cell dimensions are in the grid's native unit (conf.srid), and since
        # the output srid defaults to conf.srid, no conversion is needed.
        w = self._c.cell_width(self._conf, res)
        h = self._c.cell_height(self._conf, res)
        return min(w, h)


_ADAPTERS = {"h3": _H3Adapter(), "quadbin": _QuadbinAdapter(), "bng": _BngAdapter()}


def _adapter(grid):
    try:
        return _ADAPTERS[grid]
    except KeyError:
        raise ValueError(f"unknown grid {grid!r}; expected 'h3', 'quadbin' or 'bng'")


def cell_bbox(cellid, srid=4326, mode="centroids", grid="h3"):
    """(xmin, ymin, xmax, ymax) for one cell in `srid`.

    mode='centroids' -> the centroid point (degenerate bbox); 'spatial_envelope'
    -> the cell boundary envelope.
    """
    ad = _adapter(grid)
    key = ad.to_key(cellid)
    if mode == "centroids":
        lon, lat = ad.cell_center(key)
        lons, lats = [lon], [lat]
    elif mode == "spatial_envelope":
        b = ad.cell_boundary(key)  # [(lon, lat), ...]
        lons = [p[0] for p in b]
        lats = [p[1] for p in b]
    else:
        raise ValueError(f"unknown mode {mode!r}")
    xs, ys = _reproject(lons, lats, ad.src_crs, srid)
    return float(xs.min()), float(ys.min()), float(xs.max()), float(ys.max())


def compute_gridspec(
    cellids,
    srid=4326,
    pixel_size=None,
    mode="centroids",
    kring_pad=1,
    grid="h3",
    _ad=None,
):
    """Snapped, lattice-aligned grid spec for a cell set.

    Returns (xmin, ymin, xmax, ymax, pixel_size, width, height, srid).

    ``_ad`` may be a pre-built adapter instance (e.g. ``_CustomAdapter``).
    When provided it takes precedence over the ``grid`` name lookup.
    """
    ad = _ad if _ad is not None else _adapter(grid)
    cells = {ad.to_key(c) for c in cellids}
    if not cells:
        raise ValueError("empty cell set")
    res = ad.resolution(cells)
    if kring_pad and kring_pad > 0:
        padded = set()
        for c in cells:
            padded.update(ad.k_ring(c, kring_pad))
        cells = padded

    if mode == "centroids":
        pts = [ad.cell_center(c) for c in cells]  # (lon, lat)
        lons = [p[0] for p in pts]
        lats = [p[1] for p in pts]
    elif mode == "spatial_envelope":
        lons, lats = [], []
        for c in cells:
            for lo, la in ad.cell_boundary(c):  # (lon, lat)
                lons.append(lo)
                lats.append(la)
    else:
        raise ValueError(f"unknown mode {mode!r}")

    xs, ys = _reproject(lons, lats, ad.src_crs, srid)
    bxmin, bxmax = float(xs.min()), float(xs.max())
    bymin, bymax = float(ys.min()), float(ys.max())

    if pixel_size is None:
        pixel_size = ad.default_pixel_size(cells, res, srid, bymin, bymax)

    xmin, ymin, xmax, ymax, width, height = snap_bounds(
        bxmin, bymin, bxmax, bymax, pixel_size
    )
    return (xmin, ymin, xmax, ymax, pixel_size, width, height, srid)


_BAND_PIXELS = (
    1 << 20
)  # pixels indexed at a time, so a huge canvas holds a few arrays of this size, not six of its own


def _burn_banded(out, lut, ad, lon, lat, resolution, width):
    """Write each pixel's cell value into `out` (flat, row-major): the cell index of a band of rows at a time
    (`ad.pixel_keys`), looked up in the sorted cell ids. A pixel whose cell is not in `lut` keeps its NoData.
    Every pixel is indexed also when there are no cells, so a coordinate that is not finite is refused as it was
    when each pixel was indexed on its own.
    """
    keys = np.fromiter(lut.keys(), dtype=np.int64, count=len(lut))
    vals = np.fromiter(lut.values(), dtype="float64", count=len(lut))
    order = np.argsort(keys)
    keys, vals = keys[order], vals[order]
    step = max(1, _BAND_PIXELS // max(1, width)) * width  # whole rows
    for lo in range(0, lon.size, step):
        pk = ad.pixel_keys(lon[lo : lo + step], lat[lo : lo + step], resolution)
        if keys.size:
            pos = np.minimum(np.searchsorted(keys, pk), len(keys) - 1)
            hit = keys[pos] == pk
            out[lo : lo + pk.size][hit] = vals[pos[hit]]


def cells_to_raster(
    cell_values,
    xmin,
    ymin,
    xmax,
    ymax,
    pixel_size,
    width,
    height,
    srid,
    resolution,
    grid="h3",
    _ad=None,
):
    """Burn {cellid:int -> value:float} onto a width x height grid (centroid burn).

    Arg order matches the `compute_gridspec` 8-tuple (so callers splat it:
    `cells_to_raster(cell_values, *gridspec, resolution=res)`). The snapped grid has
    square pixels of `pixel_size`. Returns single-band float64 GTiff bytes; NoData
    where no cell covers a pixel.

    ``_ad`` may be a pre-built adapter instance (e.g. ``_CustomAdapter``).
    When provided it takes precedence over the ``grid`` name lookup.
    """
    ad = _ad if _ad is not None else _adapter(grid)
    lut = {ad.to_key(c): float(v) for c, v in cell_values.items()}
    transform = Affine(pixel_size, 0.0, xmin, 0.0, -pixel_size, ymax)

    cols = np.arange(width) + 0.5
    rows = np.arange(height) + 0.5
    gx, gy = np.meshgrid(xmin + cols * pixel_size, ymax - rows * pixel_size)  # (h, w)
    lon, lat = _reproject(gx.ravel(), gy.ravel(), srid, ad.src_crs)

    out = np.full(lon.size, _NODATA, dtype="float64")
    if hasattr(ad, "pixel_keys"):
        _burn_banded(out, lut, ad, lon, lat, resolution, width)
    else:
        # No array API for this grid: one cell index per pixel. The grid is bounded to the
        # cells' padded bbox, so this is O(pixels-in-footprint).
        for i in range(lon.size):
            v = lut.get(ad.pixel_key(float(lon[i]), float(lat[i]), resolution))
            if v is not None:
                out[i] = v

    data = out.reshape(height, width)
    decoded_bytes = data.nbytes
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float64",
        crs=crs_to_canonical(_norm_out_crs(srid)),  # canonical (EPSG/ESRI/WKT)
        transform=transform,
        nodata=_NODATA,
    )
    profile.update(
        _comp.creation_opts("float64", decoded_bytes=decoded_bytes, compress="auto")
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


_NO_VALUE = object()


def is_missing(v):
    """True for a NULL: None, NaN, or ``pd.NA``. pandas is used only when the caller already imported it (a Spark UDF
    always has), so this core stays importable without it."""
    if v is None:
        return True
    pd = sys.modules.get("pandas")
    if pd is not None:
        try:
            return bool(pd.isna(v))
        except (TypeError, ValueError):  # an array-like is a value, not a NULL
            return False
    return v != v


def max_value_per_cell(cells, vals, key=None, typed=True):
    """``{cell: value}`` for the four cell rasterizers, one value per distinct cell.

    The duplicate-cell rule of ``rst_{h3,quadbin,bng,custom}_rasterize_agg``: a cell id
    that repeats keeps its LARGEST value, the same as the heavyweight tier, which sorts
    the group by ``(cellid, value)`` and lets the last write win. Arrival order inside a
    Spark group is not fixed, so "the last row wins" would change the raster from run to
    run; the largest value does not depend on order.

    A null value never wins. A typed Double column delivers a null as ``NaN`` and a real
    ``NaN`` looks the same, so both are read as null: they are ignored whenever the cell
    has a number, and a cell whose every value is null burns ``NaN``. ``typed`` is False
    when the value was omitted (the UDF then receives an object column of ``None``, not a
    float column): that is the presence mask, every cell burns ``1.0``. A typed column
    of nulls is data and follows the rule above.

    ``key`` maps a raw cell id to the id it is stored under (``adapter.to_key``), so two
    spellings of one cell (``TQ3080`` and ``TQ 3080``, or a quadbin id as signed and
    unsigned) are one cell here too. The result is keyed by the raw id first seen.
    """
    best = {}  # stored id -> [raw id, value or None]
    for cell, val in zip(cells, vals):
        k = cell if key is None else key(cell)
        v = None if is_missing(val) else float(val)
        cur = best.get(k, _NO_VALUE)
        if cur is _NO_VALUE:
            best[k] = [cell, v]
        elif v is not None and (cur[1] is None or v > cur[1]):
            cur[1] = v
    if not typed and all(v is None for _, v in best.values()):
        return {raw: 1.0 for raw, _ in best.values()}
    return {raw: float("nan") if v is None else v for raw, v in best.values()}


def rasterize_cells(
    cell_values,
    cells,
    ad,
    grid_name,
    srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    check_canvas=None,
):
    """The gridspec and burn shared by the per-grid rasterize aggregates and their array twins.

    ``cell_values`` is ``{raw_id: float}`` (see :func:`max_value_per_cell`), ``cells`` the same raw ids, ``ad`` the
    per-grid adapter, ``srid`` the resolved output CRS. The canvas and the other per-group constants are plain values
    (None for NULL). ``check_canvas(width, height)``, when given, is called with the resolved canvas (given or derived)
    before anything is allocated, so a caller can refuse a canvas it cannot hold. Returns raw GTiff bytes.
    """
    res = ad.resolution([ad.to_key(c) for c in cells])

    def _has(v):
        return not is_missing(v)

    _mode = mode if _has(mode) else "centroids"
    _kp = int(kring_pad) if _has(kring_pad) else 1
    canvas = dict(
        xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax, width=width, height=height
    )
    given = [n for n, v in canvas.items() if _has(v)]
    if given and len(given) < len(canvas):
        # NULL xmin derives the canvas from the cells; a canvas given in part is neither that nor an explicit one
        missing = [n for n in canvas if n not in given]
        raise ValueError(
            f"{grid_name} rasterize_agg: the canvas is given in part ({', '.join(given)}), "
            f"but {', '.join(missing)} {'is' if len(missing) == 1 else 'are'} NULL. "
            "Fix: pass xmin, ymin, xmax, ymax, width and height together, or NULL for all six to derive the canvas "
            "from the cells."
        )
    if given:
        gs = (
            float(xmin),
            float(ymin),
            float(xmax),
            float(ymax),
            (float(xmax) - float(xmin)) / int(width),
            int(width),
            int(height),
            srid,
        )
    else:
        gs = compute_gridspec(
            cells,
            srid=srid,
            pixel_size=float(pixel_size) if _has(pixel_size) else None,
            mode=_mode,
            kring_pad=_kp,
            grid=grid_name,
            _ad=ad,
        )
    if check_canvas is not None:
        check_canvas(gs[5], gs[6])
    return cells_to_raster(cell_values, *gs, resolution=res, grid=grid_name, _ad=ad)


def cell_out_srid(fn, out_srid, out_crs):
    """The output CRS of an H3 or quadbin rasterizer: ``out_srid`` or ``out_crs``, never both (a blank ``out_crs`` is
    unset); neither is the grid-native EPSG:4326. An int SRID or a CRS string, which this module accepts
    interchangeably. A NULL is None or NaN."""

    def _set(v):
        return not is_missing(v)

    crs_set = _set(out_crs) and bool(str(out_crs).strip())
    srid_set = _set(out_srid)
    if crs_set and srid_set:
        raise ValueError(f"{fn}: out_srid and out_crs are both set. Fix: set one.")
    if crs_set:
        return out_crs
    return int(out_srid) if srid_set else 4326
