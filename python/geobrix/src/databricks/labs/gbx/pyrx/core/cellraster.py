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
exclusively -- quadbin cell math has a single source of truth.
"""

import math

import h3
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import Affine

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
    if src == dst:
        return np.asarray(xs, dtype="float64"), np.asarray(ys, dtype="float64")
    from pyproj import Transformer

    tr = Transformer.from_crs(src, dst, always_xy=True)
    x2, y2 = tr.transform(np.asarray(xs), np.asarray(ys))
    return np.asarray(x2, dtype="float64"), np.asarray(y2, dtype="float64")


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
        if srid == 4326:
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
        it = iter(keys)
        res = self._qb.resolution(next(it))
        for k in keys:
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
        import quadbin

        w, _s, e, _n = quadbin.cell_to_bounding_box(int(next(iter(keys))))
        edge_deg = abs(e - w)
        if srid == 4326:
            return edge_deg
        midlat = (bymin + bymax) / 2.0
        return edge_deg * 111320.0 * max(math.cos(math.radians(midlat)), 1e-6)


_ADAPTERS = {"h3": _H3Adapter(), "quadbin": _QuadbinAdapter()}


def _adapter(grid):
    try:
        return _ADAPTERS[grid]
    except KeyError:
        raise ValueError(f"unknown grid {grid!r}; expected 'h3' or 'quadbin'")


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


def snap_bounds(bxmin, bymin, bxmax, bymax, pixel_size):
    """Snap a bounding box outward to the nearest pixel_size lattice.

    Returns (xmin, ymin, xmax, ymax, width, height) where xmin and ymax are
    integer multiples of pixel_size, and the grid is at least 1x1. Independent
    grids built with the same pixel_size and these snapped origins will align.
    Note: pixel_size is NOT included in the return tuple; callers must thread
    it separately (e.g. compute_gridspec inserts it into the returned gridspec).
    """
    xmin = math.floor(bxmin / pixel_size) * pixel_size
    ymax = math.ceil(bymax / pixel_size) * pixel_size
    width = max(1, int(math.ceil((bxmax - xmin) / pixel_size)))
    height = max(1, int(math.ceil((ymax - bymin) / pixel_size)))
    xmax = xmin + width * pixel_size
    ymin = ymax - height * pixel_size
    return xmin, ymin, xmax, ymax, width, height


def compute_gridspec(
    cellids, srid=4326, pixel_size=None, mode="centroids", kring_pad=1, grid="h3"
):
    """Snapped, lattice-aligned grid spec for a cell set.

    Returns (xmin, ymin, xmax, ymax, pixel_size, width, height, srid).
    """
    ad = _adapter(grid)
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
):
    """Burn {cellid:int -> value:float} onto a width x height grid (centroid burn).

    Arg order matches the `compute_gridspec` 8-tuple (so callers splat it:
    `cells_to_raster(cell_values, *gridspec, resolution=res)`). The snapped grid has
    square pixels of `pixel_size`. Returns single-band float64 GTiff bytes; NoData
    where no cell covers a pixel.
    """
    ad = _adapter(grid)
    lut = {ad.to_key(c): float(v) for c, v in cell_values.items()}
    transform = Affine(pixel_size, 0.0, xmin, 0.0, -pixel_size, ymax)

    cols = np.arange(width) + 0.5
    rows = np.arange(height) + 0.5
    gx, gy = np.meshgrid(xmin + cols * pixel_size, ymax - rows * pixel_size)  # (h, w)
    lon, lat = _reproject(gx.ravel(), gy.ravel(), srid, ad.src_crs)

    out = np.full(lon.size, _NODATA, dtype="float64")
    # Scalar cell index per pixel (no array API). The grid is bounded to the cells'
    # padded bbox, so this is O(pixels-in-footprint). PERF FOLLOW-UP: restrict to
    # pixels within each cell's local window instead of the whole grid.
    for i in range(lon.size):
        v = lut.get(ad.pixel_key(float(lon[i]), float(lat[i]), resolution))
        if v is not None:
            out[i] = v

    data = out.reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float64",
        crs=f"EPSG:{srid}",
        transform=transform,
        nodata=_NODATA,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()
