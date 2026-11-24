"""Identity tests for the vectorized pyrx raster->grid aggregation.

These pin the vectorized ``gridagg`` against (a) the heavy point-to-cell rule
(``pygx._quadbin.point_as_cell``: floor and clamp), including a pixel centre on
the antimeridian and on tile edges, and (b) a faithful reference copy of the
original per-pixel double-loop implementation (identical output for every
grid x agg).

The downstream bench gates these functions on a SORTED cell-id hash plus
order-independent measure stats (``fingerprint_dggs_grid``), so cell ORDER does
not matter -- only the cell-id SET and the per-cell measures.
"""

import contextlib

import h3
import numpy as np
import pytest
import quadbin
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pygx import _quadbin
from databricks.labs.gbx.pyrx.core import gridagg


@contextlib.contextmanager
def _open(raster_bytes):
    # the MemoryFile reads its bytes in place, without a copy: it, and so the bytes, must outlive every read
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        yield ds


def _custom_raster(data, nodata=-9999.0, epsg=4326, origin=(10.0, 50.0), px=0.5):
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# --- reference copy of the ORIGINAL per-pixel loop (the thing we replaced) ---
def _reduce_ref(values, agg):
    arr = np.asarray(values, dtype="float64")
    if agg == "avg":
        return float(arr.mean())
    if agg == "count":
        return float(arr.size)  # float for heavy-tier parity (count is now Double)
    if agg == "min":
        return float(arr.min())
    if agg == "max":
        return float(arr.max())
    if agg == "median":
        return float(np.median(arr))
    raise ValueError(agg)


def _raster_to_grid_ref(ds, resolution, grid, agg):
    resolution = int(resolution)
    if grid == "h3":

        def cell_fn(lon, lat, res):
            return h3.str_to_int(h3.latlng_to_cell(lat, lon, res))

    else:

        def cell_fn(lon, lat, res):
            return _quadbin.point_as_cell(lon, lat, res)

    gt = ds.transform.to_gdal()
    out = []
    for bi in range(1, ds.count + 1):
        band = ds.read(bi).astype("float64")
        mask = ds.read_masks(bi)
        height, width = band.shape
        acc = {}
        for y in range(height):
            for x in range(width):
                if mask[y, x] == 0:
                    continue
                x_off = x + 0.5
                y_off = y + 0.5
                lon = gt[0] + x_off * gt[1] + y_off * gt[2]
                lat = gt[3] + x_off * gt[4] + y_off * gt[5]
                cid = cell_fn(lon, lat, resolution)
                acc.setdefault(cid, []).append(band[y, x])
        out.append(
            [
                {"cellID": cid, "measure": _reduce_ref(vals, agg)}
                for cid, vals in acc.items()
            ]
        )
    return out


def _as_map(band):
    """{cellID: measure} -- order-independent comparison (matches the bench)."""
    return {c["cellID"]: c["measure"] for c in band}


def _assert_band_equal(new_band, old_band, agg):
    new_map, old_map = _as_map(new_band), _as_map(old_band)
    # No duplicate cell ids (the group-reduce must collapse them).
    assert len(new_band) == len(new_map)
    assert set(new_map) == set(old_map)
    for cid, old_v in old_map.items():
        new_v = new_map[cid]
        assert type(new_v) is type(old_v), (agg, cid, type(new_v), type(old_v))
        assert np.isclose(new_v, old_v, rtol=0, atol=0) or np.isclose(new_v, old_v)


# --- 1. quadbin raster->grid uses heavy's point->cell rule -------------------
# Heavy ``Quadbin.pointToCellID`` and light tessellate floor the web-mercator tile
# and clamp it into [0, 2^z - 1] (``_quadbin.point_as_cell``). The CARTO library
# (``quadbin.point_to_cell``) instead wraps the tile x, so lon = 180 lands on tile
# 0, and rounds the tile y differently on an edge.
_EDGE_Z = 10
_EDGE_TILE_DEG = 360.0 / (1 << _EDGE_Z)  # 0.3515625: exact in binary


def _centres(ds):
    """Pixel-centre (lon, lat) of every pixel, as the aggregation computes them."""
    gt = ds.transform.to_gdal()
    return [
        (
            gt[0] + (c + 0.5) * gt[1] + (r + 0.5) * gt[2],
            gt[3] + (c + 0.5) * gt[4] + (r + 0.5) * gt[5],
        )
        for r in range(ds.height)
        for c in range(ds.width)
    ]


def _expected_counts(ds, z):
    exp = {}
    for lon, lat in _centres(ds):
        cid = _quadbin.point_as_cell(lon, lat, z)
        exp[cid] = exp.get(cid, 0.0) + 1.0
    return exp


def test_quadbin_raster_to_grid_tile_x_edges_and_antimeridian():
    # A point-registered 1025-column row: the centres sit exactly on every zoom-10
    # tile x edge, from lon = -180 to lon = +180 (the last column is the antimeridian).
    w = (1 << _EDGE_Z) + 1
    data = np.arange(1, w + 1, dtype="float32").reshape(1, w)
    raster = _custom_raster(
        data, origin=(-180.0 - _EDGE_TILE_DEG / 2, 10.0), px=_EDGE_TILE_DEG
    )
    with _open(raster) as ds:
        centres = _centres(ds)
        assert centres[0][0] == -180.0 and centres[-1][0] == 180.0
        exp = _expected_counts(ds, _EDGE_Z)
        got = gridagg.raster_to_grid(
            ds, _EDGE_Z, "quadbin", "count", coverage="sparse", assignment="centroid"
        )
    assert {c["cellID"]: c["measure"] for c in got[0]} == exp
    # lon = 180 is the easternmost tile (n - 1), never the wrapped tile 0.
    east_cell = _quadbin.point_as_cell(180.0, 10.0, _EDGE_Z)
    assert quadbin.cell_to_tile(east_cell)[0] == (1 << _EDGE_Z) - 1
    assert east_cell in {c["cellID"] for c in got[0]}


def test_quadbin_raster_to_grid_tile_y_edges():
    # One pixel per zoom-10 tile y edge (the latitude where web-mercator row k
    # starts), centre exactly on it. Compare to heavy's rule, and make sure the
    # fixture reaches edges where the CARTO library disagrees (else the test is vacuous).
    n = 1 << _EDGE_Z
    px = 2.0**-20
    differing = 0
    for k in range(1, n, 17):
        lat = np.degrees(np.arctan(np.sinh(np.pi * (1.0 - 2.0 * k / n))))
        raster = _custom_raster(
            np.ones((1, 1), dtype="float32"),
            origin=(10.0 - px / 2, float(lat) + px / 2),
            px=px,
        )
        with _open(raster) as ds:
            lon_c, lat_c = _centres(ds)[0]
            exp = _expected_counts(ds, _EDGE_Z)
            got = gridagg.raster_to_grid(
                ds,
                _EDGE_Z,
                "quadbin",
                "count",
                coverage="sparse",
                assignment="centroid",
            )
        assert {c["cellID"]: c["measure"] for c in got[0]} == exp, (k, lat_c)
        differing += quadbin.point_to_cell(lon_c, lat_c, _EDGE_Z) != next(iter(exp))
    assert differing > 0, "no tile-y edge distinguishes the CARTO rule from heavy's"


# --- 2. raster_to_grid identity: new == old, every grid x agg ----------------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "count", "min", "max", "median"])
def test_raster_to_grid_identity_synthetic(grid, agg):
    rng = np.random.default_rng(7)
    data = rng.normal(100.0, 20.0, size=(13, 17)).astype("float32")
    data[2, 3] = -9999.0  # nodata pixels exercising the mask==0 skip
    data[7, 11] = -9999.0
    data[0, 0] = -9999.0
    raster = _custom_raster(data, px=0.25)
    for res in (3, 6, 9):
        with _open(raster) as ds:
            new = gridagg.raster_to_grid(
                ds, res, grid, agg, coverage="sparse", assignment="centroid"
            )
        with _open(raster) as ds:
            old = _raster_to_grid_ref(ds, res, grid, agg)
        assert len(new) == len(old) == 1
        _assert_band_equal(new[0], old[0], agg)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "count", "min", "max", "median"])
def test_raster_to_grid_identity_multiband(grid, agg):
    rng = np.random.default_rng(11)
    b1 = rng.uniform(0, 50, size=(9, 9)).astype("float32")
    b2 = rng.uniform(50, 99, size=(9, 9)).astype("float32")
    b1[1, 1] = -9999.0
    profile = dict(
        driver="GTiff",
        width=9,
        height=9,
        count=2,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.3, 0.3),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(b1, 1)
            dst.write(b2, 2)
        raster = mf.read()
    with _open(raster) as ds:
        new = gridagg.raster_to_grid(
            ds, 7, grid, agg, coverage="sparse", assignment="centroid"
        )
    with _open(raster) as ds:
        old = _raster_to_grid_ref(ds, 7, grid, agg)
    assert len(new) == len(old) == 2
    for nb, ob in zip(new, old):
        _assert_band_equal(nb, ob, agg)


# --- 3. H3 covering: the exact box-polygon overlap kernel vs shapely (GEOS) ----
# The vectorised H3 covering pass weights each (cell, pixel) pair by the overlap area of the cell's hexagon and the
# pixel's box. ``_covering_band_h3_shapely_ref`` is the shapely implementation it replaced (the reference); the
# library computes the same areas with ``gridagg._box_overlap_area``, without GEOS.
def _h3_hexagons_ref(cells):
    import shapely

    rings = [[(lng, lat) for lat, lng in h3.cell_to_boundary(c)] for c in cells]
    out = np.empty(len(cells), dtype=object)
    by_n = {}
    for i, ring in enumerate(rings):
        by_n.setdefault(len(ring), []).append(i)
    for idx in by_n.values():
        out[np.asarray(idx)] = shapely.polygons(
            np.asarray([rings[i] for i in idx], dtype="float64")
        )
    return out


def _cell_pixel_pairs_ref(hexes, gt, height, width):
    import shapely

    b = shapely.bounds(hexes)
    ca, cb = (b[:, 0] - gt[0]) / gt[1], (b[:, 2] - gt[0]) / gt[1]
    ra, rb = (b[:, 1] - gt[3]) / gt[5], (b[:, 3] - gt[3]) / gt[5]
    c0 = np.clip(np.floor(np.minimum(ca, cb)), 0, width - 1).astype(np.int64)
    c1 = np.clip(np.floor(np.maximum(ca, cb)), -1, width - 1).astype(np.int64)
    r0 = np.clip(np.floor(np.minimum(ra, rb)), 0, height - 1).astype(np.int64)
    r1 = np.clip(np.floor(np.maximum(ra, rb)), -1, height - 1).astype(np.int64)
    ncol, nrow = np.maximum(c1 - c0 + 1, 0), np.maximum(r1 - r0 + 1, 0)
    n = ncol * nrow
    cell = np.repeat(np.arange(hexes.size), n)
    off = np.arange(int(n.sum())) - np.repeat(np.cumsum(n) - n, n)
    cols = np.repeat(c0, n) + off % np.repeat(np.maximum(ncol, 1), n)
    rows = np.repeat(r0, n) + off // np.repeat(np.maximum(ncol, 1), n)
    return cell, rows, cols


def _covering_band_h3_shapely_ref(
    band, mask, resolution, agg, gt, frame="lonlat", floor=0.0
):
    """The shapely covering pass (one ``shapely.intersection`` per (cell, pixel) pair), kept here as the reference.

    ``frame="lonlat"`` is the replaced implementation as it was. ``frame="pixel"`` runs the same GEOS intersection on
    the pair moved to its pixel's south-west corner, so GEOS rounds at the pixel's scale rather than the longitude's.
    ``frame="exact"`` computes each overlap in rational arithmetic. A pair is kept when its weight is above ``floor``
    (0: the replaced pass).
    """
    import shapely

    from databricks.labs.gbx.pyrx.core.tessellate import _encode_cellid

    valid_rows = np.flatnonzero(mask.any(axis=1))
    if valid_rows.size == 0:
        return []
    valid_cols = np.flatnonzero(mask.any(axis=0))
    height, width = mask.shape
    px_area = abs(gt[1] * gt[5])
    r0, r1, c0, c1 = valid_rows[0], valid_rows[-1], valid_cols[0], valid_cols[-1]
    xa, xb = gt[0] + c0 * gt[1], gt[0] + (c1 + 1) * gt[1]
    ya, yb = gt[3] + r0 * gt[5], gt[3] + (r1 + 1) * gt[5]
    west, east = min(xa, xb) - abs(gt[1]), max(xa, xb) + abs(gt[1])
    south, north = min(ya, yb) - abs(gt[5]), max(ya, yb) + abs(gt[5])
    poly = h3.LatLngPoly([(south, west), (north, west), (north, east), (south, east)])
    cells = list(h3.polygon_to_cells_experimental(poly, resolution, contain="overlap"))
    out = []
    step = 20000
    for start in range(0, len(cells), step):
        chunk = cells[start : start + step]
        hexes = _h3_hexagons_ref(chunk)
        cell, rows, cols = _cell_pixel_pairs_ref(hexes, gt, height, width)
        ok = mask[rows, cols]
        cell, rows, cols = cell[ok], rows[ok], cols[ok]
        if cell.size == 0:
            continue
        xl, xr = gt[0] + cols * gt[1], gt[0] + (cols + 1) * gt[1]
        yt, yb = gt[3] + rows * gt[5], gt[3] + (rows + 1) * gt[5]
        boxes = shapely.box(
            np.minimum(xl, xr),
            np.minimum(yt, yb),
            np.maximum(xl, xr),
            np.maximum(yt, yb),
        )
        polys = hexes[cell]
        if frame == "pixel":
            corner = shapely.bounds(boxes)[:, :2]
            n_coords = shapely.get_num_coordinates(polys)
            polys = shapely.transform(
                polys, lambda c: c - np.repeat(corner, n_coords, axis=0)
            )
            boxes = shapely.transform(boxes, lambda c: c - np.repeat(corner, 5, axis=0))
        if frame == "exact":
            # rational arithmetic per pair (``_exact_overlap``): no GEOS, so no topology error on a distorted ring
            b = shapely.bounds(boxes)
            area = [
                float(_exact_overlap(list(shapely.get_coordinates(p)[:-1]), tuple(q)))
                for p, q in zip(polys, b)
            ]
            w = np.asarray(area) / px_area
        else:
            w = shapely.area(shapely.intersection(polys, boxes)) / px_area
        keep = w > floor
        cell, w = cell[keep], w[keep]
        vals = band[rows[keep], cols[keep]].astype("float64")
        if cell.size == 0:
            continue
        uniq, measures = gridagg._grouped_measures(cell, vals, agg, weights=w)
        out.extend(
            {"cellID": _encode_cellid(chunk[int(c)], "h3"), "measure": m}
            for c, m in zip(uniq.tolist(), measures)
        )
    return out


_IOWA = (-93.70, 42.05)  # north-west corner of the test grids
_PX_30M = (30.0 / (111320.0 * np.cos(np.radians(42.0))), 30.0 / 111320.0)
_PX_4KM = (4000.0 / (111320.0 * np.cos(np.radians(42.0))), 4000.0 / 111320.0)


def _grid(shape, px, seed, valid=0.7):
    rng = np.random.default_rng(seed)
    band = rng.normal(0.5, 0.2, size=shape)
    mask = rng.random(shape) < valid
    gt = (_IOWA[0], px[0], 0.0, _IOWA[1], 0.0, -px[1])
    return band, mask, gt


# GEOS, run on (lon, lat) as the replaced pass did, rounds each intersection point at the longitude's scale: a weight is
# off by up to ~1e-11 of a pixel (CONFIRMED against rational arithmetic; the kernel is exact to ~1e-16), which shows
# beyond 1e-9 relative only in a cell that a sliver of overlap dominates. Against GEOS in the pixel's frame the states
# agree within 1e-9 relative (1e-12 absolute for a field that is rounding noise around 0, e.g. the M2 of one pixel).
_ABS_PIXEL_FRAME = 1e-12
_ABS_LONLAT_FRAME = 1e-10


def _assert_states_match(new, old, abs_tol):
    """Same cells; every partial-state field within 1e-9 relative or ``abs_tol`` absolute."""
    new_map, old_map = _as_map(new), _as_map(old)
    assert len(new) == len(new_map)
    assert set(new_map) == set(old_map)
    for cid, old_state in old_map.items():
        assert new_map[cid] == pytest.approx(old_state, rel=1e-9, abs=abs_tol), cid


_FRAMES = {"pixel": _ABS_PIXEL_FRAME, "lonlat": _ABS_LONLAT_FRAME}


@pytest.mark.parametrize(
    "shape,px,res,seed",
    [
        ((30, 40), _PX_30M, 12, 1),
        ((30, 40), _PX_30M, 12, 2),
        ((16, 20), _PX_30M, 13, 3),
        ((10, 12), _PX_4KM, 7, 4),
    ],
    ids=["30m_r12_a", "30m_r12_b", "30m_r13", "4km_r7"],
)
@pytest.mark.parametrize("valid", [0.7, 1.0])
@pytest.mark.parametrize("frame", sorted(_FRAMES))
def test_h3_covering_states_equal_the_shapely_reference(
    shape, px, res, seed, valid, frame
):
    band, mask, gt = _grid(shape, px, seed, valid)
    new = gridagg._covering_band_h3_vectorised(band, mask, res, gridagg._PARTIAL, gt)
    old = _covering_band_h3_shapely_ref(
        band, mask, res, gridagg._PARTIAL, gt, frame=frame
    )
    assert old  # the case is not vacuous
    _assert_states_match(new, old, _FRAMES[frame])


@pytest.mark.parametrize(
    "px,res", [(_PX_30M, 12), (_PX_30M, 13), (_PX_4KM, 7)], ids=["r12", "r13", "r7"]
)
@pytest.mark.parametrize("frame", sorted(_FRAMES))
def test_h3_covering_single_pixel_and_all_masked(px, res, frame):
    band, _, gt = _grid((9, 11), px, 5)
    one = np.zeros((9, 11), dtype=bool)
    one[4, 6] = True
    new = gridagg._covering_band_h3_vectorised(band, one, res, gridagg._PARTIAL, gt)
    old = _covering_band_h3_shapely_ref(
        band, one, res, gridagg._PARTIAL, gt, frame=frame
    )
    assert len(old) > 1  # the pixel is shared out over several cells
    _assert_states_match(new, old, _FRAMES[frame])
    # the pixel's weights sum to 1 (to the ~1e-12 that adjacent H3 rings, built apart, fail to tile the plane by)
    assert sum(s[0] for s in _as_map(new).values()) == pytest.approx(1.0, rel=1e-9)
    none = np.zeros((9, 11), dtype=bool)
    assert gridagg._covering_band_h3_vectorised(band, none, res, "avg", gt) == []


@pytest.mark.parametrize("agg", ["avg", "count", "sum", "min", "max", "variance"])
def test_h3_covering_statistics_equal_the_shapely_reference(agg):
    band, mask, gt = _grid((20, 24), _PX_30M, 6)
    new = gridagg._covering_band_h3_vectorised(band, mask, 12, agg, gt)
    old = _covering_band_h3_shapely_ref(band, mask, 12, agg, gt, frame="pixel")
    new_map, old_map = _as_map(new), _as_map(old)
    assert set(new_map) == set(old_map)
    for cid, v in old_map.items():
        assert new_map[cid] == pytest.approx(v, rel=1e-9, abs=_ABS_PIXEL_FRAME)


# --- the kernel itself: one (polygon, box) pair at a time, against shapely ------
def _kernel_area(ring, box):
    """``_box_overlap_area`` of one ring ((lon, lat) vertices, open) and one box (xmin, ymin, xmax, ymax)."""
    vx = np.array([[p[0] for p in ring]], dtype="float64")
    vy = np.array([[p[1] for p in ring]], dtype="float64")
    xl, yb, xr, yt = (np.array([v], dtype="float64") for v in box)
    return float(
        gridagg._box_overlap_area(vx, vy, np.zeros(1, np.intp), xl, yb, xr, yt)[0]
    )


def _shapely_area(ring, box):
    import shapely

    return float(
        shapely.area(shapely.intersection(shapely.Polygon(ring), shapely.box(*box)))
    )


def _hexagon(cx, cy, r, n=6, phase=0.1):
    t = phase + 2 * np.pi * np.arange(n) / n
    return list(zip(cx + r * np.cos(t), cy + 0.8 * r * np.sin(t)))


_HX, _HY, _HR = -93.7, 42.0, 2e-4
_KERNEL_CASES = {
    "box_inside_polygon": (
        _hexagon(_HX, _HY, _HR),
        (_HX - 5e-5, _HY - 4e-5, _HX + 5e-5, _HY + 4e-5),
    ),
    "polygon_inside_box": (
        _hexagon(_HX, _HY, _HR),
        (_HX - 3e-4, _HY - 3e-4, _HX + 3e-4, _HY + 3e-4),
    ),
    "corner_overlap": (
        _hexagon(_HX, _HY, _HR),
        (_HX + 1e-4, _HY + 1e-4, _HX + 4e-4, _HY + 4e-4),
    ),
    "straddles_one_side": (
        _hexagon(_HX, _HY, _HR),
        (_HX - 3e-4, _HY + 5e-5, _HX + 3e-4, _HY + 3e-4),
    ),
    "ten_vertices": (
        _hexagon(_HX, _HY, _HR, n=10),
        (_HX - 1.3e-4, _HY - 2e-4, _HX + 0.7e-4, _HY + 0.5e-4),
    ),
    "ten_vertices_clockwise": (
        _hexagon(_HX, _HY, _HR, n=10)[::-1],
        (_HX - 1.3e-4, _HY - 2e-4, _HX + 0.7e-4, _HY + 0.5e-4),
    ),
    "rectangle_polygon": (
        [(_HX, _HY), (_HX + 2e-4, _HY), (_HX + 2e-4, _HY + 1e-4), (_HX, _HY + 1e-4)],
        (_HX + 1e-4, _HY - 1e-4, _HX + 3e-4, _HY + 0.5e-4),
    ),
}


@pytest.mark.parametrize("case", sorted(_KERNEL_CASES))
def test_box_overlap_area_matches_shapely(case):
    ring, box = _KERNEL_CASES[case]
    want = _shapely_area(ring, box)
    assert want > 0
    assert _kernel_area(ring, box) == pytest.approx(want, rel=1e-9)


@pytest.mark.parametrize(
    "ring,box",
    [
        # disjoint, inside the hexagon's bounding box (beyond a slanted edge): the bbox test alone does not drop it
        (_hexagon(_HX, _HY, _HR), (_HX + 1.6e-4, _HY + 1.4e-4, _HX + 3e-4, _HY + 3e-4)),
        # disjoint on an axis
        (_hexagon(_HX, _HY, _HR), (_HX + 3e-4, _HY - 1e-4, _HX + 5e-4, _HY + 1e-4)),
        # touching along an edge: the polygon's edge x = _HX + 2e-4 is the box's west edge
        (
            [
                (_HX, _HY),
                (_HX + 2e-4, _HY),
                (_HX + 2e-4, _HY + 1e-4),
                (_HX, _HY + 1e-4),
            ],
            (_HX + 2e-4, _HY - 1e-4, _HX + 4e-4, _HY + 2e-4),
        ),
        # touching at a corner: the hexagon's vertex is the box's south-west corner
        (
            _hexagon(_HX, _HY, _HR, phase=0.0),
            (_HX + _HR, _HY - 1e-4, _HX + 4e-4, _HY + 0.0),
        ),
    ],
    ids=["disjoint_in_bbox", "disjoint_axis", "touching_edge", "touching_corner"],
)
def test_box_overlap_area_is_exactly_zero_without_overlap(ring, box):
    assert _shapely_area(ring, box) == 0.0
    assert (
        _kernel_area(ring, box) == 0.0
    )  # exactly 0: the covering pass drops the pair (weight > 0 is kept)


def test_box_overlap_area_mixed_vertex_counts_in_one_call():
    # one call over cells of 6 and 10 vertices: the shorter ring is padded by repeating its last vertex
    six, ten = _hexagon(_HX, _HY, _HR), _hexagon(_HX + 1e-3, _HY, _HR, n=10)
    padded = six + [six[-1]] * 4
    vx = np.array([[p[0] for p in r] for r in (padded, ten)])
    vy = np.array([[p[1] for p in r] for r in (padded, ten)])
    boxes = [
        (_HX - 1e-4, _HY - 1e-4, _HX + 1e-4, _HY + 0.5e-4),
        (_HX + 1e-3 - 1e-4, _HY, _HX + 1e-3 + 3e-4, _HY + 3e-4),
        (_HX + 1.6e-4, _HY + 1.4e-4, _HX + 3e-4, _HY + 3e-4),
    ]
    cell = np.array([0, 1, 0], dtype=np.intp)
    xl, yb, xr, yt = (np.array(v) for v in zip(*boxes))
    got = gridagg._box_overlap_area(vx, vy, cell, xl, yb, xr, yt)
    want = [_shapely_area((six, ten)[c], b) for c, b in zip(cell, boxes)]
    assert got.tolist() == pytest.approx(want, rel=1e-9)
    assert got[2] == 0.0


def test_box_overlap_area_is_exact_on_a_sliver():
    # a hexagon corner that pokes 1e-9 deg into the box: the kernel works relative to the box corner, so it matches
    # the exact (rational) area to ~1e-12 relative; GEOS, rounding intersection points at |lon| ~ 93, does not
    from fractions import Fraction as Fr

    ring = _hexagon(_HX, _HY, _HR, phase=0.0)  # vertex 0 at (_HX + _HR, _HY)
    box = (_HX + _HR - 1e-9, _HY - 1e-4, _HX + 4e-4, _HY + 1e-4)
    # exact area of the triangle cut off by x >= xl from the two edges at vertex 0 (rational arithmetic)
    xl = Fr(box[0])
    (x0, y0), (x1, y1), (x5, y5) = (tuple(map(Fr, ring[i])) for i in (0, 1, 5))
    ya = y0 + (y1 - y0) * (xl - x0) / (x1 - x0)
    yc = y0 + (y5 - y0) * (xl - x0) / (x5 - x0)
    exact = float(abs(ya - yc) * (x0 - xl) / 2)
    assert _kernel_area(ring, box) == pytest.approx(exact, rel=1e-9)


# --- real distorted H3 rings: more than 6 vertices, not convex in (lon, lat) ----
# Cells that cross an icosahedron face edge. A box beyond a reflex part of such a ring has no separating edge, so the
# kernel's integral there is rounding noise (up to ~1e-15 of the box) rather than 0; the covering pass keeps a pair
# only when its weight is above ``gridagg._COVER_WEIGHT_FLOOR``. Res 3 shows it most (the strongest distortion);
# res 13 and 9 are the fine cells on one face edge (North Atlantic, 58.5 N 28.6 W).
_DISTORTED = {
    "r3_7v": "830136fffffffff",
    "r3_8v": "8301b0fffffffff",
    "r9_8v": "89066d34da3ffff",
    "r13_8v": "8d066d34d34da3f",
}


def _ring_of(cell):
    return [(lng, lat) for lat, lng in h3.cell_to_boundary(cell)]


@pytest.mark.parametrize("key", sorted(_DISTORTED))
def test_the_distorted_rings_are_real(key):
    import shapely

    ring = _ring_of(_DISTORTED[key])
    poly = shapely.Polygon(ring)
    assert len(ring) == int(key[-2]) and poly.is_valid
    assert poly.convex_hull.area > poly.area  # not convex


def _exact_overlap(ring, box):
    """Overlap area of ``ring`` and ``box`` in rational arithmetic (Sutherland-Hodgman, then the shoelace)."""
    from fractions import Fraction as Fr

    def clip(pts, inside, cut):
        out = []
        for i, cur in enumerate(pts):
            prev = pts[i - 1]
            if inside(cur):
                if not inside(prev):
                    out.append(cut(prev, cur))
                out.append(cur)
            elif inside(prev):
                out.append(cut(prev, cur))
        return out

    def at_x(c):
        return lambda p, q: (c, p[1] + (q[1] - p[1]) * (c - p[0]) / (q[0] - p[0]))

    def at_y(c):
        return lambda p, q: (p[0] + (q[0] - p[0]) * (c - p[1]) / (q[1] - p[1]), c)

    xl, yb, xr, yt = (Fr(v) for v in box)
    pts = [(Fr(x), Fr(y)) for x, y in ring]
    for inside, cut in (
        (lambda p: p[0] >= xl, at_x(xl)),
        (lambda p: p[0] <= xr, at_x(xr)),
        (lambda p: p[1] >= yb, at_y(yb)),
        (lambda p: p[1] <= yt, at_y(yt)),
    ):
        pts = clip(pts, inside, cut) if pts else pts
    if len(pts) < 3:
        return Fr(0)
    n = len(pts)
    twice = sum(
        pts[i][0] * pts[(i + 1) % n][1] - pts[(i + 1) % n][0] * pts[i][1]
        for i in range(n)
    )
    return abs(twice) / 2


def _box_grid(ring, n, margin):
    """An n x n grid of boxes over the ring's bbox, widened by ``margin`` of its extent on each side."""
    xs, ys = [p[0] for p in ring], [p[1] for p in ring]
    w, h = max(xs) - min(xs), max(ys) - min(ys)
    gx = np.linspace(min(xs) - margin * w, max(xs) + margin * w, n + 1)
    gy = np.linspace(min(ys) - margin * h, max(ys) + margin * h, n + 1)
    return np.array(
        [(gx[a], gy[b], gx[a + 1], gy[b + 1]) for a in range(n) for b in range(n)]
    )


@pytest.mark.parametrize("key", sorted(_DISTORTED))
def test_box_overlap_weights_on_a_distorted_ring_match_rational_arithmetic(key):
    ring = _ring_of(_DISTORTED[key])
    boxes = _box_grid(ring, 25, 0.1)
    vx, vy = np.array([[p[0] for p in ring]]), np.array([[p[1] for p in ring]])
    got = gridagg._box_overlap_area(vx, vy, np.zeros(len(boxes), np.intp), *boxes.T)
    pa = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    exact = np.array([float(_exact_overlap(ring, tuple(b))) for b in boxes])
    assert (exact > 0).sum() > 100 and (exact == 0).sum() > 100
    assert (np.abs(got - exact) / pa).max() <= 1e-14
    # the pairs the covering pass keeps are exactly those that overlap
    kept = got / pa > gridagg._COVER_WEIGHT_FLOOR
    assert (kept == (exact > 0)).all()


@pytest.mark.parametrize(
    "key,res,px_frac",
    [("r3_7v", 3, 0.048), ("r3_8v", 3, 0.048), ("r9_8v", 9, 1.5), ("r13_8v", 13, 1.5)],
)
@pytest.mark.parametrize("seed", [0, 1])
def test_h3_covering_around_a_distorted_cell_equals_the_reference(
    key, res, px_frac, seed
):
    # a 25 x 25 north-up grid centred on the distorted cell, pixel side = ``px_frac`` of the cell's lon / lat extent
    # (res 3: fine pixels over the whole ring, where the kernel leaves noise beyond its reflex part; res 9 and 13:
    # pixels larger than the cells, as the covering gate takes them). The same cells as rational arithmetic with the
    # same floor, and min / max (weight-agnostic: one kept noise pair would pull in a whole pixel's value) exactly.
    ring = _ring_of(_DISTORTED[key])
    xs, ys = [p[0] for p in ring], [p[1] for p in ring]
    px = ((max(xs) - min(xs)) * px_frac, (max(ys) - min(ys)) * px_frac)
    cx, cy = (max(xs) + min(xs)) / 2, (max(ys) + min(ys)) / 2
    n = 25
    gt = (cx - n / 2 * px[0], px[0], 0.0, cy + n / 2 * px[1], 0.0, -px[1])
    rng = np.random.default_rng(seed)
    band = rng.normal(size=(n, n))
    mask = rng.random((n, n)) < 0.6
    mask[n // 2 - 1 : n // 2 + 2, n // 2 - 1 : n // 2 + 2] = (
        True  # the cell's own pixels
    )
    new = gridagg._covering_band_h3_vectorised(band, mask, res, gridagg._PARTIAL, gt)
    old = _covering_band_h3_shapely_ref(
        band,
        mask,
        res,
        gridagg._PARTIAL,
        gt,
        frame="exact",
        floor=gridagg._COVER_WEIGHT_FLOOR,
    )
    assert _DISTORTED[key] in {h3.int_to_str(c) for c in _as_map(old)}
    _assert_states_match(new, old, _ABS_PIXEL_FRAME)
    new_map = _as_map(new)
    for cid, state in _as_map(old).items():
        assert new_map[cid][3:] == state[3:], cid  # min, max


def _distorted_cases():
    """(ring, box) of a res-3 distorted ring: a box over its reflex part, and one disjoint on an axis."""
    ring = _ring_of(_DISTORTED["r3_7v"])
    boxes = _box_grid(ring, 25, 0.1)
    exact = [_exact_overlap(ring, tuple(b)) for b in boxes]
    pa = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    # a partial overlap: the box whose overlap is nearest half of it
    over = tuple(
        boxes[int(np.argmin([abs(float(e) / a - 0.5) for e, a in zip(exact, pa)]))]
    )
    xs, ys = [p[0] for p in ring], [p[1] for p in ring]
    w = max(xs) - min(xs)
    west = (min(xs) - 0.5 * w, min(ys), min(xs) - 0.1 * w, max(ys))
    return ring, over, west


def test_box_overlap_area_matches_shapely_on_a_distorted_ring():
    ring, over, _ = _distorted_cases()
    want = _shapely_area(ring, over)
    assert want > 0
    assert _kernel_area(ring, over) == pytest.approx(want, rel=1e-9)
    assert _kernel_area(ring, over) == pytest.approx(
        float(_exact_overlap(ring, over)), rel=1e-12
    )


def test_box_overlap_area_is_exactly_zero_off_a_distorted_ring():
    ring, _, west = _distorted_cases()
    assert _shapely_area(ring, west) == 0.0
    assert _kernel_area(ring, west) == 0.0  # separated on an axis
