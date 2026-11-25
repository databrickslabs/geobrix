"""Raster-to-grid where longitudes wrap or the grid ends: lon/lat rasters across 180, global and 0..360 rasters, a
projected raster that reaches past its CRS's seam, quadbin's latitudes, BNG's edges, and cells a polyfill misses.

A lon/lat CRS has no seam: its longitude runs on by whole turns, so a pixel at 250E is the pixel at 110W, and a cell
across the antimeridian meets the pixels on both sides of it. The reference below draws each cell's lon/lat ring
(straight edges, as the library's cells are), each edge walked with 64 points, moved into the raster's CRS, made
continuous, closed over the pole for a cell that holds one, and repeated by whole turns; it shares each valid pixel
over the cells by exact overlap area with shapely.
"""

import math
from test.pyrx.test_rastertogrid_native_crs import (
    _SLIVER,
    _W_TOL,
    _native_reference,
    _open,
    _partial,
    _pixel_quads,
    _ramp,
    _tif,
)

import h3
import numpy as np
import pytest
import quadbin
import shapely
from rasterio.transform import from_origin
from shapely import STRtree, affinity
from shapely.geometry import Polygon

from databricks.labs.gbx.core.crs import get_transformer
from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx.core import gridagg
from databricks.labs.gbx.pyrx.core._nodata import band_valid_mask

_ED50_LL = "+proj=longlat +ellps=intl +towgs84=-87,-98,-121,0,0,0,0 +no_defs"
_SPHERE_LL = "+proj=longlat +R=6371229 +no_defs"
# a world equidistant cylindrical on a sphere: latitude is linear in y, so a pixel row is a degree
_SPHERE_EQC = "+proj=eqc +lat_ts=0 +lon_0=0 +R=6371007 +units=m +no_defs"
_EDGE_POINTS = 64


def _cell_rings(lon, lat, res):
    """``(cell id, closed lon/lat ring)`` of every h3 cell near the points ``lon``, ``lat`` (their cells and
    neighbours): each ring's longitudes made continuous, and a ring around a pole closed over it.
    """
    seen = {h3.latlng_to_cell(float(la), float(lo), res) for lo, la in zip(lon, lat)}
    for c in set().union(*(h3.grid_disk(c, 1) for c in seen)):
        ring = np.array([(lng, la) for la, lng in h3.cell_to_boundary(c)])
        yield h3.str_to_int(c), ring


def _lonlat_reference(b, crs, res):
    """``{cell id: (count, sum, partial pairs)}``: the valid pixels of a lon/lat raster shared over h3 cells by exact
    overlap area in the raster's CRS."""
    with _open(b) as ds:
        band = ds.read(1).astype("float64").ravel()
        mask = band_valid_mask(ds, 1).ravel()
        quads, _, _ = _pixel_quads(ds)
        t = ds.transform
        x0 = min(t.c, t.c + ds.width * t.a)
        x1 = max(t.c, t.c + ds.width * t.a)
        step = min(
            abs(t.a), math.degrees(h3.average_hexagon_edge_length(res, "m") / 6.4e6) / 3
        )
        cc, rr = np.meshgrid(
            np.arange(-1.0, ds.width + 1.0 + 1e-9, step / abs(t.a)),
            np.arange(-1.0, ds.height + 1.0 + 1e-9, step / abs(t.e)),
        )
        px, py = t * (cc.ravel(), rr.ravel())
    py = np.clip(py, -90.0, 90.0)
    move = get_transformer(crs, 4326)
    glon, glat = move.transform(px, py)
    quads, vals = quads[mask], band[mask]
    px_area = shapely.area(quads[0])
    tree = STRtree(quads)
    ref = {}
    for cid, ring in _cell_rings(glon, glat, res):
        nxt = np.roll(ring, -1, axis=0)
        nxt[:, 0] = ring[:, 0] + (nxt[:, 0] - ring[:, 0] + 180.0) % 360.0 - 180.0
        f = np.arange(_EDGE_POINTS)[None, :, None] / _EDGE_POINTS
        pts = (ring[:, None, :] + f * (nxt - ring)[:, None, :]).reshape(-1, 2)
        x, y = move.transform(pts[:, 0], pts[:, 1], direction="INVERSE")
        x = np.asarray(x)
        x = x[0] + np.concatenate(
            [[0.0], np.cumsum((np.diff(x) + 180.0) % 360.0 - 180.0)]
        )
        y = np.asarray(y)
        close = x[-1] + (x[0] - x[-1] + 180.0) % 360.0 - 180.0
        if (
            abs(close - x[0]) > 180.0
        ):  # around a pole: on to the first vertex a turn on, along the pole, back
            pole = 90.0 if y.mean() > 0 else -90.0
            x = np.append(x, [close, close, x[0]])
            y = np.append(y, [y[0], pole, pole])
        poly = Polygon(zip(x, y))
        parts = [
            affinity.translate(poly, 360.0 * k)
            for k in range(
                math.floor((x0 - x.max()) / 360.0),
                math.ceil((x1 - x.min()) / 360.0) + 1,
            )
        ]
        cell = shapely.union_all(parts)
        idx = tree.query(cell, predicate="intersects")
        if idx.size == 0:
            continue
        w = shapely.area(shapely.intersection(quads[idx], cell)) / px_area
        keep = w > 0
        if keep.any():
            ref[cid] = (
                w[keep].sum(),
                (w[keep] * vals[idx][keep]).sum(),
                int((w[keep] < 1 - 1e-9).sum()),
            )
    return ref


def _assert_matches(lib, ref, label):
    real_ref = {c for c, r in ref.items() if r[0] > _SLIVER}
    assert real_ref <= set(
        lib
    ), f"{label}: cells missing: {sorted(real_ref - set(lib))[:5]}"
    extra = {c for c, r in lib.items() if r["count"] > _SLIVER} - set(ref)
    assert not extra, f"{label}: cells the reference lacks: {sorted(extra)[:5]}"
    for cid in real_ref:
        n, s, partial = ref[cid]
        # the library walks an edge to 1e-4 of the shorter of the edge and the pixel side; a pixel larger than the
        # cell meets up to six of its edges, each off by up to about 2/3 of that (twice the native file's _W_TOL)
        tol = 2.0 * _W_TOL * max(partial, 1)
        assert lib[cid]["count"] == pytest.approx(n, abs=tol), (label, cid)
        assert lib[cid]["sum"] == pytest.approx(s, abs=17.0 * tol), (label, cid)


def _mass(b):
    with _open(b) as ds:
        data = ds.read(1).astype("float64")
        m = band_valid_mask(ds, 1)
    return float(m.sum()), float(data[m].sum())


# --- lon/lat rasters across the antimeridian, any datum ------------------------------------------------------------

_LL_CRSS = ["EPSG:4326", "EPSG:4258", "EPSG:4269", "EPSG:4230", _SPHERE_LL, _ED50_LL]
_LL_RASTERS = {
    # (west, north, shape, pixel)
    "175..180": (175.0, 60.0, (10, 10), 0.5),
    "179..181": (179.0, 60.0, (10, 20), 0.1),
    "170..190": (170.0, 60.0, (10, 20), 1.0),
}


@pytest.mark.parametrize("res", [1, 2, 4])
@pytest.mark.parametrize("extent", list(_LL_RASTERS))
@pytest.mark.parametrize("crs", _LL_CRSS)
def test_a_lonlat_raster_across_180_meets_the_cells_on_both_sides(crs, extent, res):
    # its longitudes run past 180 (or a cell along its east edge crosses 180): before, such rasters lost the pixels
    # east of 180, overcounted, or raised a GEOS TopologyException
    west, north, shape, px = _LL_RASTERS[extent]
    b = _tif(_ramp(*shape), crs, from_origin(west, north, px, px))
    lib = _partial(b, "h3", res)
    n, s = _mass(b)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(n, rel=1e-6)
    assert sum(c["sum"] for c in lib.values()) == pytest.approx(s, rel=1e-6)
    _assert_matches(lib, _lonlat_reference(b, crs, res), f"{crs} {extent} r{res}")


# --- global and 0..360 rasters ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "crs,west,step,res",
    [
        ("EPSG:4326", -180.0, 10.0, 0),
        ("EPSG:4326", -180.0, 10.0, 1),
        ("EPSG:4326", -180.0, 10.0, 2),
        ("EPSG:4326", -180.0, 5.0, 1),
        (
            "EPSG:4326",
            0.0,
            10.0,
            1,
        ),  # 0..360 in EPSG:4326 (as a +lon_wrap CRS is stored)
        (_SPHERE_LL, 0.0, 10.0, 1),  # GRIB-like 0..360 sphere
        (_SPHERE_LL, -180.0, 10.0, 2),
        (_ED50_LL, -180.0, 10.0, 0),
        (_ED50_LL, -180.0, 10.0, 1),
    ],
)
def test_a_global_lonlat_raster_shares_every_pixel_out(crs, west, step, res):
    # before: 0 cells at r2/r3, 19.38 of 648 pixels at r1, or a GEOS error (pole and antimeridian cells drawn the
    # wrong way round in the planar lon/lat pass)
    shape = (int(180 / step), int(360 / step))
    b = _tif(_ramp(*shape), crs, from_origin(west, 90.0, step, step))
    lib = _partial(b, "h3", res)
    n, s = _mass(b)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(n, rel=1e-6)
    assert sum(c["sum"] for c in lib.values()) == pytest.approx(s, rel=1e-6)
    _assert_matches(lib, _lonlat_reference(b, crs, res), f"{crs} {west} {step} r{res}")
    complete = _partial(b, "h3", res, coverage="complete")
    assert len(complete) == h3.get_num_cells(res)


@pytest.mark.parametrize("assignment", ["covering", "centroid"])
def test_a_0_360_raster_grids_like_the_same_raster_in_minus_180_180(assignment):
    # the pixels east of 180 are the pixels west of the antimeridian: the same cells and states (before: h3 gave no
    # cell at 250..260E, quadbin 80 of 100 pixels)
    for crs in ("EPSG:4326", _SPHERE_LL):
        east = _tif(_ramp(10, 10), crs, from_origin(250.0, 45.0, 1.0, 1.0))
        west = _tif(_ramp(10, 10), crs, from_origin(-110.0, 45.0, 1.0, 1.0))
        for grid, res in (("h3", 3), ("h3", 4), ("quadbin", 6)):
            a = _partial(east, grid, res, assignment=assignment)
            b = _partial(west, grid, res, assignment=assignment)
            assert set(a) == set(b), (crs, grid, res)
            for cid in b:
                assert a[cid]["count"] == pytest.approx(b[cid]["count"], abs=1e-9)
                assert a[cid]["sum"] == pytest.approx(b[cid]["sum"], abs=1e-8)


def test_a_bng_raster_in_0_360_longitudes_grids_like_one_in_minus_180_180():
    # a +lon_wrap=180 CRS is stored as EPSG:4326 with longitudes 359.5..360.5: before, BNG lost its pixels east of
    # 360 under covering (r3: 0 of 1600)
    data = _ramp(40, 40)
    wrapped = _tif(data, "EPSG:4326", from_origin(359.5, 52.0, 0.025, 0.025))
    plain = _tif(data, "EPSG:4326", from_origin(-0.5, 52.0, 0.025, 0.025))
    for res in (1, 2, 3):
        a, b = _partial(wrapped, "bng", res), _partial(plain, "bng", res)
        assert set(a) == set(b), res
        for cid in b:
            assert a[cid]["count"] == pytest.approx(b[cid]["count"], abs=1e-6)


# --- quadbin's latitudes -----------------------------------------------------------------------------------------------


def _quadbin_shares(lat_n, lat_s, z, ncols, x0=-180.0):
    """``{cell: count}`` of a uniform 1-degree lon/lat raster from ``lat_n`` to ``lat_s``: each cell's rows inside
    quadbin's latitudes (and the cell's box) times its columns."""
    top = quadbin.cell_to_bounding_box(quadbin.tile_to_cell((0, 0, z)))[3]
    out = {}
    for x in range(2**z):
        for y in range(2**z):
            cell = quadbin.tile_to_cell((x, y, z))
            w, s, e, n = quadbin.cell_to_bounding_box(cell)
            rows = max(0.0, min(n, lat_n, top) - max(s, lat_s, -top))
            cols = max(0.0, min(e, x0 + ncols) - max(w, x0))
            if rows * cols > 0:
                out[cell] = rows * cols
    return out


@pytest.mark.parametrize("z", [1, 2, 3])
@pytest.mark.parametrize("crs", ["EPSG:4326", _SPHERE_EQC])
def test_pixels_beyond_quadbins_latitudes_are_dropped_not_piled_on_edge_cells(crs, z):
    # quadbin stops at 85.05 degrees: a pixel beyond it is in no cell, as a pixel outside BNG; the part of a pixel
    # inside still counts. Before, the interior fast path clamped such pixels into the first and last rows of cells
    # (a world equidistant cylindrical raster at z2: 2108.9 in each edge cell for 1668.4 pixels)
    if crs == "EPSG:4326":
        t = from_origin(-180.0, 90.0, 1.0, 1.0)
    else:
        half = math.pi * 6371007.0
        t = from_origin(-half, half / 2, 2 * half / 360, 2 * half / 360)
    b = _tif(np.ones((180, 360)), crs, t)
    lib = _partial(b, "quadbin", z)
    want = _quadbin_shares(90.0, -90.0, z, 360)
    assert set(lib) == set(want)
    for cid, n in want.items():
        assert lib[cid]["count"] == pytest.approx(n, rel=1e-6), quadbin.cell_to_tile(
            cid
        )
    # centroid: a centre beyond 85.05 is in no cell (rows 85.5..89.5 north and south: 5 x 360 pixels each)
    cen = _partial(b, "quadbin", z, assignment="centroid")
    assert sum(c["count"] for c in cen.values()) == pytest.approx(
        180 * 360 - 2 * 5 * 360
    )


# --- a projected raster that reaches past its CRS's seam ----------------------------------------------------------------

_SEAM_3857 = 20037508.342789244


@pytest.mark.parametrize(
    "grid,res", [("h3", 1), ("h3", 3), ("quadbin", 2), ("quadbin", 4), ("quadbin", 6)]
)
def test_a_mercator_raster_across_its_seam_keeps_its_pixels_past_it(grid, res):
    # 20 x 20 pixels of 156 km, the east half written past x = pi R (EPSG:3857 runs on there, a turn later): before,
    # h3 lost that half (200 of 400) and quadbin kept only its interior pixels (380 / 371 / 305 of 400)
    px = 156543.0
    b = _tif(_ramp(20, 20), "EPSG:3857", from_origin(_SEAM_3857 - 10 * px, 4e6, px, px))
    lib = _partial(b, grid, res)
    n, s = _mass(b)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(n, rel=1e-6)
    assert sum(c["sum"] for c in lib.values()) == pytest.approx(s, rel=1e-6)
    if grid == "h3":

        def unwrap(x):
            return np.where(x < 0.0, x + 2.0 * _SEAM_3857, x)

        ref = _native_reference(b, "EPSG:3857", grid, res, unwrap)
        for cid, r in ref.items():
            if r[0] > _SLIVER:
                assert lib[cid]["count"] == pytest.approx(
                    r[0], abs=_W_TOL * max(r[6], 1)
                ), cid


def test_a_pseudo_cylindrical_world_raster_is_not_taken_as_running_on():
    # a sinusoidal world raster's corners are off the globe; they hold no cell, and the CRS has no period
    R = 6371007.181
    b = _tif(
        np.ones((64, 128)),
        "+proj=sinu +lon_0=0 +R=6371007.181 +units=m +no_defs",
        from_origin(
            -math.pi * R, math.pi * R / 2, 2 * math.pi * R / 128, math.pi * R / 64
        ),
    )
    with _open(b) as ds:
        assert gridagg._Frame(ds, ds.crs, "h3").period is None
    lib = _partial(b, "h3", 1)
    # the globe is 2 / pi of the raster's area
    assert sum(c["count"] for c in lib.values()) == pytest.approx(
        64 * 128 * 2 / math.pi, rel=2e-3
    )


# --- centroid with longitudes past 180 --------------------------------------------------------------------------------


def test_centroid_bins_a_centre_past_180_in_its_cell():
    b = _tif(_ramp(10, 20), "EPSG:4326", from_origin(179.0, 60.0, 0.1, 0.1))
    with _open(b) as ds:
        rr, cc = np.nonzero(band_valid_mask(ds, 1))
        lon, lat = ds.transform * (cc + 0.5, rr + 0.5)
    lon = np.where(lon > 180.0, lon - 360.0, lon)
    for grid, res in (("h3", 6), ("quadbin", 9)):
        lib = _partial(b, grid, res, assignment="centroid")
        if grid == "h3":
            want = {
                h3.str_to_int(h3.latlng_to_cell(la, lo, res))
                for lo, la in zip(lon, lat)
            }
        else:
            want = {int(c) for c in _quadbin.point_as_cell_vec(lon, lat, res)}
        assert set(lib) == want, grid
        assert sum(c["count"] for c in lib.values()) == pytest.approx(rr.size)


# --- the planar lon/lat pass: a pixel smaller than a cell ---------------------------------------------------------------


def test_a_pixel_inside_one_cell_keeps_its_weight():
    # a 30 m EPSG:4326 pixel inside a res-9 cell near Iowa: h3's overlap polyfill returned only a neighbour, so the
    # pixel was lost (covering gave []); the candidates now always hold the cells around the pixel
    w, s, e, n = (
        -93.52824545164846,
        41.96729256462714,
        -93.52787375537152,
        41.96756995970309,
    )
    b = _tif(np.ones((1, 1)), "EPSG:4326", from_origin(w, n, e - w, n - s))
    lib = _partial(b, "h3", 9)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(1.0, rel=1e-9)
    centre = h3.str_to_int(h3.latlng_to_cell(0.5 * (s + n), 0.5 * (w + e), 9))
    assert centre in lib


# --- BNG: the planar EPSG:27700 pass, and pixels west or south of the grid ---------------------------------------------


def _bng_lattice_reference(x0, y0, px, n, res):
    """``{cell: count}`` of an n x n EPSG:27700 raster of ones: each pixel box shared over the lattice squares by exact
    area, the squares outside the grid (a negative easting or northing, or an invalid id) dropped.
    """
    edge = float(_bng.get_edge_size(res))
    ref = {}
    for r in range(n):
        for c in range(n):
            w, e, top = x0 + c * px, x0 + (c + 1) * px, y0 - r * px
            s = top - px
            for i in range(int(math.floor(w / edge)), int(math.floor(e / edge)) + 1):
                for j in range(
                    int(math.floor(s / edge)), int(math.floor(top / edge)) + 1
                ):
                    ov = max(0.0, min(e, (i + 1) * edge) - max(w, i * edge))
                    ov *= max(0.0, min(top, (j + 1) * edge) - max(s, j * edge))
                    raw = _bng.point_to_cell_id((i + 0.5) * edge, (j + 0.5) * edge, res)
                    if ov > 0 and i >= 0 and j >= 0 and _bng.is_valid(raw):
                        cid = _bng.format(raw)
                        ref[cid] = ref.get(cid, 0.0) + ov / (px * px)
    return ref


@pytest.mark.parametrize(
    "res,px,n,x0,y0",
    [
        (
            1,
            353686.9260299463,
            3,
            398776.1695638571,
            414489.49226954597,
        ),  # the polyfill missed cells under a pixel
        (1, 282290.1958466493, 5, 438867.30906006484, 912875.7213418329),
        (1, 230122.16549624113, 4, 265662.9488341204, 811654.3745213713),
        (
            1,
            42031.14943719919,
            4,
            383059.8731455732,
            124469.49006340798,
        ),  # reaches south of northing 0
        (1, 77241.09168244165, 3, 524145.60413752997, 230926.18436787857),
        (3, 1500.0, 4, -2500.0, 60000.0),  # reaches west of easting 0
    ],
)
def test_bng_covering_of_a_27700_raster_matches_the_exact_lattice_shares(
    res, px, n, x0, y0
):
    # before: _bng.polyfill (a flood fill of a buffered box) missed some cells under a large pixel, and a pixel corner
    # up to 100 km west or south of the grid was encoded into the first column or row, so the interior fast path gave
    # a pixel across the grid's edge weight 1 (up to 2.8 pixels off in 1,500 random rasters)
    b = _tif(np.ones((n, n)), "EPSG:27700", from_origin(x0, y0, px, px))
    lib = _partial(b, "bng", res)
    ref = _bng_lattice_reference(x0, y0, px, n, res)
    assert set(lib) == set(ref)
    for cid, w in ref.items():
        assert lib[cid]["count"] == pytest.approx(w, abs=1e-9), cid


@pytest.mark.parametrize("crs", ["EPSG:27700", "EPSG:4326"])
def test_a_bng_pixel_centre_south_of_the_grid_is_in_no_cell(crs):
    # the encoder truncates a northing of -1.6 km into the first row: such a centre is outside the grid and drops,
    # in the planar EPSG:27700 pass and in the native frame alike
    if crs == "EPSG:27700":
        b = _tif(np.ones((8, 8)), crs, from_origin(400000.0, 6000.0, 1000.0, 1000.0))
    else:
        b = _tif(np.ones((8, 8)), crs, from_origin(-2.0, 49.98, 0.02, 0.02))
    with _open(b) as ds:
        rr, cc = np.nonzero(band_valid_mask(ds, 1))
        x, y = ds.transform * (cc + 0.5, rr + 0.5)
    e, nn = get_transformer(crs, 27700).transform(x, y)
    inside = (np.asarray(e) >= 0) & (np.asarray(nn) >= 0)
    assert 0 < inside.sum() < inside.size
    lib = _partial(b, "bng", 3, assignment="centroid")
    assert sum(c["count"] for c in lib.values()) == pytest.approx(float(inside.sum()))
    cover = _partial(b, "bng", 3)
    assert sum(c["count"] for c in cover.values()) < inside.size - 1.0


# --- rasters at the edge of their CRS's valid area ----------------------------------------------------------------------

_ORTHO = "+proj=ortho +lat_0=0 +lon_0=0 +R=6371000 +units=m +no_defs"


@pytest.mark.parametrize("res", [3, 5])
def test_a_raster_whose_padded_outline_leaves_the_crs_domain_stays_native(
    res, monkeypatch
):
    # an orthographic raster inside the visible disk, its corner at 0.99 R: the outline padded by a pixel leaves the
    # disk, which sent the raster to the warp; it is gridded on its own pixels, unpadded
    def no_warp(*a, **k):
        raise AssertionError("the warp ran")

    monkeypatch.setattr(gridagg, "_warp_to_4326_if_needed", no_warp)
    R = 6371000.0
    b = _tif(_ramp(20, 20), _ORTHO, from_origin(0.5 * R, 0.7 * R, 0.01 * R, 0.01 * R))
    lib = _partial(b, "h3", res, coverage="complete")
    n, s = _mass(b)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(n, rel=1e-6)
    ref = _native_reference(b, _ORTHO, "h3", res)
    for cid, r in ref.items():
        if r[0] > _SLIVER:
            assert lib[cid]["count"] == pytest.approx(
                r[0], abs=_W_TOL * max(r[6], 1)
            ), cid


def test_a_raster_with_no_pixel_on_the_globe_is_refused_with_a_fix():
    # a Robinson raster at 53N past the projection's edge there (x up to the edge at the equator): no pixel
    # transforms, and the warp it fell back to failed with GDAL's "Too many points (441 out of 441) failed"
    crs = "+proj=robin +lon_0=180 +datum=WGS84 +units=m +no_defs"
    b = _tif(np.ones((30, 30)), crs, from_origin(16.85e6, 5.64e6, 5000.0, 5000.0))
    for grid, res in (("bng", 1), ("h3", 3)):
        with pytest.raises(
            ValueError,
            match=r"no pixel of the raster lies inside its CRS's valid area.*Fix:",
        ):
            _partial(b, grid, res)


# --- a traced seam at the pole: coarse world rasters ----------------------------------------------------------------------

_ED50_MERC = (
    "+proj=merc +lon_0=0 +ellps=intl +towgs84=-87,-98,-121,0,0,0,0 +units=m +no_defs"
)


@pytest.mark.parametrize("n,res", [(8, 0), (8, 1), (8, 2), (32, 3)])
def test_a_coarse_world_mercator_on_another_datum_is_cut_not_refused(n, res):
    # an ED50 world Mercator of few pixels: its pole cells were closed at a cap taken from boxes padded by a pixel
    # (which reach the pole), where the seam cannot be traced, and a pole-cell part on one side of the seam was
    # traced as if across it; both refused the raster ("a cell crosses the seam ... cannot be cut")
    half = math.pi * 6378388.0
    b = _tif(
        np.ones((n, n)),
        _ED50_MERC,
        from_origin(-half, half, 2 * half / n, 2 * half / n),
    )
    lib = _partial(b, "h3", res)
    assert sum(c["count"] for c in lib.values()) == pytest.approx(n * n, rel=1e-5)
