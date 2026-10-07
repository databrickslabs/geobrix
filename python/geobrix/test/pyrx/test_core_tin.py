"""Pure-function tests for the pyrx TIN / IDW core (Spark-free)."""

import numpy as np
import shapely.wkb
from shapely.geometry import LineString, Point

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tin


# --- idw_grid ---------------------------------------------------------------
def test_idw_grid_output_is_float64_with_nodata():
    pts = np.array([[1.0, 1.0], [3.0, 3.0]])
    vals = np.array([10.0, 20.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 4.0, 4.0, 4, 4, 32633)
    with _serde.open_tile(out) as ds:
        assert ds.count == 1
        assert ds.dtypes[0] == "float64"
        assert ds.nodata == -9999.0
        assert ds.crs.to_epsg() == 32633
        assert (ds.width, ds.height) == (4, 4)


def test_idw_grid_interior_cell_matches_hand_computed_weighted_value():
    # Two points; a 1x1 grid over a 2x2 extent => single cell center at (1, 1).
    # p0 = (0,0) v=10 ; p1 = (2,2) v=30.
    # d0 = sqrt(2), d1 = sqrt(2). power=2 => w0 = w1 = 1/2.
    # weighted mean = (10*0.5 + 30*0.5) / (0.5 + 0.5) = 20.0
    pts = np.array([[0.0, 0.0], [2.0, 2.0]])
    vals = np.array([10.0, 30.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, power=2.0, max_pts=2)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    assert np.isclose(arr[0, 0], 20.0)


def test_idw_grid_asymmetric_weights_hand_computed():
    # Single cell center at (1,1). p0=(0,1) v=0 (d=1); p1=(3,1) v=100 (d=2).
    # power=2 => w0=1/1=1, w1=1/4=0.25.
    # value = (0*1 + 100*0.25) / (1 + 0.25) = 25 / 1.25 = 20.0
    pts = np.array([[0.0, 1.0], [3.0, 1.0]])
    vals = np.array([0.0, 100.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, power=2.0, max_pts=2)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    assert np.isclose(arr[0, 0], 20.0)


def test_idw_grid_coincident_point_returns_that_value():
    # Cell center sits exactly on a point => returns that point's value (d==0).
    # 1x1 grid over [0,2]x[0,2] => center (1,1). Put a point there.
    pts = np.array([[1.0, 1.0], [0.0, 0.0]])
    vals = np.array([42.0, 7.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, power=2.0, max_pts=2)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    assert np.isclose(arr[0, 0], 42.0)


def test_idw_grid_empty_points_all_nodata():
    out = tin.idw_grid(
        np.empty((0, 2)), np.empty((0,)), 0.0, 0.0, 4.0, 4.0, 4, 4, 32633
    )
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
        assert ds.nodata == -9999.0
        assert np.all(arr == -9999.0)


def test_idw_grid_defaults_power_2_max_12():
    # Smoke: default args (power=2.0, max_pts=12) run and produce finite interior.
    rng = np.random.default_rng(0)
    pts = rng.uniform(0, 10, size=(20, 2))
    vals = rng.uniform(0, 100, size=20)
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 10.0, 10.0, 8, 8, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # all interior cells of the convex hull get an IDW value; many are valid.
    assert np.any(arr != -9999.0)


def _idw_grid_kdtree_reference(
    points_xy,
    values,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    srid,
    power=2.0,
    max_pts=12,
):
    """Reference IDW grid via the original cKDTree-k=npts path (pre-optimization).

    A verbatim copy of the historical ``idw_grid`` interior so the identity test
    can assert the new all-points fast path is numerically identical to what the
    cKDTree path produced for ``k == npts``.
    """
    from scipy.spatial import cKDTree

    from databricks.labs.gbx.pyrx.core.tin import (
        _NODATA,
        _cell_center_coords,
        _write_float64_grid,
    )

    pts = np.asarray(points_xy, dtype="float64")
    vals = np.asarray(values, dtype="float64")
    width_px = int(width_px)
    height_px = int(height_px)
    power = float(power)
    k = min(int(max_pts), pts.shape[0])
    tree = cKDTree(pts)
    centers = _cell_center_coords(xmin, ymin, xmax, ymax, width_px, height_px)
    dist, idx = tree.query(centers, k=k)
    if k == 1:
        dist = dist[:, None]
        idx = idx[:, None]
    neigh_vals = vals[idx]
    out = np.full(centers.shape[0], _NODATA, dtype="float64")
    exact = dist[:, 0] == 0.0
    if np.any(exact):
        out[exact] = neigh_vals[exact, 0]
    interp_mask = ~exact
    if np.any(interp_mask):
        d = dist[interp_mask]
        nv = neigh_vals[interp_mask]
        w = 1.0 / np.power(d, power)
        wsum = w.sum(axis=1)
        vsum = (w * nv).sum(axis=1)
        good = np.isfinite(wsum) & (wsum > 0)
        res = np.full(d.shape[0], _NODATA, dtype="float64")
        res[good] = vsum[good] / wsum[good]
        out[interp_mask] = res
    out = out.reshape(height_px, width_px)
    return _write_float64_grid(
        out, xmin, ymin, xmax, ymax, width_px, height_px, srid, _NODATA
    )


def test_idw_grid_all_points_matches_kdtree_reference():
    # All-points mode (max_pts >= npts): the new cdist fast path must be
    # numerically identical to the original cKDTree-k=npts path.
    rng = np.random.default_rng(7)
    pts = rng.uniform(0, 20, size=(40, 2))
    vals = rng.uniform(-50, 150, size=40)
    args = (pts, vals, 0.0, 0.0, 20.0, 20.0, 32, 32, 32633)
    kw = dict(power=2.0, max_pts=10_000)  # >> npts => all-points mode

    new_bytes = tin.idw_grid(*args, **kw)
    ref_bytes = _idw_grid_kdtree_reference(*args, **kw)
    with _serde.open_tile(new_bytes) as ds_new, _serde.open_tile(ref_bytes) as ds_ref:
        a_new = ds_new.read(1)
        a_ref = ds_ref.read(1)
    assert a_new.shape == a_ref.shape
    assert np.allclose(a_new, a_ref, atol=1e-9, rtol=0.0)


def test_idw_grid_all_points_exact_hit_matches_reference():
    # A point exactly on a cell center: all-points fast path must return that
    # point's value, identical to the cKDTree reference.
    # 2x2 grid over [0,4]x[0,4] => centers at (1,1),(3,1),(1,3),(3,3).
    pts = np.array([[1.0, 1.0], [3.0, 3.0], [0.5, 2.0], [3.5, 0.5]])
    vals = np.array([42.0, 99.0, 7.0, 13.0])
    args = (pts, vals, 0.0, 0.0, 4.0, 4.0, 2, 2, 32633)
    kw = dict(power=2.0, max_pts=1000)

    new_bytes = tin.idw_grid(*args, **kw)
    ref_bytes = _idw_grid_kdtree_reference(*args, **kw)
    with _serde.open_tile(new_bytes) as ds_new, _serde.open_tile(ref_bytes) as ds_ref:
        a_new = ds_new.read(1)
        a_ref = ds_ref.read(1)
    # cell (top-left) center (1,3)?? row-major: row0=top. Just assert identity +
    # that the two exact-hit cells carry the coincident point's value.
    assert np.allclose(a_new, a_ref, atol=1e-9, rtol=0.0)
    assert 42.0 in a_new
    assert 99.0 in a_new


def test_idw_grid_nearest_k_path_unchanged():
    # Nearest-k mode (small max_pts < npts) still uses the cKDTree path and must
    # match the reference exactly (path is untouched by the optimization).
    rng = np.random.default_rng(3)
    pts = rng.uniform(0, 10, size=(50, 2))
    vals = rng.uniform(0, 100, size=50)
    args = (pts, vals, 0.0, 0.0, 10.0, 10.0, 16, 16, 32633)
    kw = dict(power=2.0, max_pts=12)  # < npts => nearest-k cKDTree path

    new_bytes = tin.idw_grid(*args, **kw)
    ref_bytes = _idw_grid_kdtree_reference(*args, **kw)
    with _serde.open_tile(new_bytes) as ds_new, _serde.open_tile(ref_bytes) as ds_ref:
        a_new = ds_new.read(1)
        a_ref = ds_ref.read(1)
    assert np.array_equal(a_new, a_ref)


# --- delaunay_dtm -----------------------------------------------------------
def _plane(x, y):
    # z = 2x + 3y + 1
    return 2.0 * x + 3.0 * y + 1.0


def test_delaunay_dtm_planar_surface_interpolates_exactly_within_hull():
    # Points on a plane z = 2x + 3y + 1; barycentric interpolation of a planar
    # field is exact, so interior cells should match the plane.
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    with _serde.open_tile(out) as ds:
        assert ds.dtypes[0] == "float64"
        assert ds.nodata == -9999.0
        arr = ds.read(1)
        transform = ds.transform
    # Check a handful of interior cells against the analytic plane.
    for row, col in [(3, 3), (5, 5), (7, 2), (2, 7)]:
        wx, wy = transform * (col + 0.5, row + 0.5)
        if arr[row, col] != -9999.0:
            assert np.isclose(arr[row, col], _plane(wx, wy), atol=1e-6)
    # at least some interior cells were interpolated
    assert np.any(arr != -9999.0)


def test_delaunay_dtm_outside_hull_is_nodata():
    # Points clustered in the lower-left; cells in the far upper-right corner of
    # a larger extent fall outside the convex hull -> NoData.
    corners = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    # extent extends to 10x10 but points only span [0,2] => upper-right is outside.
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # top-right cell (row 0, col 9) center ~ (9.5, 9.5) is far outside the hull.
    assert arr[0, 9] == -9999.0


def test_delaunay_dtm_custom_nodata():
    corners = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633, no_data=-1.0)
    with _serde.open_tile(out) as ds:
        assert ds.nodata == -1.0
        arr = ds.read(1)
    assert arr[0, 9] == -1.0


def test_delaunay_dtm_breaklines_accepted_not_enforced():
    # PARITY: breaklines are accepted but NOT enforced as constraint edges.
    # With a planar field, folding breakline vertices in must not change the
    # interpolated (planar) result vs. running without breaklines.
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    # Breakline along the diagonal with Z on the same plane.
    bl = LineString([(2.0, 2.0, _plane(2.0, 2.0)), (8.0, 8.0, _plane(8.0, 8.0))])
    bl_wkb = shapely.wkb.dumps(bl, output_dimension=3)
    out_with = tin.delaunay_dtm(xyz, [bl_wkb], 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    out_without = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    with _serde.open_tile(out_with) as ds_w, _serde.open_tile(out_without) as ds_n:
        aw = ds_w.read(1)
        an = ds_n.read(1)
    # Both are valid DTMs; on a planar field the interior values are identical.
    valid = (aw != -9999.0) & (an != -9999.0)
    assert np.any(valid)
    assert np.allclose(aw[valid], an[valid], atol=1e-6)


def test_delaunay_dtm_never_emits_nan_or_inf():
    # Invariant: the DTM must only ever contain finite values or no_data -- never
    # raw NaN/Inf. Ill-conditioned sliver triangles (near-collinear points over an
    # extreme aspect ratio) can hand scipy a non-finite barycentric transform;
    # before the finiteness guard those NaNs were written into in-hull cells and
    # crashed downstream GDAL warp/isoband. A degenerate interpolation must fall
    # back to no_data instead.
    pts = [
        (0.0, 0.0, 1.0),
        (1000.0, 1e-6, 2.0),
        (2000.0, 0.0, 3.0),
        (1500.0, 2e-6, 5.0),
        (1000.0, 400.0, 4.0),
    ]
    xyz = np.array(pts, dtype="float64")
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 2000.0, 400.0, 80, 16, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
        nodata = ds.nodata
    assert not np.isnan(arr).any(), "DTM must never contain raw NaN"
    assert not np.isinf(arr).any(), "DTM must never contain Inf"
    # Every non-nodata cell is a finite interpolated elevation.
    assert np.all(np.isfinite(arr[arr != nodata]))


# --- WKB decode helpers -----------------------------------------------------
def test_points_xy_from_wkb_roundtrip():
    wkbs = [shapely.wkb.dumps(Point(1.0, 2.0)), shapely.wkb.dumps(Point(3.0, 4.0))]
    xy = tin.points_xy_from_wkb(wkbs)
    assert xy.shape == (2, 2)
    assert np.allclose(xy, [[1.0, 2.0], [3.0, 4.0]])


def test_points_xyz_from_wkb_requires_z():
    wkbs = [shapely.wkb.dumps(Point(1.0, 2.0, 9.0), output_dimension=3)]
    xyz = tin.points_xyz_from_wkb(wkbs)
    assert xyz.shape == (1, 3)
    assert np.allclose(xyz[0], [1.0, 2.0, 9.0])


def test_points_xyz_from_wkb_rejects_2d():
    import pytest

    wkbs = [shapely.wkb.dumps(Point(1.0, 2.0))]
    with pytest.raises(ValueError, match="Z coordinate"):
        tin.points_xyz_from_wkb(wkbs)
