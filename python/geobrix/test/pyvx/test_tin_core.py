import numpy as np
import pytest

pytest.importorskip("scipy")
from databricks.labs.gbx.pyvx import _tin  # noqa: E402


def test_triangulate_square_gives_two_triangles():
    pts = np.array([[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [1.0, 1.0, 0.0], [0.0, 1.0, 0.0]])
    tris = _tin.triangulate(pts, breaklines=[], merge_tolerance=0.0, snap_tolerance=0.0)
    assert len(tris) == 2
    assert all(t.shape == (3, 3) for t in tris)


def test_merge_tolerance_dedups_near_coincident():
    pts = np.array(
        [[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0], [1e-9, 1e-9, 0]], dtype=float
    )
    tris = _tin.triangulate(
        pts, breaklines=[], merge_tolerance=1e-6, snap_tolerance=0.0
    )
    assert len(tris) == 2


def test_empty_or_too_few_points():
    assert _tin.triangulate(np.zeros((0, 3)), [], 0.0, 0.0) == []
    assert (
        _tin.triangulate(np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 0.0]]), [], 0.0, 0.0)
        == []
    )


def test_breakline_appears_as_triangle_edges():
    pts = np.array(
        [[0, 0, 0], [4, 0, 0], [4, 4, 0], [0, 4, 0], [1, 3, 0], [3, 1, 0]], dtype=float
    )
    bl = [np.array([[1.0, 3.0], [3.0, 1.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)
    edges = set()
    for t in tris:
        xy = [tuple(np.round(p[:2], 6)) for p in t]
        for a, b in [(0, 1), (1, 2), (2, 0)]:
            edges.add(frozenset([xy[a], xy[b]]))
    assert frozenset([(1.0, 3.0), (3.0, 1.0)]) in edges


def test_breakline_endpoint_not_a_mass_point_becomes_edge():
    # Breakline midpoints are NOT among the corner mass points, yet the
    # constraint segment must still appear as a triangle edge (heavy CDT adds
    # every breakline coord as a site).
    pts = np.array([[0, 0, 0], [4, 0, 0], [4, 4, 0], [0, 4, 0]], dtype=float)
    bl = [np.array([[0.0, 2.0], [4.0, 2.0]])]  # midpoints, not corners
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)
    edges = set()
    for t in tris:
        xy = [tuple(np.round(p[:2], 6)) for p in t]
        for a, b in [(0, 1), (1, 2), (2, 0)]:
            edges.add(frozenset([xy[a], xy[b]]))
    assert frozenset([(0.0, 2.0), (4.0, 2.0)]) in edges


def test_recovery_terminates_on_dense_constraints():
    rng = np.random.default_rng(0)
    pts = np.column_stack([rng.random(40), rng.random(40), np.zeros(40)])
    bl = [np.array([[0.05, 0.05], [0.95, 0.95]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)  # must not hang
    assert len(tris) > 0


def test_zsnap_sets_vertex_z_along_constraint():
    # A vertex lying ON the constraint line must have its Z overwritten with
    # the interpolated breakline Z (10.0), not the mass-point Z (0.0).
    pts = np.array([[0, 0, 0], [4, 0, 0], [4, 4, 0], [0, 4, 0]], dtype=float)
    bl = [np.array([[0.0, 2.0, 10.0], [4.0, 2.0, 10.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 1e-6)
    assert len(tris) > 0  # must not crash; recovery + snap run
    # The breakline endpoints (0,2) and (4,2) became vertices (C1); on the
    # constraint line their Z must be snapped to 10.0.
    snapped = []
    for t in tris:
        for v in t:
            if abs(v[1] - 2.0) < 1e-9 and (abs(v[0]) < 1e-9 or abs(v[0] - 4.0) < 1e-9):
                snapped.append(v[2])
    assert snapped, "expected vertices on the constraint line"
    assert all(abs(z - 10.0) < 1e-9 for z in snapped)


def test_interpolate_z_on_edge_at_utm_magnitude():
    # I2: barycentric tolerance must be scale-aware. At BNG/UTM magnitudes the
    # absolute 1e-12 tol is effectively zero (orient2d is an area ~coord^2), so a
    # cell center dead-on a triangle edge gets spuriously dropped. Must interpolate.
    bx, by = 530000.0, 180000.0
    pts = np.array(
        [
            [bx, by, 0.0],
            [bx + 100.0, by, 10.0],
            [bx + 100.0, by + 100.0, 20.0],
            [bx, by + 100.0, 10.0],
        ],
        dtype=float,
    )
    tris = _tin.triangulate(pts, [], 0.0, 0.0)
    # Point exactly on the diagonal edge shared by the two triangles.
    z = _tin.interpolate_z(tris, bx + 50.0, by + 50.0)
    assert z is not None
    assert abs(z - 10.0) < 1e-6


def test_grid_bbox_centers_column_major():
    cells = list(_tin.grid_bbox(0.0, 0.0, 2.0, 2.0, 2, 2))
    assert cells == [(0.5, 0.5), (0.5, 1.5), (1.5, 0.5), (1.5, 1.5)]


def test_grid_geom_negative_celly():
    cells = list(_tin.grid_geom(0.0, 10.0, 2, 2, 5.0, -5.0))
    assert cells == [(2.5, 7.5), (2.5, 2.5), (7.5, 7.5), (7.5, 2.5)]


def test_triangulate_skips_empty_breakline():
    # F1: an empty LineString -> np.asarray(coords) has shape (0,) with no second
    # axis; b.shape[1] raised IndexError. Empty breaklines must be skipped, and a
    # normal breakline in the same list still triangulates.
    pts = np.array([[0, 0, 0], [4, 0, 0], [4, 4, 0], [0, 4, 0]], dtype=float)
    bls = [
        np.empty((0,), dtype=float),  # empty breakline
        np.array([[0.0, 2.0], [4.0, 2.0]]),  # normal breakline
    ]
    tris = _tin.triangulate(pts, bls, 0.0, 0.0)
    assert len(tris) > 0


def test_triangulate_collinear_points_returns_empty():
    # F2: scipy Delaunay on collinear points raises QhullError ("Initial simplex
    # is flat"). Heavy returns gracefully (no triangles); light must return [].
    pts = np.array(
        [[0, 0, 0], [1, 1, 0], [2, 2, 0], [3, 3, 0]], dtype=float
    )  # all on y=x
    assert _tin.triangulate(pts, [], 0.0, 0.0) == []


def test_zsnap_snaps_just_beyond_endpoint():
    # F3: a vertex within tol of a breakline ENDPOINT but just beyond it (s slightly
    # > 1) must still be snapped to that endpoint's Z (heavy's circular buffer cap),
    # not left at the mass-point Z.
    tol = 0.1
    # Breakline along x, endpoint at (4,2,10). Vertex just past it at x=4.05 (s>1),
    # within tol of the endpoint.
    bl = [np.array([[0.0, 2.0, 5.0], [4.0, 2.0, 10.0]])]
    tri = np.array([[4.05, 2.0, 0.0], [5.0, 0.0, 0.0], [5.0, 4.0, 0.0]])
    out = _tin._zsnap([tri], bl, tol)
    # The first vertex (just beyond the endpoint) should snap to endpoint Z=10.0.
    assert abs(out[0][0, 2] - 10.0) < 1e-9


def test_interpolate_known_plane_and_outside_hull():
    pts = np.array([[0, 0, 0], [1, 0, 1], [1, 1, 2], [0, 1, 1]], dtype=float)
    tris = _tin.triangulate(pts, [], 0.0, 0.0)
    z = _tin.interpolate_z(tris, 0.5, 0.5)
    assert abs(z - 1.0) < 1e-9
    assert _tin.interpolate_z(tris, 5.0, 5.0) is None
