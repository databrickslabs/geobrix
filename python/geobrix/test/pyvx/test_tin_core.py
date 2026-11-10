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


def test_snap_sets_vertex_z_along_constraint():
    # A point within snap_tolerance of a break line moves onto it, BEFORE triangulating, and takes the line's Z there
    # (10.0), not its own (0.0); the line keeps its own vertices and Z.
    pts = np.array(
        [[0, 0, 0], [4, 0, 0], [4, 4, 0], [0, 4, 0], [2, 2.01, 0]], dtype=float
    )
    bl = [np.array([[0.0, 2.0, 10.0], [4.0, 2.0, 10.0]])]
    for mode in ("constrained", "conforming"):
        tris = _tin.triangulate(pts, bl, 0.0, 0.05, mode)
        vs = {tuple(v) for t in tris for v in t.tolist()}
        assert (2.0, 2.0, 10.0) in vs, mode
        assert not any(v[1] == 2.01 for v in vs), mode
        assert all(z == 10.0 for x, y, z in vs if y == 2.0), mode


def test_interpolate_z_on_edge_at_utm_magnitude():
    # Barycentric tolerance must be scale-aware. At BNG/UTM magnitudes the
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
    # An empty LineString -> np.asarray(coords) has shape (0,) with no second
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
    # scipy Delaunay on collinear points raises QhullError ("Initial simplex
    # is flat"). Heavy returns gracefully (no triangles); light must return [].
    pts = np.array(
        [[0, 0, 0], [1, 1, 0], [2, 2, 0], [3, 3, 0]], dtype=float
    )  # all on y=x
    assert _tin.triangulate(pts, [], 0.0, 0.0) == []


def test_snap_just_beyond_an_endpoint_merges_into_it():
    # A point within reach of a break line END but past it (projection factor > 1) lands on the end and merges into
    # that vertex: it leaves the mass points and the line keeps its two vertices.
    bl = [np.array([[0.0, 2.0, 5.0], [4.0, 2.0, 10.0]])]
    pts = np.array([[4.05, 2.0, 0.0], [5.0, 0.0, 0.0], [5.0, 4.0, 0.0]])
    kept, lines = _tin.prepare_sites(pts, bl, 0.0, 0.1)
    assert kept.tolist() == [[5.0, 0.0, 0.0], [5.0, 4.0, 0.0]]
    assert lines[0].tolist() == bl[0].tolist()


def _sites(xy, z=0.0):
    return np.array([[x, y, z] for x, y in xy], dtype=float)


def test_snap_finds_horizontal_and_vertical_lines():
    # The lookup grows each segment's envelope by the tolerance: a flat envelope (a horizontal or vertical line) still
    # holds a point beside it. The post-triangulation snap this replaced looked a point up in its own envelope only.
    pts = _sites([(0, -5), (10, -5), (10, 5), (0, 5), (5, 0.01), (-3, 2)])
    bl = [
        np.array([[1.0, 0.0, 7.0], [9.0, 0.0, 7.0]]),
        np.array([[-2.99, -4.0, 9.0], [-2.99, 4.0, 9.0]]),
    ]
    kept, lines = _tin.prepare_sites(pts, bl, 0.0, 0.05)
    assert len(kept) == 4
    assert lines[0].tolist() == [[1.0, 0.0, 7.0], [5.0, 0.0, 7.0], [9.0, 0.0, 7.0]]
    assert lines[1].tolist() == [
        [-2.99, -4.0, 9.0],
        [-2.99, 2.0, 9.0],
        [-2.99, 4.0, 9.0],
    ]


def test_snap_nearest_line_wins_and_landings_merge():
    pts = _sites([(5, 0.03), (5, 1), (2, 0), (7, 0.02), (7, -0.02), (0.01, 0.01)])
    bl = [
        np.array([[0.0, 0.0, 1.0], [10.0, 0.0, 1.0]]),
        np.array([[0.0, 0.05, 2.0], [10.0, 0.05, 2.0]]),
        np.array([[0.0, 2.0, 3.0], [10.0, 2.0, 3.0]]),
    ]
    kept, lines = _tin.prepare_sites(pts, bl, 0.001, 0.05)
    # (5, 0.03) is nearer y = 0.05; (5, 1) is out of reach; (7, +-0.02) both land on (7, 0): the second merges
    assert kept.tolist() == [[5.0, 1.0, 0.0]]
    assert lines[0].tolist() == [
        [0.0, 0.0, 1.0],
        [0.01, 0.0, 1.0],
        [2.0, 0.0, 1.0],
        [7.0, 0.0, 1.0],
        [10.0, 0.0, 1.0],
    ]
    assert lines[1].tolist() == [[0.0, 0.05, 2.0], [5.0, 0.05, 2.0], [10.0, 0.05, 2.0]]
    assert len(lines[2]) == 2


def test_snap_exact_tie_goes_to_the_first_line():
    pts = _sites([(5, 0.025), (0, 9), (9, 9)])
    bl = [
        np.array([[0.0, 0.05, 2.0], [10.0, 0.05, 2.0]]),
        np.array([[0.0, 0.0, 1.0], [10.0, 0.0, 1.0]]),
    ]
    _, lines = _tin.prepare_sites(pts, bl, 0.0, 0.05)
    assert [len(b) for b in lines] == [3, 2]


def test_snap_within_merge_tolerance_of_a_vertex_merges():
    pts = _sites([(9.9995, 0.01)])
    bl = [np.array([[0.0, 0.0, 1.0], [10.0, 0.0, 1.0]])]
    kept, lines = _tin.prepare_sites(pts, bl, 0.001, 0.05)
    assert len(kept) == 0 and len(lines[0]) == 2
    assert len(_tin.prepare_sites(pts, bl, 0.0, 0.05)[1][0]) == 3


def test_snap_leaves_no_zero_area_triangle():
    # (5.007, 4.993) is 0.0099 from the diagonal break line. Moved onto the line AFTER triangulating (the old JVM
    # post-process), it left a zero-area triangle in both modes; snapped first, it is a break line vertex.
    pts = _sites([(0, 0), (10, 10), (5.007, 4.993), (4.3, 5.7)])
    bl = [np.array([[0.0, 0.0, 0.0], [10.0, 10.0, 20.0]])]
    for mode in ("constrained", "conforming"):
        tris = _tin.triangulate(pts, bl, 0.0, 0.05, mode)
        assert tris, mode
        for t in tris:
            assert abs(_tin._orient2d(t[0], t[1], t[2])) > 0.0, mode
        assert any(
            abs(v[0] - 5.0) < 1e-9
            and abs(v[1] - 5.0) < 1e-9
            and abs(v[2] - 10.0) < 1e-9
            for t in tris
            for v in t
        ), mode


def test_a_2d_break_line_has_z_0_in_both_modes():
    pts = _sites([(0, 0), (10, 0), (5, 1), (5, -1)], z=3.0)
    bl = [np.array([[0.0, 0.0], [10.0, 0.0]])]
    for mode in ("constrained", "conforming"):
        zs = [v[2] for t in _tin.triangulate(pts, bl, 0.0, 0.0, mode) for v in t]
        assert not np.isnan(zs).any() and min(zs) == 0.0, mode


def test_split_point_finder_is_checked_in_both_modes():
    pts = _sites([(0, 0), (1, 0), (0, 1)])
    for mode in ("constrained", "conforming"):
        assert _tin.triangulate(pts, [], 0.0, 0.0, mode, "midpoint")
        with pytest.raises(ValueError, match="sideways"):
            _tin.triangulate(pts, [], 0.0, 0.0, mode, "sideways")


def test_interpolate_known_plane_and_outside_hull():
    pts = np.array([[0, 0, 0], [1, 0, 1], [1, 1, 2], [0, 1, 1]], dtype=float)
    tris = _tin.triangulate(pts, [], 0.0, 0.0)
    z = _tin.interpolate_z(tris, 0.5, 0.5)
    assert abs(z - 1.0) < 1e-9
    assert _tin.interpolate_z(tris, 5.0, 5.0) is None
