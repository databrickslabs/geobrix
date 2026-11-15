import math

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
    # scipy Delaunay on collinear points raises QhullError ("Initial simplex is flat"), so the JTS port triangulates
    # them, as the JVM level does: every triangle touches JTS's frame, so there is none.
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


def test_a_multilinestring_break_line_gives_one_line_per_part():
    # as JTS's LinearComponentExtracter: each line of a multi-line string, then a polygon's shell and holes, in order
    coords, lines = _tin.sites(
        ["POINT Z (50 50 0)"],
        [
            "MULTILINESTRING Z ((0 0 1, 5 0 1), (0 5 2, 5 5 2))",
            "POLYGON Z ((10 10 3, 20 10 3, 20 20 3, 10 20 3, 10 10 3), (12 12 4, 12 14 4, 14 14 4, 12 12 4))",
        ],
    )
    assert [ln.tolist() for ln in lines] == [
        [[0.0, 0.0, 1.0], [5.0, 0.0, 1.0]],
        [[0.0, 5.0, 2.0], [5.0, 5.0, 2.0]],
        [
            [10.0, 10.0, 3.0],
            [20.0, 10.0, 3.0],
            [20.0, 20.0, 3.0],
            [10.0, 20.0, 3.0],
            [10.0, 10.0, 3.0],
        ],
        [[12.0, 12.0, 4.0], [12.0, 14.0, 4.0], [14.0, 14.0, 4.0], [12.0, 12.0, 4.0]],
    ]


def test_merge_tolerance_0_merges_only_the_same_x_and_y_at_any_magnitude():
    # a key scaled by a tiny grid once wrapped int64 above about 9.2e6, so distinct far points merged
    pts = np.array(
        [[2.0e7, 1.0e7, 1.0], [2.0e7 + 4.0, 1.0e7, 2.0], [2.0e7, 1.0e7, 3.0]]
    )
    assert _tin.prepare_sites(pts, [], 0.0, 0.0)[0].tolist() == pts[:2].tolist()
    assert _tin._jlong_floor(float("inf")) == 2**63 - 1
    assert _tin._jlong_floor(float("nan")) == 0


def test_the_merge_is_first_wins_and_the_line_vertex_wins():
    # (0.9, 0) is within 1 of the line's (0, 0): it merges into it, the line's Z kept. (3, 0) and (3.6, 0) are 0.6
    # apart, so the second joins the first; a grid of 1 m cells would split them (3 and 4 after rounding).
    bl = [np.array([[0.0, 0.0, 5.0], [0.0, 10.0, 5.0]])]
    pts = _sites([(0.9, 0.0), (3.0, 0.0), (3.6, 0.0), (8.0, 8.0)], z=1.0)
    kept, lines = _tin.prepare_sites(pts, bl, 1.0, 0.0)
    assert kept.tolist() == [[3.0, 0.0, 1.0], [8.0, 8.0, 1.0]]
    assert lines[0].tolist() == bl[0].tolist()


def test_a_line_vertex_merges_into_the_nearest_kept_vertex():
    # the second line's first vertex is 0.5 from the first line's end: it takes that coordinate, so the lines meet; a
    # line merged into one coordinate is a mass point
    bl = [
        np.array([[0.0, 0.0, 1.0], [5.0, 0.0, 1.0]]),
        np.array([[5.3, 0.4, 2.0], [9.0, 4.0, 2.0]]),
        np.array([[20.0, 20.0, 3.0], [20.2, 20.0, 3.0]]),
    ]
    kept, lines = _tin.prepare_sites(np.zeros((0, 3)), bl, 0.5, 0.0)
    assert [ln.tolist() for ln in lines] == [
        [[0.0, 0.0, 1.0], [5.0, 0.0, 1.0]],
        [[5.0, 0.0, 1.0], [9.0, 4.0, 2.0]],
    ]
    assert kept.tolist() == [[20.0, 20.0, 3.0]]


def test_snapping_on_changes_no_merge_where_nothing_snaps():
    # the two points 0.8 apart merge, the first kept, whether or not snap_tolerance is above 0
    pts = _sites([(0.4, 0.0), (-0.4, 0.0), (5.0, 5.0), (-5.0, 5.0), (0.0, -5.0)])
    pts[0, 2], pts[1, 2] = 1.0, 2.0
    bl = [np.array([[100.0, 0.0, 0.0], [100.0, 10.0, 0.0]])]
    for mode in ("constrained", "conforming"):
        got = [
            {
                tuple(v)
                for t in _tin.triangulate(pts, bl, 1.0, snap, mode)
                for v in t.tolist()
            }
            for snap in (0.0, 1e-9)
        ]
        assert got[0] == got[1], mode
        assert (0.4, 0.0, 1.0) in got[0] and not any(v[0] == -0.4 for v in got[0]), mode


def _edges(tris):
    return {
        frozenset(((t[a][0], t[a][1]), (t[b][0], t[b][1])))
        for t in tris
        for a, b in ((0, 1), (1, 2), (2, 0))
    }


# The snapped point lands at (0.45, 0.45), 1.27 from the line's first vertex, so it is a vertex of the line. A second,
# grid-cell merge once dropped it (both points round to the cell (0, 0)) and so left both of its segments unenforced.
DIAG_LINE = [np.array([[-0.45, -0.45, 0.0], [10.0, 10.0, 10.0]])]
DIAG_PTS = _sites([(0.46, 0.44), (10.0, -5.0), (-5.0, 10.0), (12.0, 0.0), (0.0, 12.0)])


def test_constrained_keeps_a_snapped_vertex_and_enforces_both_segments():
    _, lines = _tin.prepare_sites(DIAG_PTS, DIAG_LINE, 1.0, 0.05)
    assert len(lines[0]) == 3
    edges = _edges(_tin.triangulate(DIAG_PTS, DIAG_LINE, 1.0, 0.05))
    cs = [(r[0], r[1]) for r in lines[0].tolist()]
    for a, b in zip(cs, cs[1:]):
        assert frozenset((a, b)) in edges, (a, b)


def test_constrained_crossing_break_lines_raise_with_a_fix():
    pts = _sites([(0, -5), (10, -5), (10, 5), (0, 5), (5, 3), (5, -3)])
    bl = [
        np.array([[1.0, -1.0, 0.0], [9.0, 1.0, 0.0]]),
        np.array([[1.0, 1.0, 0.0], [9.0, -1.0, 0.0]]),
    ]
    with pytest.raises(_tin.ConstrainedError) as e:
        _tin.triangulate(pts, bl, 0.0, 0.0)
    assert str(e.value) == _tin.CONSTRAINED_CROSSING
    assert "Fix: split the break lines where they cross" in str(e.value)


def test_constrained_a_point_on_a_break_line_splits_it():
    grid = _sites([(x, y) for x in (0.0, 5.0, 10.0) for y in (0.0, 5.0, 10.0)])
    bl = [np.array([[0.0, 5.0, 1.0], [10.0, 5.0, 1.0]])]
    tris = _tin.triangulate(grid, bl, 0.0, 0.0)
    edges = _edges(tris)
    assert frozenset(((0.0, 5.0), (5.0, 5.0))) in edges
    assert frozenset(((5.0, 5.0), (10.0, 5.0))) in edges
    assert sum(abs(_tin._orient2d(*t)) / 2 for t in tris) == pytest.approx(100.0)


def test_constrained_makes_every_segment_of_long_lines_an_edge():
    rng = np.random.default_rng(7)
    pts = np.column_stack([rng.random(400) * 100, rng.random(400) * 100, np.zeros(400)])
    bl = [
        np.array(
            [[i * 20.0, 15.0 + 33.0 * k + rng.random() * 5, 1.0] for i in range(6)]
        )
        for k in range(3)
    ]
    edges = _edges(_tin.triangulate(pts, bl, 0.0, 0.0))
    for ln in bl:
        cs = [(r[0], r[1]) for r in ln.tolist()]
        assert all(frozenset(s) in edges for s in zip(cs, cs[1:]))


def test_point_to_segment_and_project_on_a_zero_length_segment():
    assert _tin._point_segment_distance(3.0, 4.0, 0.0, 0.0, 0.0, 0.0) == 5.0
    a = (1.0, 1.0, 7.0)
    assert _tin._project((4.0, 5.0, 0.0), a, (1.0, 1.0, 9.0)) == (0.0, a)


def test_snap_inserts_on_a_later_segment_of_a_polyline():
    bl = [np.array([[0.0, 0.0, 0.0], [10.0, 0.0, 10.0], [10.0, 10.0, 20.0]])]
    kept, lines = _tin.prepare_sites(
        _sites([(10.02, 4.0), (20.0, 20.0)]), bl, 0.0, 0.05
    )
    assert kept[:, :2].tolist() == [[20.0, 20.0]]
    assert lines[0].tolist() == [
        [0.0, 0.0, 0.0],
        [10.0, 0.0, 10.0],
        [10.0, 4.0, 14.0],
        [10.0, 10.0, 20.0],
    ]


def test_the_merge_joins_the_nearest_kept_point_and_the_lesser_on_a_tie():
    # as JTS's KdTree: (1.2, 0) joins (2, 0), the nearer; (1, 0) is 1 from both and joins (0, 0), the lesser X
    bl = [
        np.array([[0.0, 0.0, 1.0], [0.0, 10.0, 1.0]]),
        np.array([[2.0, 0.0, 2.0], [2.0, 10.0, 2.0]]),
    ]
    near = [np.array([[1.2, 0.0, 9.0], [1.2, 5.0, 9.0]])]
    _, lines = _tin.prepare_sites(np.zeros((0, 3)), bl + near, 1.5, 0.0)
    assert lines[2][0].tolist() == [2.0, 0.0, 2.0]
    tie = [np.array([[1.0, 0.0, 9.0], [1.0, 5.0, 9.0]])]
    _, lines = _tin.prepare_sites(np.zeros((0, 3)), bl + tie, 1.5, 0.0)
    assert lines[2][0].tolist() == [0.0, 0.0, 1.0]


def test_a_mass_point_near_a_snapped_vertex_merges_into_it():
    # merge_tolerance 1 is above snap_tolerance 0.1: (5, 0.05) snaps to (5, 0); (5.5, 0.3) is out of snap reach but
    # within 1 of the new vertex, so it merges into it, the line's coordinate winning
    bl = [np.array([[0.0, 0.0, 0.0], [10.0, 0.0, 10.0]])]
    pts = _sites([(5.0, 0.05), (5.5, 0.3), (5.0, 8.0)])
    kept, lines = _tin.prepare_sites(pts, bl, 1.0, 0.1)
    assert kept[:, :2].tolist() == [[5.0, 8.0]]
    assert lines[0][:, :2].tolist() == [[0.0, 0.0], [5.0, 0.0], [10.0, 0.0]]


def test_constrained_keeps_close_points_far_from_0(monkeypatch):
    # points under 1 cm apart at BNG magnitudes: Qhull on the raw coordinates leaves one out of every triangle; from
    # the corner it keeps them all, and the certificate (on the raw coordinates) takes them (the port is ruled out)
    from databricks.labs.gbx.pyvx import _conforming

    monkeypatch.setattr(_conforming, "_rust", lambda: None)
    monkeypatch.setattr(
        _tin, "_jts_delaunay", lambda *a: pytest.fail("Qhull left out a site")
    )
    rng = np.random.default_rng(0)
    base = np.column_stack(
        [530000 + rng.random(400) * 100, 180000 + rng.random(400) * 100]
    )
    pts = np.vstack([base, base[:40] + rng.random((40, 2)) * 0.01])
    pts = np.column_stack([pts, np.zeros(len(pts))])
    bl = [np.array([[530001.0, 180050.0, 0.0], [530099.0, 180050.0, 0.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)
    vs = {(v[0], v[1]) for t in tris for v in t.tolist()}
    assert {(x, y) for x, y, _ in pts.tolist()} <= vs


def _port_calls(monkeypatch):
    from databricks.labs.gbx.pyvx import _conforming

    monkeypatch.setattr(
        _conforming, "_rust", lambda: None
    )  # the scipy hybrid: no Rust build
    calls = []
    real = _tin._jts_delaunay

    def spy(xy, env):
        calls.append(len(xy))
        return real(xy, env)

    monkeypatch.setattr(_tin, "_jts_delaunay", spy)
    return calls


def test_the_jts_port_stands_in_where_qhull_leaves_out_a_site(monkeypatch):
    # forced: Qhull leaves a site out, so the certificate refuses and the JTS port gives every site and a valid
    # constrained triangulation
    import scipy.spatial

    real = scipy.spatial.Delaunay

    class LeftOut:
        def __init__(self, xy):
            d = real(xy)
            self.simplices, self.neighbors, self.coplanar = (
                d.simplices,
                d.neighbors,
                [0],
            )

    monkeypatch.setattr(scipy.spatial, "Delaunay", LeftOut)
    calls = _port_calls(monkeypatch)
    rng = np.random.default_rng(3)
    pts = np.column_stack([rng.random((30, 2)) * 10, np.zeros(30)])
    bl = [np.array([[1.0, 1.0, 1.0], [9.0, 8.0, 1.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)
    assert calls == [32]
    assert frozenset(((1.0, 1.0), (9.0, 8.0))) in _edges(tris)
    vs = {(v[0], v[1]) for t in tris for v in t.tolist()}
    assert {(x, y) for x, y, _ in pts.tolist()} <= vs


def test_constrained_takes_scipys_triangles_in_general_position_and_the_port_on_ties(
    monkeypatch,
):
    # random sites: Qhull's triangles are certified the unique Delaunay triangulation (no port); a grid is
    # co-circular, so the port makes JTS's choice among the ties, and the two paths agree wherever both apply
    calls = _port_calls(monkeypatch)
    rng = np.random.default_rng(1)
    general = np.column_stack([rng.random((200, 2)) * 100, np.zeros(200)])
    hybrid = _tin.triangulate(general, [], 0.0, 0.0)
    assert calls == []
    xy = general[:, :2]
    env = (*map(float, xy.min(axis=0)), *map(float, xy.max(axis=0)))
    port = {frozenset(t) for t in _tin._jts_delaunay(xy, env).tolist()}
    calls.clear()
    assert {frozenset(map(tuple, t[:, :2].tolist())) for t in hybrid} == {
        frozenset(map(tuple, xy[list(t)].tolist())) for t in port
    }
    grid = _sites([(x, y) for x in range(6) for y in range(6)])
    assert len(_tin.triangulate(grid, [], 0.0, 0.0)) == 50
    assert calls == [36]


def test_a_thin_hull_triangle_is_certified_and_is_the_ports():
    # (0, 0), (5, 0.001), (10, 0) is a hull triangle whose circle (radius 12500) holds a vertex of JTS's frame; JTS's
    # forceConvex rules still give the Delaunay triangulation of the sites, so the certificate takes scipy's triangles
    from databricks.labs.gbx.pyvx import _conforming

    pts = _sites([(0, 0), (10, 0), (5, 5), (2, 3), (7, 2.5), (5, 0.001)])
    xy = pts[:, :2]
    s = _conforming.unique_delaunay(xy, origin=xy.min(axis=0))
    assert s is not None
    env = (0.0, 0.0, 10.0, 5.0)
    assert {frozenset(t) for t in s.tolist()} == {
        frozenset(t) for t in _tin._jts_delaunay(xy, env).tolist()
    }


def test_a_locate_failure_raises_the_constrained_error():
    # 12 sites within a few units of one line: JTS's DelaunayTriangulationBuilder fails to locate a point, and the JVM
    # level raises the same text (InterpolateElevationTest)
    from databricks.labs.gbx.bench.oracle_tables import _tin_rows
    from databricks.labs.gbx.pyvx import _conforming

    pts = np.array(_tin_rows.NEAR_LINE)
    with pytest.raises(_tin.ConstrainedError) as e:
        _tin.triangulate(pts, [], 0.0, 0.0)
    assert str(e.value) == _tin.CONSTRAINED_LOCATE_FAILED
    # the port's conforming error does not leak out of the constrained mode
    assert not isinstance(e.value, _conforming.ConformingError)


def test_interpolate_zs_is_interpolate_z_point_for_point():
    # the batch form takes the same triangle with the same floats: on grid nodes and edges (ties between triangles),
    # inside, outside the hull, on a sliver, and at BNG magnitude
    rng = np.random.default_rng(4)
    for off in (0.0, 530000.0):
        grid = _sites([(off + x, off + y) for x in range(8) for y in range(8)])
        grid[:, 2] = rng.random(len(grid)) * 10
        sliver = _sites(
            [(off, off - 1), (off + 7, off - 1), (off + 3.5, off - 1 + 1e-9)]
        )
        tris = _tin.triangulate(np.vstack([grid, sliver]), [], 0.0, 0.0)
        arr = np.asarray(tris)
        xs = np.concatenate(
            [off + np.arange(-1, 9, 0.5), off + rng.random(300) * 9 - 1]
        )
        ys = np.concatenate(
            [off + np.arange(-1, 9, 0.5)[::-1], off + rng.random(300) * 9 - 1]
        )
        got = _tin.interpolate_zs(arr, xs, ys)
        for x, y, g in zip(xs.tolist(), ys.tolist(), got.tolist()):
            want = _tin.interpolate_z(tris, x, y)
            assert (want is None and math.isnan(g)) or g == want, (x, y)


@pytest.mark.parametrize("chunk", [65536, 7])
def test_the_batched_wkbs_are_the_per_row_ones(monkeypatch, chunk):
    monkeypatch.setattr(_tin, "_CHUNK", chunk)  # 7: every batch boundary is crossed
    from shapely import set_srid, to_wkb
    from shapely.geometry import Point, Polygon

    pts = _sites([(x, y) for x in range(6) for y in range(6)])
    pts[:, 2] = pts[:, 0] * 2 + pts[:, 1]
    tris = _tin.triangulate(pts, [], 0.0, 0.0)
    got = list(_tin.triangle_wkbs(None, None, 0.0, 0.0, parsed=(pts, [])))
    assert got == [to_wkb(Polygon([(p[0], p[1]) for p in t])) for t in tris]
    cells = list(_tin.grid_bbox(-1.0, -1.0, 6.0, 6.0, 9, 9))
    for flavor in ("extended", "iso"):
        got = list(
            _tin.elevation_wkbs(
                None, None, 0.0, 0.0, iter(cells), 27700, flavor, (pts, [])
            )
        )
        want = []
        for x, y in cells:
            z = _tin.interpolate_z(tris, x, y)
            if z is not None:
                p = set_srid(Point(x, y, z), 27700)
                want.append(to_wkb(p, output_dimension=3, flavor=flavor))
        assert got == want and len(want) > 20


def test_the_recovery_maps_read_lazily_are_the_eager_ones():
    # every edge's triangles (lower index first) and every site's triangle set, in the same set order, as a map
    # built triangle by triangle; a deleted edge is gone and a set one is back
    rng = np.random.default_rng(2)
    xy = rng.random((80, 2)) * 10
    s = _tin._delaunay(xy)
    edges, verts = {}, {}
    for ti, t in enumerate(s.tolist()):
        for k in range(3):
            edges.setdefault(_tin._ek(t[k], t[(k + 1) % 3]), []).append(ti)
            verts.setdefault(t[k], set()).add(ti)
    lazy_e = _tin._EdgeTris(np.asarray(s, dtype=np.int64), len(xy))
    lazy_v = _tin._VertTris(np.asarray(s, dtype=np.int64))
    for e, ts in edges.items():
        assert e in lazy_e and lazy_e[e] == ts
    for v, ts in verts.items():
        assert list(lazy_v[v]) == list(ts)
    assert (0, 10**6) not in lazy_e and lazy_v.get(10**6, ()) == ()
    e = next(iter(edges))
    del lazy_e[e]
    assert e not in lazy_e
    lazy_e[e] = [7, 9]
    assert e in lazy_e and lazy_e[e] == [7, 9]


def test_canonical_rings_run_counter_clockwise_from_the_lowest_vertex():
    from databricks.labs.gbx.pyvx import _conforming

    cw = np.array([[[4.0, 4.0, 1.0], [4.0, 0.0, 2.0], [0.0, 0.0, 3.0]]])
    assert _conforming.canonical_rings(cw).tolist() == [
        [[0.0, 0.0, 3.0], [4.0, 0.0, 2.0], [4.0, 4.0, 1.0]]
    ]
    # the least X first, then the least Y among equal X; a ring the float sign cannot tell keeps its order
    tie = np.array([[[1.0, 5.0, 0.0], [1.0, 2.0, 0.0], [3.0, 3.0, 0.0]]])
    assert _conforming.canonical_rings(tie)[0, 0].tolist() == [1.0, 2.0, 0.0]
    flat = np.array([[[0.2, 0.2, 0], [0.1, 0.1, 0], [0.30000000000000004] * 2 + [0]]])
    assert _conforming.canonical_rings(flat)[0, :, :2].tolist() == [
        [0.1, 0.1],
        [0.30000000000000004, 0.30000000000000004],
        [0.2, 0.2],
    ]
    assert _conforming.canonical_rings(np.zeros((0, 3, 3))).shape == (0, 3, 3)


@pytest.mark.parametrize("mode", ["constrained", "conforming"])
def test_every_path_writes_the_same_triangle_bytes(monkeypatch, mode):
    # scipy's triangles, the Python port's and the Rust kernel's: one ring per triangle, so the same WKB bytes
    from databricks.labs.gbx import _rs
    from databricks.labs.gbx.pyvx import _conforming

    rng = np.random.default_rng(8)
    grid = _sites([(x, y) for x in range(10) for y in range(10)], 0.0)
    rand = np.column_stack([rng.random((150, 2)) * 9, rng.random(150)])
    bl = [np.array([[0.5, 0.3, 1.0], [7.4, 3.2, 2.0], [4.4, 8.6, 1.0]])]
    for pts in (grid, rand):
        runs = {}
        for path in ("scipy", "port", "rust"):
            with monkeypatch.context() as m:
                if path == "rust":
                    if _rs.missing("tin_delaunay", "TinKdTree"):
                        continue
                else:
                    m.setattr(_conforming, "_rust", lambda: None)
                if path == "port":
                    m.setattr(_conforming, "unique_delaunay", lambda *a, **k: None)
                runs[path] = sorted(
                    _tin.triangle_wkbs(None, None, 0.0, 0.0, mode, parsed=(pts, bl))
                )
        first = next(iter(runs.values()))
        assert first and all(v == first for v in runs.values()), list(runs)
