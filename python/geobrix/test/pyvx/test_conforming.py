"""Unit tests of the conforming mode's port of JTS 1.20 (``_conforming``, ``_quadedge``). Parity with the JVM itself is
the oracle table's (``VECTOR_GENERATOR``) and ``test_parity_tin.py``'s."""

import numpy as np
import pytest

pytest.importorskip("scipy")
from databricks.labs.gbx.pyvx import _conforming, _tin  # noqa: E402


def _sites(xy, z=0.0):
    return np.array([[x, y, z] for x, y in xy], dtype=float)


def test_kdtree_snaps_within_tolerance_and_keeps_exact_repeats_out():
    t = _conforming.KdTree(0.5)
    assert t.insert((0.0, 0.0, 0.0))
    assert not t.insert((0.3, 0.3, 0.0))  # 0.42 away: snaps onto the node
    assert t.insert((1.0, 0.0, 0.0))
    exact = _conforming.KdTree(0.0)
    assert exact.insert((0.0, 0.0, 0.0)) and not exact.insert((0.0, 0.0, 9.0))
    assert exact.insert((1e-300, 0.0, 0.0))


def test_kdtree_query_is_in_order():
    t = _conforming.KdTree(0.0)
    for c in [(4.0, 1.0), (6.0, 1.0), (0.0, 0.0), (10.0, 0.0)]:
        t.insert((*c, 0.0))
    # root (4, 1); (6, 1) and (10, 0) right of it, (10, 0) left of (6, 1) on its Y level; (0, 0) left of the root
    got = [(n[0], n[1]) for n in t.query(-1, -5, 11, 5)]
    assert got == [(0.0, 0.0), (4.0, 1.0), (10.0, 0.0), (6.0, 1.0)]


def test_an_exact_distance_tie_picks_the_first_point_the_kdtree_finds():
    # (4, 1) and (6, 1) are both sqrt(2) from the midpoint of the break line: JTS takes the first it finds, (4, 1),
    # so the first split point is its projection (4, 0); (6, 0) comes on the next pass.
    pts = _sites([(4, 1), (6, 1)])
    bl = [np.array([[0.0, 0.0, 0.0], [10.0, 0.0, 0.0]])]
    verts, _ = _conforming.conforming_vertices(pts, bl, 0.0, "NONENCROACHING")
    steiner = [v[:2] for v in verts[4:]]
    assert steiner == [(4.0, 0.0), (6.0, 0.0)]


def test_split_points_take_the_z_of_their_segment():
    pts = _sites([(0, 0), (10, 0), (10, 10), (0, 10), (5, 5)])
    pts[:, 2] = 2 * pts[:, 0] + pts[:, 1] + 1
    bl = [np.array([[0.0, 1.0, 2.0], [10.0, 9.5, 30.5]])]  # on the plane z = 2x + y + 1
    inputs = {(x, y) for x, y, _ in pts.tolist()} | {(0.0, 1.0), (10.0, 9.5)}
    for finder, want in (
        # MIDPOINT halves the segment towards the centre point until no half's circle holds it
        (
            "MIDPOINT",
            {
                (5.0, 5.25),
                (2.5, 3.125),
                (3.75, 4.1875),
                (4.375, 4.71875),
                (4.6875, 4.984375),
                (4.84375, 5.1171875),
            },
        ),
        ("NONENCROACHING", {(4.8766328011611035, 5.145137880986938)}),
    ):
        verts, _ = _conforming.conforming_vertices(pts, bl, 0.0, finder)
        steiner = [v for v in verts if (v[0], v[1]) not in inputs]
        assert {v[:2] for v in steiner} == want, finder
        for x, y, z in steiner:
            assert z == pytest.approx(2 * x + y + 1, abs=1e-12)


def test_crossing_break_lines_raise_after_99_passes():
    # two break lines crossing at a shallow angle away from any vertex: the split points chase the crossing
    pts = _sites([(0, -5), (10, -5), (10, 5), (0, 5)])
    bl = [
        np.array([[1.0, -0.5, 0.0], [9.0, 0.5, 0.0]]),
        np.array([[2.0, 0.5, 0.0], [8.63, -0.5, 0.0]]),
    ]
    with pytest.raises(
        _conforming.ConformingError, match="split crossing break lines where they cross"
    ):
        _tin.triangulate(pts, bl, 0.0, 0.0, "conforming")


def _key(tris):
    return {frozenset(map(tuple, t[:, :2].tolist())) for t in tris}


def test_hybrid_takes_scipy_when_unique_and_the_port_on_ties():
    rng = np.random.default_rng(3)
    general = np.column_stack([rng.random((60, 2)) * 100, np.zeros(60)])
    grid = _sites(
        [(x, y) for x in range(5) for y in range(5)]
    )  # co-circular: many Delaunay triangulations
    for pts, fallback in ((general, False), (grid, True)):
        stats = {}
        hybrid = _conforming.triangulate(pts, [], 0.0, stats=stats)
        assert stats["fallback"] is fallback
        port = _conforming.triangulate(pts, [], 0.0, delaunay="jts")
        assert _key(hybrid) == _key(port)


def test_certificate_refuses_cocircular_points():
    square = np.array([[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]])
    assert _conforming.unique_delaunay(square) is None
    assert (
        _conforming.unique_delaunay(
            np.array([[0.0, 0.0], [1.0, 0.0], [0.4, 0.9], [0.3, -0.8]])
        )
        is not None
    )


def test_hashmap_order_follows_java_buckets():
    keys = [
        (float(i), 0.5 * i) for i in range(13)
    ]  # 13 keys: the table grows from 16 to 32 buckets
    order = _conforming.hashmap_order(keys)
    assert sorted(order) == list(range(13))
    h = [_conforming._java_coord_hash(x, y) for x, y in keys]
    buckets = [(v ^ (v >> 16)) & 31 for v in h]
    assert [buckets[i] for i in order] == sorted(buckets)


def test_hashmap_order_matches_java_when_a_full_bucket_resizes_the_table():
    # 9 of these 11 keys share the low 4 bits of their spread hash: the 9th in one bucket below capacity 64 makes Java
    # resize the table (treeifyBin), before the load factor would. The order is the one Java 17's HashMap gave.
    keys = [
        (131.0, 22.5),
        (797.0, 422.0),
        (373.0, 339.5),
        (122.0, 435.0),
        (768.0, 7.0),
        (1.25, 7.5),
        (3.5, 2.25),
        (871.0, 42.5),
        (197.0, 337.0),
        (171.0, 470.5),
        (734.0, 158.5),
    ]
    assert _conforming.hashmap_order(keys) == [5, 0, 1, 3, 4, 7, 9, 10, 6, 2, 8]


def test_a_repeated_break_line_vertex_keeps_its_slot_and_takes_the_last_z():
    # java.util.HashMap.put replaces the value of a key it holds: the shared vertex (5, 0) carries the second Z
    pts = _sites([(0, 3), (10, 3)])
    bl = [
        np.array([[0.0, 0.0, 1.0], [5.0, 0.0, 1.0]]),
        np.array([[5.0, 0.0, 9.0], [10.0, 0.0, 9.0]]),
    ]
    verts, _ = _conforming.conforming_vertices(pts, bl, 0.0)
    assert [v for v in verts if v[:2] == (5.0, 0.0)] == [(5.0, 0.0, 9.0)]


def test_a_point_location_that_does_not_converge_raises_with_a_fix(monkeypatch):
    from databricks.labs.gbx.pyvx import _quadedge

    # a walk that never finds its triangle: every edge has the point on its right
    monkeypatch.setattr(_quadedge.Subdivision, "_right_of", lambda self, v, e: True)
    pts = _sites([(0, 0), (1, 0), (0, 1), (1, 1.2)])
    verts, env = _conforming.conforming_vertices(pts, [], 0.0)
    with pytest.raises(
        _conforming.ConformingError, match="locating a point did not converge"
    ):
        _conforming.jts_delaunay(verts, env, 0.0)


def test_the_port_takes_a_point_on_an_edge_with_a_merge_tolerance(monkeypatch):
    # with merge_tolerance > 0 JTS deletes the edge a new point lies on and connects the point to the quadrilateral:
    # the break line vertex (5, 0) lands on the edge (0, 0)-(10, 0). The triangles are the ones JTS 1.20 gave.
    from databricks.labs.gbx.pyvx import _quadedge

    hits = []
    on_edge = _quadedge.Subdivision._on_edge

    def spy(self, e, v):
        hits.append(on_edge(self, e, v))
        return hits[-1]

    monkeypatch.setattr(_quadedge.Subdivision, "_on_edge", spy)
    pts = _sites([(0, 0), (10, 0), (5, 6), (5, -6), (2, 9), (8, -9)])
    bl = [np.array([[5.0, 0.0, 1.0], [5.0, -3.0, 1.0]])]
    port = _conforming.triangulate(pts, bl, 0.01, delaunay="jts")
    assert any(hits)
    a, b, c, d = (0.0, 0.0), (10.0, 0.0), (5.0, 6.0), (5.0, -6.0)
    m, n = (5.0, 0.0), (5.0, -3.0)
    want = [
        (a, (2.0, 9.0), c),
        (a, d, n),
        (a, n, m),
        (a, m, c),
        (d, n, b),
        (d, (8.0, -9.0), b),
        (n, m, b),
        (m, c, b),
    ]
    assert _key(port) == {frozenset(t) for t in want}


def test_the_certificate_takes_a_hull_triangle_whose_circle_holds_a_frame_vertex():
    # (0, 0), (5, 0.001), (10, 0) is a hull triangle with a circle of radius 12500: it holds a frame vertex of JTS's
    # subdivision, yet JTS's forceConvex rules give the Delaunay triangulation of the sites, so the hybrid keeps
    # scipy's triangles, and they are the port's
    pts = _sites([(0, 0), (10, 0), (5, 5), (2, 3), (7, 2.5), (5, 0.001)])
    verts, env = _conforming.conforming_vertices(pts, [], 0.0)
    xy = np.array(verts)[:, :2]
    assert _conforming.unique_delaunay(xy) is not None
    stats = {}
    hybrid = _conforming.triangulate(pts, [], 0.0, stats=stats)
    assert stats["fallback"] is False
    assert _key(hybrid) == _key(_conforming.triangulate(pts, [], 0.0, delaunay="jts"))


def test_the_certificate_refuses_a_folded_or_non_convex_tiling(monkeypatch):
    # the proof is on the sites themselves: a candidate whose triangles overlap, or whose hull is not convex, is
    # refused (each candidate: two triangles sharing the edge 0-2)
    import scipy.spatial

    class Fake:
        def __init__(self, p):
            self.simplices = np.array([[0, 1, 2], [0, 2, 3]])
            self.neighbors = np.array([[-1, 1, -1], [-1, -1, 0]])
            self.coplanar = []

    monkeypatch.setattr(scipy.spatial, "Delaunay", Fake)
    # a fold: (3, 1) is inside the triangle (0, 0), (4, 0), (4, 4), so both triangles lie on one side of 0-2
    assert (
        _conforming.unique_delaunay(
            np.array([[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [3.0, 1.0]])
        )
        is None
    )
    # a reflex hull corner at (1, 1): the triangles tile a dart, not the convex hull
    assert (
        _conforming.unique_delaunay(
            np.array([[0.0, 0.0], [4.0, 0.0], [1.0, 1.0], [0.0, 4.0]])
        )
        is None
    )
    # the same two triangles over a convex quadrilateral (0-2 its Delaunay diagonal) are taken
    assert (
        _conforming.unique_delaunay(
            np.array([[0.0, 0.0], [4.0, 0.0], [4.0, 3.0], [-1.0, 3.0]])
        )
        is not None
    )


def test_the_certificate_refuses_broken_neighbours_and_a_non_disk(monkeypatch):
    import scipy.spatial

    quad = np.array([[0.0, 0.0], [4.0, 0.0], [4.0, 3.0], [-1.0, 3.0]])

    def fake(simplices, neighbors):
        class Fake:
            def __init__(self, p):
                self.simplices = np.array(simplices)
                self.neighbors = np.array(neighbors)
                self.coplanar = []

        monkeypatch.setattr(scipy.spatial, "Delaunay", Fake)

    # the second triangle does not name the first as its neighbour across 0-2
    fake([[0, 1, 2], [0, 2, 3]], [[-1, 1, -1], [-1, -1, -1]])
    assert _conforming.unique_delaunay(quad) is None
    # two triangles apart: two hull cycles, not one disk
    two = np.array(
        [[0.0, 0.0], [1.0, 0.0], [0.0, 1.0], [5.0, 5.0], [6.0, 5.0], [5.0, 6.0]]
    )
    fake([[0, 1, 2], [3, 4, 5]], [[-1, -1, -1], [-1, -1, -1]])
    assert _conforming.unique_delaunay(two) is None
