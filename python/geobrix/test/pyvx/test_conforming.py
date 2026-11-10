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
