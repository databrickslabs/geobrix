# python/geobrix/test/pygx/test_dilate_engine.py
import itertools

import pytest
from shapely.geometry import Polygon, box

from databricks.labs.gbx.pygx import _dilate as D


# Synthetic grid: cell id = (x<<20)|y for 0<=x,y<2^20; 8-connected neighbors.
def _cid(x, y):
    return (x << 20) | y


def _xy(c):
    return (c >> 20, c & 0xFFFFF)


def _neighbors(c):
    x, y = _xy(c)
    return [
        _cid(x + dx, y + dy)
        for dx in (-1, 0, 1)
        for dy in (-1, 0, 1)
        if not (dx == 0 and dy == 0) and 0 <= x + dx and 0 <= y + dy
    ]


def test_dilate_yields_chebyshev_shells_from_single_seed():
    seed = {_cid(5, 5)}
    # islice(2): take first 2 shells — the generator is unbounded with admit=True.
    shells = dict(itertools.islice(D.dilate(seed, seed, _neighbors, lambda n: True), 2))
    assert (
        shells[1]
        == {_cid(5 + dx, 5 + dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)} - seed
    )
    assert len(shells[1]) == 8 and len(shells[2]) == 16  # square rings


def test_dilate_visited_blocks_reentry():
    # Seeding visited with a 3x3 block: frontier is the block's border, grows only outward.
    block = {_cid(x, y) for x in (4, 5, 6) for y in (4, 5, 6)}
    # islice(1): only need the first shell; generator is unbounded with admit=True.
    shells = dict(
        itertools.islice(D.dilate(block, block, _neighbors, lambda n: True), 1)
    )
    assert block.isdisjoint(shells[1])  # never re-emits the seed
    assert len(shells[1]) == 16  # perimeter of the 5x5 minus 3x3


def test_admit_prunes_frontier_and_bounds_walk():
    seed = {_cid(5, 5)}

    def admit(n):  # only walk in +x half-plane
        return _xy(n)[0] >= 5

    # islice(5): take 5 shells for a meaningful sample; admit still leaves +x unbounded.
    shells = dict(itertools.islice(D.dilate(seed, seed, _neighbors, admit), 5))
    assert all(_xy(c)[0] >= 5 for shell in shells.values() for c in shell)


@pytest.fixture
def holed_cls():
    # A ~10x10 solid box with a ~3x3 hole at half-integer coords so cells straddle
    # boundaries, creating genuine border (partial) cells vs core (full) cells.
    outer = box(0.5, 0.5, 9.5, 9.5)
    hole = box(3.5, 3.5, 6.5, 6.5)
    geom = Polygon(outer.exterior.coords, [list(hole.exterior.coords)])

    def polyfill_fn(g, res):
        return [_cid(x, y) for x in range(-2, 13) for y in range(-2, 13)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    return D.classify(geom, 1, polyfill_fn, cell_geom_fn)


def test_classify_partitions_cover_core_holes(holed_cls):
    c = holed_cls
    assert c.p_core and c.p_border and c.h_core and c.h_border
    assert c.p_core.isdisjoint(c.h_cover)  # core is outside holes
    assert c.p_border == (c.p_cover - c.p_core)


def test_boundary_out_ring_is_filled_and_matches_dilation(holed_cls):
    r = D.geom_expand("ring", 1, "boundary-out", holed_cls, _neighbors)
    assert holed_cls.p_core <= r  # filled: core kept
    assert holed_cls.p_cover <= r


def test_boundary_in_ring_stays_inside_geom(holed_cls):
    r = D.geom_expand("ring", 2, "boundary-in", holed_cls, _neighbors)
    assert r <= (holed_cls.p_core | holed_cls.p_border)  # never leaves P
    assert r.isdisjoint(holed_cls.h_core)  # respects holes


def test_hole_in_fills_hole_from_edge(holed_cls):
    r = D.geom_expand("ring", 5, "hole-in", holed_cls, _neighbors)
    assert holed_cls.h_core <= r  # hole gets filled inward
    assert r.isdisjoint(holed_cls.p_core)  # never enters the solid


def test_hole_out_stays_in_solid(holed_cls):
    r = D.geom_expand("ring", 2, "hole-out", holed_cls, _neighbors)
    assert r <= (holed_cls.p_core | holed_cls.h_border)
    assert r.isdisjoint(holed_cls.h_core)


def test_hole_out_ignore_geom_may_exceed_outer(holed_cls):
    r = D.geom_expand("ring", 12, "hole-out-ignore-geom", holed_cls, _neighbors)
    outside_outer = {c for c in r if c not in holed_cls.s_cover}
    assert outside_outer  # unbounded by the outer ring


def test_loop_is_ring_difference(holed_cls):
    for mode in ("boundary-out", "boundary-in"):
        ring_k = D.geom_expand("ring", 3, mode, holed_cls, _neighbors)
        ring_km1 = D.geom_expand("ring", 2, mode, holed_cls, _neighbors)
        loop_k = D.geom_expand("loop", 3, mode, holed_cls, _neighbors)
        assert loop_k == (ring_k - ring_km1)


def test_k0_loop_is_covering_set(holed_cls):
    assert (
        D.geom_expand("loop", 0, "boundary-out", holed_cls, _neighbors)
        == holed_cls.p_cover
    )


def test_boundary_in_ignore_holes_crosses_hole_interior(holed_cls):
    # boundary-in-ignore-holes admits s_core (solid + hole interior);
    # boundary-in admits p_core only (solid, hole excluded).
    k = 3
    r_ign = D.geom_expand("ring", k, "boundary-in-ignore-holes", holed_cls, _neighbors)
    r_std = D.geom_expand("ring", k, "boundary-in", holed_cls, _neighbors)
    # ignore-holes reaches hole-region cells (s_core cells not in p_core)
    hole_region = holed_cls.s_core - holed_cls.p_core
    assert r_ign & hole_region  # some hole-region cells reached
    assert r_std.isdisjoint(holed_cls.h_core)  # boundary-in respects the hole
    assert r_std <= r_ign  # ignoring holes only ever adds cells
    assert r_ign > r_std  # strictly more cells when hole reachable


def test_no_holes_hole_modes_return_empty():
    solid = box(0, 0, 10, 10)

    def polyfill_fn(g, res):
        return [_cid(x, y) for x in range(-1, 12) for y in range(-1, 12)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    cls = D.classify(solid, 1, polyfill_fn, cell_geom_fn)
    # all three hole modes share the empty-frontier path when H is None
    assert D.geom_expand("ring", 3, "hole-in", cls, _neighbors) == set()
    assert D.geom_expand("ring", 3, "hole-out", cls, _neighbors) == set()
    assert D.geom_expand("ring", 3, "hole-out-ignore-geom", cls, _neighbors) == set()


def test_bad_mode_raises():
    with pytest.raises(ValueError):
        D.mode_setup(
            "sideways", D.Classification(set(), set(), set(), set(), set(), set())
        )


def test_bad_kind_raises(holed_cls):
    with pytest.raises(ValueError):
        D.geom_expand("disk", 1, "boundary-out", holed_cls, _neighbors)


def test_boundary_out_holed_does_not_fill_hole(holed_cls):
    """FIX: boundary-out must not seed from hole-rim cells and must not fill the hole.

    Bug: with p_border frontier, h_core cells get seeded at k=1 because hole-rim
    cells are in the frontier AND h_core cells are not in visited0 (p_cover).
    Fix: s_border (outer-rim only) is the frontier; h_core is never reached because
    the path from the outer rim to hole interior goes through p_cover (= visited0).
    """
    # k=2 confirms the hole is not filled (with the bug it fills at k=1)
    r = D.geom_expand("ring", 2, "boundary-out", holed_cls, _neighbors)
    # FAILS with buggy p_border frontier: hole-rim cells seed h_core at k=1
    assert r.isdisjoint(
        holed_cls.h_core
    ), "boundary-out must not fill the hole; h_core cells found in result"
    # outward expansion beyond the geom IS present (fix must not break outward growth)
    assert r > holed_cls.p_cover, "boundary-out k=2 must expand beyond p_cover"


def test_boundary_in_holed_k0_excludes_hole_rim(holed_cls):
    """FIX: boundary-in k=0 must seed only from s_border (outer rim), not hole rim.

    Bug: with p_border seed, h_border (hole-rim) cells appear in k=0 because
    p_border = p_cover - p_core includes hole-rim cells (they touch P but are not
    fully inside it).  Fix: k0 = s_border (outer ring only), disjoint from h_border.
    """
    # loop(0) == k0
    k0 = D.geom_expand("loop", 0, "boundary-in", holed_cls, _neighbors)
    # FAILS with buggy code: p_border ⊇ h_border → k0 contains hole-rim cells
    assert k0.isdisjoint(holed_cls.h_border), (
        "boundary-in k=0 must not include hole-rim (h_border) cells; "
        "only outer-boundary (s_border) cells should seed this mode"
    )


def test_boundary_in_ignore_holes_holed_k0_excludes_hole_rim(holed_cls):
    """FIX: boundary-in-ignore-holes k=0 must also use s_border, not p_border.

    Same structural issue as boundary-in: the ignore-holes variant must seed only
    from the outer ring (s_border), not from p_border (which includes hole-rim cells).
    """
    k0 = D.geom_expand("loop", 0, "boundary-in-ignore-holes", holed_cls, _neighbors)
    # FAILS with buggy code: k0 = p_border ⊇ h_border
    assert k0.isdisjoint(
        holed_cls.h_border
    ), "boundary-in-ignore-holes k=0 must not include hole-rim (h_border) cells"


def test_boundary_modes_holeless_s_border_equals_p_border():
    """REGRESSION GUARD: for a no-hole polygon, s_border == p_border; boundary-* unchanged.

    For P without holes: S = P → s_core = p_core, s_cover = p_cover →
    s_border = p_border.  The fix (seeding from s_border instead of p_border)
    is therefore a no-op for holeless polygons.
    """
    solid = box(0, 0, 10, 10)

    def polyfill_fn(g, res):
        return [_cid(x, y) for x in range(-2, 13) for y in range(-2, 13)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    cls = D.classify(solid, 1, polyfill_fn, cell_geom_fn)
    # For no-hole polygon: s_border must equal p_border (S = P when no holes)
    assert (
        cls.s_border == cls.p_border
    ), "no-hole polygon: s_border must equal p_border (S = P when no holes)"
    # boundary-in k=0 must be the full p_border (= s_border) for holeless case
    k0_bi = D.geom_expand("loop", 0, "boundary-in", cls, _neighbors)
    assert (
        k0_bi == cls.p_border
    ), "holeless boundary-in k=0 must equal p_border (= s_border for no holes)"


def test_classify_filtering_polyfill_finds_hcore():
    """Fix B: classify re-polyfills S (solid), so a filtering polyfill still populates h_core.

    A filtering polyfill over the original donut never returns hole-interior cells.
    classify must call polyfill_fn(S, res) internally so hole modes work correctly.
    """
    outer = box(0.5, 0.5, 9.5, 9.5)
    hole = box(3.5, 3.5, 6.5, 6.5)
    geom = Polygon(outer.exterior.coords, [list(hole.exterior.coords)])

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    # Filtering polyfill: only returns cells that have area-overlap with the polyfill target.
    # When the target is the donut (geom), hole-interior cells are excluded.
    # classify must call polyfill_fn(S, res) internally so those cells become candidates.
    def filtering_polyfill_fn(g, res):
        return [
            _cid(x, y)
            for x in range(-1, 11)
            for y in range(-1, 11)
            if cell_geom_fn(_cid(x, y)).intersects(g)
            and cell_geom_fn(_cid(x, y)).intersection(g).area > 0
        ]

    cls = D.classify(geom, 1, filtering_polyfill_fn, cell_geom_fn)
    assert (
        cls.h_core
    ), "h_core empty: fix B — classify must polyfill S (solid), not original geom"
    r = D.geom_expand("ring", 5, "hole-in", cls, _neighbors)
    assert cls.h_core <= r  # hole filled inward
    assert r.isdisjoint(cls.p_core)  # never enters the solid
