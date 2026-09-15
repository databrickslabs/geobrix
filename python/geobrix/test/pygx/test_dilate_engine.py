# python/geobrix/test/pygx/test_dilate_engine.py
import itertools

import pytest
from shapely.geometry import LineString, Point, Polygon, box

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


def test_boundary_out_ring_is_full_band_plus_outward_band(holed_cls):
    """boundary-out (coveras default): k0 = the FULL straddling band (s_cover - s_core);
    the geom INTERIOR (s_core) is excluded; k>=1 adds the outward band."""
    full_band = frozenset(holed_cls.s_cover) - frozenset(holed_cls.s_core)
    k0 = D.geom_expand("loop", 0, "boundary-out", holed_cls, _neighbors)
    assert k0 == full_band, "boundary-out k0 must be the full coveras straddling band"
    r = D.geom_expand("ring", 1, "boundary-out", holed_cls, _neighbors)
    assert r, "full band + outward band must be non-empty"
    assert r.isdisjoint(holed_cls.s_core), "boundary-out excludes the geom interior"
    assert r - holed_cls.s_cover, "boundary-out k=1 must add an outward band"


def test_boundary_in_ring_stays_inside_geom(holed_cls):
    r = D.geom_expand("ring", 2, "boundary-in", holed_cls, _neighbors)
    assert r <= (holed_cls.p_core | holed_cls.p_border)  # never leaves P
    assert r.isdisjoint(holed_cls.h_core)  # respects holes


def test_hole_in_fills_hole_from_edge(holed_cls):
    r = D.geom_expand("ring", 5, "hole-in", holed_cls, _neighbors)
    assert holed_cls.h_core <= r  # hole gets filled inward
    assert r.isdisjoint(holed_cls.p_core)  # never enters the solid


def test_hole_out_stays_in_solid(holed_cls):
    # hole-out admits P_X (= p_cover under the default coveras basis), so the result
    # stays within p_cover and never enters the hole interior (h_core).
    r = D.geom_expand("ring", 2, "hole-out", holed_cls, _neighbors)
    assert r <= holed_cls.p_cover
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


def test_k0_loop_boundary_out_is_full_band(holed_cls):
    """boundary-out loop(0) == the full coveras straddling band (s_cover - s_core).
    boundary-in loop(0) is the op ∩ p_x ring (⊆ the band) — they differ now."""
    full_band = frozenset(holed_cls.s_cover) - frozenset(holed_cls.s_core)
    k0_out = D.geom_expand("loop", 0, "boundary-out", holed_cls, _neighbors)
    assert k0_out == full_band, "boundary-out loop(0) = full coveras band"
    k0_in = D.geom_expand("loop", 0, "boundary-in", holed_cls, _neighbors)
    assert k0_in <= full_band, "boundary-in k0 (op ∩ p_x) ⊆ the boundary band"


def test_boundary_in_ignore_holes_crosses_hole_interior(holed_cls):
    # boundary-in-ignore-holes admits S_X (= s_cover under coveras — crosses the
    # hole); boundary-in admits P_X (= p_cover — respects the hole interior).
    k = 6  # enough inward steps to reach the hole from the outer perimeter
    r_ign = D.geom_expand("ring", k, "boundary-in-ignore-holes", holed_cls, _neighbors)
    r_std = D.geom_expand("ring", k, "boundary-in", holed_cls, _neighbors)
    assert r_std <= r_ign  # ignoring holes only ever adds cells
    assert r_std.isdisjoint(holed_cls.h_core)  # boundary-in never fills the hole
    assert r_ign & holed_cls.h_core  # ignore-holes reaches hole-interior cells


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
    # full band (k0) + outward band; the INTERIOR (fully-contained s_core) is excluded.
    assert r, "boundary-out k=2 must be non-empty (full band + outward band)"
    assert r.isdisjoint(
        holed_cls.s_core
    ), "boundary-out excludes the geom interior (s_core)"


def test_boundary_in_holed_k0_excludes_hole_rim(holed_cls):
    """FIX: boundary-in k=0 must seed only from the outer perimeter, not the hole rim.

    Bug: with p_border seed, h_border (hole-rim) cells appear in k=0 because
    p_border = p_cover - p_core includes hole-rim cells (they touch P but are not
    fully inside it).
    Fix (Layer-2): k0 = outer_perimeter(s_cover, neighbors) — hole-rim cells lie
    inside the filled solid s and have all their neighbors in s_cover, so they are
    not on the outer perimeter and are correctly excluded.
    """
    # loop(0) == k0
    k0 = D.geom_expand("loop", 0, "boundary-in", holed_cls, _neighbors)
    # FAILS with buggy code: p_border ⊇ h_border → k0 contains hole-rim cells
    assert k0.isdisjoint(holed_cls.h_border), (
        "boundary-in k=0 must not include hole-rim (h_border) cells; "
        "only outer-perimeter cells should seed this mode"
    )


def test_boundary_in_ignore_holes_holed_k0_excludes_hole_rim(holed_cls):
    """FIX: boundary-in-ignore-holes k=0 must seed from the outer perimeter, not p_border.

    Same structural issue as boundary-in: the ignore-holes variant must seed only
    from the outer perimeter (outer_perimeter(s_cover, neighbors)), not from p_border
    which includes hole-rim cells.
    """
    k0 = D.geom_expand("loop", 0, "boundary-in-ignore-holes", holed_cls, _neighbors)
    # FAILS with buggy code: k0 = p_border ⊇ h_border
    assert k0.isdisjoint(
        holed_cls.h_border
    ), "boundary-in-ignore-holes k=0 must not include hole-rim (h_border) cells"


def test_boundary_modes_holeless_classification_and_perimeter():
    """Classification guard + perimeter-model k=0 check for a no-hole polygon.

    For P without holes: S = P → s_border == p_border (a Classification invariant).

    Under the Layer-2 perimeter fix, boundary-in/ignore-holes k=0 is the outer
    perimeter of s_cover (not s_border / p_border, which may be empty for grid-aligned
    polygons).  boundary-out k=0 is the same boundary ring (symmetric with boundary-in).
    """
    solid = box(0, 0, 10, 10)

    def polyfill_fn(g, res):
        return [_cid(x, y) for x in range(-2, 13) for y in range(-2, 13)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    cls = D.classify(solid, 1, polyfill_fn, cell_geom_fn)
    # Classification invariant (unchanged): s_border == p_border when S = P.
    assert (
        cls.s_border == cls.p_border
    ), "no-hole polygon: s_border must equal p_border (S = P when no holes)"
    # boundary-out k=0 is the boundary covering ring (== boundary-in k0).
    k0_bo = D.geom_expand("loop", 0, "boundary-out", cls, _neighbors)
    assert k0_bo == D.outer_perimeter(
        cls.s_cover, _neighbors
    ), "boundary-out k=0 must be the boundary covering ring"
    # Under the perimeter fix, boundary-in k=0 = outer_perimeter(s_cover, neighbors).
    # For a grid-aligned polygon s_border may be empty, but the perimeter is always
    # non-empty — this is the alignment-robustness the perimeter fix delivers.
    k0_bi = D.geom_expand("loop", 0, "boundary-in", cls, _neighbors)
    expected_op = D.outer_perimeter(cls.s_cover, _neighbors)
    assert (
        k0_bi == expected_op
    ), "boundary-in k=0 must equal outer_perimeter(s_cover) under the perimeter fix"
    assert k0_bi, (
        "boundary-in k=0 must be non-empty even for a grid-aligned polygon "
        "(outer_perimeter is always non-empty for non-empty s_cover)"
    )


@pytest.fixture
def aligned_cls():
    """Grid-aligned 5×5 solid: zero straddling cells (p_border = s_border = empty).

    box(2,2,7,7) aligns exactly with unit-cell boundaries → every cell in s_cover
    is fully contained by the solid → s_border empty.  Reproduces the custom-grid
    failure: Layer-1 s_border seed is empty → boundary-* produce nothing.
    """
    solid = box(2, 2, 7, 7)

    def polyfill_fn(g, res):
        return [_cid(x, y) for x in range(20) for y in range(20)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    cls = D.classify(solid, 1, polyfill_fn, cell_geom_fn)
    assert cls.s_border == set(), "aligned_cls fixture: s_border must be empty"
    return cls


def test_outer_perimeter_aligned_solid_nonempty(aligned_cls):
    """outer_perimeter of a non-empty covering set is always non-empty."""
    op = D.outer_perimeter(aligned_cls.s_cover, _neighbors)
    assert op, "outer_perimeter must be non-empty for any non-empty s_cover"
    assert op <= aligned_cls.s_cover, "outer_perimeter must be a subset of s_cover"


def test_aligned_solid_boundary_out_expands_outward(aligned_cls):
    """Layer-2 fix: boundary-out at k=2 expands outward even when s_border is empty.

    RED under Layer-1 (s_border = empty → frontier = empty → no expansion beyond p_cover).
    GREEN after the perimeter fix (op = outer ring of s_cover → outward BFS works).
    """
    r2 = D.geom_expand("ring", 2, "boundary-out", aligned_cls, _neighbors)
    outward = r2 - aligned_cls.p_cover
    assert outward, (
        "boundary-out k=2 must expand outward beyond p_cover for a grid-aligned solid; "
        "FAILS on Layer-1 (empty s_border seed), PASSES after outer_perimeter fix"
    )


def test_aligned_solid_boundary_in_nonempty_setback(aligned_cls):
    """Layer-2 fix: boundary-in at k=2 produces a non-empty inward setback.

    RED under Layer-1 (s_border = empty → frontier = empty → result is empty).
    GREEN after the perimeter fix (op = outer ring → inward BFS into p_core works).
    """
    r2 = D.geom_expand("ring", 2, "boundary-in", aligned_cls, _neighbors)
    assert r2, (
        "boundary-in k=2 must be non-empty for a grid-aligned solid; "
        "FAILS on Layer-1 (empty s_border seed), PASSES after outer_perimeter fix"
    )
    assert (
        r2 <= aligned_cls.p_cover
    ), "boundary-in result must stay within p_cover (the solid's covering cells)"


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


# ===========================================================================
# PART A — point/line support (dimension-aware candidate generation + coverage)
# ===========================================================================


@pytest.fixture
def point_cls_candidates():
    """A point geometry where polyfill_fn already returns the containing cell.

    Exercises the dimension-aware COVERAGE FILTER only (not the fallback):
    with the old area>0 check, the point/cell intersection has area=0 and the
    cell is dropped from p_cover; with the new dim=0 intersects-only check the
    cell is kept.  polyfill_fn behaviour is the same before and after; only the
    filter changes.
    """
    geom = Point(5.5, 5.5)  # strictly inside cell (5,5) = box(5,5,6,6)

    def polyfill_fn(g, res):
        # Return the single containing cell (as a quadbin bbox-polyfill would for a point).
        return [_cid(5, 5)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    return D.classify(geom, 1, polyfill_fn, cell_geom_fn)


@pytest.fixture
def line_cls_candidates():
    """A horizontal line where polyfill_fn returns cells in the line's bbox.

    Exercises the dimension-aware COVERAGE FILTER (not the fallback): the old
    area>0 check drops all cells (line∩cell area=0); the new length>0 check
    keeps only the cells the line actually crosses.
    """
    geom = LineString([(2.5, 5.5), (5.5, 5.5)])  # crosses cells (2,5),(3,5),(4,5),(5,5)

    def polyfill_fn(g, res):
        # Cells in the bounding box of the line (mimics quadbin bbox-polyfill).
        minx, miny, maxx, maxy = g.bounds
        return [
            _cid(x, y)
            for x in range(int(minx), int(maxx) + 1)
            for y in range(int(miny), int(maxy) + 1)
        ]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    return D.classify(geom, 1, polyfill_fn, cell_geom_fn)


@pytest.fixture
def point_cls_fallback():
    """A point geometry with an empty polyfill_fn (BNG centroid-BFS style).

    Exercises the FALLBACK path: polyfill returns nothing for a point input;
    classify must use point_to_cell_fn to obtain the single containing cell.
    RED until point_to_cell_fn parameter is both accepted and acted on.
    """
    geom = Point(5.5, 5.5)

    def polyfill_fn(g, res):
        return []  # centroid-BFS returns nothing for a point geometry

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    def point_to_cell_fn(x, y):
        return _cid(int(x), int(y))  # floor(x), floor(y) = containing cell

    return D.classify(geom, 1, polyfill_fn, cell_geom_fn, point_to_cell_fn)


@pytest.fixture
def line_cls_fallback():
    """A horizontal line with an empty polyfill_fn.

    Exercises the FALLBACK path: polyfill returns nothing; classify must sample
    points along the line and map each via point_to_cell_fn to candidate cells,
    then apply the length>0 coverage filter to keep only cells the line crosses.
    """
    geom = LineString([(2.5, 5.5), (5.5, 5.5)])

    def polyfill_fn(g, res):
        return []  # empty — forces fallback path

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    def point_to_cell_fn(x, y):
        return _cid(int(x), int(y))

    return D.classify(geom, 1, polyfill_fn, cell_geom_fn, point_to_cell_fn)


# --------------------------------------------------------------------------
# Filter-fix tests (RED: area>0 drops everything; GREEN: dim-aware filter)
# --------------------------------------------------------------------------


def test_classify_point_filter_fix_cover_nonempty(point_cls_candidates):
    """Part A filter fix: p_cover must be non-empty for a point (was empty with area>0).

    RED on current code: classify uses area>0 → point/cell intersection area=0 → dropped.
    GREEN after dim-aware fix: dim=0 uses intersects → the containing cell is kept.
    """
    cls = point_cls_candidates
    assert cls.p_cover, (
        "p_cover empty for a point geometry with a valid candidate cell; "
        "FIX: classify must use intersects (not area>0) for dim=0 geometries"
    )
    assert len(cls.p_cover) == 1, "point must cover exactly one cell"
    assert not cls.p_core, "point has no 2D interior → p_core must be empty"
    assert cls.s_cover == cls.p_cover, "s_cover must equal p_cover for a holeless point"


def test_classify_line_filter_fix_cover_crossing_cells(line_cls_candidates):
    """Part A filter fix: p_cover contains the cells the line crosses (was empty with area>0).

    RED on current code: line∩cell intersection area=0 → all dropped from p_cover.
    GREEN after dim-aware fix: dim=1 uses length>0 → cells the line crosses are kept.
    """
    cls = line_cls_candidates
    assert cls.p_cover, (
        "p_cover empty for a line geometry with valid candidate cells; "
        "FIX: classify must use intersection.length>0 (not area>0) for dim=1"
    )
    # The line from (2.5, 5.5) to (5.5, 5.5) crosses 4 cells at y=5:
    # cell(2,5), cell(3,5), cell(4,5), cell(5,5)
    assert len(cls.p_cover) == 4, f"expected 4 crossing cells; got {len(cls.p_cover)}"
    assert not cls.p_core, "line has no 2D interior → p_core must be empty"


# --------------------------------------------------------------------------
# Fallback-path tests (RED: old classify() has no point_to_cell_fn param)
# --------------------------------------------------------------------------


def test_classify_point_fallback_cover_nonempty(point_cls_fallback):
    """Part A fallback: p_cover non-empty when polyfill returns empty but point_to_cell_fn given.

    RED on current code: classify() has no point_to_cell_fn parameter
    (TypeError) — or, if param is added but not acted on, p_cover stays empty.
    GREEN after full fix: point_to_cell_fn maps (5.5, 5.5) → cell(5,5), which
    passes the intersects filter → p_cover = {cell(5,5)}.
    """
    cls = point_cls_fallback
    assert cls.p_cover, (
        "p_cover empty even though point_to_cell_fn was provided; "
        "FIX: classify must use point_to_cell_fn fallback when polyfill returns nothing"
    )
    assert len(cls.p_cover) == 1
    assert not cls.p_core


def test_classify_line_fallback_cover_crossing_cells(line_cls_fallback):
    """Part A fallback: line cover non-empty via point_to_cell_fn sampling + length>0 filter.

    RED on current code: no point_to_cell_fn param → TypeError or empty result.
    GREEN: sampled points along the line map to 4 distinct cells; all pass length>0.
    """
    cls = line_cls_fallback
    assert cls.p_cover, "p_cover empty for line with point_to_cell_fn fallback"
    # The line (2.5,5.5)→(5.5,5.5) must yield cells (2,5),(3,5),(4,5),(5,5)
    # (floor of sampled x-coords 2.5..5.5 at y=5.5, all land at y-cell 5).
    assert (
        len(cls.p_cover) >= 3
    ), f"expected at least 3 line-crossing cells via fallback; got {len(cls.p_cover)}"
    assert not cls.p_core


# --------------------------------------------------------------------------
# Expansion behaviour for point/line (uses filter-fixed coverage)
# --------------------------------------------------------------------------


def test_point_boundary_out_expands_outward(point_cls_candidates):
    """boundary-out on a point: k0 = the containing cell (boundary ring), and the
    k=1 loop is the 8-neighbour outward ring."""
    k0 = D.geom_expand("loop", 0, "boundary-out", point_cls_candidates, _neighbors)
    assert k0 == point_cls_candidates.p_cover, "boundary-out k0 = the point's cell"
    loop1 = D.geom_expand("loop", 1, "boundary-out", point_cls_candidates, _neighbors)
    assert loop1, "boundary-out k=1 loop on a point must be non-empty (outward ring)"
    assert loop1.isdisjoint(
        point_cls_candidates.p_cover
    ), "the outward ring excludes the point's own cell"
    assert (
        len(loop1) == 8
    ), "boundary-out k=1 loop on a single-cell point = 8 neighbours"


def test_point_boundary_in_stays_within_cover(point_cls_candidates):
    """boundary-in on a point stays within p_cover (empty p_core → no inward expansion)."""
    r = D.geom_expand("ring", 1, "boundary-in", point_cls_candidates, _neighbors)
    assert (
        r <= point_cls_candidates.p_cover
    ), "boundary-in on a point must not expand beyond p_cover (no 2D interior)"


def test_line_boundary_out_expands_outward(line_cls_candidates):
    """boundary-out on a line: k0 = the crossing cells (boundary ring), and the k=1
    loop is one outward band around the line."""
    k0 = D.geom_expand("loop", 0, "boundary-out", line_cls_candidates, _neighbors)
    assert k0 == line_cls_candidates.p_cover, "boundary-out k0 = the line's cells"
    loop1 = D.geom_expand("loop", 1, "boundary-out", line_cls_candidates, _neighbors)
    assert loop1, "boundary-out k=1 loop on a line must be a non-empty outward band"
    assert loop1.isdisjoint(
        line_cls_candidates.p_cover
    ), "the outward band excludes the line's own crossing cells"


def test_line_boundary_in_stays_within_cover(line_cls_candidates):
    """boundary-in on a line stays within p_cover (empty p_core → no inward expansion)."""
    r = D.geom_expand("ring", 1, "boundary-in", line_cls_candidates, _neighbors)
    assert r <= line_cls_candidates.p_cover


# ===========================================================================
# PART B — hole-* alignment robustness (perimeter seed replaces h_border seed)
# ===========================================================================


@pytest.fixture
def aligned_hole_cls():
    """Grid-aligned 10x10 solid with a 4x4 grid-ALIGNED hole: h_border is empty.

    outer = box(1,1,11,11); hole = box(3,3,7,7) — corners sit exactly on unit-cell
    boundaries.  Every cell inside the hole range is FULLY inside the hole polygon
    (no straddling) → h_cover = h_core → h_border = {}.

    This reproduces the alignment bug: old hole-* modes return empty because
    frontier0 = h_border = {} → nothing to dilate from.
    GREEN after the perimeter-seed fix: void_hole_edge and solid_hole_edge are
    always non-empty for a non-empty hole, regardless of alignment.
    """
    outer = box(1, 1, 11, 11)
    hole_poly = box(3, 3, 7, 7)
    geom = Polygon(outer.exterior.coords, [list(hole_poly.exterior.coords)])

    def polyfill_fn(g, res):
        return [_cid(x, y) for x in range(-1, 13) for y in range(-1, 13)]

    def cell_geom_fn(c):
        x, y = _xy(c)
        return box(x, y, x + 1, y + 1)

    cls = D.classify(geom, 1, polyfill_fn, cell_geom_fn)
    assert cls.h_border == set(), "aligned_hole_cls fixture: h_border must be empty"
    assert cls.h_core, "aligned_hole_cls fixture: h_core must be non-empty"
    return cls


def test_aligned_hole_hole_in_nonempty(aligned_hole_cls):
    """Part B: hole-in on an ALIGNED hole must be non-empty.

    RED on current code: h_border = empty → frontier = empty → nothing returned.
    GREEN after void_hole_edge seed: h_cover cells adjacent to non-h_cover cells
    are always present when h_core is non-empty, regardless of alignment.
    """
    r = D.geom_expand("ring", 1, "hole-in", aligned_hole_cls, _neighbors)
    assert r, (
        "hole-in k=1 on an aligned hole must be non-empty; "
        "FAILS with h_border seed (h_border empty for aligned holes); "
        "PASSES after void_hole_edge perimeter seed"
    )
    assert r <= aligned_hole_cls.h_cover, "hole-in must stay within h_cover"
    assert r.isdisjoint(aligned_hole_cls.p_core), "hole-in must not enter p_core"


def test_aligned_hole_hole_in_fills_hcore(aligned_hole_cls):
    """Part B: hole-in k=large must fill the aligned hole's h_core."""
    r = D.geom_expand("ring", 10, "hole-in", aligned_hole_cls, _neighbors)
    assert (
        aligned_hole_cls.h_core <= r
    ), "hole-in k=10 must fill the entire h_core of the aligned hole"


def test_aligned_hole_hole_out_nonempty(aligned_hole_cls):
    """Part B: hole-out on an ALIGNED hole must be non-empty.

    RED with h_border seed (empty); GREEN with solid_hole_edge seed (non-empty).
    """
    r = D.geom_expand("ring", 1, "hole-out", aligned_hole_cls, _neighbors)
    assert r, (
        "hole-out k=1 on an aligned hole must be non-empty; "
        "FAILS with h_border seed; PASSES after solid_hole_edge perimeter seed"
    )
    assert r <= aligned_hole_cls.p_cover, "hole-out must stay within p_cover"
    assert r.isdisjoint(
        aligned_hole_cls.h_core
    ), "hole-out must not include h_core cells"


def test_aligned_hole_hole_out_ignore_geom_nonempty(aligned_hole_cls):
    """Part B: hole-out-ignore-geom on an ALIGNED hole must be non-empty."""
    r = D.geom_expand("ring", 1, "hole-out-ignore-geom", aligned_hole_cls, _neighbors)
    assert r, "hole-out-ignore-geom on aligned hole must be non-empty"
    assert r.isdisjoint(aligned_hole_cls.h_core)


def test_aligned_hole_hole_out_reaches_solid(aligned_hole_cls):
    """Part B: hole-out k=large expands into the solid and stays there."""
    r = D.geom_expand("ring", 5, "hole-out", aligned_hole_cls, _neighbors)
    assert r, "hole-out k=5 must be non-empty for an aligned hole"
    assert r <= aligned_hole_cls.p_cover
    assert r.isdisjoint(aligned_hole_cls.h_core)


def test_aligned_hole_hole_out_ignore_geom_may_exceed_outer(aligned_hole_cls):
    """Part B: hole-out-ignore-geom can grow past the outer solid boundary."""
    r = D.geom_expand("ring", 20, "hole-out-ignore-geom", aligned_hole_cls, _neighbors)
    outside_outer = {c for c in r if c not in aligned_hole_cls.s_cover}
    assert outside_outer, "hole-out-ignore-geom k=20 must reach cells outside s_cover"


# --------------------------------------------------------------------------
# Part B regression: non-aligned holed polygon hole modes still work
# --------------------------------------------------------------------------


def test_nonaligend_hole_hole_in_still_works(holed_cls):
    """Part B regression: non-aligned hole hole-in still fills h_core."""
    r = D.geom_expand("ring", 5, "hole-in", holed_cls, _neighbors)
    assert holed_cls.h_core <= r
    assert r.isdisjoint(holed_cls.p_core)


def test_nonaligned_hole_hole_out_still_works(holed_cls):
    """Part B regression: non-aligned hole hole-out still expands into solid."""
    r = D.geom_expand("ring", 2, "hole-out", holed_cls, _neighbors)
    assert r
    assert r.isdisjoint(holed_cls.h_core)
    assert r <= holed_cls.p_cover
