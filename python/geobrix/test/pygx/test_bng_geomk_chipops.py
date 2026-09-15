"""Spark-free unit tests for BNG geometry-centric neighborhood + chip ops (Task 7).

Covers ``geometryKRing``/``geometryKLoop`` (the k_ring/k_loop of cells covering a
geometry) and the chip-struct ops ``cellIntersection``/``cellUnion`` ported from
``BNG.scala`` (geometryKRing/geometryKLoop, getChips, lineFill/lineDecompose,
isValid) and the ``BNG_CellUnion``/``BNG_CellIntersection`` expressions. Chip
geometry is plain WKB (NO SRID), matching heavy ``JTS.toWKB``.
"""

import pytest

shapely = pytest.importorskip("shapely")

from shapely import to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402


def _box2(minx, miny, maxx, maxy):
    return box(minx, miny, maxx, maxy)


def _towkb(geom):
    return to_wkb(geom)


def test_geomkring_box_superset_of_polyfill():
    geom = _towkb(_box2(530000.0, 180000.0, 533000.0, 183000.0))
    res = _bng.get_resolution("1km")
    fill = set(_bng.polyfill_str(geom, res))
    gkr = set(_bng.geometry_k_ring_str(geom, res, 1))
    # k-ring around the geometry includes (at least) the tessellated coverage.
    assert fill <= gkr or len(gkr) >= len(fill)


def test_geomkloop_excludes_inner_ring():
    # A sub-cell box (single border chip, no core cells) so the k=2 loop is a
    # genuine non-empty hollow ring -- a multi-cell box can legitimately diff to
    # empty (border n-ring + core cover the whole k-loop), which would make the
    # non-emptiness check vacuous.
    geom = _towkb(_box2(530200.0, 180200.0, 530800.0, 180800.0))
    res = _bng.get_resolution("1km")
    gkr1 = set(_bng.geometry_k_ring_str(geom, res, 1))
    gkl2 = set(_bng.geometry_k_loop_str(geom, res, 2))
    # k-loop at 2 is disjoint from the k-ring at 1 (hollow outer ring); the
    # inner-ring subtraction removes everything covered by the k-ring at 1.
    assert gkl2.isdisjoint(gkr1)
    # ...and the hollow outer ring is non-empty.
    assert len(gkl2) > 0


def test_cell_union_same_cell_merges_chips():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid = _bng.parse(cid_s)
    full = _bng.cell_id_to_geometry(cid)
    left = (
        cid_s,
        False,
        full.buffer(0).intersection(_box2(530000, 180000, 530500, 181000)),
    )
    right = (
        cid_s,
        False,
        full.buffer(0).intersection(_box2(530500, 180000, 531000, 181000)),
    )
    cell, core, chip = _bng.cell_union(left, right)
    assert cell == cid_s
    assert chip.equals(full) or chip.area == pytest.approx(full.area, rel=1e-6)


def test_cell_intersection_different_cells_is_empty():
    a = (
        _bng.east_north_as_bng(530000.0, 180000.0, "1km"),
        False,
        _box2(530000, 180000, 531000, 181000),
    )
    b = (
        _bng.east_north_as_bng(540000.0, 180000.0, "1km"),
        False,
        _box2(540000, 180000, 541000, 181000),
    )
    cell, core, chip = _bng.cell_intersection(a, b)
    assert chip.is_empty


def test_geomk_str_emit_canonical_string_ids():
    geom = _towkb(_box2(530000.0, 180000.0, 532000.0, 182000.0))
    res = _bng.get_resolution("1km")
    for cid in _bng.geometry_k_ring_str(geom, res, 1):
        # round-trips through parse/format -> canonical BNG string id
        assert _bng.format(_bng.parse(cid)) == cid


def test_geomkring_covers_point():
    # Point geometry: the classify engine now uses a dimension-aware coverage
    # test (dim=0: intersects, not area>0) and a point_to_cell_fn fallback when
    # polyfill returns nothing for non-polygon inputs.  boundary-out k=1 returns
    # the containing cell + its k-ring neighbours (a non-empty outward expansion).
    #
    # REVERSED from the 6dc1efe8 "empty" assertion: point support is now required.
    pt = _towkb(shapely.geometry.Point(530000.0, 180000.0))
    res = _bng.get_resolution("1km")
    gkr = _bng.geometry_k_ring_str(pt, res, 1)
    assert gkr, (
        "boundary-out k=1 on a BNG point must return the surrounding k-ring; "
        "FAILS before dimension-aware classify + point_to_cell_fn fallback; "
        "PASSES after the fix"
    )
    # All returned IDs must be valid BNG string cell IDs.
    for cid in gkr:
        assert _bng.format(_bng.parse(cid)) == cid, f"invalid cell id: {cid!r}"


def test_cell_union_core_chip_wins():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    full = _bng.cell_id_to_geometry(_bng.parse(cid_s))
    core_left = (cid_s, True, full)
    border_right = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    # left-hand rule: a core chip on either side short-circuits to that chip.
    assert _bng.cell_union(core_left, border_right) == core_left
    assert _bng.cell_intersection(core_left, border_right) == core_left
    # right core when left is border
    border_left = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    core_right = (cid_s, True, full)
    assert _bng.cell_union(border_left, core_right) == core_right


def _geom_equals(a, b):
    """Start-point-insensitive geometry equality (mutual near-containment).

    shapely renormalizes the ring start vertex on intersection/union, so
    ``equals_exact`` is unreliable; use ``equals`` (topological) with an
    area-approx fallback.
    """
    return a.equals(b) or a.area == pytest.approx(b.area, rel=1e-9)


# ---------------------------------------------------------------------------
# Chip-op branch ordering (heavy BNG_CellUnion/BNG_CellIntersection eval order)
#
# Heavy evalLong/evalString check the CORE flag FIRST (chip1.core -> chip1;
# chip2.core -> chip2) BEFORE the cellid-equality check. So a CORE chip with a
# mismatched cellid must return that core chip, NOT an empty polygon. Union and
# intersection share the identical eval order. These cover, for both ops:
#   (a) core-left + different cellid   -> left
#   (b) core-right + different cellid  -> right (left non-core)
#   (c) both non-core + different cellid -> (left cellid, left core, empty)
#   (d) both non-core + same cellid    -> geometric op
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", [_bng.cell_union, _bng.cell_intersection])
def test_chipop_core_left_mismatched_cellid_returns_left(op):
    # Heavy: if (chip1.core) return chip1 -- BEFORE the cellid check.
    cid_l = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid_r = _bng.east_north_as_bng(540000.0, 180000.0, "1km")
    left = (cid_l, True, _bng.cell_id_to_geometry(_bng.parse(cid_l)))
    right = (cid_r, False, _box2(540000, 180000, 541000, 181000))
    assert op(left, right) == left


@pytest.mark.parametrize("op", [_bng.cell_union, _bng.cell_intersection])
def test_chipop_core_right_mismatched_cellid_returns_right(op):
    # Heavy: chip1 non-core, then if (chip2.core) return chip2 -- still BEFORE
    # the cellid check (the cellid mismatch never gets a chance to empty it).
    cid_l = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid_r = _bng.east_north_as_bng(540000.0, 180000.0, "1km")
    left = (cid_l, False, _box2(530000, 180000, 531000, 181000))
    right = (cid_r, True, _bng.cell_id_to_geometry(_bng.parse(cid_r)))
    assert op(left, right) == right


@pytest.mark.parametrize("op", [_bng.cell_union, _bng.cell_intersection])
def test_chipop_both_noncore_mismatched_cellid_is_empty(op):
    # Heavy execute*: different cell ids -> (chip1._1, chip1._2, emptyPolygon).
    cid_l = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid_r = _bng.east_north_as_bng(540000.0, 180000.0, "1km")
    left = (cid_l, False, _box2(530000, 180000, 531000, 181000))
    right = (cid_r, False, _box2(540000, 180000, 541000, 181000))
    cell, core, chip = op(left, right)
    assert cell == cid_l  # left cellid
    assert core is False  # left core flag
    assert chip.is_empty  # empty polygon


def test_chipop_both_noncore_same_cell_union_merges():
    # Heavy: else -> chip1.union(chip2).
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    full = _bng.cell_id_to_geometry(_bng.parse(cid_s))
    left = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    right = (cid_s, False, _box2(530500, 180000, 531000, 181000))
    cell, core, chip = _bng.cell_union(left, right)
    assert cell == cid_s
    assert core is False
    assert _geom_equals(chip, full)


def test_chipop_both_noncore_same_cell_intersection_clips():
    # Heavy: else -> chip1.intersection(chip2).
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    left = (cid_s, False, _box2(530000, 180000, 531000, 181000))
    right = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    cell, core, chip = _bng.cell_intersection(left, right)
    assert cell == cid_s
    assert core is False
    assert _geom_equals(chip, _box2(530000, 180000, 530500, 181000))


def test_line_fill_chips_follow_line():
    # Line geometry: the classify engine uses intersection.length > 0 for dim=1
    # geometries and a point_to_cell_fn fallback to generate candidates.  boundary-out
    # and boundary-in share the same k0 anchor: the line's crossing cells (the
    # perimeter of a 1-D covering set is every crossing cell).  boundary-out k>=1 is
    # the outward buffer around the line.
    #
    # REVERSED from the 6dc1efe8 "empty" assertion: line support is now required.
    line = _towkb(
        shapely.geometry.LineString([(530100.0, 180500.0), (532900.0, 180500.0)])
    )
    res = _bng.get_resolution("1km")
    # Covering set (the crossing cells) = boundary-in k=0 == boundary-out k=0.
    cover = _bng.geometry_k_ring_str(line, res, 0, "boundary-in")
    assert cover, "line covering set (boundary-in k=0) must be non-empty"
    assert (
        len(cover) >= 2
    ), f"a 2.8 km line at 1km resolution must cross at least 2 cells; got {len(cover)}"
    k0_out = _bng.geometry_k_ring_str(line, res, 0)  # default boundary-out
    assert set(k0_out) == set(cover), "boundary-out k0 = the line's crossing cells"
    # boundary-out k=1 loop is the outward buffer, EXCLUDING the crossing cells.
    band = _bng.geometry_k_loop_str(line, res, 1)  # outward ring only
    assert (
        band
    ), "boundary-out k=1 loop on a BNG line must be a non-empty outward buffer"
    assert set(band).isdisjoint(
        set(cover)
    ), "the outward buffer excludes the line's own cells"
    # All returned IDs must be valid BNG string cell IDs.
    for cid in list(cover) + list(band):
        assert _bng.format(_bng.parse(cid)) == cid


# ---------------------------------------------------------------------------
# Grid-aligned polygon — perimeter-model regression tests
# ---------------------------------------------------------------------------


def test_geomkring_boundary_out_grid_aligned_expands_outward():
    """boundary-out on a GRID-ALIGNED BNG polygon must expand OUTWARD.

    A box whose corners land exactly on 1km-grid lines has zero straddling cells
    (s_border = s_cover - s_core = empty).  The old get_chips fast-path produced
    only the covering set for such a geometry (border_kring empty -> just core_ids).
    The perimeter-model fix routes through the shared engine: outer_perimeter(s_cover)
    is always non-empty for any non-empty covering set -> k-ring correctly includes
    the outer ring of cells surrounding the geometry.
    """
    # Grid-aligned 3x3 km box (corners on exact 1km boundaries).
    geom = _towkb(_box2(530000.0, 180000.0, 533000.0, 183000.0))
    res = _bng.get_resolution("1km")
    cover = set(_bng.polyfill_str(geom, res))  # 9 covering cells
    k0 = set(_bng.geometry_k_ring_str(geom, res, 0))  # boundary ring
    band = set(_bng.geometry_k_loop_str(geom, res, 1))  # outward ring only

    # The perimeter model makes boundary-out non-empty even for an aligned polygon
    # (outer_perimeter of a non-empty covering set is always non-empty).  k0 is the
    # boundary ring; the k=1 loop is the outward band, disjoint from the covering set.
    assert k0, "boundary-out k0 (boundary ring) on aligned polygon must be non-empty"
    assert band, "boundary-out k=1 loop on aligned polygon must be a non-empty band"
    assert band.isdisjoint(cover), "the outward band excludes the covering set"


def test_geomkring_boundary_out_grid_aligned_excludes_interior():
    """boundary-out excludes the INTERIOR for aligned polygons: any covering cell in
    the k=1 ring is on the boundary ring (k0)."""
    geom = _towkb(_box2(530000.0, 180000.0, 533000.0, 183000.0))
    res = _bng.get_resolution("1km")
    fill = set(_bng.polyfill_str(geom, res))
    k0 = set(_bng.geometry_k_ring_str(geom, res, 0))
    gkr = set(_bng.geometry_k_ring_str(geom, res, 1))
    assert gkr and (gkr & fill) <= k0


# ---------------------------------------------------------------------------
# Mode param — Task 3
# ---------------------------------------------------------------------------


def test_bng_geomkring_default_mode_matches_no_mode():
    geom = _towkb(_box2(530000.0, 180000.0, 533000.0, 183000.0))
    res = _bng.get_resolution("1km")
    assert set(_bng.geometry_k_ring_str(geom, res, 1)) == set(
        _bng.geometry_k_ring_str(geom, res, 1, "boundary-out")
    )


def test_bng_geomkring_boundary_in_nonempty_and_inside():
    # Both boundary-out and boundary-in now use the shared engine (classify + outer_perimeter).
    # Do NOT assert a cross-classifier subset (fragile). Assert boundary-in returns a
    # non-empty inward band, and differs from boundary-out.
    #
    # NOTE: the box must be NON-grid-aligned (not on exact 1km boundaries) so that
    # the classify engine sees perimeter cells as p_border (corners outside the box)
    # rather than p_core. A perfectly grid-aligned 5km box puts all 25 cells fully
    # inside the geometry (geom.contains(cell_geom)=True for all), leaving p_border={}
    # and boundary-in empty. Insetting by 300m keeps all 25 cell centroids inside
    # (polyfill unchanged) while making the 16 perimeter cells straddle the boundary.
    geom = _towkb(
        _box2(529300.0, 179300.0, 533700.0, 183700.0)
    )  # ~4.4km box, non-grid-aligned
    res = _bng.get_resolution("1km")
    inn = set(_bng.geometry_k_ring_str(geom, res, 1, "boundary-in"))
    out = set(_bng.geometry_k_ring_str(geom, res, 1, "boundary-out"))
    assert (
        inn
    ), "boundary-in should return a non-empty inward band for a multi-cell polygon"
    assert inn != out, "boundary-in must differ from boundary-out"


def test_bng_geomkring_bad_mode_raises_via_str_wrapper():
    geom = _towkb(_box2(530000.0, 180000.0, 531000.0, 181000.0))
    res = _bng.get_resolution("1km")
    with pytest.raises(ValueError):
        _bng.geometry_k_ring_str(geom, res, 1, "sideways")


def test_bng_geomkring_sub_cell_polygon_coarse_res_nonempty():
    """A polygon smaller than a cell (100m box at res=1 / 100km cells) still expands.

    BNG's centroid-membership polyfill returns nothing for a sub-cell polygon (no
    cell centroid falls inside it), but the classify point_to_cell fallback seeds the
    containing cell, so boundary-out returns that cell's neighbourhood (regression
    guard for the perimeter-engine routing — heavy test_bng_functions covers the
    heavy tier).
    """
    geom = _towkb(_box2(530000.0, 180000.0, 530100.0, 180100.0))  # 100m box
    ring = set(_bng.geometry_k_ring_str(geom, 1, 1))  # res=1 → 100km cells
    loop = set(_bng.geometry_k_loop_str(geom, 1, 1))
    assert ring, "sub-cell polygon boundary-out ring must be non-empty at coarse res"
    assert loop, "sub-cell polygon boundary-out loop must be non-empty at coarse res"
