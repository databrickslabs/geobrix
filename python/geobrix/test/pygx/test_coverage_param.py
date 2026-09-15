"""Coverage-param engine tests (0.5.1 re-cut) — the LIGHT-ENGINE correctness heart.

Locked design: .superpowers/specs/2026-09-14-geom-aware-boundary-perimeter-fix.md
("COVERAGE PARAM — LOCKED").

Three classify bases per region P/S/H:
  - cover    (``*_cover``)    — overlap                       -> coverage="coveras" (DEFAULT)
  - centroid (``*_centroid``) — cell centroid inside region   -> coverage="polyfill"
  - core     (``*_core``)     — cell fully contained          -> coverage="core"
Nested: cover ⊇ centroid ⊇ core.

LOCKED model: SEEDS are always the coveras (overlap) topology; ``coverage`` governs
only the ADMIT predicate (k>=1) and which frontier cells are RETURNED at k0.
boundary-out k0 = the full coveras straddling band under coveras/core, the centroid-
out cells under polyfill; the outward k>=1 rings are identical across coverages.

Exercised at the engine level (synthetic grid) and on two real grids (custom,
quadbin); full light-vs-heavy parity lives in test_parity_*_geomk.py.
"""

import pytest
from shapely.geometry import LineString, Point, Polygon, box

from databricks.labs.gbx.pygx import _dilate as D

# ---------------------------------------------------------------------------
# Synthetic 8-connected grid: cell id = (x<<20)|y; cell = unit box at (x,y).
# ---------------------------------------------------------------------------


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


def _cell_geom(c):
    x, y = _xy(c)
    return box(x, y, x + 1, y + 1)


def _wide_polyfill(g, res):
    return [_cid(x, y) for x in range(-3, 14) for y in range(-3, 14)]


@pytest.fixture
def holed_cls():
    """Holed solid offset 0.3 from the grid → strict cover ⊃ centroid ⊃ core.

    outer=box(0.3,0.3,9.7,9.7), hole=box(3.3,3.3,6.7,6.7).  The 0.3 inset makes the
    exterior + hole boundaries cut cells such that the three bases are strictly
    nested (verified: |p_core|=48 < |p_centroid|=84 < |p_cover|=96).
    """
    outer = box(0.3, 0.3, 9.7, 9.7)
    hole = box(3.3, 3.3, 6.7, 6.7)
    geom = Polygon(outer.exterior.coords, [list(hole.exterior.coords)])
    return D.classify(geom, 1, _wide_polyfill, _cell_geom)


@pytest.fixture
def point_cls():
    """Point strictly inside cell (5,5); coveras → 1 cover cell, centroid/core ∅."""
    geom = Point(5.5, 5.5)
    return D.classify(geom, 1, lambda g, r: [_cid(5, 5)], _cell_geom)


@pytest.fixture
def line_cls():
    """Horizontal line crossing cells (2,5)..(5,5); coveras → 4 cover cells."""
    geom = LineString([(2.5, 5.5), (5.5, 5.5)])

    def polyfill(g, res):
        minx, miny, maxx, maxy = g.bounds
        return [
            _cid(x, y)
            for x in range(int(minx) - 1, int(maxx) + 2)
            for y in range(int(miny) - 1, int(maxy) + 2)
        ]

    return D.classify(geom, 1, polyfill, _cell_geom)


def _region(cls, coverage, letter):
    """Belongs-to set for region letter ('p'/'s'/'h') under a coverage value."""
    basis = {"coveras": "cover", "polyfill": "centroid", "core": "core"}[coverage]
    return getattr(cls, f"{letter}_{basis}")


# ---------------------------------------------------------------------------
# classify — three bases and their nesting
# ---------------------------------------------------------------------------


def test_classification_exposes_centroid_basis(holed_cls):
    for attr in ("p_centroid", "s_centroid", "h_centroid"):
        assert hasattr(holed_cls, attr), f"Classification missing {attr}"


def test_three_basis_nesting_p_s_h(holed_cls):
    c = holed_cls
    assert c.p_core <= c.p_centroid <= c.p_cover
    assert c.s_core <= c.s_centroid <= c.s_cover
    assert c.h_core <= c.h_centroid <= c.h_cover
    # P has a genuine three-way gap for this fixture.
    assert c.p_core < c.p_centroid < c.p_cover, "expected strict cover⊃centroid⊃core"


# ---------------------------------------------------------------------------
# coverage validation + default
# ---------------------------------------------------------------------------


def test_coverage_default_is_coveras(holed_cls):
    for mode in D.MODES:
        default = D.geom_expand("ring", 2, mode, holed_cls, _neighbors)
        coveras = D.geom_expand("ring", 2, mode, holed_cls, _neighbors, "coveras")
        assert default == coveras, f"mode={mode}: default must equal coveras"


def test_coverage_invalid_raises(holed_cls):
    with pytest.raises(ValueError):
        D.geom_expand("ring", 1, "boundary-out", holed_cls, _neighbors, "nonsense")
    with pytest.raises(ValueError):
        D.mode_setup("boundary-out", holed_cls, _neighbors, coverage="nonsense")


# ---------------------------------------------------------------------------
# boundary-out (LOCKED) — coveras/core k0 = full coveras band; polyfill k0 =
# centroid-out; frontier = full band for all ⇒ outward rings identical.
# ---------------------------------------------------------------------------


def test_boundary_out_k0_coverage_dependent(holed_cls):
    s_cover = frozenset(holed_cls.s_cover)
    full_band = s_cover - frozenset(holed_cls.s_core)
    k0_cov = D.geom_expand("loop", 0, "boundary-out", holed_cls, _neighbors, "coveras")
    k0_core = D.geom_expand("loop", 0, "boundary-out", holed_cls, _neighbors, "core")
    k0_poly = D.geom_expand(
        "loop", 0, "boundary-out", holed_cls, _neighbors, "polyfill"
    )
    assert k0_cov == full_band, "coveras boundary-out k0 = full straddling band"
    assert k0_core == full_band, "core boundary-out k0 = full band (== coveras)"
    assert k0_poly <= k0_cov, "polyfill boundary-out k0 ⊆ coveras k0"


def test_boundary_out_polyfill_is_centroid_out():
    # A disk has a fuzzy 2-ring boundary over the unit grid: an outer ring centroid-OUT
    # and an inner ring centroid-in-but-not-core ⇒ strict s_core ⊂ s_centroid ⊂ s_cover.
    # So polyfill boundary-out k0 is the strict centroid-out subset of the coveras band.
    disk = Point(6.5, 6.5).buffer(5.0)
    cls = D.classify(disk, 1, _wide_polyfill, _cell_geom)
    assert cls.s_core < cls.s_centroid < cls.s_cover, "fixture must nest strictly"
    centroid_out = frozenset(cls.s_cover) - frozenset(cls.s_centroid)
    k0_poly = D.geom_expand("loop", 0, "boundary-out", cls, _neighbors, "polyfill")
    k0_cov = D.geom_expand("loop", 0, "boundary-out", cls, _neighbors, "coveras")
    assert k0_poly == centroid_out, "polyfill boundary-out k0 = centroid-out cells"
    assert k0_poly < k0_cov, "polyfill k0 is a strict subset of the coveras full band"


def test_boundary_out_outward_identical_across_coverage(holed_cls):
    # Frontier = full coveras band for every coverage ⇒ the outward rings (k>=1) are
    # byte-identical; only k0 differs.  Guards the "no gaps at k=1 for polyfill" fix.
    for k in (1, 2, 3):
        rings = {
            cov: D.geom_expand("loop", k, "boundary-out", holed_cls, _neighbors, cov)
            for cov in D.COVERAGE
        }
        assert (
            rings["coveras"] == rings["polyfill"] == rings["core"]
        ), f"boundary-out k={k} outward ring must be identical across coverages"


def test_boundary_out_excludes_interior_every_coverage(holed_cls):
    # The geom INTERIOR (fully-contained s_core) is never returned by boundary-out.
    for cov in D.COVERAGE:
        r = D.geom_expand("ring", 3, "boundary-out", holed_cls, _neighbors, cov)
        assert r.isdisjoint(
            holed_cls.s_core
        ), f"boundary-out leaked interior (s_core) under coverage={cov}"


# ---------------------------------------------------------------------------
# boundary-in / ignore-holes — seed = outer perimeter of region S under coverage
# ---------------------------------------------------------------------------


def test_boundary_in_k0_is_ring_intersect_region_every_coverage(holed_cls):
    # LOCKED: boundary-* seed from the coveras outer ring op; k0 = op ∩ region_X
    # (so core drops the straddling ring when it has no core cells).
    op = D.outer_perimeter(holed_cls.s_cover, _neighbors)
    for cov in D.COVERAGE:
        p_x = frozenset(_region(holed_cls, cov, "p"))
        s_x = frozenset(_region(holed_cls, cov, "s"))
        k0_in = D.geom_expand("loop", 0, "boundary-in", holed_cls, _neighbors, cov)
        k0_ign = D.geom_expand(
            "loop", 0, "boundary-in-ignore-holes", holed_cls, _neighbors, cov
        )
        assert k0_in == (op & p_x), f"boundary-in k0 must equal op ∩ P_{cov}"
        assert k0_ign == (op & s_x), f"ignore-holes k0 must equal op ∩ S_{cov}"


def test_boundary_in_respects_holes_coveras(holed_cls):
    r = D.geom_expand("ring", 3, "boundary-in", holed_cls, _neighbors, "coveras")
    assert r.isdisjoint(holed_cls.h_core), "boundary-in must not enter the hole core"


# ---------------------------------------------------------------------------
# 3 coverage × 6 modes matrix — no crash + result within region envelope
# ---------------------------------------------------------------------------


def test_coverage_mode_matrix_envelopes(holed_cls):
    for cov in D.COVERAGE:
        region_p = _region(holed_cls, cov, "p")
        region_s = _region(holed_cls, cov, "s")
        region_h = _region(holed_cls, cov, "h")
        for mode in D.MODES:
            r = D.geom_expand("ring", 2, mode, holed_cls, _neighbors, cov)
            # loop(k) == ring(k) − ring(k−1) invariant holds for every cell/mode.
            r3 = D.geom_expand("ring", 3, mode, holed_cls, _neighbors, cov)
            r2 = D.geom_expand("ring", 2, mode, holed_cls, _neighbors, cov)
            loop3 = D.geom_expand("loop", 3, mode, holed_cls, _neighbors, cov)
            assert loop3 == (r3 - r2), f"cov={cov} mode={mode}: loop != ring diff"
            # per-mode envelope
            if mode == "boundary-out":
                # interior (fully-contained s_core) excluded
                assert r.isdisjoint(
                    holed_cls.s_core
                ), f"cov={cov} boundary-out leaked interior"
            elif mode == "boundary-in":
                assert r <= region_p, f"cov={cov} boundary-in escaped region P"
            elif mode == "boundary-in-ignore-holes":
                assert r <= region_s, f"cov={cov} ignore-holes escaped region S"
            elif mode == "hole-in":
                assert r <= region_h, f"cov={cov} hole-in escaped region H"
            elif mode == "hole-out":
                assert r <= region_p, f"cov={cov} hole-out escaped region P"


# ---------------------------------------------------------------------------
# Point / line under each coverage
# ---------------------------------------------------------------------------


def test_point_coveras_supported(point_cls):
    # cover basis: exactly the containing cell
    assert len(point_cls.p_cover) == 1
    # boundary-in k0 = perimeter of s_cover = the single covering cell (it has
    # outside neighbours) → the covering cell is retrievable under coveras.
    k0_in = D.geom_expand("loop", 0, "boundary-in", point_cls, _neighbors, "coveras")
    assert (
        k0_in == point_cls.p_cover
    ), "point covering cell must appear (boundary-in k0)"
    # boundary-out k0 = the point's covering cell (boundary ring == that cell);
    # the k=1 loop is the outward ring, EXCLUDING the covering cell.
    k0_out = D.geom_expand("loop", 0, "boundary-out", point_cls, _neighbors, "coveras")
    assert k0_out == point_cls.p_cover, "boundary-out k0 = the point's covering cell"
    loop1 = D.geom_expand("loop", 1, "boundary-out", point_cls, _neighbors, "coveras")
    assert loop1, "boundary-out k=1 loop on a point must be a non-empty outward ring"
    assert loop1.isdisjoint(point_cls.p_cover), "outward ring excludes the point's cell"


def test_point_polyfill_core_boundary_out_nonempty_rest_empty(point_cls):
    # A point has no 2D interior (p_centroid = p_core = ∅).  boundary-out's frontier is
    # the coveras band (the point's cell — all-boundary, never core), so boundary-out is
    # non-empty (cell + outward) under EVERY coverage; the belonging-inward modes are empty.
    assert point_cls.p_centroid == set()
    assert point_cls.p_core == set()
    for cov in ("polyfill", "core"):
        bo = D.geom_expand("ring", 2, "boundary-out", point_cls, _neighbors, cov)
        assert bo, f"point boundary-out cov={cov} must be non-empty (cell + outward)"
        assert point_cls.p_cover <= bo, "the point's cell is present at k0"
        for mode in (m for m in D.MODES if m != "boundary-out"):
            r = D.geom_expand("ring", 2, mode, point_cls, _neighbors, cov)
            assert r == set(), f"point coverage={cov} mode={mode} must be empty"


def test_line_coveras_supported(line_cls):
    assert (
        len(line_cls.p_cover) == 4
    ), f"line should cross 4 cells; got {len(line_cls.p_cover)}"
    k0_in = D.geom_expand("loop", 0, "boundary-in", line_cls, _neighbors, "coveras")
    assert k0_in == line_cls.p_cover, "all crossed cells must appear (boundary-in k0)"
    # boundary-out k0 = the line's crossed cells; the k=1 loop is the outward band.
    k0_out = D.geom_expand("loop", 0, "boundary-out", line_cls, _neighbors, "coveras")
    assert k0_out == line_cls.p_cover, "boundary-out k0 = the line's crossed cells"
    loop1 = D.geom_expand("loop", 1, "boundary-out", line_cls, _neighbors, "coveras")
    assert loop1, "boundary-out k=1 loop on a line must be a non-empty outward band"
    assert loop1.isdisjoint(line_cls.p_cover), "outward band excludes the line's cells"


def test_line_polyfill_core_boundary_out_nonempty_rest_empty(line_cls):
    # Like a point: a line has no 2D interior; boundary-out's coveras frontier is the
    # crossed cells (all-boundary), so boundary-out is non-empty under every coverage;
    # the belonging-inward modes are empty.
    assert line_cls.p_centroid == set()
    assert line_cls.p_core == set()
    for cov in ("polyfill", "core"):
        bo = D.geom_expand("ring", 2, "boundary-out", line_cls, _neighbors, cov)
        assert bo, f"line boundary-out cov={cov} must be non-empty (cells + outward)"
        assert line_cls.p_cover <= bo, "the line's crossed cells are present at k0"
        for mode in (m for m in D.MODES if m != "boundary-out"):
            r = D.geom_expand("ring", 2, mode, line_cls, _neighbors, cov)
            assert r == set(), f"line coverage={cov} mode={mode} must be empty"


# ===========================================================================
# Grid-level: custom + quadbin (2 real grids)
# ===========================================================================

from shapely import to_wkb  # noqa: E402

from databricks.labs.gbx.pygx import _custom, _quadbin  # noqa: E402
from databricks.labs.gbx.pygx._custom import CustomGridConf  # noqa: E402


def _custom_conf():
    return CustomGridConf(
        bound_x_min=0,
        bound_x_max=1_000_000,
        bound_y_min=0,
        bound_y_max=1_000_000,
        cell_splits=2,
        root_cell_size_x=1000,
        root_cell_size_y=1000,
        srid=-1,
    )


_CUSTOM_GEOM = box(530000, 180000, 535000, 185000)
_CUSTOM_RES = 0
_QUADBIN_GEOM = box(-73.99, 40.71, -73.95, 40.75)
_QUADBIN_RES = 12


# ---- custom ----------------------------------------------------------------


def test_custom_coverage_default_is_coveras():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    for mode in D.MODES:
        default = set(_custom.geometry_k_ring(conf, g, _CUSTOM_RES, 1, mode=mode))
        coveras = set(
            _custom.geometry_k_ring(
                conf, g, _CUSTOM_RES, 1, mode=mode, coverage="coveras"
            )
        )
        assert default == coveras, f"custom mode={mode}: default != coveras"


def test_custom_boundary_out_k0_coverage():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))

    def k0(cov):
        return set(
            _custom.geometry_k_ring(
                conf, g, _CUSTOM_RES, 0, mode="boundary-out", coverage=cov
            )
        )

    assert k0("coveras") == k0("core"), "coveras & core boundary-out k0 identical"
    assert k0("polyfill") <= k0("coveras"), "polyfill boundary-out k0 ⊆ coveras k0"
    assert k0("coveras"), "coveras boundary-out k0 must be non-empty"


def test_custom_boundary_out_outward_identical_across_coverage():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    for k in (1, 2):
        rings = {
            cov: set(
                _custom.geometry_k_loop(
                    conf, g, _CUSTOM_RES, k, mode="boundary-out", coverage=cov
                )
            )
            for cov in D.COVERAGE
        }
        assert (
            rings["coveras"] == rings["polyfill"] == rings["core"]
        ), f"custom boundary-out k={k} outward ring must be identical across coverages"


def test_custom_matrix_no_crash_all_coverage():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    for cov in D.COVERAGE:
        for mode in D.MODES:
            cells = _custom.geometry_k_ring(
                conf, g, _CUSTOM_RES, 2, mode=mode, coverage=cov
            )
            assert all(isinstance(c, int) for c in cells)


def test_custom_point_coverage():
    conf = _custom_conf()
    pt = bytes(to_wkb(Point(530500, 180500)))
    # boundary-out on a point = its cell + outward, coverage-independent (the cell is
    # all-boundary / never core), so non-empty under every coverage.  The belonging-
    # inward modes have no centroid-in / contained cell → empty under polyfill/core.
    for cov in D.COVERAGE:
        assert _custom.geometry_k_ring(
            conf, pt, _CUSTOM_RES, 1, mode="boundary-out", coverage=cov
        ), f"custom point boundary-out cov={cov} must be non-empty"
    for cov in ("polyfill", "core"):
        for mode in (m for m in D.MODES if m != "boundary-out"):
            assert (
                _custom.geometry_k_ring(
                    conf, pt, _CUSTOM_RES, 2, mode=mode, coverage=cov
                )
                == []
            ), f"custom point coverage={cov} mode={mode} must be empty"


# ---- quadbin ---------------------------------------------------------------


def test_quadbin_coverage_default_is_coveras():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    for mode in D.MODES:
        default = set(_quadbin.geometry_k_ring(g, _QUADBIN_RES, 1, mode=mode))
        coveras = set(
            _quadbin.geometry_k_ring(g, _QUADBIN_RES, 1, mode=mode, coverage="coveras")
        )
        assert default == coveras, f"quadbin mode={mode}: default != coveras"


def test_quadbin_boundary_out_k0_coverage():
    g = bytes(to_wkb(_QUADBIN_GEOM))

    def k0(cov):
        return set(
            _quadbin.geometry_k_ring(
                g, _QUADBIN_RES, 0, mode="boundary-out", coverage=cov
            )
        )

    assert k0("coveras") == k0("core"), "coveras & core boundary-out k0 identical"
    assert k0("polyfill") <= k0("coveras"), "polyfill boundary-out k0 ⊆ coveras k0"
    assert k0("coveras"), "coveras boundary-out k0 must be non-empty"


def test_quadbin_boundary_out_outward_identical_across_coverage():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    for k in (1, 2):
        rings = {
            cov: set(
                _quadbin.geometry_k_loop(
                    g, _QUADBIN_RES, k, mode="boundary-out", coverage=cov
                )
            )
            for cov in D.COVERAGE
        }
        assert (
            rings["coveras"] == rings["polyfill"] == rings["core"]
        ), f"quadbin boundary-out k={k} outward ring identical across coverages"


def test_quadbin_matrix_no_crash_all_coverage():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    for cov in D.COVERAGE:
        for mode in D.MODES:
            cells = _quadbin.geometry_k_ring(
                g, _QUADBIN_RES, 2, mode=mode, coverage=cov
            )
            assert all(isinstance(c, int) for c in cells)
