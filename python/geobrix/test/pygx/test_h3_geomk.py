"""Unit tests for pygx h3 geometry-aware kring/kloop — geom-taking API.

Tests the geom-taking ``_h3.geom_expand(kind, geom, resolution, k, mode)``
engine and the ``_h3_geomkring``/``_h3_geomkloop`` UDF wrappers, constructing
input geometries with shapely and passing WKB bytes or WKT strings directly.

The h3 library performs both the polyfill (polygon_to_cells_experimental) and
the neighbour walk (grid_disk), so all tests run locally with no Databricks
session required.

Run locally (no Databricks):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_geomk.py

Integration (cross-check with product h3_* SQL) requires a Databricks session:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_geomk.py \\
        --with-integration
"""

import pytest
from shapely import to_wkb, to_wkt
from shapely.geometry import Polygon, box

from databricks.labs.gbx.pygx import _h3 as _h3mod

# ---------------------------------------------------------------------------
# Module-level geometry fixtures
# ---------------------------------------------------------------------------

# Small NYC area box (no holes) — used for basic ring/loop and holeless tests.
_NYC_BOX = box(-73.99, 40.71, -73.95, 40.75)
_NYC_WKB = to_wkb(_NYC_BOX)
_NYC_WKT = to_wkt(_NYC_BOX)
_RES = 9  # H3 res-9, edge ~0.17 km, suits the ~4 km NYC box

# Large donut polygon: outer 8°×8°, inner hole 4°×4° centred around NYC.
# At res-5 (edge ~61 km), both the solid ring and the hole contain multiple cells.
_OUTER = Polygon(
    [(-78.0, 36.0), (-70.0, 36.0), (-70.0, 44.0), (-78.0, 44.0), (-78.0, 36.0)]
)
_HOLE = Polygon(
    [(-76.0, 38.0), (-72.0, 38.0), (-72.0, 42.0), (-76.0, 42.0), (-76.0, 38.0)]
)
_DONUT = _OUTER.difference(_HOLE)
_DONUT_WKB = to_wkb(_DONUT)
_COARSE_RES = 5  # edge ~61 km, suits the large polygon/hole


# ---------------------------------------------------------------------------
# boundary-out k-ring monotonicity
# ---------------------------------------------------------------------------


def test_h3_geomkring_boundary_out_k1_superset_of_k0():
    """boundary-out k=1 ⊃ k=0 and strictly expands.

    ring(k=0) = the initial covering set; ring(k=1) expands by one dilation
    step outward.  For any real polygon the frontier is non-empty so k=1 must
    be strictly larger.
    """
    r0 = _h3mod.geom_expand("ring", _NYC_WKB, _RES, 0, "boundary-out")
    r1 = _h3mod.geom_expand("ring", _NYC_WKB, _RES, 1, "boundary-out")
    assert r0 <= r1, "ring k=1 must be a superset of ring k=0"
    assert len(r1) > len(r0), "ring k=1 must be strictly larger than ring k=0"
    assert all(isinstance(c, int) for c in r1), "All cell IDs must be Python int"


# ---------------------------------------------------------------------------
# kloop hollowness
# ---------------------------------------------------------------------------


def test_h3_geomkloop_k1_is_hollow():
    """kloop k=1 ⊆ kring k=1 and is disjoint from kring k=0 (hollow shell).

    kloop = cells first reached at exactly step k (the outer shell).  It must
    be a subset of kring k=1 and have no overlap with kring k=0 (the interior).
    """
    r0 = _h3mod.geom_expand("ring", _NYC_WKB, _RES, 0, "boundary-out")
    r1 = _h3mod.geom_expand("ring", _NYC_WKB, _RES, 1, "boundary-out")
    l1 = _h3mod.geom_expand("loop", _NYC_WKB, _RES, 1, "boundary-out")
    assert l1 <= r1, "kloop k=1 must be a subset of kring k=1"
    assert l1.isdisjoint(r0), "kloop k=1 must be disjoint from kring k=0"
    assert len(l1) > 0, "kloop k=1 must be non-empty"


# ---------------------------------------------------------------------------
# Holed polygon — hole-in and hole-out modes
# ---------------------------------------------------------------------------


def test_h3_geomkring_holed_polygon_hole_in_nonempty():
    """hole-in returns cells inside the hole of a holed polygon.

    hole-in grows from h_border (cells on the hole boundary) into h_core (cells
    fully inside the hole).  The 4°×4° hole at res-5 contains many cells.
    """
    result = _h3mod.geom_expand("ring", _DONUT_WKB, _COARSE_RES, 1, "hole-in")
    assert (
        len(result) > 0
    ), f"hole-in on holed polygon should return cells inside the hole; got {result}"
    assert all(isinstance(c, int) for c in result), "All cell IDs must be Python int"


def test_h3_geomkring_holed_polygon_hole_out_nonempty():
    """hole-out returns cells expanding outward from the hole boundary.

    hole-out grows from h_border into p_core (the solid polygon interior).
    With a 4°×4° hole in an 8°×8° outer ring at res-5 there are many solid
    interior cells adjacent to the hole boundary.
    """
    result = _h3mod.geom_expand("ring", _DONUT_WKB, _COARSE_RES, 1, "hole-out")
    assert (
        len(result) > 0
    ), f"hole-out on holed polygon should return cells in the solid ring; got {result}"


# ---------------------------------------------------------------------------
# Holeless polygon — hole-in must be empty
# ---------------------------------------------------------------------------


def test_h3_geomkring_holeless_hole_in_is_empty():
    """hole-in on a polygon with no holes returns an empty set.

    No interior rings → h_shapes is empty → h_cover = h_core = ∅ →
    h_border = ∅ → frontier0 = ∅ → result = ∅ for all k.
    """
    result = _h3mod.geom_expand("ring", _NYC_WKB, _RES, 1, "hole-in")
    assert (
        result == set()
    ), f"holeless polygon hole-in should be empty; got {len(result)} cells"


# ---------------------------------------------------------------------------
# boundary-in-ignore-holes vs boundary-in
# ---------------------------------------------------------------------------


def test_h3_geomkring_boundary_in_ignore_holes_gte_boundary_in():
    """boundary-in-ignore-holes ⊇ boundary-in on a holed polygon.

    boundary-in admits cells in p_core (fully inside the donut).
    boundary-in-ignore-holes admits cells in s_core (fully inside the filled
    solid polygon), which is a superset of p_core — it includes cells in the
    hole area that the donut excludes.  So boundary-in-ignore-holes >= boundary-in.
    """
    bi = _h3mod.geom_expand("ring", _DONUT_WKB, _COARSE_RES, 1, "boundary-in")
    bigh = _h3mod.geom_expand(
        "ring", _DONUT_WKB, _COARSE_RES, 1, "boundary-in-ignore-holes"
    )
    assert (
        bigh >= bi
    ), f"boundary-in-ignore-holes ({len(bigh)}) must be >= boundary-in ({len(bi)})"


# ---------------------------------------------------------------------------
# Input format: WKT string
# ---------------------------------------------------------------------------


def test_h3_geomkring_wkt_input_works():
    """WKT string input produces the same result as WKB bytes input."""
    result_wkt = _h3mod.geom_expand("ring", _NYC_WKT, _RES, 1, "boundary-out")
    result_wkb = _h3mod.geom_expand("ring", _NYC_WKB, _RES, 1, "boundary-out")
    assert result_wkt == result_wkb, "WKT and WKB inputs must produce identical results"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_h3_geomkring_bad_mode_raises():
    """Unknown mode raises ValueError (from _dilate.mode_setup)."""
    with pytest.raises(ValueError, match="unknown mode"):
        _h3mod.geom_expand("ring", _NYC_WKB, _RES, 1, "BOGUS")


def test_h3_geomkring_none_geom_returns_none():
    """None geom returns None (NULL propagation) via the _h3_geomkring UDF wrapper."""
    from databricks.labs.gbx.pygx.functions import _h3_geomkring  # noqa: PLC0415

    assert _h3_geomkring(None, _RES, 1) is None


# ---------------------------------------------------------------------------
# Integration tests (require Databricks + product h3_* SQL functions)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_h3_geomkring_boundary_out_expands_beyond_cover(spark):
    """Cross-check: gbx_h3_geomkring boundary-out k=1 genuinely expands beyond cover.

    Passes the NYC box WKB as a column; the geom-taking UDF polyfills and
    dilates entirely via the h3 library.

    Asserts:
      1. result >= covering set (no cells lost)
      2. result > covering set (expansion happened)
    """
    from databricks.labs.gbx import manager as gbx_manager

    pytest.skip(
        "Integration test: requires a Databricks session with gbx_h3_geomkring "
        "registered via gx.register(spark). Run with --with-integration."
    )

    from databricks.labs.gbx.pygx.functions import _h3_geomkring

    gbx_manager.register_family(
        spark, ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"), level="L3"
    )

    geom_wkb = bytes(_NYC_WKB)
    r0 = set(_h3mod.geom_expand("ring", geom_wkb, _RES, 0, "boundary-out"))
    r1_udf = _h3_geomkring(geom_wkb, _RES, 1, "boundary-out") or []
    r1 = set(r1_udf)

    assert (
        r1 >= r0
    ), f"boundary-out result must contain all k=0 cells; missing: {r0 - r1}"
    assert len(r1) > len(
        r0
    ), "boundary-out k=1 must add cells beyond the k=0 covering set"
