import json

import pytest
from shapely import from_wkb, from_wkt, get_srid, is_ccw, is_valid, set_srid

from databricks.labs.gbx.pyvx import _validity as v

# GEOS-verified corpus (shapely 2.1.2 / GEOS 3.13.1):
# reason strings embed location as "Reason[x y]".
BOWTIE = "POLYGON((0 0,1 1,1 0,0 1,0 0))"  # Self-intersection[0.5 0.5]
HOLE_OUT = (
    "POLYGON((0 0,0 3,3 3,3 0,0 0),(5 5,5 6,6 5,5 5))"  # Hole lies outside shell[5 5]
)
NESTED = (
    "POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1),(2 2,2 8,8 8,8 2,2 2))"
)
#  ^ Holes are nested[2 2]
TOOFEW = "POLYGON((0 0,1 1,0 0))"  # Too few points in geometry component[0 0]
VALID = "POLYGON((0 0,0 1,1 1,1 0,0 0))"  # Valid Geometry

# Extended corpus — full GEOS single-geometry validity-reason taxonomy.
#
# Reason codes mapped in _REASON_CODE but NOT triggerable via from_wkt with GEOS 3.13.1:
#   "Invalid coordinate"  (code 2): WKT parser rejects NaN/Inf coordinates.
#   "Ring not closed"     (code 3): shapely's from_wkt auto-closes all rings.
#   "Repeated point"      (code 4): GEOS 3.13.1 treats consecutive duplicate
#       ring points as Valid Geometry (spike row 5); no reason string emitted.
#   "Duplicate rings"     (code 24): two identical holes in a polygon produce
#       overlapping edges; GEOS 3.13.1 detects the edge overlap as
#       "Self-intersection" before the duplicate-ring check fires.
#       Mapping kept; not testable via a plain polygon+duplicate-holes WKT.

RING_SELF_INTERSECT = (
    "POLYGON((0 0,2 2,4 0,4 4,2 2,0 4,0 0))"  # Ring Self-intersection[2 2]
)
INTERIOR_DISCONNECTED = (
    "POLYGON((0 0,0 10,10 10,10 0,0 0),(0 5,5 10,10 5,5 0,0 5))"
    # Interior is disconnected[10 5]: diamond hole touches all 4 sides of the
    # exterior at their midpoints, splitting the interior into 4 disconnected
    # triangular corner regions.
)
NESTED_SHELLS = (
    "MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0)),((2 2,2 8,8 8,8 2,2 2)))"
    # Nested shells: second polygon fully inside first, no hole relationship.
)
DUPLICATE_HOLES_WKT = (
    "POLYGON((0 0,0 10,10 10,10 0,0 0),(2 2,2 8,8 8,8 2,2 2),(2 2,2 8,8 8,8 2,2 2))"
    # Two identical holes: GEOS 3.13.1 reports "Self-intersection" (overlapping
    # edges detected first) rather than "Duplicate rings".  Retained as a corpus
    # fixture; the "Duplicate rings" (code 24) entry in _REASON_CODE is mapped for
    # completeness but not directly triggerable via from_wkt with GEOS 3.13.1.
)

# Orientation corpus (spike finding: product enforces OGC ring-orientation rule;
# both rings CW → product-invalid, Shapely-valid).
SAME_WINDING = "POLYGON((0 0,0 10,10 10,10 0,0 0),(2 2,2 4,4 4,4 2,2 2))"


# ---------------------------------------------------------------------------
# st_makevalid — basic repair behaviour
# ---------------------------------------------------------------------------


def test_makevalid_fixes_self_intersection():
    out = v.st_makevalid(from_wkt(BOWTIE).wkb)
    g = from_wkb(out)
    assert is_valid(g)
    assert g.geom_type in ("MultiPolygon", "Polygon", "GeometryCollection")


def test_makevalid_default_level_is_linework():
    # default equals explicit linework (both apply orient_polygons)
    assert v.st_makevalid(from_wkt(BOWTIE).wkb) == v.st_makevalid(
        from_wkt(BOWTIE).wkb, "linework"
    )


def test_makevalid_structure_level():
    out = v.st_makevalid(from_wkt(BOWTIE).wkb, "structure")
    assert is_valid(from_wkb(out))


def test_makevalid_rejects_full_level():
    with pytest.raises(ValueError, match="level"):
        v.st_makevalid(from_wkt(BOWTIE).wkb, "full")


def test_makevalid_preserves_srid():
    g = set_srid(from_wkt(BOWTIE), 4326)
    out = v.st_makevalid(v_to_ewkb(g))
    assert get_srid(from_wkb(out)) == 4326


def v_to_ewkb(g):
    from shapely import to_wkb

    return to_wkb(g, include_srid=True)


def test_makevalid_accepts_wkt_input():
    out = v.st_makevalid(BOWTIE)  # WKT string in
    assert is_valid(from_wkb(out))


def test_makevalid_null_on_unparseable():
    assert v.st_makevalid(None) is None


# ---------------------------------------------------------------------------
# st_makevalid — orientation (product-validity gate, Change 1)
# ---------------------------------------------------------------------------


def test_makevalid_default_orients_to_opposite_winding():
    """Default (linework) applies orient_polygons(exterior_cw=False).

    SAME_WINDING has both rings CW (Shapely-valid, product-invalid per spike).
    After st_makevalid default: exterior becomes CCW, hole remains CW →
    opposite winding → passes the product's ST_IsValid ring-orientation rule.
    """
    inp = from_wkt(SAME_WINDING)
    out = from_wkb(v.st_makevalid(inp.wkb))
    # exterior CCW (is_ccw True) vs hole CW (is_ccw False) → opposite
    assert is_ccw(out.exterior) != is_ccw(out.interiors[0])


def test_makevalid_ogc_does_not_orient():
    """ogc level applies topology repair only — does NOT force opposite winding.

    SAME_WINDING is already Shapely-valid, so make_valid returns it unchanged.
    default applies orient_polygons (exterior CCW); ogc preserves original CW.
    """
    inp = from_wkt(SAME_WINDING)
    wkb = inp.wkb
    default_out = from_wkb(v.st_makevalid(wkb))
    ogc_out = from_wkb(v.st_makevalid(wkb, "ogc"))
    # default reorients (exterior CCW is_ccw=True);
    # ogc preserves original (exterior CW is_ccw=False)
    assert is_ccw(default_out.exterior) != is_ccw(ogc_out.exterior)


# ---------------------------------------------------------------------------
# explain_validity — full GEOS single-geometry reason taxonomy (Change 3)
# ---------------------------------------------------------------------------


def test_explain_valid_geometry():
    d = json.loads(v.explain_validity(VALID))
    assert d["valid"] is True
    assert d["reason"] == "Valid Geometry"
    assert d["code"] == 0
    assert d["location"] is None


def test_explain_self_intersection_has_code_and_location():
    d = json.loads(v.explain_validity(BOWTIE))
    assert d["valid"] is False
    assert d["reason"].startswith("Self-intersection")
    assert d["code"] == 10
    assert d["location"] == "POINT(0.5 0.5)"


def test_explain_ring_self_intersection():
    # POLYGON self-touching at a vertex; GEOS reports Ring Self-intersection.
    d = json.loads(v.explain_validity(RING_SELF_INTERSECT))
    assert d["valid"] is False
    assert d["reason"].startswith("Ring Self-intersection")
    assert d["code"] == 11
    assert d["location"] == "POINT(2 2)"


def test_explain_hole_outside_shell():
    d = json.loads(v.explain_validity(HOLE_OUT))
    assert d["valid"] is False
    assert d["reason"].startswith("Hole lies outside shell")
    assert d["code"] == 20
    assert d["location"] == "POINT(5 5)"


def test_explain_holes_are_nested():
    d = json.loads(v.explain_validity(NESTED))
    assert d["valid"] is False
    assert d["reason"].startswith("Holes are nested")
    assert d["code"] == 21
    assert d["location"] == "POINT(2 2)"


def test_explain_interior_disconnected():
    # Diamond hole touching all 4 exterior midpoints splits the interior into
    # 4 disconnected corner regions.  GEOS reports the first junction point.
    d = json.loads(v.explain_validity(INTERIOR_DISCONNECTED))
    assert d["valid"] is False
    assert d["reason"].startswith("Interior is disconnected")
    assert d["code"] == 22
    assert d["location"] is not None


def test_explain_nested_shells():
    # MultiPolygon whose second shell is entirely inside the first shell.
    d = json.loads(v.explain_validity(NESTED_SHELLS))
    assert d["valid"] is False
    assert d["reason"].startswith("Nested shells")
    assert d["code"] == 23
    assert d["location"] is not None


def test_explain_duplicate_holes_actual_geos_reason():
    # Two identical holes: GEOS 3.13.1 reports "Self-intersection" (not
    # "Duplicate rings") because the overlapping ring edges are found first.
    # Confirms the geometry IS invalid and the JSON is well-formed.
    d = json.loads(v.explain_validity(DUPLICATE_HOLES_WKT))
    assert d["valid"] is False
    assert d["reason"].startswith("Self-intersection")
    assert d["code"] == 10  # Self-intersection bucket
    assert d["location"] is not None


def test_explain_toofew_maps_structural_code():
    # "Too few points in geometry component" (code 1).
    # GEOS 3.13.1 embeds a location in this reason string; the test asserts
    # code maps correctly and the JSON is well-formed.
    d = json.loads(v.explain_validity(TOOFEW))
    assert d["valid"] is False
    assert d["reason"].startswith("Too few points in geometry component")
    assert d["code"] == 1


def test_explain_null_on_unparseable():
    assert v.explain_validity(None) is None
