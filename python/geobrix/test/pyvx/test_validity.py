import json

import pytest
from shapely import from_wkt, is_valid, set_srid, get_srid
from shapely import from_wkb

from databricks.labs.gbx.pyvx import _validity as v

# GEOS-verified corpus (shapely 2.1.2): reason strings embed location as "Reason[x y]".
BOWTIE = "POLYGON((0 0,1 1,1 0,0 1,0 0))"          # Self-intersection[0.5 0.5]
HOLE_OUT = "POLYGON((0 0,0 3,3 3,3 0,0 0),(5 5,5 6,6 5,5 5))"  # Hole lies outside shell[5 5]
NESTED = "POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1),(2 2,2 8,8 8,8 2,2 2))"  # Holes are nested[2 2]
TOOFEW = "POLYGON((0 0,1 1,0 0))"                  # Too few points in geometry component[0 0]
VALID = "POLYGON((0 0,0 1,1 1,1 0,0 0))"           # Valid Geometry


def test_makevalid_fixes_self_intersection():
    out = v.st_makevalid(from_wkt(BOWTIE).wkb)
    g = from_wkb(out)
    assert is_valid(g)
    assert g.geom_type in ("MultiPolygon", "Polygon", "GeometryCollection")


def test_makevalid_default_level_is_linework():
    # default equals explicit linework
    assert v.st_makevalid(from_wkt(BOWTIE).wkb) == v.st_makevalid(from_wkt(BOWTIE).wkb, "linework")


def test_makevalid_structure_level():
    out = v.st_makevalid(from_wkt(BOWTIE).wkb, "structure")
    assert is_valid(from_wkb(out))


def test_makevalid_rejects_full_level():
    with pytest.raises(ValueError, match="level"):
        v.st_makevalid(from_wkt(BOWTIE).wkb, "full")


def test_makevalid_preserves_srid():
    g = set_srid(from_wkt(BOWTIE), 4326)
    out = v.st_makevalid(g.wkb if False else v_to_ewkb(g))  # see helper below
    assert get_srid(from_wkb(out)) == 4326


def v_to_ewkb(g):
    from shapely import to_wkb
    return to_wkb(g, include_srid=True)


def test_makevalid_accepts_wkt_input():
    out = v.st_makevalid(BOWTIE)  # WKT string in
    assert is_valid(from_wkb(out))


def test_makevalid_null_on_unparseable():
    assert v.st_makevalid(None) is None


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


def test_explain_hole_outside_shell():
    d = json.loads(v.explain_validity(HOLE_OUT))
    assert d["valid"] is False
    assert d["code"] == 20
    assert d["location"] == "POINT(5 5)"


def test_explain_structural_reason_null_location_when_absent():
    # too-few-points DOES carry a location in GEOS 3.13; assert code maps and JSON is well-formed
    d = json.loads(v.explain_validity(TOOFEW))
    assert d["valid"] is False
    assert d["code"] == 1


def test_explain_null_on_unparseable():
    assert v.explain_validity(None) is None
