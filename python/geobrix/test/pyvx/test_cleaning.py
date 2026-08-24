from shapely import from_wkb, from_wkt, get_srid, is_valid, set_srid, to_wkb

from databricks.labs.gbx.pyvx import _cleaning as c


def _ewkb(wkt, srid=0):
    g = from_wkt(wkt)
    if srid:
        g = set_srid(g, srid)
    return to_wkb(g, include_srid=True)


def test_simplifypreservetopology_drops_collinear_and_stays_valid():
    # near-collinear extra vertex on the left edge
    wkt = "POLYGON((0 0,0 5,0.001 8,0 10,10 10,10 0,0 0))"
    out = from_wkb(c.st_simplifypreservetopology(wkt, 1.0))
    assert is_valid(out)
    assert out.geom_type == "Polygon"  # topology preserved (not collapsed/split)
    assert len(out.exterior.coords) < 7  # a vertex was dropped


def test_simplifypreservetopology_null_on_unparseable():
    assert c.st_simplifypreservetopology(None, 1.0) is None


def test_removerepeatedpoints_default_removes_exact_duplicates():
    out = from_wkb(c.st_removerepeatedpoints("LINESTRING(0 0,0 0,1 1,1 1,2 2)"))
    assert list(out.coords) == [(0, 0), (1, 1), (2, 2)]


def test_removerepeatedpoints_tolerance_removes_near_duplicates():
    out = from_wkb(c.st_removerepeatedpoints("LINESTRING(0 0,0.1 0,5 5)", 0.5))
    assert len(out.coords) == 2  # (0 0) and (0.1 0) collapse within tol


def test_reduceprecision_snaps_to_grid():
    out = from_wkb(c.st_reduceprecision("POINT(1.234 5.678)", 1.0))
    assert list(out.coords) == [(1.0, 6.0)]


def test_node_splits_self_intersecting_linework():
    # a figure-eight linestring nodes into multiple parts at the crossing
    out = from_wkb(c.st_node("LINESTRING(0 0,10 10,0 10,10 0)"))
    assert out.geom_type in ("MultiLineString", "LineString")
    assert is_valid(out)


def test_snap_aligns_near_miss_vertex_to_reference():
    ref = "LINESTRING(0 0,10 0)"
    out = from_wkb(c.st_snap("LINESTRING(0 0.4,10 0.4)", ref, 0.5))
    # the near-miss endpoints snap onto the reference within tolerance
    assert any(abs(y) < 1e-9 for _, y in out.coords)


def test_snap_null_on_unparseable():
    assert c.st_snap(None, "LINESTRING(0 0,1 1)", 0.5) is None
    assert c.st_snap("LINESTRING(0 0,1 1)", None, 0.5) is None


def test_all_preserve_srid():
    e = _ewkb("POLYGON((0 0,0 5,0.001 8,0 10,10 10,10 0,0 0))", 4326)
    assert get_srid(from_wkb(c.st_simplifypreservetopology(e, 1.0))) == 4326
    assert get_srid(from_wkb(c.st_reduceprecision(e, 1.0))) == 4326
    assert get_srid(from_wkb(c.st_removerepeatedpoints(e))) == 4326


def test_wkt_input_accepted():
    assert c.st_reduceprecision("POINT(1.234 5.678)", 1.0) is not None
