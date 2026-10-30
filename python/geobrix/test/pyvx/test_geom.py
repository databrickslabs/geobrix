import pytest

shapely = pytest.importorskip("shapely")
from shapely import get_srid, set_srid, to_wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx._geom import parse_geom  # noqa: E402


def test_parse_geom_wkb_point():
    pt = Point(1, 2)
    g = parse_geom(to_wkb(pt))
    assert g.geom_type == "Point"
    assert (g.x, g.y) == (1.0, 2.0)


def test_parse_geom_wkt_point():
    g = parse_geom("POINT (3 4)")
    assert g.geom_type == "Point"
    assert (g.x, g.y) == (3.0, 4.0)


def test_parse_geom_ewkt_srid():
    g = parse_geom("SRID=4326;POINT(1 2)")
    assert g.geom_type == "Point"
    assert get_srid(g) == 4326


def test_parse_geom_ewkb_srid_roundtrip():
    pt = set_srid(Point(5, 6), 4326)
    ewkb = to_wkb(pt, include_srid=True)
    g = parse_geom(ewkb)
    assert get_srid(g) == 4326


def test_parse_geom_none():
    assert parse_geom(None) is None


def test_parse_geom_empty_string():
    # An empty / whitespace-only string must be None, not a from_wkt parse error.
    assert parse_geom("") is None
    assert parse_geom("   ") is None


def test_pyvx_reexports_shared_parse_geom():
    # pyvx._geom.parse_geom must BE the shared gbx._geom.parse_geom (single
    # source of truth) so pyvx behavior is unchanged after centralization.
    from databricks.labs.gbx._geom import parse_geom as shared_parse_geom

    assert parse_geom is shared_parse_geom
