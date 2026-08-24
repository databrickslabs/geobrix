from shapely import from_wkb, get_parts, get_srid, set_srid, to_wkb
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from databricks.labs.gbx.pyvx import _antimeridian as am


def test_st_shiftlongitude_shifts_negative_x_into_0_360():
    out = am.st_shiftlongitude(to_wkb(Point(-170.0, 10.0)))
    g = from_wkb(out)
    assert (g.x, g.y) == (190.0, 10.0)


def test_st_shiftlongitude_leaves_positive_x_untouched():
    g = from_wkb(am.st_shiftlongitude(to_wkb(Point(20.0, 10.0))))
    assert (g.x, g.y) == (20.0, 10.0)


def test_st_shiftlongitude_preserves_srid():
    out = am.st_shiftlongitude(
        to_wkb(set_srid(Point(-170.0, 10.0), 4326), include_srid=True)
    )
    assert get_srid(from_wkb(out)) == 4326


def test_st_shiftlongitude_none_returns_none():
    assert am.st_shiftlongitude(None) is None


def test_st_wrapx_negative_direction_wraps_high_x_back():
    # ST_WrapX(POINT(190 10), 180, -360) -> POINT(-170 10)
    out = am.st_wrapx(to_wkb(Point(190.0, 10.0)), 180.0, -360.0)
    g = from_wkb(out)
    assert (g.x, g.y) == (-170.0, 10.0)


def test_st_wrapx_positive_direction_wraps_low_x_forward():
    # ST_WrapX(POINT(-10 10), 0, 360) -> POINT(350 10)
    g = from_wkb(am.st_wrapx(to_wkb(Point(-10.0, 10.0)), 0.0, 360.0))
    assert (g.x, g.y) == (350.0, 10.0)


def test_st_wrapx_preserves_srid_and_none():
    out = am.st_wrapx(
        to_wkb(set_srid(Point(190.0, 10.0), 4326), include_srid=True), 180.0, -360.0
    )
    assert get_srid(from_wkb(out)) == 4326
    assert am.st_wrapx(None, 180.0, -360.0) is None


# ---------------------------------------------------------------------------
# st_split acceptance corpus (Task 3)
# ---------------------------------------------------------------------------

_MERIDIAN = LineString([(180.0, -90.0), (180.0, 90.0)])


def _split_parts(wkb_out):
    g = from_wkb(wkb_out)
    assert g.geom_type == "GeometryCollection"
    return list(get_parts(g))


def test_st_split_polygon_by_meridian_yields_two_parts():
    poly = Polygon([(170, -10), (190, -10), (190, 10), (170, 10)])
    parts = _split_parts(am.st_split(to_wkb(poly), to_wkb(_MERIDIAN)))
    assert len(parts) == 2
    assert all(p.geom_type == "Polygon" for p in parts)


def test_st_split_multipolygon_decomposes_each_part():
    mp = MultiPolygon(
        [
            Polygon([(170, -10), (190, -10), (190, 10), (170, 10)]),
            Polygon([(170, 20), (190, 20), (190, 40), (170, 40)]),
        ]
    )
    parts = _split_parts(am.st_split(to_wkb(mp), to_wkb(_MERIDIAN)))
    assert len(parts) == 4  # each of the 2 polygons split in two


def test_st_split_no_intersection_returns_collection_of_the_input():
    poly = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])
    parts = _split_parts(am.st_split(to_wkb(poly), to_wkb(_MERIDIAN)))
    assert len(parts) == 1


def test_st_split_preserves_srid_and_none():
    poly = set_srid(Polygon([(170, -10), (190, -10), (190, 10), (170, 10)]), 4326)
    out = am.st_split(to_wkb(poly, include_srid=True), to_wkb(_MERIDIAN))
    assert get_srid(from_wkb(out)) == 4326
    assert am.st_split(None, to_wkb(_MERIDIAN)) is None
    assert am.st_split(to_wkb(poly), None) is None
