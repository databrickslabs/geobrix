import numpy as np
from shapely import from_wkb, get_srid, set_srid, to_wkb
from shapely.geometry import Point, Polygon
from databricks.labs.gbx.pyvx import _antimeridian as am


def test_st_shiftlongitude_shifts_negative_x_into_0_360():
    out = am.st_shiftlongitude(to_wkb(Point(-170.0, 10.0)))
    g = from_wkb(out)
    assert (g.x, g.y) == (190.0, 10.0)


def test_st_shiftlongitude_leaves_positive_x_untouched():
    g = from_wkb(am.st_shiftlongitude(to_wkb(Point(20.0, 10.0))))
    assert (g.x, g.y) == (20.0, 10.0)


def test_st_shiftlongitude_preserves_srid():
    out = am.st_shiftlongitude(to_wkb(set_srid(Point(-170.0, 10.0), 4326), include_srid=True))
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
    out = am.st_wrapx(to_wkb(set_srid(Point(190.0, 10.0), 4326), include_srid=True), 180.0, -360.0)
    assert get_srid(from_wkb(out)) == 4326
    assert am.st_wrapx(None, 180.0, -360.0) is None
