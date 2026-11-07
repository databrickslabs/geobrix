"""The medium-preserving core of the pyvx CRS functions: text in, text out; bytes in, bytes out.

What the SQL functions answer, for every geometry shape and CRS spelling, is the rows of the VECTOR_SCALAR oracle table
(test/manager/test_family_cross_tier.py). This file holds what a SQL column cannot show: the medium of the answer,
the text it is written in, and the Python defaults.
"""

import pytest
from shapely import from_wkb, from_wkt, get_srid, set_srid, to_wkb
from shapely.geometry import Point

from databricks.labs.gbx.pyvx import _crs, _validity

UTM51 = (500000.0, 5649824.888255088)


def _ewkb(srid, x=15.0, y=51.0):
    return to_wkb(set_srid(Point(x, y), srid), include_srid=True)


def _text_geom(text):
    return from_wkt(text.split(";", 1)[1] if ";" in text else text)


@pytest.mark.parametrize(
    "geom,crs,srid,medium",
    [
        (to_wkb(Point(15, 51)), "EPSG:32633", 32633, bytes),
        (_ewkb(4326), "EPSG:32633", 32633, bytes),
        ("POINT (15 51)", "ESRI:54008", 54008, str),
        ("SRID=4326;POINT (15 51)", "EPSG:32633", 32633, str),
    ],
    ids=["wkb", "ewkb", "wkt", "ewkt"],
)
def test_setcrs_answers_in_the_medium_it_was_given(geom, crs, srid, medium):
    out = _crs.st_setcrs(geom, crs)
    assert isinstance(out, medium if medium is str else (bytes, bytearray))
    if medium is str:
        assert out.upper().startswith(f"SRID={srid};")
    else:
        assert get_srid(from_wkb(out)) == srid


def test_setcrs_writes_the_digits_it_was_given():
    out = _crs.st_setcrs("POINT (11.123456789 42.987654321)", "EPSG:4326")
    g = _text_geom(out)
    assert (g.x, g.y) == (11.123456789, 42.987654321)


def test_setcrs_of_nothing_is_nothing():
    assert _crs.st_setcrs(None, "EPSG:4326") is None
    assert _crs.st_setcrs(b"NOT_WKB", "EPSG:4326") is None


@pytest.mark.parametrize(
    "geom,source",
    [("SRID=4326;POINT (15 51)", None), ("POINT (15 51)", "EPSG:4326")],
    ids=["ewkt", "wkt-with-source"],
)
def test_transformcrs_of_text_is_text_with_the_target_srid(geom, source):
    out = _crs.st_transformcrs(geom, "EPSG:32633", source)
    assert isinstance(out, str) and out.startswith("SRID=32633;")
    assert _text_geom(out).x == pytest.approx(UTM51[0], rel=1e-9)


def test_transformcrs_of_text_to_an_authority_less_target_has_no_srid():
    custom_tm = (
        'PROJCS["Custom_TM",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],'
        'PARAMETER["central_meridian",13.7],PARAMETER["scale_factor",0.9996],UNIT["metre",1]]'
    )
    out = _crs.st_transformcrs("SRID=4326;POINT (11 42)", custom_tm)
    assert isinstance(out, str) and "SRID=" not in out.upper()
    assert from_wkt(out).x == pytest.approx(-223616.3728811576)


def test_transformcrs_of_bytes_is_bytes_and_loses_no_digits_on_the_way_back():
    out = _crs.st_transformcrs(_ewkb(4326), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray)) and get_srid(from_wkb(out)) == 32633
    back = from_wkb(_crs.st_transformcrs(out, "EPSG:4326"))
    assert (back.x, back.y) == pytest.approx((15.0, 51.0), abs=1e-8)


@pytest.mark.parametrize(
    "fn,target", [("st_setcrs", "EPSG:4326"), ("st_transformcrs", "EPSG:32633")]
)
def test_a_measure_never_survives_in_text(fn, target):
    out = getattr(_crs, fn)("SRID=4326;LINESTRING ZM (15 51 5 99, 16 52 6 98)", target)
    assert "ZM" not in out.upper() and "Z" in out.upper().split(";")[1]


def test_unparseable_data_is_nothing_and_a_bad_definition_is_an_error():
    assert _crs.st_transformcrs(b"THIS_IS_NOT_WKB", "EPSG:3857") is None
    with pytest.raises(ValueError):
        _crs.st_transformcrs(_ewkb(4326), "EPSG:99999")


def test_the_untouched_2d_stays_2d_in_the_bytes_the_sql_functions_write():
    # 25 bytes is an EWKB point with no Z slot: a 2D geometry never gains a Z of 0
    assert len(_crs._udf_st_transformcrs(_ewkb(4326), "EPSG:32633")) == 25
    assert len(_crs._udf_st_setcrs("POINT (11 42)", "EPSG:4326")) == 25
    assert _crs._udf_st_transformcrs(_ewkb(4326), None) is None


def test_makevalid_defaults_to_the_linework_level():
    bowtie = from_wkt("POLYGON((0 0,1 1,1 0,0 1,0 0))").wkb
    assert _validity.st_makevalid(bowtie) == _validity.st_makevalid(bowtie, "linework")
