"""The session (light) array twins agree with the Unity Catalog forms on the contract: a wrong argument raises with a
Fix, and only unreadable data is NULL."""

import json

import pytest

pytest.importorskip("scipy")
shapely = pytest.importorskip("shapely")

NAMES = [
    "gbx_st_coverageisvalid_array",
    "gbx_st_coverageinvalidedges_array",
    "gbx_st_asmvt_array",
]


@pytest.fixture(scope="module")
def twins(spark):
    from databricks.labs.gbx import register

    register(spark, only=NAMES, _prefer={n: "L3" for n in NAMES})
    return spark


def _one(spark, sql):
    return spark.sql(sql).collect()[0][0]


def _box(x0, x1):
    return shapely.box(x0, 0, x1, 1).wkb


def _polys(spark, members):
    spark.createDataFrame([(members,)], "g array<binary>").createOrReplaceTempView(
        "twin_polys"
    )


@pytest.mark.parametrize("name", NAMES[:2])
@pytest.mark.parametrize("gap", ["-1.0D", "CAST('NaN' AS DOUBLE)"])
def test_a_negative_or_non_finite_gap_width_raises_with_a_fix(twins, name, gap):
    _polys(twins, [bytearray(_box(0, 1)), bytearray(_box(1, 2))])
    with pytest.raises(Exception, match=r"gap_width.*Fix: "):
        _one(twins, f"SELECT {name}(g, {gap}) FROM twin_polys")


@pytest.mark.parametrize("name", NAMES[:2])
def test_a_corrupt_member_gives_null_not_an_error(twins, name):
    _polys(twins, [bytearray(_box(0, 1)), bytearray(b"\x01junk")])
    assert _one(twins, f"SELECT {name}(g, 0.0D) FROM twin_polys") is None


def test_a_clean_coverage_still_answers(twins):
    _polys(twins, [bytearray(_box(0, 1)), bytearray(_box(1, 2))])
    assert _one(twins, "SELECT gbx_st_coverageisvalid_array(g, 0.0D) FROM twin_polys")


def _features(spark, attrs):
    spark.createDataFrame(
        [([{"geom": bytearray(shapely.Point(5, 7).wkb), "attrs": attrs}],)],
        "f array<struct<geom:binary,attrs:string>>",
    ).createOrReplaceTempView("twin_feats")


@pytest.mark.parametrize("attrs", ["[1, 2]", "not json", "3", "{1, a}"])
def test_attrs_that_are_no_json_object_raise_with_a_fix(twins, attrs):
    _features(twins, attrs)
    with pytest.raises(Exception, match=r"attrs.*Fix: "):
        _one(twins, "SELECT gbx_st_asmvt_array(f, 'lyr') FROM twin_feats")


@pytest.mark.parametrize("extent", ["0", "-5"])
def test_a_wrong_extent_raises_with_a_fix(twins, extent):
    _features(twins, json.dumps({"a": 1}))
    with pytest.raises(Exception, match=r"extent.*Fix: "):
        _one(twins, f"SELECT gbx_st_asmvt_array(f, 'lyr', {extent}) FROM twin_feats")


def test_json_object_attrs_and_a_whole_extent_encode(twins):
    _features(twins, json.dumps({"a": 1}))
    assert _one(twins, "SELECT gbx_st_asmvt_array(f, 'lyr', 512) FROM twin_feats")
    _features(twins, None)
    assert _one(twins, "SELECT gbx_st_asmvt_array(f, 'lyr') FROM twin_feats")
