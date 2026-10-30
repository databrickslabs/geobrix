import pytest

pytest.importorskip("scipy")
shapely = pytest.importorskip("shapely")
from shapely import to_wkb, wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx  # noqa: E402


def _pts_wkb(coords):
    return [bytearray(to_wkb(Point(*c))) for c in coords]


def test_st_triangulate_emits_triangles(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0), (0, 1, 0)])
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    df.createOrReplaceTempView("v")
    rows = spark.sql(
        "SELECT t.triangle FROM v, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t"
    ).collect()
    assert len(rows) == 2
    assert all(wkb.loads(bytes(r["triangle"])).geom_type == "Polygon" for r in rows)


def test_st_triangulate_multipoint_element(spark):
    # A single MULTIPOINT WKB as one array element must flatten to its member
    # points (shapely MultiPoint has .geoms, not .coords) and emit triangles —
    # not crash on a missing .coords attribute.
    from shapely.geometry import MultiPoint

    mp = bytearray(to_wkb(MultiPoint([(0, 0), (1, 0), (1, 1), (0, 1)])))
    df = spark.createDataFrame(
        [([mp], [], 0.0, 0.0, "NONENCROACHING")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    df.createOrReplaceTempView("vmp")
    rows = spark.sql(
        "SELECT t.triangle FROM vmp, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t"
    ).collect()
    assert len(rows) == 2
    assert all(wkb.loads(bytes(r["triangle"])).geom_type == "Polygon" for r in rows)


def test_st_triangulate_two_arg_default_mode(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0)])
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    df.createOrReplaceTempView("vd")
    rows = spark.sql(
        "SELECT t.triangle FROM vd, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf) t"
    ).collect()
    assert len(rows) == 1  # default mode = constrained


def test_st_triangulate_conforming_raises(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0)])
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "MIDPOINT")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    df.createOrReplaceTempView("v2")
    with pytest.raises(Exception, match="conforming"):
        spark.sql(
            "SELECT t.* FROM v2, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'conforming') t"
        ).collect()


def test_interpolateelevationbbox_emits_points(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (10, 0, 10), (10, 10, 20), (0, 10, 10)])
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING", 0.0, 0.0, 10.0, 10.0, 5, 5, 0)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "xmin double, ymin double, xmax double, ymax double, w int, h int, srid int",
    )
    df.createOrReplaceTempView("vb")
    rows = spark.sql(
        "SELECT t.elevation_point FROM vb, LATERAL "
        "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, w, h, srid, 'constrained') t"
    ).collect()
    assert len(rows) == 25
    from shapely import wkb

    assert wkb.loads(bytes(rows[0]["elevation_point"])).has_z


def test_interpolateelevationgeom_emits_points(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (10, 0, 10), (10, 10, 20), (0, 10, 10)])
    origin = bytearray(to_wkb(Point(0.0, 10.0)))
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING", origin, 5, 5, 2.0, -2.0)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "origin binary, cols int, rows int, cx double, cy double",
    )
    df.createOrReplaceTempView("vg")
    rows = spark.sql(
        "SELECT t.elevation_point FROM vg, LATERAL "
        "gbx_st_interpolateelevationgeom(pts, bl, mt, st, spf, origin, cols, rows, cx, cy, 'constrained') t"
    ).collect()
    assert len(rows) == 25


def test_interpolateelevation_default_mode_and_conforming_raises(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (10, 0, 10), (10, 10, 20), (0, 10, 10)])
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING", 0.0, 0.0, 10.0, 10.0, 3, 3, 0)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "xmin double, ymin double, xmax double, ymax double, w int, h int, srid int",
    )
    df.createOrReplaceTempView("vc")
    n = spark.sql(
        "SELECT t.* FROM vc, LATERAL gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, w, h, srid) t"
    ).count()
    assert n == 9  # 13-arg minus mode -> default constrained, 3x3 grid
    with pytest.raises(Exception, match="conforming"):
        spark.sql(
            "SELECT t.* FROM vc, LATERAL gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, w, h, srid, 'conforming') t"
        ).collect()
