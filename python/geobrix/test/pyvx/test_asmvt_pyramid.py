import pytest

mvt = pytest.importorskip(
    "mapbox_vector_tile",
    reason="mapbox-vector-tile not installed (a light-tier extra (e.g. geobrix[light_env6]) or [test] required)",
)
from shapely import to_wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx  # noqa: E402


def test_st_asmvt_pyramid_fans_out_per_tile(spark):
    vx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(Point(0.0, 0.0))), "a", 7)],
        "geom binary, name string, id int",
    )
    df.createOrReplaceTempView("feats")
    out = spark.sql(
        "SELECT t.z, t.x, t.y, t.mvt_bytes "
        "FROM feats, LATERAL gbx_st_asmvt_pyramid(geom, struct(name, id), 0, 2, 'layer', 4096) t"
    ).collect()
    zs = sorted({r["z"] for r in out})
    assert zs == [0, 1, 2]
    blob = bytes([r for r in out if r["z"] == 2][0]["mvt_bytes"])
    feats = mvt.decode(blob)["layer"]["features"]
    assert feats[0]["properties"]["id"] == 7


def test_pyramid_accepts_wkt(spark):
    # Geom-input consistency: st_asmvt_pyramid must accept the same encodings as every
    # other geom-accepting pyvx fn (WKB/EWKB/WKT/EWKT) via _geom.parse_geom. A WKT geom
    # must fan out and emit tiles just like the WKB path.
    vx.register(spark)
    df = spark.createDataFrame(
        [("SRID=4326;POINT (0 0)", "a", 7)],
        "geom string, name string, id int",
    )
    df.createOrReplaceTempView("feats_wkt")
    out = spark.sql(
        "SELECT t.z, t.x, t.y, t.mvt_bytes "
        "FROM feats_wkt, LATERAL gbx_st_asmvt_pyramid(geom, struct(name, id), 0, 2, 'layer', 4096) t"
    ).collect()
    zs = sorted({r["z"] for r in out})
    assert zs == [0, 1, 2]
    blob = bytes([r for r in out if r["z"] == 2][0]["mvt_bytes"])
    feats = mvt.decode(blob)["layer"]["features"]
    assert feats[0]["properties"]["id"] == 7


def test_pyramid_cap_raises(spark):
    vx.register(spark)
    from shapely.geometry import box

    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(-179, -85, 179, 85))),)], "geom binary"
    )
    df.createOrReplaceTempView("big")
    import pytest

    with pytest.raises(Exception):
        spark.sql(
            "SELECT t.z FROM big, LATERAL gbx_st_asmvt_pyramid(geom, struct(), 0, 20, 'l', 4096) t"
        ).collect()
