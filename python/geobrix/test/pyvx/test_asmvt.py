import pytest

mvt = pytest.importorskip(
    "mapbox_vector_tile",
    reason="mapbox-vector-tile not installed (a light-tier extra (e.g. geobrix[light_env6]) or [test] required)",
)
from shapely import to_wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx  # noqa: E402


def test_st_asmvt_aggregates_group_to_one_tile(spark):
    vx.register(spark)
    rows = [
        (0, 0, 0, bytearray(to_wkb(Point(100.0, 200.0))), "a", 1),
        (0, 0, 0, bytearray(to_wkb(Point(300.0, 400.0))), "b", 2),
    ]
    df = spark.createDataFrame(
        rows, "z int, x int, y int, geom binary, name string, pop int"
    )
    from pyspark.sql import functions as f

    out = (
        df.groupBy("z", "x", "y")
        .agg(vx.st_asmvt(f.col("geom"), f.struct("name", "pop"), "layer").alias("mvt"))
        .collect()
    )
    assert len(out) == 1
    blob = bytes(out[0]["mvt"])
    feats = mvt.decode(blob)["layer"]["features"]
    assert len(feats) == 2
    pops = sorted(ff["properties"]["pop"] for ff in feats)
    assert pops == [1, 2]
    assert all(isinstance(ff["properties"]["pop"], int) for ff in feats)


def test_st_asmvt_accepts_wkt(spark):
    # Geom-input consistency: st_asmvt must accept the same encodings as every other
    # geom-accepting pyvx fn (WKB/EWKB/WKT/EWKT) via the shared _geom.parse_geom contract.
    # Heavy accepts WKB only; light is a strict superset. WKT (and EWKT) feature must
    # encode the same as the WKB path.
    vx.register(spark)
    from pyspark.sql import functions as f

    rows = [
        (0, 0, 0, "POINT (100 200)", "a", 1),
        (0, 0, 0, "SRID=4326;POINT (300 400)", "b", 2),
    ]
    df = spark.createDataFrame(
        rows, "z int, x int, y int, geom string, name string, pop int"
    )
    out = (
        df.groupBy("z", "x", "y")
        .agg(vx.st_asmvt(f.col("geom"), f.struct("name", "pop"), "layer").alias("mvt"))
        .collect()
    )
    assert len(out) == 1
    blob = bytes(out[0]["mvt"])
    feats = mvt.decode(blob)["layer"]["features"]
    assert len(feats) == 2
    pops = sorted(ff["properties"]["pop"] for ff in feats)
    assert pops == [1, 2]
