"""Comprehensive Python tests for the 9 quadbin functions.

Mirrors the Scala QuadbinFunctionsTest end-to-end: register the SQL
functions, build small DataFrames, evaluate each Column wrapper, and
assert on collected rows.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[3] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=INFO,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def quadbin_registered(spark):
    """Register quadbin functions once for all tests."""
    from databricks.labs.gbx.gridx.quadbin import functions as qx

    qx.register(spark)
    return qx


def _cell_at(spark, qx, lon: float, lat: float, z: int) -> int:
    """Compute a quadbin cell id via the SQL function (round-trip through Spark)."""
    df = spark.createDataFrame([(lon, lat)], ["lon", "lat"])
    return df.select(
        qx.quadbin_pointascell(f.col("lon"), f.col("lat"), z).alias("cell")
    ).first()["cell"]


def test_quadbin_pointascell(spark, quadbin_registered):
    qx = quadbin_registered
    df = spark.createDataFrame([(-122.4194, 37.7749)], ["lon", "lat"])
    row = df.select(
        qx.quadbin_pointascell(f.col("lon"), f.col("lat"), 10).alias("cell")
    ).first()
    assert row["cell"] is not None
    assert isinstance(row["cell"], int)
    assert row["cell"] != 0


def test_quadbin_resolution(spark, quadbin_registered):
    qx = quadbin_registered
    cell = _cell_at(spark, qx, 0.0, 0.0, 12)
    df = spark.createDataFrame([(cell,)], ["cell"])
    row = df.select(qx.quadbin_resolution(f.col("cell")).alias("z")).first()
    assert row["z"] == 12


def test_quadbin_aswkb(spark, quadbin_registered):
    qx = quadbin_registered
    cell = _cell_at(spark, qx, 0.0, 0.0, 8)
    df = spark.createDataFrame([(cell,)], ["cell"])
    row = df.select(qx.quadbin_aswkb(f.col("cell")).alias("wkb")).first()
    wkb = row["wkb"]
    assert wkb is not None
    assert isinstance(wkb, (bytes, bytearray))
    assert len(wkb) > 0


def test_quadbin_centroid(spark, quadbin_registered):
    qx = quadbin_registered
    cell = _cell_at(spark, qx, 151.2093, -33.8688, 12)
    df = spark.createDataFrame([(cell,)], ["cell"])
    row = df.select(qx.quadbin_centroid(f.col("cell")).alias("c")).first()
    assert row["c"] is not None
    assert isinstance(row["c"], (bytes, bytearray))


def test_quadbin_polyfill(spark, quadbin_registered):
    qx = quadbin_registered
    # Small bbox near (0, 0) → small number of cells
    wkt = "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"
    df = spark.createDataFrame([(wkt,)], ["geom"])
    cells = df.select(qx.quadbin_polyfill(f.col("geom"), 5).alias("cells")).first()[
        "cells"
    ]
    assert cells is not None
    assert len(cells) >= 1


def test_quadbin_kring(spark, quadbin_registered):
    qx = quadbin_registered
    cell = _cell_at(spark, qx, 0.0, 0.0, 10)
    df = spark.createDataFrame([(cell,)], ["cell"])
    ring = df.select(qx.quadbin_kring(f.col("cell"), 1).alias("r")).first()["r"]
    assert ring is not None
    assert len(ring) == 9


def test_quadbin_tessellate(spark, quadbin_registered):
    qx = quadbin_registered
    wkt = "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"
    df = spark.createDataFrame([(wkt,)], ["geom"])
    chips = df.select(qx.quadbin_tessellate(f.col("geom"), 5).alias("chips")).first()[
        "chips"
    ]
    assert chips is not None
    assert len(chips) >= 1
    for chip in chips:
        assert chip["cell"] is not None
        assert chip["geom"] is not None
        assert len(chip["geom"]) > 0


def test_quadbin_cellunion(spark, quadbin_registered):
    qx = quadbin_registered
    centre = _cell_at(spark, qx, 0.0, 0.0, 8)
    df = spark.createDataFrame([(centre,)], ["cell"])
    ring = df.select(qx.quadbin_kring(f.col("cell"), 1).alias("r")).first()["r"]
    df2 = spark.createDataFrame([(list(ring),)], ["cells"])
    u = df2.select(qx.quadbin_cellunion(f.col("cells")).alias("u")).first()["u"]
    assert u is not None
    assert isinstance(u, (bytes, bytearray))
    assert len(u) > 0


def test_quadbin_distance(spark, quadbin_registered):
    qx = quadbin_registered
    centre = _cell_at(spark, qx, 0.0, 0.0, 10)
    df = spark.createDataFrame([(centre,)], ["cell"])
    ring = df.select(qx.quadbin_kring(f.col("cell"), 1).alias("r")).first()["r"]
    neighbour = next(c for c in ring if c != centre)
    df2 = spark.createDataFrame([(centre, centre, neighbour)], ["a", "b", "c"])
    row = df2.select(
        qx.quadbin_distance(f.col("a"), f.col("b")).alias("d0"),
        qx.quadbin_distance(f.col("a"), f.col("c")).alias("d1"),
    ).first()
    assert row["d0"] == 0
    assert row["d1"] == 1


def _rows(df):
    return sorted(tuple(r) for r in df.collect())


@pytest.mark.parametrize("name", ["geomkringexplode", "geomkloopexplode"])
def test_quadbin_geom_explode_wrappers_match_sql(spark, quadbin_registered, name):
    """The heavy wrappers bind geom/resolution/k[/mode/coverage] in the SQL
    function's positional order: same rows as the SQL call, with defaults and
    with an explicit non-default mode + coverage (which change the result)."""
    qx = quadbin_registered
    wrapper = getattr(qx, f"quadbin_{name}")
    df = spark.createDataFrame(
        [("POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))",)], ["geom"]
    )
    got = _rows(df.select(wrapper(f.col("geom"), 5, 1)))
    want = _rows(df.selectExpr(f"gbx_quadbin_{name}(geom, 5, 1)"))
    assert got and got == want
    got_in = _rows(df.select(wrapper(f.col("geom"), 5, 1, "boundary-in", "core")))
    want_in = _rows(
        df.selectExpr(f"gbx_quadbin_{name}(geom, 5, 1, 'boundary-in', 'core')")
    )
    assert got_in == want_in and want_in != want
