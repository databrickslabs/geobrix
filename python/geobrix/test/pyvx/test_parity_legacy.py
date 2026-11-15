"""Light (pyvx) vs heavy (vectorx) st_legacyaswkb decoded-geometry parity.

Both tiers decode the legacy Mosaic geometry struct to ISO WKB. They register
the SAME SQL name (``gbx_st_legacyaswkb``), so in one Spark session the later
registration overwrites the earlier: collect the light result first, THEN
register heavy and collect its result, then compare the decoded geometries.

Heavy requires the geobrix JAR (the legacy decode is Scala/JTS). The JAR is
present in the geobrix-dev Docker container; this test auto-skips when the JAR
is not staged under ``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyvx/test_parity_legacy.py \\
        --with-integration --log parity-legacy.log
"""

import logging
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyvx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: it has no effect if a JVM (and therefore
    # a Spark session) is already live in this process. Skip instead of producing a
    # misleading failure when another test suite already created a JAR-free session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run this test in isolation: "
                "gbx:test:python --path python/geobrix/test/pyvx/test_parity_legacy.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pyvx-legacy-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


def test_legacy_parity_polygon_with_hole_and_z(spark_with_jar):
    from databricks.labs.gbx import manager as gbx_manager

    spark = spark_with_jar
    from shapely import wkb

    outer = [
        [0.0, 0.0, 1.0],
        [10.0, 0.0, 1.0],
        [10.0, 10.0, 1.0],
        [0.0, 10.0, 1.0],
        [0.0, 0.0, 1.0],
    ]
    hole = [
        [2.0, 2.0, 1.0],
        [4.0, 2.0, 1.0],
        [4.0, 4.0, 1.0],
        [2.0, 4.0, 1.0],
        [2.0, 2.0, 1.0],
    ]
    schema = (
        "g struct<typeId:int,srid:int,"
        "boundaries:array<array<array<double>>>,"
        "holes:array<array<array<array<double>>>>>"
    )
    df = spark.createDataFrame(
        [({"typeId": 5, "srid": 0, "boundaries": [outer], "holes": [[hole]]},)], schema
    )
    df.createOrReplaceTempView("legv")

    # light first
    gbx_manager.register_family(
        spark, ("gbx_st_",), only=["gbx_st_legacyaswkb"], level="L3"
    )
    light = bytes(
        spark.sql("SELECT gbx_st_legacyaswkb(g) AS w FROM legv").collect()[0]["w"]
    )

    # heavy overwrites the same SQL name

    gbx_manager.register_family(
        spark, ("gbx_st_legacy",), only=["gbx_st_legacyaswkb"], level="L1"
    )
    heavy = bytes(
        spark.sql("SELECT gbx_st_legacyaswkb(g) AS w FROM legv").collect()[0]["w"]
    )

    lg, hg = wkb.loads(light), wkb.loads(heavy)
    assert lg.equals(hg)
    assert len(lg.interiors) == 1 and len(hg.interiors) == 1
    assert lg.has_z and hg.has_z


def test_legacy_parity_multipolygon_hole_on_second(spark_with_jar):
    from databricks.labs.gbx import manager as gbx_manager

    spark = spark_with_jar
    from shapely import wkb
    from shapely.geometry import MultiPolygon

    def sq(o, s):
        return [[o, o], [o + s, o], [o + s, o + s], [o, o + s], [o, o]]

    poly0 = sq(0.0, 10.0)
    poly1 = sq(20.0, 10.0)
    hole1 = sq(22.0, 2.0)  # hole on the SECOND polygon
    schema = (
        "g struct<typeId:int,srid:int,"
        "boundaries:array<array<array<double>>>,"
        "holes:array<array<array<array<double>>>>>"
    )
    df = spark.createDataFrame(
        [
            (
                {
                    "typeId": 6,
                    "srid": 0,
                    "boundaries": [poly0, poly1],
                    "holes": [[], [hole1]],
                },
            )
        ],
        schema,
    )
    df.createOrReplaceTempView("legmp")

    # light first
    gbx_manager.register_family(
        spark, ("gbx_st_",), only=["gbx_st_legacyaswkb"], level="L3"
    )
    light = bytes(
        spark.sql("SELECT gbx_st_legacyaswkb(g) AS w FROM legmp").collect()[0]["w"]
    )

    # heavy overwrites the same SQL name

    gbx_manager.register_family(
        spark, ("gbx_st_legacy",), only=["gbx_st_legacyaswkb"], level="L1"
    )
    heavy = bytes(
        spark.sql("SELECT gbx_st_legacyaswkb(g) AS w FROM legmp").collect()[0]["w"]
    )

    lg, hg = wkb.loads(light), wkb.loads(heavy)
    assert isinstance(lg, MultiPolygon) and isinstance(hg, MultiPolygon)
    assert lg.equals(hg)
    # the SECOND polygon retains its interior ring in both tiers
    assert len(lg.geoms[1].interiors) == 1
    assert len(hg.geoms[1].interiors) == 1
    assert len(lg.geoms[0].interiors) == 0
    assert len(hg.geoms[0].interiors) == 0
