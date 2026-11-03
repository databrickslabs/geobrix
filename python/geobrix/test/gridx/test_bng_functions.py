"""
Comprehensive tests for all BNG (British National Grid) functions.

Tests use valid BNG cell IDs and resolutions based on Scala test patterns:
- Valid cell IDs: TQ388791, TQ388792, TQ388793
- Valid resolutions: 1, 3 (grid levels) or "50m", "500m" (meters)
- Valid coordinates: 100000, 200000 (within BNG grid)
"""

import logging
import struct
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
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
def bng_registered(spark):
    """Register BNG functions once for all tests."""
    from databricks.labs.gbx.gridx.bng import functions as bng_funcs

    bng_funcs.register(spark)
    return bng_funcs


@pytest.fixture(scope="session")
def test_data(spark):
    """Create test data with valid BNG cell IDs."""
    return spark.createDataFrame(
        [("TQ388791",), ("TQ388792",), ("TQ388793",)], ["cellId"]
    )


# ====== Coordinate Conversion ======


def test_bng_eastnorthasbng(spark, bng_registered):
    """Test converting east/north coordinates to BNG cell."""
    # Use valid BNG coordinates (100000, 200000) with resolution 3 (from Scala tests)
    result = spark.range(1).select(
        bng_registered.bng_eastnorthasbng(f.lit(100000), f.lit(200000), f.lit(3)).alias(
            "cell_id"
        )
    )
    row = result.collect()[0]
    assert row["cell_id"] is not None
    assert isinstance(row["cell_id"], str)
    assert len(row["cell_id"]) > 0


def test_bng_eastnorthasbng_string_resolution(spark, bng_registered):
    """Test converting east/north coordinates with string resolution."""
    result = spark.range(1).select(
        bng_registered.bng_eastnorthasbng(
            f.lit(100000), f.lit(200000), f.lit("50m")
        ).alias("cell_id")
    )
    row = result.collect()[0]
    assert row["cell_id"] is not None
    assert isinstance(row["cell_id"], str)


# ====== Tessellate ======


def test_bng_tessellate_wkt(spark, bng_registered):
    """Test tessellate with WKT polygon."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_tessellate(
            f.lit(polygon_wkt), f.lit(1), keep_core_geom=True
        ).alias("tessellation")
    )
    row = result.collect()[0]
    assert row["tessellation"] is not None


def test_bng_tessellate_string_resolution(spark, bng_registered):
    """Test tessellate with string resolution."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_tessellate(
            f.lit(polygon_wkt), f.lit("50m"), keep_core_geom=False
        ).alias("tessellation")
    )
    row = result.collect()[0]
    assert row["tessellation"] is not None


# ====== K-Ring Functions ======


# The documented 3 km x 3 km polygon (EPSG:27700) and its centre cell at 1 km.
_DOC_POLY = "POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))"


def test_bng_tessellate_keep_core_geom_defaults_to_false(spark, bng_registered):
    """bng_tessellate leaves core chips NULL unless keep_core_geom is set, like SQL's 2-arg form."""
    df = spark.createDataFrame([(_DOC_POLY,)], "geom string")

    def chips(col):
        return {c["cellid"]: c for c in df.select(col.alias("t")).collect()[0]["t"]}

    default = chips(bng_registered.bng_tessellate(f.col("geom"), 3))
    kept = chips(bng_registered.bng_tessellate(f.col("geom"), 3, keep_core_geom=True))
    df.createOrReplaceTempView("bng_tess_default")
    sql_two_arg = {
        c["cellid"]: c
        for c in spark.sql(
            "SELECT gbx_bng_tessellate(geom, 3) AS t FROM bng_tess_default"
        ).collect()[0]["t"]
    }

    assert len(default) == 9 and default["TQ3080"]["core"]
    assert default["TQ3080"]["chip"] is None
    assert kept["TQ3080"]["chip"] is not None
    assert default == sql_two_arg


# ====== Aggregator Functions ======


# ====== Generator/Explode Functions ======
# The Python explode wrappers return the generator itself; select it directly.


def test_bng_tessellateexplode_wkt(spark, bng_registered):
    """Tessellate explode returns cellid, core and chip; the chip is WKB (BINARY) for WKT input too."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    df = spark.createDataFrame([(polygon_wkt,)], "geom string")
    result = bng_registered.bng_tessellateexplode(
        df, "geom", 1, keep_core_geom=True
    ).select("cellid", "core", "chip")
    assert result.dtypes == [
        ("cellid", "string"),
        ("core", "boolean"),
        ("chip", "binary"),
    ]
    rows = result.collect()
    assert len(rows) > 0
    # keep_core_geom=True: every chip, core or border, carries its geometry.
    assert all(r["cellid"] is not None and r["chip"] is not None for r in rows)


def test_bng_tessellateexplode_wkb_chip_is_binary(spark, bng_registered):
    """A WKB geometry gives a BINARY chip; keep_core_geom=False leaves core chips NULL."""
    # little-endian WKB of a 2.5 km box on exact 1 km grid lines: one core cell, eight border cells
    ring = [
        (530000.0, 180000.0),
        (532500.0, 180000.0),
        (532500.0, 182500.0),
        (530000.0, 182500.0),
        (530000.0, 180000.0),
    ]
    wkb = struct.pack("<BIII", 1, 3, 1, len(ring)) + b"".join(
        struct.pack("<dd", x, y) for x, y in ring
    )
    df = spark.createDataFrame([(bytearray(wkb),)], "geom binary")
    result = bng_registered.bng_tessellateexplode(
        df, "geom", 3, keep_core_geom=False
    ).select("cellid", "core", "chip")
    assert result.dtypes == [
        ("cellid", "string"),
        ("core", "boolean"),
        ("chip", "binary"),
    ]
    rows = result.collect()
    assert any(r["core"] and r["chip"] is None for r in rows)
    assert any(not r["core"] and r["chip"] is not None for r in rows)


def test_bng_tessellateexplode_keep_core_geom_defaults_to_false(spark, bng_registered):
    """Without keep_core_geom the core chips are NULL, as in the lightweight tier."""
    ring = [
        (530000.0, 180000.0),
        (532500.0, 180000.0),
        (532500.0, 182500.0),
        (530000.0, 182500.0),
        (530000.0, 180000.0),
    ]
    wkb = struct.pack("<BIII", 1, 3, 1, len(ring)) + b"".join(
        struct.pack("<dd", x, y) for x, y in ring
    )
    df = spark.createDataFrame([(bytearray(wkb),)], "geom binary")
    default_rows = (
        bng_registered.bng_tessellateexplode(df, "geom", 3)
        .select("cellid", "core", "chip")
        .collect()
    )
    explicit_rows = (
        bng_registered.bng_tessellateexplode(df, "geom", 3, keep_core_geom=False)
        .select("cellid", "core", "chip")
        .collect()
    )
    assert sorted(default_rows) == sorted(explicit_rows)
    assert any(r["core"] and r["chip"] is None for r in default_rows)

    # the two-argument SQL form (the lightweight tier's signature) is the same call
    df.createOrReplaceTempView("bng_te_two_arg")
    sql_rows = spark.sql(
        "SELECT cellid, core, chip FROM bng_te_two_arg "
        "LATERAL VIEW gbx_bng_tessellateexplode(geom, 3) t AS cellid, core, chip"
    ).collect()
    assert sorted(sql_rows) == sorted(default_rows)


def test_bng_tessellateexplode_string_resolution(spark, bng_registered):
    """Test tessellate explode with string resolution."""
    polygon_wkt = "POLYGON ((530000 180000, 530500 180000, 530500 180500, 530000 180500, 530000 180000))"
    # the resolution is a literal: a str such as "500m" is the size, not a column name
    df = spark.createDataFrame([(polygon_wkt,)], "geom string")
    result = bng_registered.bng_tessellateexplode(
        df, "geom", "500m", keep_core_geom=True
    ).select("cellid", "core", "chip")
    assert result.columns == ["cellid", "core", "chip"]
    rows = result.collect()
    assert len(rows) > 0
