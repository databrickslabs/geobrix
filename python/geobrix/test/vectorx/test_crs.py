"""End-to-end Python tests for the CRS family wrappers: st_crs, st_setcrs, st_transformcrs."""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.appName("gbx-vectorx-crs-tests")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    from databricks.labs.gbx.vectorx import functions as vx

    vx.register(s)
    yield s


def test_st_crs_returns_authority_string(spark):
    """st_crs(geom) on an EWKB geometry with embedded SRID returns an authority string."""
    from databricks.labs.gbx.vectorx import functions as vx

    # WKB for POINT(0.5 0.5) with SRID 4326 (EWKB: includes 4-byte SRID).
    # EWKB format: 01 (LE) + 01000020 (POINT + has SRID) + 4-byte SRID + coords
    # SRID 4326 in LE: E6 10 00 00 (little-endian bytes)
    # Coords: x=0.5, y=0.5 (double, little-endian)
    ewkb_4326 = bytes.fromhex(
        "0101000020"  # POINT with SRID marker
        "E6100000"  # SRID 4326 (little-endian)
        "000000000000E03F"  # x = 0.5
        "000000000000E03F"  # y = 0.5
    )
    df = spark.createDataFrame([(ewkb_4326,)], ["geom"])
    result = df.select(vx.st_crs(col("geom")).alias("crs")).collect()
    assert len(result) == 1
    crs_value = result[0]["crs"]
    assert (
        crs_value is not None
    ), "st_crs should return a non-null CRS for EWKB with SRID"
    assert "4326" in str(crs_value), f"Expected 4326 in CRS string, got {crs_value}"


def test_st_setcrs_bare_string_literal(spark):
    """st_setcrs(geom, 'EPSG:4326') with bare string literal should not resolve as column name."""
    from databricks.labs.gbx.vectorx import functions as vx

    # WKB for POINT(0.5 0.5) without SRID.
    pt_wkb = bytes.fromhex("0101000000000000000000E03F000000000000E03F")
    df = spark.createDataFrame([(pt_wkb,)], ["geom"])

    # Apply st_setcrs with bare string — the _crs_col helper must lift it via f.lit
    # so it is NOT resolved as a column name (which would be an error).
    result = df.select(
        vx.st_setcrs(col("geom"), "EPSG:4326").alias("geom_out"),
        vx.st_crs(vx.st_setcrs(col("geom"), "EPSG:4326")).alias("crs_out"),
    ).collect()

    assert len(result) == 1
    crs_value = result[0]["crs_out"]
    assert crs_value is not None, "st_crs on stamped geom should return non-null"
    assert "4326" in str(crs_value), f"Expected 4326 in CRS string, got {crs_value}"


def test_st_transformcrs_2arg_form(spark):
    """st_transformcrs(geom, target_crs) 2-arg form should reproject."""
    from databricks.labs.gbx.vectorx import functions as vx

    # WKB for POINT(0.5 0.5) in WGS84 (EPSG:4326, no embedded SRID)
    pt_wkb = bytes.fromhex("0101000000000000000000E03F000000000000E03F")
    df = spark.createDataFrame([(pt_wkb,)], ["geom"])

    # Transform from WGS84 (4326) to Web Mercator (3857).
    # Since the source WKB has no embedded SRID, we provide source_crs explicitly in 3-arg form.
    result = df.select(
        vx.st_transformcrs(col("geom"), "EPSG:3857", "EPSG:4326").alias("geom_out"),
        vx.st_crs(vx.st_transformcrs(col("geom"), "EPSG:3857", "EPSG:4326")).alias(
            "crs_out"
        ),
    ).collect()

    assert len(result) == 1
    geom_value = result[0]["geom_out"]
    crs_value = result[0]["crs_out"]
    assert geom_value is not None, "st_transformcrs should return a non-null geometry"
    assert crs_value is not None, "CRS of transformed geom should be non-null"
    assert "3857" in str(
        crs_value
    ), f"Expected 3857 in transformed CRS, got {crs_value}"


def test_st_transformcrs_3arg_form(spark):
    """st_transformcrs(geom, target_crs, source_crs) 3-arg form with explicit source_crs."""
    from databricks.labs.gbx.vectorx import functions as vx

    # WKB for POINT(0.5 0.5) in WGS84 (no embedded SRID).
    pt_wkb = bytes.fromhex("0101000000000000000000E03F000000000000E03F")
    df = spark.createDataFrame([(pt_wkb,)], ["geom"])

    # Explicit 3-arg form with source_crs.
    result = df.select(
        vx.st_transformcrs(col("geom"), "EPSG:3857", "EPSG:4326").alias("geom_out"),
    ).collect()

    assert len(result) == 1
    geom_value = result[0]["geom_out"]
    assert (
        geom_value is not None
    ), "st_transformcrs 3-arg should return a non-null geometry"
