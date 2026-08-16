"""Tests for VectorX SQL examples.

Ensures all SQL examples in documentation are executable and produce valid results.
Mirrors the per-package test driver pattern used by ``test_rasterx_functions_sql.py``
and ``test_gridx_functions_sql.py``. Each example function in
``vectorx_functions_sql`` returns a SQL string; this driver runs it against the
docs-test Spark session (from ``conftest.py``) and asserts non-empty output.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import vectorx_functions_sql  # noqa: E402


def _statement(sql: str) -> str:
    """Strip the trailing statement terminator so pyspark's single-statement sql() accepts it.

    Only the FINAL ``;`` is removed, not every ``;`` in the text: EWKT literals
    such as ``'SRID=4326;POINT (13 42)'`` contain a semicolon that is data, not a
    statement terminator.
    """
    return sql.strip().rstrip(";")


@pytest.fixture(scope="module")
def vectorx_registered(spark):
    """Register VectorX SQL functions and create the shared fixture views.

    Creates the ``vector_geoms`` temp view (1 row: ``geom STRING =
    'SRID=4326;POINT (13 42)'``) so SQL examples that reference it via
    ``FROM vector_geoms`` execute correctly.
    """
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415
    from ._fixtures import create_setup_views_vectorx_heavy  # noqa: PLC0415

    vx.register(spark)
    create_setup_views_vectorx_heavy(spark)
    yield spark


def test_st_asmvt_sql_example(vectorx_registered):
    """Run the ``gbx_st_asmvt`` SQL example and assert a non-empty MVT blob."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_asmvt_sql_example()
    result = spark.sql(_statement(sql)).collect()
    assert len(result) == 1
    assert result[0]["mvt_bytes_len"] > 0


def test_st_asmvt_pyramid_sql_example(vectorx_registered):
    """Run the ``gbx_st_asmvt_pyramid`` SQL example and assert one row per tile."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_asmvt_pyramid_sql_example()
    result = spark.sql(_statement(sql)).collect()
    # The example rectangle straddles the prime meridian at z=2 → two tiles emitted.
    assert len(result) == 2
    for row in result:
        assert row["z"] == 2
        assert row["mvt_bytes_len"] > 0


def test_st_crs_sql_example(vectorx_registered):
    """Run the ``gbx_st_crs`` SQL example; asserts EPSG:4326 for the SRID=4326 fixture."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_crs_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["crs"] == "EPSG:4326", f"Expected EPSG:4326, got {row['crs']!r}"


def test_st_setcrs_sql_example(vectorx_registered):
    """Run the ``gbx_st_setcrs`` SQL example and assert non-null EWKB binary output."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_setcrs_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["stamped"] is not None, "st_setcrs should not return None"
    assert isinstance(
        row["stamped"], (bytes, bytearray)
    ), f"Expected bytes (EWKB), got {type(row['stamped'])}"
    assert len(row["stamped"]) > 0, "EWKB bytes should be non-empty"


def test_st_setcrs_stamps_different_crs(vectorx_registered):
    """Verify st_setcrs changes the embedded SRID: stamp EPSG:3857 on an SRID=4326 geom."""
    spark = vectorx_registered
    # Round-trip: stamp 3857, then read back with gbx_st_crs — must change from 4326.
    row = spark.sql(
        "SELECT gbx_st_crs(gbx_st_setcrs(geom, 'EPSG:3857')) AS new_crs FROM vector_geoms"
    ).first()
    assert (
        row["new_crs"] == "EPSG:3857"
    ), f"Expected EPSG:3857 after stamping (not a no-op), got {row['new_crs']!r}"


def test_st_setcrs_sql_returns_binary_for_text_input(vectorx_registered):
    """The documented always-BINARY SQL contract: STRING geometry in, BINARY out."""
    from pyspark.sql.types import BinaryType

    spark = vectorx_registered
    df = spark.sql("SELECT gbx_st_setcrs('SRID=4326;POINT (11 42)', 'EPSG:32633') AS g")
    assert isinstance(df.schema["g"].dataType, BinaryType)
    assert df.first()["g"] is not None


def test_st_transformcrs_sql_example(vectorx_registered):
    """Run the ``gbx_st_transformcrs`` SQL example; asserts non-null EWKB for in-domain input."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_transformcrs_sql_example()
    row = spark.sql(_statement(sql)).first()
    # POINT(13, 42) is inside UTM zone 33N's area of use → non-null EWKB result.
    assert (
        row["utm33n"] is not None
    ), "st_transformcrs with in-domain coords should not return None"
    assert isinstance(
        row["utm33n"], (bytes, bytearray)
    ), f"Expected bytes (EWKB), got {type(row['utm33n'])}"
    assert len(row["utm33n"]) > 0, "EWKB bytes should be non-empty"


def test_st_transformcrs_sql_returns_binary_for_text_input(vectorx_registered):
    """The documented always-BINARY SQL contract for the reprojecting function."""
    from pyspark.sql.types import BinaryType

    spark = vectorx_registered
    df = spark.sql(
        "SELECT gbx_st_transformcrs('SRID=4326;POINT (13 42)', 'EPSG:32633') AS g"
    )
    assert isinstance(df.schema["g"].dataType, BinaryType)
    assert df.first()["g"] is not None
