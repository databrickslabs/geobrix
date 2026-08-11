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

    Only the FINAL ``;`` is removed, not every ``;`` in the text: the CRS examples contain
    EWKT literals (``'SRID=4326;POINT (11 42)'``) whose semicolon is data. Blanking those
    turned every CRS lookup into NULL and made the examples silently document the wrong
    output.
    """
    return sql.strip().rstrip(";")


@pytest.fixture(scope="module")
def vectorx_registered(spark):
    """Register VectorX expression-level SQL functions for this test module."""
    from databricks.labs.gbx.vectorx import functions as vx
    vx.register(spark)
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
    """Run the ``gbx_st_crs`` SQL example; the documented output must be the real output."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_crs_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["wgs84"] == "EPSG:4326"
    # ESRI-range codes classify as ESRI, not EPSG — the authoritative rule.
    assert row["sinusoidal"] == "ESRI:54008"
    # Plain WKT carries no SRID.
    assert row["no_srid"] is None


def test_st_setcrs_sql_example(vectorx_registered):
    """Run the ``gbx_st_setcrs`` SQL example and assert each stamped CRS reads back."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_setcrs_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["stamped_wgs84"] == "EPSG:4326"
    assert row["stamped_esri"] == "ESRI:54008"
    # WKB geometry + integer CRS argument.
    assert row["stamped_from_wkb"] == "EPSG:32633"


def test_st_setcrs_sql_returns_binary_for_text_input(vectorx_registered):
    """The documented always-BINARY SQL contract: STRING geometry in, BINARY out."""
    from pyspark.sql.types import BinaryType

    spark = vectorx_registered
    df = spark.sql("SELECT gbx_st_setcrs('SRID=4326;POINT (11 42)', 'EPSG:32633') AS g")
    assert isinstance(df.schema["g"].dataType, BinaryType)
    assert df.first()["g"] is not None


def test_st_transformcrs_sql_example(vectorx_registered):
    """Run the ``gbx_st_transformcrs`` SQL example and assert the SRID-follows-target rule."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_transformcrs_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["to_utm33n"] == "EPSG:32633"
    # 3-arg form: explicit source_crs for a plain (SRID-less) geometry.
    assert row["to_sinusoidal"] == "ESRI:54008"
    # A PROJ4 target carries no authority code, so the stale SRID is cleared.
    assert row["to_proj4"] is None


def test_st_transformcrs_sql_returns_binary_for_text_input(vectorx_registered):
    """The documented always-BINARY SQL contract for the reprojecting function."""
    from pyspark.sql.types import BinaryType

    spark = vectorx_registered
    df = spark.sql(
        "SELECT gbx_st_transformcrs('SRID=4326;POINT (13 42)', 'EPSG:32633') AS g"
    )
    assert isinstance(df.schema["g"].dataType, BinaryType)
    assert df.first()["g"] is not None
