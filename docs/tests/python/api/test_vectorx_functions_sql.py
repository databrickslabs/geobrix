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

    Registers both the main ``vectorx`` package (TIN, MVT, CRS functions) and the
    ``vectorx.jts.legacy`` package (``gbx_st_legacyaswkb``), then creates the four
    canonical fixture views used by the SQL examples on this page.
    """
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415
    from databricks.labs.gbx.vectorx.jts.legacy import (
        functions as legacy_vx,
    )  # noqa: PLC0415
    from ._fixtures import create_setup_views_vectorx_heavy  # noqa: PLC0415

    vx.register(spark)
    legacy_vx.register(spark)
    create_setup_views_vectorx_heavy(spark)
    yield spark


def test_st_asmvt_sql_example(vectorx_registered):
    """Run the ``gbx_st_asmvt`` SQL example against the ``mvt_features`` fixture view.

    The fixture has 2 rows in tile (z=0, x=0, y=0), so one aggregated MVT blob is produced.
    The SQL returns the raw BINARY (column ``mvt``); we assert non-null and non-empty bytes.
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_asmvt_sql_example()
    result = spark.sql(_statement(sql)).collect()
    # Both fixture rows are in the same tile → exactly 1 MVT blob.
    assert len(result) == 1
    mvt = result[0]["mvt"]
    assert mvt is not None, "st_asmvt should return non-null MVT bytes"
    assert len(mvt) > 0, "MVT bytes should be non-empty"


def test_st_asmvt_pyramid_sql_example(vectorx_registered):
    """Run the ``gbx_st_asmvt_pyramid`` SQL example and assert one row per tile.

    POINT(0, 0) WGS-84 at zoom 0–2 intersects exactly 3 tiles:
    (z=0, x=0, y=0), (z=1, x=1, y=1), (z=2, x=2, y=2).
    The SQL returns raw ``mvt_bytes`` BINARY; we assert non-empty for each tile.
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_asmvt_pyramid_sql_example()
    result = spark.sql(_statement(sql)).collect()
    # POINT(0, 0) at zoom 0–2 → 3 tiles, one per zoom level.
    assert len(result) == 3
    for row in result:
        assert row["mvt_bytes"] is not None
        assert len(row["mvt_bytes"]) > 0


def test_st_triangulate_sql_example(vectorx_registered):
    """Run the ``gbx_st_triangulate`` SQL example against the ``tin_survey`` fixture view.

    4 POINT Z corners of a 10×10 m square → exactly 2 Delaunay triangle polygons.
    Each ``triangle`` column value is a WKB-encoded polygon (non-null, non-empty bytes).
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_triangulate_sql_example()
    result = spark.sql(_statement(sql)).collect()
    assert (
        len(result) == 2
    ), f"Expected 2 triangles from 4-corner fixture, got {len(result)}"
    for row in result:
        assert row["triangle"] is not None, "triangle should be non-null WKB bytes"
        assert isinstance(
            row["triangle"], (bytes, bytearray)
        ), f"Expected bytes (WKB polygon), got {type(row['triangle'])}"
        assert len(row["triangle"]) > 0, "triangle WKB bytes should be non-empty"


def test_st_interpolateelevationbbox_sql_example(vectorx_registered):
    """Run the ``gbx_st_interpolateelevationbbox`` SQL example against ``tin_survey``.

    3×3 grid over the 10×10 m extent (SRID=0) → 9 POINT Z rows (all cell centres
    fall inside the TIN convex hull for this fixture).
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_interpolateelevationbbox_sql_example()
    result = spark.sql(_statement(sql)).collect()
    assert (
        len(result) == 9
    ), f"Expected 9 elevation points (3×3 grid, all in hull), got {len(result)}"
    for row in result:
        assert row["elevation_point"] is not None, "elevation_point should be non-null"
        assert isinstance(
            row["elevation_point"], (bytes, bytearray)
        ), f"Expected bytes (WKB POINT Z), got {type(row['elevation_point'])}"
        assert (
            len(row["elevation_point"]) > 0
        ), "elevation_point WKB bytes should be non-empty"


def test_st_interpolateelevationgeom_sql_example(vectorx_registered):
    """Run the ``gbx_st_interpolateelevationgeom`` SQL example against ``tin_survey``.

    3×3 origin-anchored grid → 9 POINT Z rows (all cell centres inside the TIN hull).
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_interpolateelevationgeom_sql_example()
    result = spark.sql(_statement(sql)).collect()
    assert (
        len(result) == 9
    ), f"Expected 9 elevation points (3×3 origin-anchored grid), got {len(result)}"
    for row in result:
        assert row["elevation_point"] is not None, "elevation_point should be non-null"
        assert isinstance(
            row["elevation_point"], (bytes, bytearray)
        ), f"Expected bytes (WKB POINT Z), got {type(row['elevation_point'])}"
        assert (
            len(row["elevation_point"]) > 0
        ), "elevation_point WKB bytes should be non-empty"


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


def test_st_legacyaswkb_sql_example(vectorx_registered):
    """Run the ``gbx_st_legacyaswkb`` SQL example against the ``legacy_geoms`` fixture view.

    The fixture has 1 row encoding POINT(13, 42) as a legacy Mosaic InternalGeometry struct.
    The SQL returns raw BINARY WKB (column ``wkb``); we assert non-null and non-empty bytes,
    then parse via shapely to confirm the geometry type is a POINT.
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_legacyaswkb_sql_example()
    result = spark.sql(_statement(sql)).collect()
    assert (
        len(result) == 1
    ), f"Expected 1 row from legacy_geoms fixture, got {len(result)}"
    wkb = result[0]["wkb"]
    assert wkb is not None, "st_legacyaswkb should return non-null WKB bytes"
    assert isinstance(
        wkb, (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(wkb)}"
    assert len(wkb) > 0, "WKB bytes should be non-empty"
    from shapely.wkb import loads as wkb_loads  # noqa: PLC0415

    geom = wkb_loads(bytes(wkb))
    assert (
        geom.geom_type == "Point"
    ), f"Expected Point geometry type, got {geom.geom_type}"
