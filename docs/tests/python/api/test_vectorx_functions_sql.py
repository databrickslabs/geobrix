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

    Registers:
    - ``vectorx`` (heavy tier): TIN, MVT, CRS functions
    - ``vectorx.jts.legacy`` (heavy tier): ``gbx_st_legacyaswkb``
    - ``pyvx`` (light tier): light-only functions including the antimeridian
      family (``gbx_st_shiftlongitude``, ``gbx_st_wrapx``, ``gbx_st_split``)

    The light registration must come after the heavy registration so the last
    write to the SQL function registry wins; both tiers register the same
    ``gbx_st_*`` SQL names, and pyvx's scalar UDFs override the heavy
    expressions for the light-only antimeridian functions (which have no heavy
    counterpart).

    Also creates the four canonical fixture views used by the SQL examples.
    """
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415
    from databricks.labs.gbx.vectorx.jts.legacy import (
        functions as legacy_vx,
    )  # noqa: PLC0415
    from databricks.labs.gbx.pyvx import functions as pyvx  # noqa: PLC0415
    from ._fixtures import create_setup_views_vectorx_heavy  # noqa: PLC0415

    vx.register(spark)
    legacy_vx.register(spark)
    pyvx.register(spark)
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


# ---------------------------------------------------------------------------
# Antimeridian family — st_shiftlongitude, st_wrapx, st_split
# These are light-only (pyvx tier) SQL UDFs registered in the fixture above.
# All three tests use inline WKT string inputs; pyvx functions accept WKT/EWKT
# strings directly via parse_geom, so no ST_AsBinary/ST_GeomFromText wrapper
# is needed in these Docker-executable examples.
# ---------------------------------------------------------------------------


def test_st_shiftlongitude_sql_example(vectorx_registered):
    """Run the ``gbx_st_shiftlongitude`` SQL example; assert BINARY output with shifted coords.

    Input: POLYGON with x in [-170, -150].  After shift all x are moved +360
    into [190, 210] (positive [0,360] space).
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_shiftlongitude_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["shifted"] is not None, "st_shiftlongitude should return non-null WKB bytes"
    assert isinstance(
        row["shifted"], (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(row['shifted'])}"
    assert len(row["shifted"]) > 0, "WKB bytes should be non-empty"
    from shapely.wkb import loads as wkb_loads  # noqa: PLC0415

    geom = wkb_loads(bytes(row["shifted"]))
    xs = [c[0] for c in geom.exterior.coords]
    assert all(x >= 0 for x in xs), (
        f"All x coords should be >= 0 after shift (x in [0,360]), got: {xs}"
    )


def test_st_wrapx_sql_example(vectorx_registered):
    """Run the ``gbx_st_wrapx`` SQL example; assert POINT(-170,10) for POINT(190,10) input.

    wrap_x_origin=180, wrap_direction=-360: any x > 180 is shifted by -360.
    x=190 → x=-170.
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_wrapx_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["wrapped"] is not None, "st_wrapx should return non-null WKB bytes"
    assert isinstance(
        row["wrapped"], (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(row['wrapped'])}"
    from shapely.wkb import loads as wkb_loads  # noqa: PLC0415

    geom = wkb_loads(bytes(row["wrapped"]))
    assert abs(geom.x - (-170.0)) < 1e-9, f"Expected x=-170, got {geom.x}"
    assert abs(geom.y - 10.0) < 1e-9, f"Expected y=10, got {geom.y}"


def test_st_split_sql_example(vectorx_registered):
    """Run the ``gbx_st_split`` SQL example; assert a 2-piece GeometryCollection.

    A polygon spanning x=[170,190] is split by the 180° meridian (x=180).
    The result is a GeometryCollection with exactly 2 polygon pieces.
    """
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_split_sql_example()
    row = spark.sql(_statement(sql)).first()
    assert row["pieces"] is not None, "st_split should return non-null WKB bytes"
    assert isinstance(
        row["pieces"], (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(row['pieces'])}"
    from shapely.wkb import loads as wkb_loads  # noqa: PLC0415

    gc = wkb_loads(bytes(row["pieces"]))
    assert gc.geom_type == "GeometryCollection", (
        f"Expected GeometryCollection, got {gc.geom_type}"
    )
    assert len(gc.geoms) == 2, f"Expected 2 pieces from antimeridian split, got {len(gc.geoms)}"


def test_antimeridian_pattern_sql_example(vectorx_registered):
    """Validate the antimeridian composition SQL string is well-formed (structural test)
    and verify the full shift→split→wrap→union chain returns a MultiPolygon (integration test).

    The composition SQL uses Databricks built-ins (ST_Dump, ST_Union, ST_Multi,
    ST_GeomFromWKB) that are available on DBR 17.3+ but not in vanilla Spark.
    The structural test checks the SQL string is non-empty and references all three
    antimeridian functions.  The integration test builds the equivalent chain using
    the pyvx Python Column API + shapely to confirm the geometry result is correct.
    """
    # Part 1: structural — the example SQL string is non-empty and references all functions
    sql = vectorx_functions_sql.antimeridian_pattern_sql_example()
    assert isinstance(sql, str) and len(sql) > 0, "antimeridian_pattern_sql_example must return a non-empty SQL string"
    assert "gbx_st_shiftlongitude" in sql, "composition SQL must reference gbx_st_shiftlongitude"
    assert "gbx_st_split" in sql, "composition SQL must reference gbx_st_split"
    assert "gbx_st_wrapx" in sql, "composition SQL must reference gbx_st_wrapx"

    # Part 2: integration — the full chain produces a valid normalized geometry
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely.wkb import loads as wkb_loads  # noqa: PLC0415
    from shapely.ops import unary_union  # noqa: PLC0415

    spark = vectorx_registered

    # shift → split: antimeridian-crossing polygon in [-180,180] space.
    # Vertices at 170°E and 170°W (-170°) — crosses the antimeridian.
    # shiftlongitude moves x=-170 → 190, making it contiguous at [170, 190]
    # so the split at x=180 divides it cleanly into two 10° pieces.
    df = spark.createDataFrame(
        [("POLYGON((170 -10, -170 -10, -170 10, 170 10, 170 -10))",)], ["geom"]
    )
    split_row = df.select(
        vx.st_split(
            vx.st_shiftlongitude("geom"),
            f.lit("LINESTRING(180 -90, 180 90)"),
        ).alias("split_geom")
    ).first()
    assert split_row["split_geom"] is not None, "shift→split chain returned None"
    gc = wkb_loads(bytes(split_row["split_geom"]))
    assert gc.geom_type == "GeometryCollection", f"Expected GeometryCollection, got {gc.geom_type}"
    assert len(gc.geoms) == 2, f"Expected 2 split pieces, got {len(gc.geoms)}"

    # wrap each piece back to [-180,180] and union into a MultiPolygon
    wrapped_parts = []
    for part in gc.geoms:
        wrap_row = (
            spark.createDataFrame([(part.wkb,)], ["geom"])
            .select(vx.st_wrapx("geom", f.lit(180.0), f.lit(-360.0)).alias("w"))
            .first()
        )
        assert wrap_row["w"] is not None, "st_wrapx returned None for a split piece"
        wrapped_parts.append(wkb_loads(bytes(wrap_row["w"])))

    normalized = unary_union(wrapped_parts)
    assert normalized.geom_type in ("MultiPolygon", "Polygon"), (
        f"Expected Polygon or MultiPolygon after union, got {normalized.geom_type}"
    )
    assert not normalized.is_empty, "Normalized geometry should not be empty"
    bounds = normalized.bounds  # (minx, miny, maxx, maxy)
    # After wrapping, all x should be within [-180, 180].
    # Note: st_wrapx uses strict x > origin (not >=), so the shared split edge
    # at x=180 stays at 180 — hence maxx == 180, not < 180.
    assert bounds[0] >= -180, f"minx should be >= -180: {bounds}"
    assert bounds[2] <= 180, f"maxx should be <= 180: {bounds}"
    # The wrap moved the western piece (originally x > 180) to negative x,
    # so the result should span both negative and positive x.
    assert bounds[0] < 0, f"Expected some negative x after wrapping: {bounds}"
    assert bounds[2] > 0, f"Expected some positive x (eastern piece): {bounds}"
