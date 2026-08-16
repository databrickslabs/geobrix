"""
Tests for VectorX Function Reference Examples

Validates heavy-tier (vectorx) Python examples: st_legacyaswkb and
the CRS family (st_crs, st_setcrs, st_transformcrs).
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import vectorx_functions


@pytest.fixture(scope="module")
def vectorx_heavy_setup(spark):
    """Register VectorX (full) and create setup views for heavy-tier CRS examples.

    The ``vector_geoms`` view (1 row: geom = 'SRID=4326;POINT (13 42)') is used
    by all three CRS heavy Python examples; this fixture creates it once per module.
    """
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415
    from ._fixtures import create_setup_views_vectorx_heavy  # noqa: PLC0415

    vx.register(spark)
    create_setup_views_vectorx_heavy(spark)
    yield spark


def test_vectorx_setup_example():
    """Common setup example exists and has output constant."""
    assert hasattr(vectorx_functions, "vectorx_setup_example")
    assert callable(vectorx_functions.vectorx_setup_example)
    assert hasattr(vectorx_functions, "vectorx_setup_example_output")


def test_st_legacyaswkb_sql_example_constant():
    """SQL example constant exists for docs."""
    assert hasattr(vectorx_functions, "ST_LEGACYASWKB_SQL_EXAMPLE")
    assert "gbx_st_legacyaswkb" in vectorx_functions.ST_LEGACYASWKB_SQL_EXAMPLE
    assert hasattr(vectorx_functions, "ST_LEGACYASWKB_SQL_EXAMPLE_output")


def test_st_legacyaswkb_python_example_callable(spark):
    """st_legacyaswkb_python_example is defined and callable."""
    assert callable(vectorx_functions.st_legacyaswkb_python_example)


def test_st_legacyaswkb_python_example_executes(spark):
    """st_legacyaswkb_python_example runs and returns one row with wkb column."""
    result = vectorx_functions.st_legacyaswkb_python_example(spark)
    rows = result.collect()
    assert len(rows) == 1
    assert "wkb" in result.columns
    assert rows[0]["wkb"] is not None


# ---------------------------------------------------------------------------
# T4: TIN family heavy-tier tests
# Fixture: ``tin_survey`` view — 1 row: pts ARRAY<BINARY> (4 POINT Z), bl empty.
# The 4 points form a 10×10 m square → 2 Delaunay triangles.
# ---------------------------------------------------------------------------


def test_st_triangulate_python_heavy_example(vectorx_heavy_setup):
    """st_triangulate Generator Column emits 2 triangle WKBs for the 4-corner fixture."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_triangulate_python_heavy_example(spark)
    assert result is not None, "st_triangulate should return non-null triangle bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB polygon), got {type(result)}"
    assert len(result) > 0, "triangle WKB bytes should be non-empty"


def test_st_interpolateelevationbbox_python_heavy_example(vectorx_heavy_setup):
    """st_interpolateelevationbbox emits 9 POINT Z rows for the 3×3 grid fixture."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_interpolateelevationbbox_python_heavy_example(spark)
    assert result is not None, "st_interpolateelevationbbox should return non-null bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB POINT Z), got {type(result)}"
    assert len(result) > 0, "elevation_point WKB bytes should be non-empty"


def test_st_interpolateelevationgeom_python_heavy_example(vectorx_heavy_setup):
    """st_interpolateelevationgeom emits 9 POINT Z rows for the 3×3 origin-anchored grid."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_interpolateelevationgeom_python_heavy_example(spark)
    assert result is not None, "st_interpolateelevationgeom should return non-null bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB POINT Z), got {type(result)}"
    assert len(result) > 0, "elevation_point WKB bytes should be non-empty"


# ---------------------------------------------------------------------------
# T3: Vector-tile family heavy-tier tests
# Fixtures: ``mvt_features`` view (for st_asmvt) and inline WGS-84 data
#            (for st_asmvt_pyramid) — both created/available via vectorx_heavy_setup.
# ---------------------------------------------------------------------------


def test_st_asmvt_python_heavy_example(vectorx_heavy_setup):
    """st_asmvt returns non-empty MVT BINARY for the tile-local mvt_features fixture."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_asmvt_python_heavy_example(spark)
    assert result is not None, "st_asmvt should return non-null MVT bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (MVT BINARY), got {type(result)}"
    assert len(result) > 0, "MVT bytes should be non-empty"


def test_st_asmvt_pyramid_python_heavy_example(vectorx_heavy_setup):
    """st_asmvt_pyramid Generator Column emits non-empty MVT bytes for WGS-84 POINT(0,0)."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_asmvt_pyramid_python_heavy_example(spark)
    assert (
        result is not None
    ), "st_asmvt_pyramid should emit at least one tile for POINT(0,0) zoom 0-2"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (MVT BINARY), got {type(result)}"
    assert len(result) > 0, "MVT bytes should be non-empty"


# ---------------------------------------------------------------------------
# T2: CRS family heavy-tier tests
# Fixture: ``vector_geoms`` view via ``vectorx_heavy_setup``.
# ---------------------------------------------------------------------------


def test_st_crs_python_heavy_example(vectorx_heavy_setup):
    """st_crs returns 'EPSG:4326' for the SRID=4326 EWKT fixture."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_crs_python_heavy_example(spark)
    assert result == "EPSG:4326", f"Expected EPSG:4326, got {result!r}"


def test_st_setcrs_python_heavy_example(vectorx_heavy_setup):
    """st_setcrs returns non-null EWKB bytes with SRID=4326 stamped."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_setcrs_python_heavy_example(spark)
    assert result is not None, "st_setcrs should not return None"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


def test_st_transformcrs_python_heavy_example(vectorx_heavy_setup):
    """st_transformcrs returns non-null EWKB bytes for in-domain POINT(13,42) -> EPSG:32633."""
    spark = vectorx_heavy_setup
    result = vectorx_functions.st_transformcrs_python_heavy_example(spark)
    # POINT(13, 42) is inside UTM zone 33N's area of use — must be non-null.
    assert (
        result is not None
    ), "st_transformcrs with in-domain coords should not return None"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"
