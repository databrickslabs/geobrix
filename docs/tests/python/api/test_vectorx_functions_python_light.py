"""
Tests for the VectorX light-tier (pyvx) Python examples.

Verifies that the scaffold module imports cleanly, the setup function exists
and executes, and the autouse view fixture wires the four VectorX temp views.

Per-function example tests are added by T2–T5 (one batch per function family).
"""

import pytest

try:
    from . import vectorx_functions_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import vectorx_functions_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


@pytest.fixture(autouse=True)
def _vectorx_light_setup_views(spark):
    """Register pyvx + create the four VectorX light-tier Setup views so every
    per-function example can read ``spark.table("tin_survey")`` etc.

    Mirrors the test/setup pattern from test_rasterx_*_python_light.py.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from ._fixtures import create_setup_views_vectorx_light  # noqa: PLC0415

    vx.register(spark)
    create_setup_views_vectorx_light(spark)


# ---------------------------------------------------------------------------
# Module-level smoke tests (always run, even before per-function tests exist)
# ---------------------------------------------------------------------------


def test_vectorx_light_module_imports():
    """vectorx_functions_python_light module imports cleanly."""
    assert light_examples is not None, (
        "vectorx_functions_python_light failed to import — check for syntax errors "
        "or missing dependencies."
    )


def test_vectorx_light_setup_function_exists():
    """vectorx_light_setup_example() is defined and has an output constant."""
    assert hasattr(
        light_examples, "vectorx_light_setup_example"
    ), "vectorx_light_setup_example function missing from module"
    assert callable(
        light_examples.vectorx_light_setup_example
    ), "vectorx_light_setup_example is not callable"
    assert hasattr(
        light_examples, "vectorx_light_setup_example_output"
    ), "vectorx_light_setup_example_output constant missing"


def test_vectorx_light_setup_executes(spark):
    """vectorx_light_setup_example(spark) runs without raising."""
    # Re-registration is idempotent; the autouse fixture already called it.
    light_examples.vectorx_light_setup_example(spark)


def test_vectorx_light_setup_views_created(spark):
    """The four VectorX Setup views are accessible via spark.table()."""
    for view in ("tin_survey", "mvt_features", "vector_geoms", "legacy_geoms"):
        df = spark.table(view)
        assert (
            df.count() >= 1
        ), f"VectorX light view '{view}' is empty after autouse setup"


# ---------------------------------------------------------------------------
# Per-function tests are appended below by T2–T5.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T3: Vector-tile family — st_asmvt, st_asmvt_pyramid
# ---------------------------------------------------------------------------


def test_st_asmvt_python_light_example(spark):
    """st_asmvt returns non-empty MVT BINARY for the tile-local mvt_features fixture."""
    assert light_examples is not None
    result = light_examples.st_asmvt_python_light_example(spark)
    assert result is not None, "st_asmvt should return non-null MVT bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (MVT BINARY), got {type(result)}"
    assert len(result) > 0, "MVT bytes should be non-empty"


def test_st_asmvt_pyramid_python_light_example(spark):
    """st_asmvt_pyramid LATERAL emits >=1 tile row for WGS-84 POINT(0,0) zoom 0-2."""
    assert light_examples is not None
    result = light_examples.st_asmvt_pyramid_python_light_example(spark)
    # Returns the z value of the first row — should be a non-negative integer.
    assert (
        result is not None
    ), "st_asmvt_pyramid should emit at least one tile for POINT(0,0) zoom 0-2"
    assert isinstance(result, int), f"Expected int z value, got {type(result)!r}"
    assert result >= 0, f"Zoom level should be >= 0, got {result!r}"


# ---------------------------------------------------------------------------
# T2: CRS family — st_crs, st_setcrs, st_transformcrs
# Fixture view: ``vector_geoms`` — 1 row: geom = 'SRID=4326;POINT (13 42)'
# ---------------------------------------------------------------------------


def test_st_crs_python_light_example(spark):
    """st_crs returns 'EPSG:4326' for the SRID=4326 EWKT fixture."""
    assert light_examples is not None
    result = light_examples.st_crs_python_light_example(spark)
    assert result == "EPSG:4326", f"Expected EPSG:4326, got {result!r}"


def test_st_setcrs_python_light_example(spark):
    """st_setcrs returns non-null EWKB bytes with SRID=4326 stamped."""
    assert light_examples is not None
    result = light_examples.st_setcrs_python_light_example(spark)
    assert result is not None, "st_setcrs should not return None for in-domain input"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


def test_st_transformcrs_python_light_example(spark):
    """st_transformcrs returns non-null EWKB bytes for in-domain POINT(13,42) -> EPSG:32633."""
    assert light_examples is not None
    result = light_examples.st_transformcrs_python_light_example(spark)
    # POINT(13, 42) is inside UTM zone 33N's area of use — must be non-null.
    assert (
        result is not None
    ), "st_transformcrs with in-domain coords (POINT(13,42) -> EPSG:32633) should not return None"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


def test_st_setcrs_stamps_different_crs(spark):
    """st_setcrs changes the embedded SRID — not a no-op when target differs from source."""
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    # Stamp EPSG:3857 (not the fixture's 4326) and read back via st_crs.
    result = df.select(
        vx.st_crs(vx.st_setcrs("geom", "EPSG:3857")).alias("new_crs")
    ).first()
    assert (
        result["new_crs"] == "EPSG:3857"
    ), f"Expected EPSG:3857 after stamping (not a no-op), got {result['new_crs']!r}"
