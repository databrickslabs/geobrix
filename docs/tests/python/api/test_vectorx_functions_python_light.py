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
    assert hasattr(light_examples, "vectorx_light_setup_example"), (
        "vectorx_light_setup_example function missing from module"
    )
    assert callable(light_examples.vectorx_light_setup_example), (
        "vectorx_light_setup_example is not callable"
    )
    assert hasattr(light_examples, "vectorx_light_setup_example_output"), (
        "vectorx_light_setup_example_output constant missing"
    )


def test_vectorx_light_setup_executes(spark):
    """vectorx_light_setup_example(spark) runs without raising."""
    # Re-registration is idempotent; the autouse fixture already called it.
    light_examples.vectorx_light_setup_example(spark)


def test_vectorx_light_setup_views_created(spark):
    """The four VectorX Setup views are accessible via spark.table()."""
    for view in ("tin_survey", "mvt_features", "vector_geoms", "legacy_geoms"):
        df = spark.table(view)
        assert df.count() >= 1, f"VectorX light view '{view}' is empty after autouse setup"


# ---------------------------------------------------------------------------
# Per-function tests are appended below by T2–T5.
# ---------------------------------------------------------------------------
