"""
Test structure of GridX (BNG) functions documentation examples.

Structural tests (hasattr) run without Spark; execution tests for
*_python_heavy_example functions require the bng_heavy_setup fixture.
"""

import pytest

# Allow imports to work even if pyspark not available
try:
    from . import gridx_functions
except (ModuleNotFoundError, ImportError):
    try:
        import gridx_functions
    except ModuleNotFoundError:
        # PySpark not available, create placeholder
        pass


# Common setup
def test_gridx_setup_example():
    assert hasattr(gridx_functions, "gridx_setup_example")
    assert callable(gridx_functions.gridx_setup_example)
    assert hasattr(gridx_functions, "gridx_setup_example_output")


# Conversion Functions
def test_bng_aswkb_example():
    assert hasattr(gridx_functions, "bng_aswkb_example")
    assert callable(gridx_functions.bng_aswkb_example)


def test_bng_aswkt_example():
    assert hasattr(gridx_functions, "bng_aswkt_example")
    assert callable(gridx_functions.bng_aswkt_example)


# Core Functions
def test_bng_cellarea_example():
    assert hasattr(gridx_functions, "bng_cellarea_example")
    assert callable(gridx_functions.bng_cellarea_example)


def test_bng_centroid_example():
    assert hasattr(gridx_functions, "bng_centroid_example")
    assert callable(gridx_functions.bng_centroid_example)


def test_bng_distance_example():
    assert hasattr(gridx_functions, "bng_distance_example")
    assert callable(gridx_functions.bng_distance_example)


def test_bng_euclideandistance_example():
    assert hasattr(gridx_functions, "bng_euclideandistance_example")
    assert callable(gridx_functions.bng_euclideandistance_example)


# Cell Operations
def test_bng_cellintersection_example():
    assert hasattr(gridx_functions, "bng_cellintersection_example")
    assert callable(gridx_functions.bng_cellintersection_example)


def test_bng_cellunion_example():
    assert hasattr(gridx_functions, "bng_cellunion_example")
    assert callable(gridx_functions.bng_cellunion_example)


# Coordinate Conversion
def test_bng_eastnorthasbng_example():
    assert hasattr(gridx_functions, "bng_eastnorthasbng_example")
    assert callable(gridx_functions.bng_eastnorthasbng_example)


def test_bng_pointascell_example():
    assert hasattr(gridx_functions, "bng_pointascell_example")
    assert callable(gridx_functions.bng_pointascell_example)


def test_bng_pointascell_python_api_example():
    assert hasattr(gridx_functions, "bng_pointascell_python_api_example")
    assert callable(gridx_functions.bng_pointascell_python_api_example)


# K-Ring Functions
def test_bng_kring_example():
    assert hasattr(gridx_functions, "bng_kring_example")
    assert callable(gridx_functions.bng_kring_example)


def test_bng_kloop_example():
    assert hasattr(gridx_functions, "bng_kloop_example")
    assert callable(gridx_functions.bng_kloop_example)


# Note: bng_geomkring_example, bng_geomkloop_example, bng_polyfill_example,
# bng_tessellate_example: requires DBR for st_geomfromtext; tested here when view/sql available.


# Aggregator Functions
def test_bng_cellintersection_agg_example():
    assert hasattr(gridx_functions, "bng_cellintersection_agg_example")
    assert callable(gridx_functions.bng_cellintersection_agg_example)


def test_bng_cellunion_agg_example():
    assert hasattr(gridx_functions, "bng_cellunion_agg_example")
    assert callable(gridx_functions.bng_cellunion_agg_example)


# Generator Functions
def test_bng_kringexplode_example():
    assert hasattr(gridx_functions, "bng_kringexplode_example")
    assert callable(gridx_functions.bng_kringexplode_example)


def test_bng_kloopexplode_example():
    assert hasattr(gridx_functions, "bng_kloopexplode_example")
    assert callable(gridx_functions.bng_kloopexplode_example)


# Note: bng_geomkringexplode_example, bng_geomkloopexplode_example,
# bng_tessellateexplode_example: requires DBR for st_geomfromtext; tested here when available.


# ---------------------------------------------------------------------------
# BNG aggregators — the heavy-tier fixture
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bng_heavy_setup(spark):
    """Register BNG (heavyweight bx) + create the ten GridX heavy Setup views.

    Fixture views created: ``bng_cells``, ``bng_cell_pairs``, ``bng_points``,
    ``bng_polygons``, ``bng_chips``, ``quadbin_cells``, ``quadbin_cell_pairs``,
    ``quadbin_polygons``, ``quadbin_kring_cells``, ``custom_grids``.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415
    from ._fixtures import create_setup_views_gridx_heavy  # noqa: PLC0415

    bx.register(spark)
    create_setup_views_gridx_heavy(spark)
    yield spark


# ---------------------------------------------------------------------------
# T5: BNG aggregators — heavy-tier structural checks
# bng_cellintersection_agg, bng_cellunion_agg
#
# Note: the 5 BNG *explode UDTFs have no Python Column form on any tier;
# only the 2 aggregators have *_python_heavy_example functions.
# ---------------------------------------------------------------------------


def test_bng_cellintersection_agg_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_cellintersection_agg_python_heavy_example")
    assert callable(gridx_functions.bng_cellintersection_agg_python_heavy_example)
    assert hasattr(
        gridx_functions, "bng_cellintersection_agg_python_heavy_example_output"
    )


def test_bng_cellunion_agg_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_cellunion_agg_python_heavy_example")
    assert callable(gridx_functions.bng_cellunion_agg_python_heavy_example)
    assert hasattr(gridx_functions, "bng_cellunion_agg_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T5: BNG aggregators — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# ---------------------------------------------------------------------------


def test_bng_cellintersection_agg_python_heavy_example(bng_heavy_setup):
    """bng_cellintersection_agg (heavy) returns STRUCT with cellid=TQ3080, core=True."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_cellintersection_agg_python_heavy_example(spark)
    assert (
        result is not None
    ), "bng_cellintersection_agg (heavy) should return non-null chip struct for TQ3080"
    assert (
        result["cellid"] == "TQ3080"
    ), f"Expected cellid='TQ3080' in common_chip struct, got {result['cellid']!r}"
    assert (
        result["core"] is True
    ), f"Expected core=True for TQ3080 core chip, got {result['core']!r}"


def test_bng_cellunion_agg_python_heavy_example(bng_heavy_setup):
    """bng_cellunion_agg (heavy) returns STRUCT with cellid=TQ3080, core=True."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_cellunion_agg_python_heavy_example(spark)
    assert (
        result is not None
    ), "bng_cellunion_agg (heavy) should return non-null chip struct for TQ3080"
    assert (
        result["cellid"] == "TQ3080"
    ), f"Expected cellid='TQ3080' in union_chip struct, got {result['cellid']!r}"
    assert (
        result["core"] is True
    ), f"Expected core=True for TQ3080 core chip, got {result['core']!r}"


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg — heavy-tier structural checks
# quadbin_kring, quadbin_polyfill, quadbin_tessellate,
# quadbin_cellunion, quadbin_cellunion_agg
# ---------------------------------------------------------------------------


def test_quadbin_cellunion_agg_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_cellunion_agg_python_heavy_example")
    assert callable(gridx_functions.quadbin_cellunion_agg_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_cellunion_agg_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# The quadbin heavy functions use the quadbin subpackage (gridx.quadbin),
# so we also register gridx.quadbin within each test.
# ---------------------------------------------------------------------------


def test_quadbin_cellunion_agg_python_heavy_example(bng_heavy_setup):
    """quadbin_cellunion_agg (heavy) returns non-empty BINARY EWKB (both tiers = BINARY)."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_cellunion_agg_python_heavy_example(spark)
    assert (
        result is not None
    ), "quadbin_cellunion_agg (heavy) should return non-null BINARY EWKB"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (BINARY EWKB), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"
