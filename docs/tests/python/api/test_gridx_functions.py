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
    from databricks.labs.gbx import manager as gbx_manager
    from ._fixtures import create_setup_views_gridx_heavy  # noqa: PLC0415

    gbx_manager.register_family(spark, ('gbx_bng_',))
    create_setup_views_gridx_heavy(spark)
    yield spark


# ---------------------------------------------------------------------------
# T5: BNG aggregators — heavy-tier structural checks
# bng_cellintersection_agg, bng_cellunion_agg
#
# Note: the 5 BNG *explode UDTFs have no Python Column form on any tier;
# only the 2 aggregators have *_python_heavy_example functions.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T5: BNG aggregators — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg — heavy-tier structural checks
# quadbin_kring, quadbin_polyfill, quadbin_tessellate,
# quadbin_cellunion, quadbin_cellunion_agg
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# The quadbin heavy functions use the quadbin subpackage (gridx.quadbin),
# so we also register gridx.quadbin within each test.
# ---------------------------------------------------------------------------

