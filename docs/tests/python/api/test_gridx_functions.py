"""
Test structure of GridX (BNG) functions documentation examples.

Structural tests (hasattr) run without Spark; execution tests for
*_python_heavy_example functions require the bng_heavy_setup fixture.
"""

import sys
from pathlib import Path

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
# T2: BNG codec + accessors — heavy-tier structural checks
# ---------------------------------------------------------------------------


def test_bng_aswkb_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_aswkb_python_heavy_example")
    assert callable(gridx_functions.bng_aswkb_python_heavy_example)
    assert hasattr(gridx_functions, "bng_aswkb_python_heavy_example_output")


def test_bng_aswkt_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_aswkt_python_heavy_example")
    assert callable(gridx_functions.bng_aswkt_python_heavy_example)
    assert hasattr(gridx_functions, "bng_aswkt_python_heavy_example_output")


def test_bng_cellarea_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_cellarea_python_heavy_example")
    assert callable(gridx_functions.bng_cellarea_python_heavy_example)
    assert hasattr(gridx_functions, "bng_cellarea_python_heavy_example_output")


def test_bng_centroid_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_centroid_python_heavy_example")
    assert callable(gridx_functions.bng_centroid_python_heavy_example)
    assert hasattr(gridx_functions, "bng_centroid_python_heavy_example_output")


def test_bng_eastnorthasbng_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_eastnorthasbng_python_heavy_example")
    assert callable(gridx_functions.bng_eastnorthasbng_python_heavy_example)
    assert hasattr(gridx_functions, "bng_eastnorthasbng_python_heavy_example_output")


def test_bng_pointascell_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_pointascell_python_heavy_example")
    assert callable(gridx_functions.bng_pointascell_python_heavy_example)
    assert hasattr(gridx_functions, "bng_pointascell_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T2: BNG codec + accessors — heavy-tier execution tests
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


def test_bng_aswkb_python_heavy_example(bng_heavy_setup):
    """bng_aswkb (heavy) returns non-empty WKB bytes for 'TQ3080'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_aswkb_python_heavy_example(spark)
    assert result is not None, "bng_aswkb (heavy) should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(result)}"
    assert len(result) > 0, "WKB bytes should be non-empty"


def test_bng_aswkt_python_heavy_example(bng_heavy_setup):
    """bng_aswkt (heavy) returns the WKT polygon string for 'TQ3080'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_aswkt_python_heavy_example(spark)
    assert result is not None, "bng_aswkt (heavy) should return non-null WKT string"
    assert isinstance(result, str), f"Expected str (WKT), got {type(result)}"
    assert result.startswith("POLYGON"), f"Expected POLYGON WKT, got {result!r}"
    assert "531000" in result, f"Expected easting 531000 in WKT, got {result!r}"


def test_bng_cellarea_python_heavy_example(bng_heavy_setup):
    """bng_cellarea (heavy) returns 1.0 sq km for 'TQ3080'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_cellarea_python_heavy_example(spark)
    assert result is not None, "bng_cellarea (heavy) should return non-null area"
    assert result == 1.0, f"Expected 1.0 sq km for TQ3080, got {result!r}"


def test_bng_centroid_python_heavy_example(bng_heavy_setup):
    """bng_centroid (heavy) returns non-empty WKB bytes for 'TQ3080'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_centroid_python_heavy_example(spark)
    assert result is not None, "bng_centroid (heavy) should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB POINT), got {type(result)}"
    assert len(result) > 0, "WKB centroid bytes should be non-empty"


def test_bng_eastnorthasbng_python_heavy_example(bng_heavy_setup):
    """bng_eastnorthasbng (heavy) returns 'TQ3080' for easting=530000, northing=180000."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_eastnorthasbng_python_heavy_example(spark)
    assert (
        result == "TQ3080"
    ), f"Expected 'TQ3080' for easting=530000, northing=180000, res='1km', got {result!r}"


def test_bng_pointascell_python_heavy_example(bng_heavy_setup):
    """bng_pointascell (heavy) returns 'TQ3080' for POINT(530000 180000) at res='1km'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_pointascell_python_heavy_example(spark)
    assert (
        result == "TQ3080"
    ), f"Expected 'TQ3080' for POINT(530000 180000) at res='1km', got {result!r}"


# ---------------------------------------------------------------------------
# T3: BNG distance + chip ops — heavy-tier structural checks
# ---------------------------------------------------------------------------


def test_bng_distance_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_distance_python_heavy_example")
    assert callable(gridx_functions.bng_distance_python_heavy_example)
    assert hasattr(gridx_functions, "bng_distance_python_heavy_example_output")


def test_bng_euclideandistance_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_euclideandistance_python_heavy_example")
    assert callable(gridx_functions.bng_euclideandistance_python_heavy_example)
    assert hasattr(gridx_functions, "bng_euclideandistance_python_heavy_example_output")


def test_bng_cellintersection_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_cellintersection_python_heavy_example")
    assert callable(gridx_functions.bng_cellintersection_python_heavy_example)
    assert hasattr(gridx_functions, "bng_cellintersection_python_heavy_example_output")


def test_bng_cellunion_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_cellunion_python_heavy_example")
    assert callable(gridx_functions.bng_cellunion_python_heavy_example)
    assert hasattr(gridx_functions, "bng_cellunion_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T3: BNG distance + chip ops — heavy-tier execution tests
# ---------------------------------------------------------------------------


def test_bng_distance_python_heavy_example(bng_heavy_setup):
    """bng_distance (heavy) returns 1 for adjacent cells 'TQ3080' and 'TQ3081'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_distance_python_heavy_example(spark)
    assert result is not None, "bng_distance (heavy) should return non-null value"
    assert (
        result == 1
    ), f"Expected grid-step distance 1 for adjacent cells, got {result!r}"


def test_bng_euclideandistance_python_heavy_example(bng_heavy_setup):
    """bng_euclideandistance (heavy) returns 1 for adjacent cells 'TQ3080' and 'TQ3081'."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_euclideandistance_python_heavy_example(spark)
    assert (
        result is not None
    ), "bng_euclideandistance (heavy) should return non-null value"
    assert (
        result == 1
    ), f"Expected Chebyshev distance 1 for adjacent cells, got {result!r}"


def test_bng_cellintersection_python_heavy_example(bng_heavy_setup):
    """bng_cellintersection (heavy) returns core chip struct for TQ3080."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_cellintersection_python_heavy_example(spark)
    assert (
        result is not None
    ), "bng_cellintersection (heavy) should return non-null chip struct"
    assert (
        result["cellid"] == "TQ3080"
    ), f"Expected cellid='TQ3080', got {result['cellid']!r}"
    assert (
        result["core"] is True
    ), f"Expected core=True for the interior TQ3080 chip, got {result['core']!r}"


def test_bng_cellunion_python_heavy_example(bng_heavy_setup):
    """bng_cellunion (heavy) returns core chip struct for TQ3080."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_cellunion_python_heavy_example(spark)
    assert (
        result is not None
    ), "bng_cellunion (heavy) should return non-null chip struct"
    assert (
        result["cellid"] == "TQ3080"
    ), f"Expected cellid='TQ3080', got {result['cellid']!r}"
    assert (
        result["core"] is True
    ), f"Expected core=True for the interior TQ3080 chip, got {result['core']!r}"


# ---------------------------------------------------------------------------
# T4: BNG neighbourhood/fill — heavy-tier structural checks
# bng_kring, bng_kloop, bng_geomkring, bng_geomkloop, bng_polyfill,
# bng_tessellate
# ---------------------------------------------------------------------------


def test_bng_kring_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_kring_python_heavy_example")
    assert callable(gridx_functions.bng_kring_python_heavy_example)
    assert hasattr(gridx_functions, "bng_kring_python_heavy_example_output")


def test_bng_kloop_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_kloop_python_heavy_example")
    assert callable(gridx_functions.bng_kloop_python_heavy_example)
    assert hasattr(gridx_functions, "bng_kloop_python_heavy_example_output")


def test_bng_geomkring_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_geomkring_python_heavy_example")
    assert callable(gridx_functions.bng_geomkring_python_heavy_example)
    assert hasattr(gridx_functions, "bng_geomkring_python_heavy_example_output")


def test_bng_geomkloop_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_geomkloop_python_heavy_example")
    assert callable(gridx_functions.bng_geomkloop_python_heavy_example)
    assert hasattr(gridx_functions, "bng_geomkloop_python_heavy_example_output")


def test_bng_polyfill_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_polyfill_python_heavy_example")
    assert callable(gridx_functions.bng_polyfill_python_heavy_example)
    assert hasattr(gridx_functions, "bng_polyfill_python_heavy_example_output")


def test_bng_tessellate_python_heavy_example_exists():
    assert hasattr(gridx_functions, "bng_tessellate_python_heavy_example")
    assert callable(gridx_functions.bng_tessellate_python_heavy_example)
    assert hasattr(gridx_functions, "bng_tessellate_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T4: BNG neighbourhood/fill — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# ---------------------------------------------------------------------------


def test_bng_kring_python_heavy_example(bng_heavy_setup):
    """bng_kring (heavy) returns a 9-cell list for 'TQ3080' at k=1."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_kring_python_heavy_example(spark)
    assert result is not None, "bng_kring (heavy) should return non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 9
    ), f"Expected 9 cells for k=1 ring around TQ3080, got {len(result)}"
    assert "TQ3080" in result, "Center cell TQ3080 should be in the k=1 ring"


def test_bng_kloop_python_heavy_example(bng_heavy_setup):
    """bng_kloop (heavy) returns an 8-cell list for 'TQ3080' at k=1 (center excluded)."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_kloop_python_heavy_example(spark)
    assert result is not None, "bng_kloop (heavy) should return non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert len(result) == 8, f"Expected 8 cells for k=1 hollow ring, got {len(result)}"
    assert "TQ3080" not in result, "Center cell TQ3080 should NOT be in the k=1 loop"


def test_bng_geomkring_python_heavy_example(bng_heavy_setup):
    """bng_geomkring (heavy) returns 25 cells for the BNG polygon at res=3, k=1."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_geomkring_python_heavy_example(spark)
    assert result is not None, "bng_geomkring (heavy) should return non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 25
    ), f"Expected 25 cells for geomkring(BNG polygon, res=3, k=1), got {len(result)}"


def test_bng_geomkloop_python_heavy_example(bng_heavy_setup):
    """bng_geomkloop (heavy) returns 16 cells for the BNG polygon at res=3, k=1."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_geomkloop_python_heavy_example(spark)
    assert result is not None, "bng_geomkloop (heavy) should return non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 16
    ), f"Expected 16 cells for geomkloop(BNG polygon, res=3, k=1), got {len(result)}"


def test_bng_polyfill_python_heavy_example(bng_heavy_setup):
    """bng_polyfill (heavy) returns 9 cells for the 3km × 3km BNG polygon at res=3."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_polyfill_python_heavy_example(spark)
    assert result is not None, "bng_polyfill (heavy) should return non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 9
    ), f"Expected 9 cells for polyfill(BNG polygon, res=3), got {len(result)}"
    assert "TQ3080" in result, "Center cell TQ3080 should be in the polyfill"


def test_bng_tessellate_python_heavy_example(bng_heavy_setup):
    """bng_tessellate (heavy) returns 9 chips for the 3km × 3km BNG polygon at res=3."""
    spark = bng_heavy_setup
    result = gridx_functions.bng_tessellate_python_heavy_example(spark)
    assert result is not None, "bng_tessellate (heavy) should return non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRUCT>), got {type(result)}"
    assert (
        len(result) == 9
    ), f"Expected 9 chips for tessellate(BNG polygon, res=3), got {len(result)}"
    core_chips = [c for c in result if c["core"] is True]
    assert len(core_chips) >= 1, "Expected at least one core chip"
    assert "TQ3080" in {
        c["cellid"] for c in core_chips
    }, "Expected TQ3080 to be the core chip"


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
# T6a: Quadbin codec + scalar — heavy-tier structural checks
# quadbin_pointascell, quadbin_aswkb, quadbin_centroid,
# quadbin_resolution, quadbin_distance
# ---------------------------------------------------------------------------


def test_quadbin_pointascell_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_pointascell_python_heavy_example")
    assert callable(gridx_functions.quadbin_pointascell_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_pointascell_python_heavy_example_output")


def test_quadbin_aswkb_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_aswkb_python_heavy_example")
    assert callable(gridx_functions.quadbin_aswkb_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_aswkb_python_heavy_example_output")


def test_quadbin_centroid_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_centroid_python_heavy_example")
    assert callable(gridx_functions.quadbin_centroid_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_centroid_python_heavy_example_output")


def test_quadbin_resolution_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_resolution_python_heavy_example")
    assert callable(gridx_functions.quadbin_resolution_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_resolution_python_heavy_example_output")


def test_quadbin_distance_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_distance_python_heavy_example")
    assert callable(gridx_functions.quadbin_distance_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_distance_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T6a: Quadbin codec + scalar — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# The quadbin heavy functions use the quadbin subpackage (gridx.quadbin),
# so we also register gridx.quadbin within each test.
# ---------------------------------------------------------------------------


def test_quadbin_pointascell_python_heavy_example(bng_heavy_setup):
    """quadbin_pointascell (heavy) returns 5233961839712272383 for SF at zoom 10."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_pointascell_python_heavy_example(spark)
    assert (
        result is not None
    ), "quadbin_pointascell (heavy) should return non-null cell ID"
    assert (
        result == 5233961839712272383
    ), f"Expected SF z10 cell 5233961839712272383, got {result!r}"


def test_quadbin_aswkb_python_heavy_example(bng_heavy_setup):
    """quadbin_aswkb (heavy) returns non-empty EWKB bytes for SF z10 cell."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_aswkb_python_heavy_example(spark)
    assert result is not None, "quadbin_aswkb (heavy) should return non-null EWKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB binary), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


def test_quadbin_centroid_python_heavy_example(bng_heavy_setup):
    """quadbin_centroid (heavy) returns non-empty EWKB POINT bytes for SF z10 cell."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_centroid_python_heavy_example(spark)
    assert (
        result is not None
    ), "quadbin_centroid (heavy) should return non-null EWKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB POINT), got {type(result)}"
    assert len(result) > 0, "EWKB centroid bytes should be non-empty"


def test_quadbin_resolution_python_heavy_example(bng_heavy_setup):
    """quadbin_resolution (heavy) returns 10 for SF z10 cell."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_resolution_python_heavy_example(spark)
    assert result is not None, "quadbin_resolution (heavy) should return non-null INT"
    assert result == 10, f"Expected resolution 10 for SF z10 cell, got {result!r}"


def test_quadbin_distance_python_heavy_example(bng_heavy_setup):
    """quadbin_distance (heavy) returns 1 for two adjacent z10 cells (0.0 vs 0.1 lat)."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_distance_python_heavy_example(spark)
    assert result is not None, "quadbin_distance (heavy) should return non-null INT"
    assert (
        result == 1
    ), f"Expected Chebyshev distance 1 for adjacent z10 cells, got {result!r}"


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg — heavy-tier structural checks
# quadbin_kring, quadbin_polyfill, quadbin_tessellate,
# quadbin_cellunion, quadbin_cellunion_agg
# ---------------------------------------------------------------------------


def test_quadbin_kring_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_kring_python_heavy_example")
    assert callable(gridx_functions.quadbin_kring_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_kring_python_heavy_example_output")


def test_quadbin_polyfill_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_polyfill_python_heavy_example")
    assert callable(gridx_functions.quadbin_polyfill_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_polyfill_python_heavy_example_output")


def test_quadbin_tessellate_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_tessellate_python_heavy_example")
    assert callable(gridx_functions.quadbin_tessellate_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_tessellate_python_heavy_example_output")


def test_quadbin_cellunion_python_heavy_example_exists():
    assert hasattr(gridx_functions, "quadbin_cellunion_python_heavy_example")
    assert callable(gridx_functions.quadbin_cellunion_python_heavy_example)
    assert hasattr(gridx_functions, "quadbin_cellunion_python_heavy_example_output")


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


def test_quadbin_kring_python_heavy_example(bng_heavy_setup):
    """quadbin_kring (heavy) returns 9 cells for SF z10 at k=1."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_kring_python_heavy_example(spark)
    assert result is not None, "quadbin_kring (heavy) should return non-null array"
    assert len(result) == 9, f"Expected 9 cells at k=1, got {len(result)}"
    assert (
        5233961839712272383 in result
    ), "Expected SF z10 center cell 5233961839712272383 in kring result"


def test_quadbin_polyfill_python_heavy_example(bng_heavy_setup):
    """quadbin_polyfill (heavy) returns 4 cells for WGS84 polygon at zoom 5."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_polyfill_python_heavy_example(spark)
    assert result is not None, "quadbin_polyfill (heavy) should return non-null array"
    assert len(result) == 4, f"Expected 4 cells for polygon at z5, got {len(result)}"
    assert (
        5211790668774506495 in result
    ), "polyfill z5 result must contain the displayed first cell"


def test_quadbin_tessellate_python_heavy_example(bng_heavy_setup):
    """quadbin_tessellate (heavy) returns 4 chips for WGS84 polygon at zoom 5."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_tessellate_python_heavy_example(spark)
    assert result is not None, "quadbin_tessellate (heavy) should return non-null array"
    assert len(result) == 4, f"Expected 4 chips for polygon at z5, got {len(result)}"
    assert 5211790668774506495 in [
        c["cell"] for c in result
    ], "tessellate z5 result must contain the displayed first cell"


def test_quadbin_cellunion_python_heavy_example(bng_heavy_setup):
    """quadbin_cellunion (heavy) returns non-empty EWKB bytes for SF z10 kring."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    result = gridx_functions.quadbin_cellunion_python_heavy_example(spark)
    assert (
        result is not None
    ), "quadbin_cellunion (heavy) should return non-null EWKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB binary), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


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


# ---------------------------------------------------------------------------
# T7: Custom grid — existence tests (no Spark needed)
# ---------------------------------------------------------------------------


def test_custom_grid_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_grid_python_heavy_example")
    assert callable(gridx_functions.custom_grid_python_heavy_example)
    assert hasattr(gridx_functions, "custom_grid_python_heavy_example_output")


def test_custom_pointascell_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_pointascell_python_heavy_example")
    assert callable(gridx_functions.custom_pointascell_python_heavy_example)
    assert hasattr(gridx_functions, "custom_pointascell_python_heavy_example_output")


def test_custom_cellaswkb_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_cellaswkb_python_heavy_example")
    assert callable(gridx_functions.custom_cellaswkb_python_heavy_example)
    assert hasattr(gridx_functions, "custom_cellaswkb_python_heavy_example_output")


def test_custom_cellaswkt_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_cellaswkt_python_heavy_example")
    assert callable(gridx_functions.custom_cellaswkt_python_heavy_example)
    assert hasattr(gridx_functions, "custom_cellaswkt_python_heavy_example_output")


def test_custom_centroid_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_centroid_python_heavy_example")
    assert callable(gridx_functions.custom_centroid_python_heavy_example)
    assert hasattr(gridx_functions, "custom_centroid_python_heavy_example_output")


def test_custom_polyfill_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_polyfill_python_heavy_example")
    assert callable(gridx_functions.custom_polyfill_python_heavy_example)
    assert hasattr(gridx_functions, "custom_polyfill_python_heavy_example_output")


def test_custom_kring_python_heavy_example_exists():
    assert hasattr(gridx_functions, "custom_kring_python_heavy_example")
    assert callable(gridx_functions.custom_kring_python_heavy_example)
    assert hasattr(gridx_functions, "custom_kring_python_heavy_example_output")


# ---------------------------------------------------------------------------
# T7: Custom grid — heavy-tier execution tests
# Fixture: bng_heavy_setup registers bx + creates the ten GridX heavy views.
# The custom heavy functions use the gridx.custom subpackage (cx).
# Each test registers cx before calling the example.
# ---------------------------------------------------------------------------


def test_custom_grid_python_heavy_example(bng_heavy_setup):
    """custom_grid (heavy) returns a non-null grid descriptor struct."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_grid_python_heavy_example(spark)
    assert result is not None, "custom_grid (heavy) should return non-null grid struct"


def test_custom_pointascell_python_heavy_example(bng_heavy_setup):
    """custom_pointascell (heavy) returns 360287970373976640 for POINT(530000 180000) at res=5."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_pointascell_python_heavy_example(spark)
    assert (
        result is not None
    ), "custom_pointascell (heavy) should return non-null cell ID"
    assert (
        result == 360287970373976640
    ), f"Expected cell 360287970373976640, got {result!r}"


def test_custom_cellaswkb_python_heavy_example(bng_heavy_setup):
    """custom_cellaswkb (heavy) returns non-empty WKB bytes for cell 360287970373976640."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_cellaswkb_python_heavy_example(spark)
    assert (
        result is not None
    ), "custom_cellaswkb (heavy) should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(result)}"
    assert len(result) > 0, "WKB bytes should be non-empty"


def test_custom_cellaswkt_python_heavy_example(bng_heavy_setup):
    """custom_cellaswkt (heavy) returns a WKT POLYGON string for cell 360287970373976640."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_cellaswkt_python_heavy_example(spark)
    assert (
        result is not None
    ), "custom_cellaswkt (heavy) should return non-null WKT string"
    assert isinstance(result, str), f"Expected str (WKT), got {type(result)}"
    assert result.startswith("POLYGON"), f"Expected POLYGON WKT, got {result!r}"
    assert "530031.25" in result, f"Expected x=530031.25 in WKT, got {result!r}"


def test_custom_centroid_python_heavy_example(bng_heavy_setup):
    """custom_centroid (heavy) returns non-empty WKB bytes for cell 360287970373976640."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_centroid_python_heavy_example(spark)
    assert (
        result is not None
    ), "custom_centroid (heavy) should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB POINT), got {type(result)}"
    assert len(result) > 0, "WKB centroid bytes should be non-empty"


def test_custom_polyfill_python_heavy_example(bng_heavy_setup):
    """custom_polyfill (heavy) returns exactly 36 cells (500m) at res=1 for the 3km BNG polygon."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_polyfill_python_heavy_example(spark)
    assert result is not None, "custom_polyfill (heavy) should return non-null array"
    assert (
        len(result) == 36
    ), f"Expected 36 cells at res=1 (500m, 6 per side), got {len(result)}"
    assert (
        72057594038644994 in result
    ), "Expected first cell 72057594038644994 in polyfill result"


def test_custom_kring_python_heavy_example(bng_heavy_setup):
    """custom_kring (heavy) returns 9 cells for k=1 around cell 360287970373976640."""
    spark = bng_heavy_setup
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    result = gridx_functions.custom_kring_python_heavy_example(spark)
    assert result is not None, "custom_kring (heavy) should return non-null array"
    assert len(result) == 9, f"Expected 9 cells at k=1, got {len(result)}"
    assert (
        360287970373976640 in result
    ), "Expected center cell 360287970373976640 in kring result"
