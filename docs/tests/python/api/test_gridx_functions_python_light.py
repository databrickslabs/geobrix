"""
Tests for the GridX light-tier (pygx) Python examples.

Verifies that the scaffold module imports cleanly, the setup function exists
and executes, and the autouse view fixture wires the ten GridX temp views.

Per-function example tests are added by T2–T7 (one batch per function family).
"""

import pytest

try:
    from . import gridx_functions_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import gridx_functions_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


@pytest.fixture(autouse=True)
def _gridx_light_setup_views(spark):
    """Register pygx + create the ten GridX light-tier Setup views so every
    per-function example can read ``spark.table("bng_cells")`` etc.

    Mirrors the test/setup pattern from test_vectorx_functions_python_light.py.
    """
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415
    from ._fixtures import create_setup_views_gridx_light  # noqa: PLC0415

    gx.register(spark)
    create_setup_views_gridx_light(spark)


# ---------------------------------------------------------------------------
# Module-level smoke tests (always run, even before per-function tests exist)
# ---------------------------------------------------------------------------


def test_gridx_light_module_imports():
    """gridx_functions_python_light module imports cleanly."""
    assert light_examples is not None, (
        "gridx_functions_python_light failed to import — check for syntax errors "
        "or missing dependencies."
    )


def test_gridx_light_setup_function_exists():
    """gridx_light_setup_example() is defined and has an output constant."""
    assert hasattr(
        light_examples, "gridx_light_setup_example"
    ), "gridx_light_setup_example function missing from module"
    assert callable(
        light_examples.gridx_light_setup_example
    ), "gridx_light_setup_example is not callable"
    assert hasattr(
        light_examples, "gridx_light_setup_example_output"
    ), "gridx_light_setup_example_output constant missing"


def test_gridx_light_setup_executes(spark):
    """gridx_light_setup_example(spark) runs without error."""
    assert light_examples is not None
    # Should not raise; pygx is already registered by the autouse fixture,
    # but register() is idempotent so a second call must also succeed.
    light_examples.gridx_light_setup_example(spark)


# ---------------------------------------------------------------------------
# T2: BNG codec + accessors
# bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid,
# bng_eastnorthasbng, bng_pointascell
#
# Fixture views used:
#   bng_cells  — 1 row: cellid STRING = 'TQ3080'
#   bng_points — 1 row: easting=530000, northing=180000, geom='POINT(530000 180000)'
# Both views are created by the autouse _gridx_light_setup_views fixture above.
# ---------------------------------------------------------------------------


def test_bng_aswkb_python_light_example(spark):
    """bng_aswkb returns non-empty WKB bytes for 'TQ3080'."""
    assert light_examples is not None
    result = light_examples.bng_aswkb_python_light_example(spark)
    assert result is not None, "bng_aswkb should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(result)}"
    assert len(result) > 0, "WKB bytes should be non-empty"


def test_bng_aswkt_python_light_example(spark):
    """bng_aswkt returns the WKT polygon string for 'TQ3080'."""
    assert light_examples is not None
    result = light_examples.bng_aswkt_python_light_example(spark)
    assert result is not None, "bng_aswkt should return non-null WKT string"
    assert isinstance(result, str), f"Expected str (WKT), got {type(result)}"
    assert result.startswith("POLYGON"), f"Expected POLYGON WKT, got {result!r}"
    # TQ3080 is a 1km cell; NE corner is (531000, 181000) in EPSG:27700
    assert "531000" in result, f"Expected easting 531000 in WKT, got {result!r}"


def test_bng_cellarea_python_light_example(spark):
    """bng_cellarea returns 1.0 sq km for the 1km 'TQ3080' cell."""
    assert light_examples is not None
    result = light_examples.bng_cellarea_python_light_example(spark)
    assert result is not None, "bng_cellarea should return non-null area"
    assert (
        result == 1.0
    ), f"Expected area 1.0 sq km for TQ3080 (1km cell), got {result!r}"


def test_bng_centroid_python_light_example(spark):
    """bng_centroid returns non-empty WKB bytes for 'TQ3080'."""
    assert light_examples is not None
    result = light_examples.bng_centroid_python_light_example(spark)
    assert result is not None, "bng_centroid should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB POINT), got {type(result)}"
    assert len(result) > 0, "WKB centroid bytes should be non-empty"


def test_bng_eastnorthasbng_python_light_example(spark):
    """bng_eastnorthasbng returns 'TQ3080' for easting=530000, northing=180000, res='1km'."""
    assert light_examples is not None
    result = light_examples.bng_eastnorthasbng_python_light_example(spark)
    assert (
        result == "TQ3080"
    ), f"Expected 'TQ3080' for easting=530000, northing=180000, res='1km', got {result!r}"


def test_bng_pointascell_python_light_example(spark):
    """bng_pointascell returns 'TQ3080' for POINT(530000 180000) at res='1km'."""
    assert light_examples is not None
    result = light_examples.bng_pointascell_python_light_example(spark)
    assert (
        result == "TQ3080"
    ), f"Expected 'TQ3080' for POINT(530000 180000) at res='1km', got {result!r}"


# ---------------------------------------------------------------------------
# T3: BNG distance + chip ops
# bng_distance, bng_euclideandistance, bng_cellintersection, bng_cellunion
#
# Fixture views used:
#   bng_cell_pairs — 1 row: cellid1='TQ3080', cellid2='TQ3081'
#   bng_chips      — 9 rows: chip STRUCT<cellid,core,chip> from bng_tessellate
# Both views are created by the autouse _gridx_light_setup_views fixture above.
# ---------------------------------------------------------------------------


def test_bng_distance_python_light_example(spark):
    """bng_distance returns 1 (grid step) for adjacent cells 'TQ3080' and 'TQ3081'."""
    assert light_examples is not None
    result = light_examples.bng_distance_python_light_example(spark)
    assert result is not None, "bng_distance should return non-null value"
    assert (
        result == 1
    ), f"Expected grid-step distance 1 for adjacent cells, got {result!r}"


def test_bng_euclideandistance_python_light_example(spark):
    """bng_euclideandistance returns 1 (grid unit) for adjacent cells 'TQ3080' and 'TQ3081'."""
    assert light_examples is not None
    result = light_examples.bng_euclideandistance_python_light_example(spark)
    assert result is not None, "bng_euclideandistance should return non-null value"
    assert (
        result == 1
    ), f"Expected Chebyshev distance 1 for adjacent cells, got {result!r}"


def test_bng_cellintersection_python_light_example(spark):
    """bng_cellintersection returns the core chip struct for TQ3080 intersected with itself."""
    assert light_examples is not None
    result = light_examples.bng_cellintersection_python_light_example(spark)
    assert result is not None, "bng_cellintersection should return non-null chip struct"
    assert (
        result["cellid"] == "TQ3080"
    ), f"Expected cellid='TQ3080' in intersection_chip, got {result['cellid']!r}"
    assert (
        result["core"] is True
    ), f"Expected core=True for the interior TQ3080 chip, got {result['core']!r}"


def test_bng_cellunion_python_light_example(spark):
    """bng_cellunion returns the core chip struct for TQ3080 unioned with itself."""
    assert light_examples is not None
    result = light_examples.bng_cellunion_python_light_example(spark)
    assert result is not None, "bng_cellunion should return non-null chip struct"
    assert (
        result["cellid"] == "TQ3080"
    ), f"Expected cellid='TQ3080' in union_chip, got {result['cellid']!r}"
    assert (
        result["core"] is True
    ), f"Expected core=True for the interior TQ3080 chip, got {result['core']!r}"


# ---------------------------------------------------------------------------
# T4: BNG neighbourhood/fill
# bng_kring, bng_kloop, bng_geomkring, bng_geomkloop, bng_polyfill,
# bng_tessellate
#
# Fixture views used:
#   bng_cells    — 1 row: cellid STRING = 'TQ3080'
#   bng_polygons — 1 row: geom STRING (3km × 3km BNG polygon, EPSG:27700)
# Both views are created by the autouse _gridx_light_setup_views fixture above.
# ---------------------------------------------------------------------------


def test_bng_kring_python_light_example(spark):
    """bng_kring returns a 9-cell list for 'TQ3080' at k=1 (center + 8 neighbors)."""
    assert light_examples is not None
    result = light_examples.bng_kring_python_light_example(spark)
    assert result is not None, "bng_kring should return a non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 9
    ), f"Expected 9 cells for k=1 ring around TQ3080, got {len(result)}"
    assert "TQ3080" in result, "Center cell TQ3080 should be in the k=1 ring"


def test_bng_kloop_python_light_example(spark):
    """bng_kloop returns an 8-cell list for 'TQ3080' at k=1 (hollow ring, center excluded)."""
    assert light_examples is not None
    result = light_examples.bng_kloop_python_light_example(spark)
    assert result is not None, "bng_kloop should return a non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert len(result) == 8, f"Expected 8 cells for k=1 hollow ring, got {len(result)}"
    assert "TQ3080" not in result, "Center cell TQ3080 should NOT be in the k=1 loop"


def test_bng_geomkring_python_light_example(spark):
    """bng_geomkring returns a 25-cell list for the BNG polygon at res=3, k=1."""
    assert light_examples is not None
    result = light_examples.bng_geomkring_python_light_example(spark)
    assert result is not None, "bng_geomkring should return a non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 25
    ), f"Expected 25 cells for geomkring(BNG polygon, res=3, k=1), got {len(result)}"
    assert (
        len(result) > 0
    ), "geomkring must return non-empty results with EPSG:27700 geometry"


def test_bng_geomkloop_python_light_example(spark):
    """bng_geomkloop returns a 16-cell list for the BNG polygon at res=3, k=1 (outer ring)."""
    assert light_examples is not None
    result = light_examples.bng_geomkloop_python_light_example(spark)
    assert result is not None, "bng_geomkloop should return a non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 16
    ), f"Expected 16 cells for geomkloop(BNG polygon, res=3, k=1), got {len(result)}"
    assert (
        len(result) > 0
    ), "geomkloop must return non-empty results with EPSG:27700 geometry"


def test_bng_polyfill_python_light_example(spark):
    """bng_polyfill returns a 9-cell list for the 3km × 3km BNG polygon at res=3."""
    assert light_examples is not None
    result = light_examples.bng_polyfill_python_light_example(spark)
    assert result is not None, "bng_polyfill should return a non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRING>), got {type(result)}"
    assert (
        len(result) == 9
    ), f"Expected 9 cells for polyfill(BNG polygon, res=3), got {len(result)}"
    assert "TQ3080" in result, "Center cell TQ3080 should be in the polyfill"
    assert (
        len(result) > 0
    ), "polyfill must return non-empty results with EPSG:27700 geometry"


def test_bng_tessellate_python_light_example(spark):
    """bng_tessellate returns a 9-chip list for the 3km × 3km BNG polygon at res=3."""
    assert light_examples is not None
    result = light_examples.bng_tessellate_python_light_example(spark)
    assert result is not None, "bng_tessellate should return a non-null list"
    assert isinstance(
        result, list
    ), f"Expected list (ARRAY<STRUCT>), got {type(result)}"
    assert (
        len(result) == 9
    ), f"Expected 9 chips for tessellate(BNG polygon, res=3), got {len(result)}"
    # Verify the core cell is present and correctly identified
    core_chips = [c for c in result if c["core"] is True]
    assert len(core_chips) >= 1, "Expected at least one core chip (fully interior cell)"
    core_cellids = {c["cellid"] for c in core_chips}
    assert (
        "TQ3080" in core_cellids
    ), f"Expected TQ3080 to be the core chip; got core cells {core_cellids!r}"


# ---------------------------------------------------------------------------
# T5: BNG aggregators + explode UDTFs
# bng_cellintersection_agg, bng_cellunion_agg,
# bng_kringexplode, bng_kloopexplode, bng_geomkringexplode,
# bng_geomkloopexplode, bng_tessellateexplode
#
# Fixture views used:
#   bng_chips    — 9 rows: chip STRUCT<cellid,core,chip> from bng_tessellate
#   bng_cells    — 1 row: cellid='TQ3080'
#   bng_polygons — 1 row: geom (3km × 3km BNG polygon, EPSG:27700)
# ---------------------------------------------------------------------------


def test_bng_cellintersection_agg_python_light_example(spark):
    """bng_cellintersection_agg (light) returns BINARY (dissolved chip WKB) for TQ3080."""
    assert light_examples is not None
    result = light_examples.bng_cellintersection_agg_python_light_example(spark)
    assert (
        result is not None
    ), "bng_cellintersection_agg (light) should return non-null BINARY for TQ3080"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Light tier should return BINARY (bytes); got {type(result)}"
    assert len(result) > 0, "Dissolved chip WKB bytes should be non-empty"


def test_bng_cellunion_agg_python_light_example(spark):
    """bng_cellunion_agg (light) returns BINARY (dissolved chip WKB) for TQ3080."""
    assert light_examples is not None
    result = light_examples.bng_cellunion_agg_python_light_example(spark)
    assert (
        result is not None
    ), "bng_cellunion_agg (light) should return non-null BINARY for TQ3080"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Light tier should return BINARY (bytes); got {type(result)}"
    assert len(result) > 0, "Dissolved chip WKB bytes should be non-empty"


def test_bng_kringexplode_python_light_example(spark):
    """bng_kringexplode (light) LATERAL returns 9 rows for TQ3080 at k=1."""
    assert light_examples is not None
    rows = light_examples.bng_kringexplode_python_light_example(spark)
    assert rows is not None, "bng_kringexplode should return non-null rows"
    assert isinstance(rows, list), f"Expected list of Row, got {type(rows)}"
    assert (
        len(rows) == 9
    ), f"Expected 9 rows for kringexplode(TQ3080, k=1), got {len(rows)}"
    cellids = {r["cellid"] for r in rows}
    assert "TQ3080" in cellids, "Center cell TQ3080 should be in the k=1 ring"


def test_bng_kloopexplode_python_light_example(spark):
    """bng_kloopexplode (light) LATERAL returns 8 rows for TQ3080 at k=1 (center excluded)."""
    assert light_examples is not None
    rows = light_examples.bng_kloopexplode_python_light_example(spark)
    assert rows is not None, "bng_kloopexplode should return non-null rows"
    assert isinstance(rows, list), f"Expected list of Row, got {type(rows)}"
    assert (
        len(rows) == 8
    ), f"Expected 8 rows for kloopexplode(TQ3080, k=1), got {len(rows)}"
    cellids = {r["cellid"] for r in rows}
    assert "TQ3080" not in cellids, "Center cell TQ3080 should NOT be in the k=1 loop"


def test_bng_geomkringexplode_python_light_example(spark):
    """bng_geomkringexplode (light) LATERAL returns 25 rows for BNG polygon at res=3, k=1."""
    assert light_examples is not None
    rows = light_examples.bng_geomkringexplode_python_light_example(spark)
    assert rows is not None, "bng_geomkringexplode should return non-null rows"
    assert isinstance(rows, list), f"Expected list of Row, got {type(rows)}"
    assert (
        len(rows) == 25
    ), f"Expected 25 rows for geomkringexplode(BNG polygon, res=3, k=1), got {len(rows)}"
    assert (
        len(rows) > 0
    ), "geomkringexplode must return non-empty results with BNG coords"


def test_bng_geomkloopexplode_python_light_example(spark):
    """bng_geomkloopexplode (light) LATERAL returns 16 rows for BNG polygon at res=3, k=1."""
    assert light_examples is not None
    rows = light_examples.bng_geomkloopexplode_python_light_example(spark)
    assert rows is not None, "bng_geomkloopexplode should return non-null rows"
    assert isinstance(rows, list), f"Expected list of Row, got {type(rows)}"
    assert (
        len(rows) == 16
    ), f"Expected 16 rows for geomkloopexplode(BNG polygon, res=3, k=1), got {len(rows)}"
    assert (
        len(rows) > 0
    ), "geomkloopexplode must return non-empty results with BNG coords"


def test_bng_tessellateexplode_python_light_example(spark):
    """bng_tessellateexplode (light) LATERAL returns 9 rows with cellid/core/chip columns."""
    assert light_examples is not None
    rows = light_examples.bng_tessellateexplode_python_light_example(spark)
    assert rows is not None, "bng_tessellateexplode should return non-null rows"
    assert isinstance(rows, list), f"Expected list of Row, got {type(rows)}"
    assert (
        len(rows) == 9
    ), f"Expected 9 rows for tessellateexplode(BNG polygon, res=3), got {len(rows)}"
    # Verify the 3-column schema (cellid, core, chip) is present
    assert hasattr(rows[0], "cellid"), "Row should have 'cellid' column"
    assert hasattr(rows[0], "core"), "Row should have 'core' column"
    assert hasattr(rows[0], "chip"), "Row should have 'chip' column"
    # Verify TQ3080 is the core cell
    core_rows = [r for r in rows if r["core"] is True]
    assert len(core_rows) >= 1, "Expected at least one core row (core=true)"
    core_cellids = {r["cellid"] for r in core_rows}
    assert (
        "TQ3080" in core_cellids
    ), f"Expected TQ3080 to be the core cell; got {core_cellids!r}"


# ---------------------------------------------------------------------------
# T6a: Quadbin codec + scalar
# quadbin_pointascell, quadbin_aswkb, quadbin_centroid,
# quadbin_resolution, quadbin_distance
#
# Fixture views used (created by the autouse fixture):
#   quadbin_cells      — 1 row: cell LONG = 5233961839712272383 (SF at z10)
#   quadbin_cell_pairs — 1 row: cell1 LONG, cell2 LONG (z10, distance=1)
# quadbin_pointascell uses an inline one-row DataFrame.
# ---------------------------------------------------------------------------


def test_quadbin_pointascell_python_light_example(spark):
    """quadbin_pointascell (light) returns 5233961839712272383 for SF at zoom 10."""
    assert light_examples is not None
    result = light_examples.quadbin_pointascell_python_light_example(spark)
    assert (
        result is not None
    ), "quadbin_pointascell (light) should return non-null cell ID"
    assert (
        result == 5233961839712272383
    ), f"Expected SF z10 cell 5233961839712272383, got {result!r}"


def test_quadbin_aswkb_python_light_example(spark):
    """quadbin_aswkb (light) returns non-empty EWKB bytes for SF z10 cell."""
    assert light_examples is not None
    result = light_examples.quadbin_aswkb_python_light_example(spark)
    assert result is not None, "quadbin_aswkb (light) should return non-null EWKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB binary), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


def test_quadbin_centroid_python_light_example(spark):
    """quadbin_centroid (light) returns non-empty EWKB POINT bytes for SF z10 cell."""
    assert light_examples is not None
    result = light_examples.quadbin_centroid_python_light_example(spark)
    assert (
        result is not None
    ), "quadbin_centroid (light) should return non-null EWKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB POINT), got {type(result)}"
    assert len(result) > 0, "EWKB centroid bytes should be non-empty"


def test_quadbin_resolution_python_light_example(spark):
    """quadbin_resolution (light) returns 10 for SF z10 cell."""
    assert light_examples is not None
    result = light_examples.quadbin_resolution_python_light_example(spark)
    assert result is not None, "quadbin_resolution (light) should return non-null INT"
    assert result == 10, f"Expected resolution 10 for SF z10 cell, got {result!r}"


def test_quadbin_distance_python_light_example(spark):
    """quadbin_distance (light) returns 1 for two adjacent z10 cells (0.0 vs 0.1 lat)."""
    assert light_examples is not None
    result = light_examples.quadbin_distance_python_light_example(spark)
    assert result is not None, "quadbin_distance (light) should return non-null INT"
    assert (
        result == 1
    ), f"Expected Chebyshev distance 1 for adjacent z10 cells, got {result!r}"


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg
# quadbin_kring, quadbin_polyfill, quadbin_tessellate,
# quadbin_cellunion, quadbin_cellunion_agg
#
# Fixture views used (created by the autouse fixture):
#   quadbin_cells       — 1 row: cell LONG = 5233961839712272383 (SF at z10)
#   quadbin_polygons    — 1 row: geom STRING (WGS84 polygon near origin)
#   quadbin_kring_cells — 9 rows: cell LONG (kring of SF-z10, k=1)
# ---------------------------------------------------------------------------


def test_quadbin_kring_python_light_example(spark):
    """quadbin_kring (light) returns 9 cells for SF z10 at k=1."""
    assert light_examples is not None
    result = light_examples.quadbin_kring_python_light_example(spark)
    assert result is not None, "quadbin_kring (light) should return non-null array"
    assert len(result) == 9, f"Expected 9 cells at k=1, got {len(result)}"
    # Center cell must be present in the kring
    assert (
        5233961839712272383 in result
    ), "Expected SF z10 center cell 5233961839712272383 in kring result"


def test_quadbin_polyfill_python_light_example(spark):
    """quadbin_polyfill (light) returns 4 cells for WGS84 polygon at zoom 5."""
    assert light_examples is not None
    result = light_examples.quadbin_polyfill_python_light_example(spark)
    assert result is not None, "quadbin_polyfill (light) should return non-null array"
    assert len(result) == 4, f"Expected 4 cells for polygon at z5, got {len(result)}"
    assert (
        5211790668774506495 in result
    ), "polyfill z5 result must contain the displayed first cell"


def test_quadbin_tessellate_python_light_example(spark):
    """quadbin_tessellate (light) returns 4 chips for WGS84 polygon at zoom 5."""
    assert light_examples is not None
    result = light_examples.quadbin_tessellate_python_light_example(spark)
    assert result is not None, "quadbin_tessellate (light) should return non-null array"
    assert len(result) == 4, f"Expected 4 chips for polygon at z5, got {len(result)}"
    assert 5211790668774506495 in [
        c["cell"] for c in result
    ], "tessellate z5 result must contain the displayed first cell"


def test_quadbin_cellunion_python_light_example(spark):
    """quadbin_cellunion (light) returns non-empty EWKB bytes for SF z10 kring."""
    assert light_examples is not None
    result = light_examples.quadbin_cellunion_python_light_example(spark)
    assert (
        result is not None
    ), "quadbin_cellunion (light) should return non-null EWKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (EWKB binary), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


def test_quadbin_cellunion_agg_python_light_example(spark):
    """quadbin_cellunion_agg (light) returns non-empty BINARY EWKB bytes (both tiers = BINARY)."""
    assert light_examples is not None
    result = light_examples.quadbin_cellunion_agg_python_light_example(spark)
    assert (
        result is not None
    ), "quadbin_cellunion_agg (light) should return non-null BINARY EWKB"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (BINARY EWKB), got {type(result)}"
    assert len(result) > 0, "EWKB bytes should be non-empty"


# ---------------------------------------------------------------------------
# T7: Custom grid — all 7 functions
# custom_grid, custom_pointascell, custom_cellaswkb, custom_cellaswkt,
# custom_centroid, custom_polyfill, custom_kring
#
# Fixture view used (created by the autouse fixture):
#   custom_grids — 1 row: grid STRUCT, cell LONG = 360287970373976640,
#                         point STRING = 'POINT(530000 180000)'
# custom_grid uses an inline one-row DataFrame (demonstrates the constructor call).
# ---------------------------------------------------------------------------


def test_custom_grid_python_light_example_exists():
    """custom_grid_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_grid_python_light_example")
    assert callable(light_examples.custom_grid_python_light_example)
    assert hasattr(light_examples, "custom_grid_python_light_example_output")


def test_custom_pointascell_python_light_example_exists():
    """custom_pointascell_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_pointascell_python_light_example")
    assert callable(light_examples.custom_pointascell_python_light_example)
    assert hasattr(light_examples, "custom_pointascell_python_light_example_output")


def test_custom_cellaswkb_python_light_example_exists():
    """custom_cellaswkb_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_cellaswkb_python_light_example")
    assert callable(light_examples.custom_cellaswkb_python_light_example)
    assert hasattr(light_examples, "custom_cellaswkb_python_light_example_output")


def test_custom_cellaswkt_python_light_example_exists():
    """custom_cellaswkt_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_cellaswkt_python_light_example")
    assert callable(light_examples.custom_cellaswkt_python_light_example)
    assert hasattr(light_examples, "custom_cellaswkt_python_light_example_output")


def test_custom_centroid_python_light_example_exists():
    """custom_centroid_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_centroid_python_light_example")
    assert callable(light_examples.custom_centroid_python_light_example)
    assert hasattr(light_examples, "custom_centroid_python_light_example_output")


def test_custom_polyfill_python_light_example_exists():
    """custom_polyfill_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_polyfill_python_light_example")
    assert callable(light_examples.custom_polyfill_python_light_example)
    assert hasattr(light_examples, "custom_polyfill_python_light_example_output")


def test_custom_kring_python_light_example_exists():
    """custom_kring_python_light_example is defined with an output constant."""
    assert light_examples is not None
    assert hasattr(light_examples, "custom_kring_python_light_example")
    assert callable(light_examples.custom_kring_python_light_example)
    assert hasattr(light_examples, "custom_kring_python_light_example_output")


def test_custom_grid_python_light_example(spark):
    """custom_grid (light) returns a non-null grid descriptor struct."""
    assert light_examples is not None
    result = light_examples.custom_grid_python_light_example(spark)
    assert result is not None, "custom_grid (light) should return non-null grid struct"


def test_custom_pointascell_python_light_example(spark):
    """custom_pointascell (light) returns 360287970373976640 for POINT(530000 180000) at res=5."""
    assert light_examples is not None
    result = light_examples.custom_pointascell_python_light_example(spark)
    assert (
        result is not None
    ), "custom_pointascell (light) should return non-null cell ID"
    assert (
        result == 360287970373976640
    ), f"Expected cell 360287970373976640, got {result!r}"


def test_custom_cellaswkb_python_light_example(spark):
    """custom_cellaswkb (light) returns non-empty WKB bytes for cell 360287970373976640."""
    assert light_examples is not None
    result = light_examples.custom_cellaswkb_python_light_example(spark)
    assert (
        result is not None
    ), "custom_cellaswkb (light) should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB binary), got {type(result)}"
    assert len(result) > 0, "WKB bytes should be non-empty"


def test_custom_cellaswkt_python_light_example(spark):
    """custom_cellaswkt (light) returns a WKT POLYGON string for cell 360287970373976640."""
    assert light_examples is not None
    result = light_examples.custom_cellaswkt_python_light_example(spark)
    assert (
        result is not None
    ), "custom_cellaswkt (light) should return non-null WKT string"
    assert isinstance(result, str), f"Expected str (WKT), got {type(result)}"
    assert result.startswith("POLYGON"), f"Expected POLYGON WKT, got {result!r}"
    assert "530031.25" in result, f"Expected x=530031.25 in WKT, got {result!r}"


def test_custom_centroid_python_light_example(spark):
    """custom_centroid (light) returns non-empty WKB bytes for cell 360287970373976640."""
    assert light_examples is not None
    result = light_examples.custom_centroid_python_light_example(spark)
    assert (
        result is not None
    ), "custom_centroid (light) should return non-null WKB bytes"
    assert isinstance(
        result, (bytes, bytearray)
    ), f"Expected bytes (WKB POINT), got {type(result)}"
    assert len(result) > 0, "WKB centroid bytes should be non-empty"


def test_custom_polyfill_python_light_example(spark):
    """custom_polyfill (light) returns exactly 36 cells (500m) at res=1 for the 3km BNG polygon."""
    assert light_examples is not None
    result = light_examples.custom_polyfill_python_light_example(spark)
    assert result is not None, "custom_polyfill (light) should return non-null array"
    assert (
        len(result) == 36
    ), f"Expected 36 cells at res=1 (500m, 6 per side), got {len(result)}"
    assert all(
        isinstance(c, int) for c in result
    ), "custom_polyfill should return ARRAY<BIGINT>"
    assert (
        72057594038644994 in result
    ), "Expected first cell 72057594038644994 in polyfill result"


def test_custom_kring_python_light_example(spark):
    """custom_kring (light) returns 9 cells for k=1 around cell 360287970373976640."""
    assert light_examples is not None
    result = light_examples.custom_kring_python_light_example(spark)
    assert result is not None, "custom_kring (light) should return non-null array"
    assert len(result) == 9, f"Expected 9 cells at k=1, got {len(result)}"
    assert (
        360287970373976640 in result
    ), "Expected center cell 360287970373976640 in kring result"
