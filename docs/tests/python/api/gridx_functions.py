"""
Python code examples for GridX (BNG) Function Reference documentation.
Single source of truth for docs/docs/api/gridx-functions.mdx

All function names match the actual GeoBrix source code in:
src/main/scala/com/databricks/labs/gbx/gridx/bng/functions.scala

Display convention for examples:
- Use .show() for single-row results; use .limit(3).show() only when the result has multiple rows.
- Use .show(vertical=True) for wide or complex output.
- Optional: add a constant named <example_name>_output with triple-quoted
  show() output so docs can display "Example output" via CodeFromTest outputConstant.
"""

# ============================================================================
# COMMON SETUP
# ============================================================================


def gridx_setup_example(spark):
    """Common setup: register GridX (BNG) functions. Run once before examples."""
    from databricks.labs.gbx.gridx.bng import functions as bx

    bx.register(spark)


gridx_setup_example_output = """
GridX (BNG) registered. You can now use bng_* functions in Python and gbx_bng_* in SQL.
"""


# ============================================================================
# CONVERSION FUNCTIONS
# ============================================================================


def bng_aswkb_example(spark):
    """Convert a BNG cell ID to Well-Known Binary (WKB) format. Requires gridx_setup_example first."""
    df = spark.sql("""
        SELECT gbx_bng_aswkb('TQ3080') as wkb_geom
    """)
    df.show()
    return df


def bng_aswkt_example(spark):
    """Convert a BNG cell ID to Well-Known Text (WKT) format. Requires gridx_setup_example first."""
    df = spark.sql("""
        SELECT gbx_bng_aswkt('TQ3080') as wkt_geom
    """)
    df.show()
    return df


# ============================================================================
# CORE FUNCTIONS
# ============================================================================


def bng_cellarea_example(spark):
    """Calculate the area of a BNG cell (returns square kilometres). Requires gridx_setup_example first."""
    df = spark.sql("""
        SELECT 
            'TQ3080' as cell_id,
            gbx_bng_cellarea('TQ3080') as area_km2
    """)
    df.show()
    return df


def bng_centroid_example(spark):
    """Get the centroid point of a BNG cell"""
    # Get centroid as a point geometry
    df = spark.sql("""
        SELECT 
            'TQ3080' as cell_id,
            gbx_bng_centroid('TQ3080') as centroid_point
    """)
    df.show()
    return df


def bng_distance_example(spark):
    """Calculate distance between two BNG cells"""
    # Calculate distance between two cells
    df = spark.sql("""
        SELECT 
            gbx_bng_distance('TQ3080', 'TQ3081') as distance_m
    """)
    df.show()
    return df


def bng_euclideandistance_example(spark):
    """Calculate Euclidean distance between two BNG cells"""
    # Calculate Euclidean distance
    df = spark.sql("""
        SELECT 
            gbx_bng_euclideandistance('TQ3080', 'TQ3081') as euclidean_distance_m
    """)
    df.show()
    return df


# ============================================================================
# CELL OPERATIONS
# ============================================================================


def bng_cellintersection_example(spark):
    """Get the intersection of two BNG cells"""
    # Find intersection between two cells
    df = spark.sql("""
        SELECT 
            gbx_bng_cellintersection('TQ3080', 'TQ3081') as intersection_cell
    """)
    df.show()
    return df


def bng_cellunion_example(spark):
    """Get the union of two BNG cells"""
    # Find union of two cells
    df = spark.sql("""
        SELECT 
            gbx_bng_cellunion('TQ3080', 'TQ3081') as union_cell
    """)
    df.show()
    return df


# ============================================================================
# COORDINATE CONVERSION
# ============================================================================


def bng_eastnorthasbng_example(spark):
    """Convert easting/northing coordinates to BNG cell"""
    # Convert OS coordinates to BNG cell at 1km resolution
    df = spark.sql("""
        SELECT 
            gbx_bng_eastnorthasbng(530000, 180000, '1km') as bng_cell
    """)
    df.show()
    return df


def bng_pointascell_example(spark):
    """Convert a point geometry to BNG cell. Point must be WKT or WKB (not DBR st_point)."""
    # Convert point to BNG cell at 1km resolution (point as WKT)
    df = spark.sql("""
        SELECT 
            gbx_bng_pointascell('POINT(530000 180000)', '1km') as bng_cell
    """)
    df.show()
    return df


def bng_pointascell_python_api_example(spark):
    """Use Python API: point as WKT column. Do not use st_point()—GeoBrix expects WKT or WKB."""
    from databricks.labs.gbx.gridx.bng import functions as bx
    from pyspark.sql.functions import lit

    bx.register(spark)
    df = spark.range(1).select(
        bx.bng_pointascell(lit("POINT(530000 180000)"), lit("1km")).alias("bng_cell")
    )
    df.show()
    return df


# ============================================================================
# K-RING FUNCTIONS
# ============================================================================


def bng_kring_example(spark):
    """Generate k-ring of cells around a center cell"""
    # Get all cells within k=2 rings
    df = spark.sql("""
        SELECT 
            'TQ3080' as center,
            gbx_bng_kring('TQ3080', 2) as kring_cells
    """)
    df.show(truncate=False)
    return df


def bng_kloop_example(spark):
    """Generate k-loop (hollow ring) of cells around a center cell"""
    # Get only cells at k=2 distance (not interior)
    df = spark.sql("""
        SELECT 
            'TQ3080' as center,
            gbx_bng_kloop('TQ3080', 2) as kloop_cells
    """)
    df.show(truncate=False)
    return df


# ============================================================================
# AGGREGATOR FUNCTIONS
# ============================================================================


def bng_cellintersection_agg_example(spark):
    """Aggregate intersection of multiple BNG cells"""
    # Find common cell across multiple rows
    df = spark.sql("""
        WITH cells AS (
            SELECT 'TQ3080' as cell UNION ALL
            SELECT 'TQ3080' as cell UNION ALL
            SELECT 'TQ3081' as cell
        )
        SELECT gbx_bng_cellintersection_agg(cell) as common_cell
        FROM cells
    """)
    df.show()
    return df


def bng_cellunion_agg_example(spark):
    """Aggregate union of multiple BNG cells"""
    # Find union cell covering all rows
    df = spark.sql("""
        WITH cells AS (
            SELECT 'TQ3080' as cell UNION ALL
            SELECT 'TQ3081' as cell UNION ALL
            SELECT 'TQ3082' as cell
        )
        SELECT gbx_bng_cellunion_agg(cell) as union_cell
        FROM cells
    """)
    df.show()
    return df


# ============================================================================
# GENERATOR FUNCTIONS (Explode variants)
# ============================================================================


# ============================================================================
# GEOMETRY-BASED GRID OPERATIONS (st_geomfromtext / geometry column examples)
# ============================================================================


def bng_geomkring_example(spark):
    """Generate k-ring from a geometry at specified resolution"""
    df = spark.sql("""
        SELECT gbx_bng_geomkring(
            st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
            3, 1
        ) as kring_cells
    """)
    df.show(truncate=False)
    return df


def bng_geomkloop_example(spark):
    """Generate k-loop from a geometry at specified resolution"""
    df = spark.sql("""
        SELECT gbx_bng_geomkloop(
            st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
            3, 1
        ) as kloop_cells
    """)
    df.show(truncate=False)
    return df


def bng_polyfill_example(spark):
    """Fill a geometry with BNG cells at specified resolution"""
    df = spark.sql("""
        SELECT gbx_bng_polyfill(
            st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
            3
        ) as cells
    """)
    df.show(truncate=False)
    return df


def bng_tessellate_example(spark):
    """Tessellate a geometry into BNG cells with geometries"""
    df = spark.sql("""
        SELECT gbx_bng_tessellate(
            st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
            3
        ) as tessellation
    """)
    df.show(truncate=False)
    return df


def bng_tessellateexplode_example(spark):
    """Explode tessellated cells into separate rows"""
    df = spark.sql("""
        SELECT explode(gbx_bng_tessellate(
            st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
            3
        )) as cell_info
    """)
    df.show(truncate=False)
    return df


# ---------------------------------------------------------------------------
# T2 heavy-tier per-function examples (tabbed docs)
# bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid,
# bng_eastnorthasbng, bng_pointascell
#
# These are the *_python_heavy_example functions consumed by FunctionExamples
# and scanned by generate-function-info.py for the "python-heavy" binding.
#
# Fixture views required (created by bng_heavy_setup in test_gridx_functions.py):
#   bng_cells  — 1 row: cellid STRING = 'TQ3080'
#   bng_points — 1 row: easting INT, northing INT, geom STRING
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T3: BNG distance + chip ops (heavy-tier tabbed docs)
# bng_distance, bng_euclideandistance, bng_cellintersection, bng_cellunion
#
# Fixture views (created by bng_heavy_setup in test_gridx_functions.py):
#   bng_cell_pairs — 1 row: cellid1='TQ3080', cellid2='TQ3081'
#   bng_chips      — 9 rows: chip STRUCT from bng_tessellate
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T4: BNG neighbourhood/fill (heavy bng tier)
# bng_kring, bng_kloop, bng_geomkring, bng_geomkloop, bng_polyfill,
# bng_tessellate
#
# Fixture views created by the autouse fixture in test_gridx_functions.py:
#   bng_cells    — 1 row: cellid STRING = 'TQ3080'
#   bng_polygons — 1 row: geom STRING (3km × 3km BNG polygon, EPSG:27700)
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T5: BNG aggregators (heavy bng tier)
# bng_cellintersection_agg, bng_cellunion_agg
#
# Fixture view: bng_chips — 9 rows chip STRUCT from bng_tessellate (res=3)
#
# DIVERGE: heavy returns STRUCT<cellid, core, chip>; light returns BINARY.
#   The 5 BNG *explode UDTFs have no Python Column form on any tier and are
#   invoked via SQL LATERAL only — NO heavy Python examples for those.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T6a: Quadbin codec + scalar — heavy tier
# quadbin_pointascell, quadbin_aswkb, quadbin_centroid,
# quadbin_resolution, quadbin_distance
#
# Fixture views required (created by bng_heavy_setup in test_gridx_functions.py):
#   quadbin_cells      — 1 row: cell LONG = 5233961839712272383 (SF at z10)
#   quadbin_cell_pairs — 1 row: cell1 LONG, cell2 LONG (z10, distance=1)
#
# quadbin_pointascell uses an inline one-row DataFrame (no coord-fixture view).
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg — heavy tier
# quadbin_kring, quadbin_polyfill, quadbin_tessellate,
# quadbin_cellunion, quadbin_cellunion_agg
#
# Fixture views required (created by bng_heavy_setup in test_gridx_functions.py):
#   quadbin_cells      — 1 row: cell LONG = 5233961839712272383 (SF at z10)
#   quadbin_polygons   — 1 row: geom STRING (WGS84 polygon near origin)
#   quadbin_kring_cells— 9 rows: cell LONG (kring of SF-z10, k=1)
#
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T7: Custom grid — all 7 functions (heavy tier)
# custom_grid, custom_pointascell, custom_cellaswkb, custom_cellaswkt,
# custom_centroid, custom_polyfill, custom_kring
#
# Fixture view: custom_grids — 1 row:
#   grid STRUCT<...>  — BNG-like custom grid (0,1000000,0,1000000,2,1000,1000,27700)
#   cell LONG         = 360287970373976640  (POINT(530000 180000) at res=5)
#   point STRING      = 'POINT(530000 180000)' (EPSG:27700)
#
# All four tabs share ONE example per function (AGREE — no tier divergence).
# custom_grid calls the constructor directly; all others read from custom_grids.
# ---------------------------------------------------------------------------

_CUSTOM_BNG_POLY_WKT = (
    "POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))"
)


# ---------------------------------------------------------------------------
# Quadbin geometry-aware kring/kloop (heavy gridx.quadbin tier)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Custom-grid geometry-aware kring/kloop (heavy gridx.custom tier)
# ---------------------------------------------------------------------------


# Offset polygon avoids exact cell-boundary alignment at all power-of-2 resolutions.
# (The grid root is 1km; coordinates divisible by 1000 align at every resolution.)
_CUSTOM_GEOMK_WKT = (
    "POLYGON((529100 179100,529100 182100,532100 182100,532100 179100,529100 179100))"
)


# ---------------------------------------------------------------------------
# Batch E: cellfill (grouped aggregate) + kloop / distance (scalar)
#
# bng_cellfill, quadbin_cellfill, h3_cellfill, custom_cellfill — aggregators
# custom_kloop, quadbin_kloop — hollow-ring scalars (ARRAY<BIGINT>)
# custom_distance — Chebyshev cell-to-cell distance (BIGINT)
#
# Heavy tier cellfill returns ARRAY<STRUCT<cellid, value>> — diverges from
# light (BINARY).  Quadbin/custom/h3 cellfill: same divergence as BNG.
# ---------------------------------------------------------------------------


