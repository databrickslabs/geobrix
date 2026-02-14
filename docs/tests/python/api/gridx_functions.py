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

from pyspark.sql import functions as f
from pyspark.sql.functions import lit, col, expr


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

def bng_kringexplode_example(spark):
    """Explode k-ring cells into separate rows. Requires gridx_setup_example first."""
    # Explode k-ring into individual rows
    df = spark.sql("""
        SELECT 
            'TQ3080' as center_cell,
            explode(gbx_bng_kring('TQ3080', 2)) as nearby_cell
    """)
    df.show()
    return df


def bng_kloopexplode_example(spark):
    """Explode k-loop cells into separate rows"""
    # Explode k-loop into individual rows
    df = spark.sql("""
        SELECT 
            'TQ3080' as center_cell,
            explode(gbx_bng_kloop('TQ3080', 2)) as ring_cell
    """)
    df.show()
    return df


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


def bng_geomkringexplode_example(spark):
    """Explode geometry k-ring cells into separate rows"""
    df = spark.sql("""
        SELECT explode(gbx_bng_geomkring(
            st_geomfromtext('POINT(-0.1278 51.5074)'), 3, 1
        )) as cell
    """)
    df.show()
    return df


def bng_geomkloopexplode_example(spark):
    """Explode geometry k-loop cells into separate rows"""
    df = spark.sql("""
        SELECT explode(gbx_bng_geomkloop(
            st_geomfromtext('POINT(-0.1278 51.5074)'), 3, 1
        )) as cell
    """)
    df.show()
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


# ============================================================================
# EXAMPLE OUTPUT (for docs "Example output" block via CodeFromTest outputConstant)
# ============================================================================

bng_aswkb_example_output = """
+--------------------+
|wkb_geom            |
+--------------------+
|[BINARY]            |
+--------------------+
"""

bng_aswkt_example_output = """
+------------------------------------------+
|wkt_geom                                  |
+------------------------------------------+
|POLYGON ((...))                           |
+------------------------------------------+
"""

bng_cellarea_example_output = """
+------+----------+
|cell_id|area_km2  |
+------+----------+
|TQ3080|1.0       |
+------+----------+
"""

bng_centroid_example_output = """
+------+--------------------+
|cell_id|centroid_point     |
+------+--------------------+
|TQ3080|[POINT (...)]       |
+------+--------------------+
"""

bng_distance_example_output = """
+-----------+
|distance_m |
+-----------+
|1000.0     |
+-----------+
"""

bng_cellintersection_example_output = """
+------------------+
|intersection_cell |
+------------------+
|TQ3080            |
+------------------+
"""

bng_kring_example_output = """
+------+--------------------------------+
|cell_id|kring_cells                    |
+------+--------------------------------+
|TQ3080|[TQ3079, TQ3081, TQ2979, ...]   |
+------+--------------------------------+
"""

bng_eastnorthasbng_example_output = """
+--------+--------+--------+
|east_m  |north_m |cell_id |
+--------+--------+--------+
|530000.0|180000.0|TQ3080  |
+--------+--------+--------+
"""

bng_euclideandistance_example_output = """
+----------------+
|euclidean_dist_m|
+----------------+
|1414.21         |
+----------------+
"""

bng_cellunion_example_output = """
+-----------+
|union_cells|
+-----------+
|TQ3079     |
+-----------+
"""

bng_pointascell_example_output = """
+----+--------+
|pt  |cell_id |
+----+--------+
|... |TQ3080  |
+----+--------+
"""

bng_kloop_example_output = """
+------+-------------------------------+
|cell_id|kloop_cells                   |
+------+-------------------------------+
|TQ3080|[TQ3079, TQ3081, TQ2979, ...]  |
+------+-------------------------------+
"""

bng_geomkring_example_output = """
+------+--------------------------------+
|cell_id|geom_kring                     |
+------+--------------------------------+
|TQ3080|[POLYGON (...), POLYGON (...)]  |
+------+--------------------------------+
"""

bng_geomkloop_example_output = """
+------+--------------------------------+
|cell_id|geom_kloop                     |
+------+--------------------------------+
|TQ3080|[POLYGON (...), ...]            |
+------+--------------------------------+
"""

bng_polyfill_example_output = """
+----------+------------------+
|geom      |polyfill_cells    |
+----------+------------------+
|POLYGON..|[TQ3079, TQ3080,..]|
+----------+------------------+
"""

bng_tessellate_example_output = """
+----------------------------------------+
|cell_info                               |
+----------------------------------------+
|{cellId=TQ3080, wkb=[BINARY], ...}      |
+----------------------------------------+
"""

bng_cellintersection_agg_example_output = """
+------------------+
|intersection_cell |
+------------------+
|TQ3080            |
+------------------+
"""

bng_cellunion_agg_example_output = """
+--------------------------------+
|union_cells                     |
+--------------------------------+
|[TQ3079, TQ3080, TQ3081, ...]   |
+--------------------------------+
"""

bng_kringexplode_example_output = """
+------+---------+
|cell_id|neighbor|
+------+---------+
|TQ3080|TQ3079   |
|TQ3080|TQ3081   |
+------+---------+
"""

bng_kloopexplode_example_output = """
+------+---------+
|cell_id|neighbor|
+------+---------+
|TQ3080|TQ3079   |
|TQ3080|TQ3081   |
+------+---------+
"""

bng_geomkringexplode_example_output = """
+------+--------+
|cell_id|geom   |
+------+--------+
|TQ3080|POLYGON |
+------+--------+
"""

bng_geomkloopexplode_example_output = """
+------+--------+
|cell_id|geom   |
+------+--------+
|TQ3080|POLYGON |
+------+--------+
"""

bng_tessellateexplode_example_output = """
+----------------------------------------+
|cell_info                               |
+----------------------------------------+
|{cellId=TQ3080, wkb=[BINARY], ...}      |
+----------------------------------------+
"""

