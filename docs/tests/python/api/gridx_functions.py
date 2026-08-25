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


def bng_aswkb_python_heavy_example(spark):
    """Convert a BNG cell ID to Well-Known Binary (WKB) geometry (heavy bng tier).

    Reads the ``bng_cells`` setup view (1 row: ``cellid = 'TQ3080'``, a 1km cell
    in central London, EPSG:27700).  Returns the WKB bytes for the cell footprint
    polygon — a 1km square in BNG eastings/northings with no embedded SRID.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cells")
    result = df.select(bx.bng_aswkb(f.col("cellid")).alias("wkb")).first()
    return result["wkb"]


bng_aswkb_python_heavy_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (WKB binary)
"""


def bng_aswkt_python_heavy_example(spark):
    """Convert a BNG cell ID to Well-Known Text (WKT) geometry string (heavy bng tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  Returns the WKT
    polygon string for the TQ3080 cell footprint in EPSG:27700 coordinates.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cells")
    result = df.select(bx.bng_aswkt(f.col("cellid")).alias("wkt")).first()
    return result["wkt"]


bng_aswkt_python_heavy_example_output = """
+----------------------------------------------------------+
|wkt                                                       |
+----------------------------------------------------------+
|POLYGON ((531000 180000, 531000 181000, 530000 181000, ...|
+----------------------------------------------------------+
POLYGON ((531000 180000, 531000 181000, 530000 181000, 530000 180000, 531000 180000))
"""


def bng_cellarea_python_heavy_example(spark):
    """Return the area of a BNG cell in square kilometres (heavy bng tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  TQ3080 is a
    1km × 1km cell at resolution 3 — area = 1.0 sq km.  Identical to the
    lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cells")
    result = df.select(bx.bng_cellarea(f.col("cellid")).alias("area_km2")).first()
    return result["area_km2"]


bng_cellarea_python_heavy_example_output = """
+---------+
|area_km2 |
+---------+
|1.0      |
+---------+
"""


def bng_centroid_python_heavy_example(spark):
    """Return the centroid of a BNG cell as a WKB POINT (heavy bng tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  The centroid
    of the 1km TQ3080 cell is POINT(530500 180500) in EPSG:27700 — returned as
    plain WKB (no SRID).  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cells")
    result = df.select(bx.bng_centroid(f.col("cellid")).alias("centroid")).first()
    return result["centroid"]


bng_centroid_python_heavy_example_output = """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (WKB binary — POINT(530500 180500) in EPSG:27700)
"""


def bng_eastnorthasbng_python_heavy_example(spark):
    """Convert BNG easting/northing coordinates to a BNG cell reference (heavy bng tier).

    Reads the ``bng_points`` setup view (easting=530000, northing=180000,
    EPSG:27700).  Resolution string ``'1km'`` maps the coordinates to
    cell ``'TQ3080'``.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_points")
    result = df.select(
        bx.bng_eastnorthasbng(f.col("easting"), f.col("northing"), f.lit("1km")).alias(
            "bng_cell"
        )
    ).first()
    return result["bng_cell"]


bng_eastnorthasbng_python_heavy_example_output = """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+
"""


def bng_pointascell_python_heavy_example(spark):
    """Convert a WKT point in BNG coordinates to a BNG cell reference (heavy bng tier).

    Reads the ``bng_points`` setup view (``geom = 'POINT(530000 180000)'`` in
    EPSG:27700, central London).  Resolution ``'1km'`` → cell ``'TQ3080'``.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_points")
    result = df.select(
        bx.bng_pointascell(f.col("geom"), f.lit("1km")).alias("bng_cell")
    ).first()
    return result["bng_cell"]


bng_pointascell_python_heavy_example_output = """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+
"""


# ---------------------------------------------------------------------------
# T3: BNG distance + chip ops (heavy-tier tabbed docs)
# bng_distance, bng_euclideandistance, bng_cellintersection, bng_cellunion
#
# Fixture views (created by bng_heavy_setup in test_gridx_functions.py):
#   bng_cell_pairs — 1 row: cellid1='TQ3080', cellid2='TQ3081'
#   bng_chips      — 9 rows: chip STRUCT from bng_tessellate
# ---------------------------------------------------------------------------


def bng_distance_python_heavy_example(spark):
    """Grid-step distance between two adjacent BNG cells (heavy bng tier).

    Reads the ``bng_cell_pairs`` setup view (cellid1='TQ3080', cellid2='TQ3081',
    two adjacent 1km cells).  The cells share an edge — distance = 1 grid step.
    Returns LONG (grid steps, not metres).  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cell_pairs")
    result = df.select(
        bx.bng_distance(f.col("cellid1"), f.col("cellid2")).alias("dist_steps")
    ).first()
    return result["dist_steps"]


bng_distance_python_heavy_example_output = """
+----------+
|dist_steps|
+----------+
|1         |
+----------+
"""


def bng_euclideandistance_python_heavy_example(spark):
    """Chebyshev grid-unit distance between two adjacent BNG cells (heavy bng tier).

    Reads the ``bng_cell_pairs`` setup view (cellid1='TQ3080', cellid2='TQ3081').
    Returns LONG (grid units).  For adjacent cells at the same resolution,
    Chebyshev distance = 1.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cell_pairs")
    result = df.select(
        bx.bng_euclideandistance(f.col("cellid1"), f.col("cellid2")).alias(
            "euclidean_dist"
        )
    ).first()
    return result["euclidean_dist"]


bng_euclideandistance_python_heavy_example_output = """
+--------------+
|euclidean_dist|
+--------------+
|1             |
+--------------+
"""


def bng_cellintersection_python_heavy_example(spark):
    """Intersect two BNG chip structs — returns the dissolved intersection chip (heavy bng tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Filters for the core chip (TQ3080, core=True,
    chip=None) and intersects it with itself.  Returns STRUCT<cellid, core, chip>.
    Chip inputs must come from ``bng_tessellate`` — plain cell-id strings throw
    ClassCastException.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_chips")
    result = (
        df.filter(f.col("chip.cellid") == "TQ3080")
        .select(
            bx.bng_cellintersection(f.col("chip"), f.col("chip")).alias(
                "intersection_chip"
            )
        )
        .first()
    )
    return result["intersection_chip"]


bng_cellintersection_python_heavy_example_output = """
+--------------------+
|intersection_chip   |
+--------------------+
|{TQ3080, true, null}|
+--------------------+
"""


def bng_cellunion_python_heavy_example(spark):
    """Union two BNG chip structs — returns the dissolved union chip (heavy bng tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Filters for the core chip (TQ3080, core=True,
    chip=None) and unions it with itself.  Returns STRUCT<cellid, core, chip>.
    Chip inputs must come from ``bng_tessellate`` — plain cell-id strings throw
    ClassCastException.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_chips")
    result = (
        df.filter(f.col("chip.cellid") == "TQ3080")
        .select(bx.bng_cellunion(f.col("chip"), f.col("chip")).alias("union_chip"))
        .first()
    )
    return result["union_chip"]


bng_cellunion_python_heavy_example_output = """
+--------------------+
|union_chip          |
+--------------------+
|{TQ3080, true, null}|
+--------------------+
"""


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


def bng_kring_python_heavy_example(spark):
    """Filled disk of BNG cells within k grid steps of a center cell (heavy bng tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  At k=1 the result
    is the center cell plus the 8 surrounding 1km cells → 9 cells total.
    Returns ARRAY<STRING>.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cells")
    result = df.select(bx.bng_kring(f.col("cellid"), f.lit(1)).alias("kring")).first()
    return result["kring"]


bng_kring_python_heavy_example_output = """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells: center TQ3080 plus 8 surrounding cells at k=1)
"""


def bng_kloop_python_heavy_example(spark):
    """Hollow ring of BNG cells at exactly k grid steps from a center cell (heavy bng tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  At k=1 the result
    is the 8 surrounding cells (excludes center) → 8 cells total.
    Returns ARRAY<STRING>.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_cells")
    result = df.select(bx.bng_kloop(f.col("cellid"), f.lit(1)).alias("kloop")).first()
    return result["kloop"]


bng_kloop_python_heavy_example_output = """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (8 cells: hollow ring at k=1, center TQ3080 excluded)
"""


def bng_geomkring_python_heavy_example(spark):
    """Polyfill a BNG geometry then expand by k ring steps (heavy bng tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polyfill covers 9 cells; k=1 expands by one ring →
    25 cells total.  Returns ARRAY<STRING>.  Identical to the lightweight output
    (AGREE).  Geometry MUST be in EPSG:27700; WGS84 yields an empty array.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_polygons")
    result = df.select(
        bx.bng_geomkring(f.col("geom"), f.lit(3), f.lit(1)).alias("kring")
    ).first()
    return result["kring"]


bng_geomkring_python_heavy_example_output = """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (25 cells: polyfill of BNG polygon expanded by k=1 ring)
"""


def bng_geomkloop_python_heavy_example(spark):
    """Return only the outer ring of BNG cells around a geometry (heavy bng tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polyfill covers 9 cells; k=1 returns the 16 outer cells
    (hollow shell, excluding the interior 9).  Returns ARRAY<STRING>.  Identical
    to the lightweight output (AGREE).  Geometry MUST be in EPSG:27700.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_polygons")
    result = df.select(
        bx.bng_geomkloop(f.col("geom"), f.lit(3), f.lit(1)).alias("kloop")
    ).first()
    return result["kloop"]


bng_geomkloop_python_heavy_example_output = """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (16 cells: outer ring at k=1 around the BNG polygon polyfill)
"""


def bng_polyfill_python_heavy_example(spark):
    """Fill a geometry with all BNG cells at given resolution (heavy bng tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polygon is covered by 9 cells.  Returns ARRAY<STRING>.
    Identical to the lightweight output (AGREE).  Geometry MUST be in EPSG:27700;
    WGS84 yields an empty array.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_polygons")
    result = df.select(bx.bng_polyfill(f.col("geom"), f.lit(3)).alias("cells")).first()
    return result["cells"]


bng_polyfill_python_heavy_example_output = """
+-----------------------------+
|cells                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells covering the 3km × 3km BNG polygon at 1km resolution)
"""


def bng_tessellate_python_heavy_example(spark):
    """Tessellate a geometry into BNG cells, splitting border cells at the boundary (heavy bng tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polygon produces 9 chips.  Center cell TQ3080 is a core
    chip (core=True, chip=None); the 8 border cells carry a WKB clipped polygon.
    Returns ARRAY<STRUCT<cellid STRING, core BOOLEAN, chip BINARY>>.
    Identical to the lightweight output (AGREE).  Geometry MUST be in EPSG:27700.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    df = spark.table("bng_polygons")
    result = df.select(
        bx.bng_tessellate(f.col("geom"), f.lit(3)).alias("chips")
    ).first()
    return result["chips"]


bng_tessellate_python_heavy_example_output = """
+--------------------------------------------+
|chips                                       |
+--------------------------------------------+
|[{TQ2979, false, [binary]}, {TQ3080, true,..|
+--------------------------------------------+
... (9 chips; TQ3080 is core (core=true, chip=null); border cells carry WKB clip geometry)
"""


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


def bng_cellintersection_agg_python_heavy_example(spark):
    """Aggregate intersection of chip structs per BNG cell (heavy bng tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Groups by ``chip.cellid`` and intersects chips
    within each group.  Each group holds exactly one chip, so the aggregate returns
    that chip unchanged.

    The heavy tier returns **STRUCT<cellid STRING, core BOOLEAN, chip BINARY>**.
    The light tier returns **BINARY** (dissolved chip WKB) — a genuine divergence.
    Chip inputs must come from ``bng_tessellate``; plain STRING cell IDs raise
    ``ClassCastException``.

    Note: ``bx.register(spark)`` is called explicitly here because the ``bng_chips``
    view helper (``bng_chips_df_heavy``) delegates to ``bng_chips_df`` which calls
    ``gx.register(spark)`` as a side effect, replacing the heavy agg UDF with the
    light BINARY variant.  Re-registering restores the STRUCT-returning heavy version.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    bx.register(spark)  # ensure the heavy (STRUCT-returning) BNG tier is registered
    df = spark.table("bng_chips")
    df_keyed = df.select(
        f.col("chip"),
        f.col("chip.cellid").alias("cellid"),
    )
    result = (
        df_keyed.groupBy("cellid")
        .agg(bx.bng_cellintersection_agg("chip").alias("common_chip"))
        .filter(f.col("cellid") == "TQ3080")
        .first()
    )
    return result["common_chip"]


bng_cellintersection_agg_python_heavy_example_output = """
+-------+--------------------+
|cellid |common_chip         |
+-------+--------------------+
|TQ3080 |{TQ3080, true, null}|
+-------+--------------------+
... (STRUCT — heavy tier returns chip struct; TQ3080 core chip: cellid=TQ3080, core=true, chip=null)
"""


def bng_cellunion_agg_python_heavy_example(spark):
    """Aggregate union of chip structs per BNG cell (heavy bng tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Groups by ``chip.cellid`` and dissolves chips
    within each group.  Each group holds exactly one chip, so the aggregate returns
    that chip unchanged.

    The heavy tier returns **STRUCT<cellid STRING, core BOOLEAN, chip BINARY>**.
    The light tier returns **BINARY** (dissolved chip WKB) — a genuine divergence.
    Chip inputs must come from ``bng_tessellate``; plain STRING cell IDs raise
    ``ClassCastException``.

    Note: ``bx.register(spark)`` is called explicitly here because the ``bng_chips``
    view helper (``bng_chips_df_heavy``) delegates to ``bng_chips_df`` which calls
    ``gx.register(spark)`` as a side effect, replacing the heavy agg UDF with the
    light BINARY variant.  Re-registering restores the STRUCT-returning heavy version.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    bx.register(spark)  # ensure the heavy (STRUCT-returning) BNG tier is registered
    df = spark.table("bng_chips")
    df_keyed = df.select(
        f.col("chip"),
        f.col("chip.cellid").alias("cellid"),
    )
    result = (
        df_keyed.groupBy("cellid")
        .agg(bx.bng_cellunion_agg("chip").alias("union_chip"))
        .filter(f.col("cellid") == "TQ3080")
        .first()
    )
    return result["union_chip"]


bng_cellunion_agg_python_heavy_example_output = """
+-------+--------------------+
|cellid |union_chip          |
+-------+--------------------+
|TQ3080 |{TQ3080, true, null}|
+-------+--------------------+
... (STRUCT — heavy tier returns chip struct; TQ3080 core chip: cellid=TQ3080, core=true, chip=null)
"""


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


def quadbin_pointascell_python_heavy_example(spark):
    """Convert WGS84 lon/lat to a quadbin cell at a given zoom (heavy quadbin tier).

    Creates an inline one-row DataFrame (lon=-122.4194, lat=37.7749, zoom=10)
    representing San Francisco at zoom 10.  Returns BIGINT cell ID
    ``5233961839712272383``.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.sql("SELECT -122.4194 AS lon, 37.7749 AS lat, 10 AS zoom")
    result = df.select(
        qx.quadbin_pointascell(f.col("lon"), f.col("lat"), f.col("zoom")).alias(
            "sf_cell"
        )
    ).first()
    return result["sf_cell"]


quadbin_pointascell_python_heavy_example_output = """
+-------------------+
|sf_cell            |
+-------------------+
|5233961839712272383|
+-------------------+
"""


def quadbin_aswkb_python_heavy_example(spark):
    """Return the quadbin cell footprint as EWKB geometry (heavy quadbin tier).

    Reads the ``quadbin_cells`` setup view (1 row: ``cell = 5233961839712272383``,
    San Francisco at zoom 10).  Returns the four-corner cell boundary polygon as
    EWKB bytes with embedded SRID 4326.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(qx.quadbin_aswkb(f.col("cell")).alias("wkb")).first()
    return result["wkb"]


quadbin_aswkb_python_heavy_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (EWKB binary — quadbin cell footprint polygon, SRID 4326)
"""


def quadbin_centroid_python_heavy_example(spark):
    """Return the quadbin cell centroid as an EWKB POINT (heavy quadbin tier).

    Reads the ``quadbin_cells`` setup view (``cell = 5233961839712272383``,
    San Francisco at zoom 10).  Returns the bbox-corner mean of the cell as an
    EWKB POINT with embedded SRID 4326.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(qx.quadbin_centroid(f.col("cell")).alias("centroid")).first()
    return result["centroid"]


quadbin_centroid_python_heavy_example_output = """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (EWKB binary — POINT at SF z10 cell centroid, SRID 4326)
"""


def quadbin_resolution_python_heavy_example(spark):
    """Return the resolution (zoom) of a quadbin cell (heavy quadbin tier).

    Reads the ``quadbin_cells`` setup view (``cell = 5233961839712272383``,
    San Francisco at zoom 10).  Extracts the zoom level → returns ``10`` as INT.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(qx.quadbin_resolution(f.col("cell")).alias("z")).first()
    return result["z"]


quadbin_resolution_python_heavy_example_output = """
+--+
|z |
+--+
|10|
+--+
"""


def quadbin_distance_python_heavy_example(spark):
    """Chebyshev distance between two adjacent quadbin cells (heavy quadbin tier).

    Reads the ``quadbin_cell_pairs`` setup view (1 row: ``cell1`` and ``cell2``
    — zoom-10 cells at (0.0, 0.0) and (0.0, 0.1), Chebyshev distance = 1).
    Returns INT.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_cell_pairs")
    result = df.select(
        qx.quadbin_distance(f.col("cell1"), f.col("cell2")).alias("d")
    ).first()
    return result["d"]


quadbin_distance_python_heavy_example_output = """
+--+
|d |
+--+
|1 |
+--+
"""


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


def quadbin_kring_python_heavy_example(spark):
    """Return all quadbin cells within Chebyshev distance k of a cell (heavy quadbin tier).

    Reads the ``quadbin_cells`` setup view (cell = 5233961839712272383, SF at
    zoom 10).  At k=1, returns the center cell plus the 8 surrounding cells →
    9 cells total.  Returns ARRAY<BIGINT>.  Identical to the lightweight output
    (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(qx.quadbin_kring(f.col("cell"), f.lit(1)).alias("kring")).first()
    return result["kring"]


quadbin_kring_python_heavy_example_output = """
+-------------------------------------+
|kring                                |
+-------------------------------------+
|[5233961839712272383, ..., (9 cells)]|
+-------------------------------------+
... (9 cells: SF z10 center plus 8 surrounding cells at k=1)
"""


def quadbin_polyfill_python_heavy_example(spark):
    """Polyfill a WGS84 geometry with quadbin cells at a given zoom (heavy quadbin tier).

    Reads the ``quadbin_polygons`` setup view (WGS84 polygon
    ``POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))`` near the origin).
    At zoom 5, the polygon is covered by 4 cells.  Returns ARRAY<BIGINT>.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_polygons")
    result = df.select(
        qx.quadbin_polyfill(f.col("geom"), f.lit(5)).alias("cells")
    ).first()
    return result["cells"]


quadbin_polyfill_python_heavy_example_output = """
+--------------------------+
|cells                     |
+--------------------------+
|[5211790668774506495, ...]|
+--------------------------+
... (4 cells covering the WGS84 polygon at zoom 5)
"""


def quadbin_tessellate_python_heavy_example(spark):
    """Tessellate a WGS84 geometry into quadbin cells with per-cell clip geometry (heavy quadbin tier).

    Reads the ``quadbin_polygons`` setup view (WGS84 polygon near origin).
    At zoom 5, produces 4 chips — each a STRUCT<cell BIGINT, geom BINARY> where
    ``geom`` is the cell's clipped boundary as EWKB (SRID 4326).
    Returns ARRAY<STRUCT<cell BIGINT, geom BINARY>>.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_polygons")
    result = df.select(
        qx.quadbin_tessellate(f.col("geom"), f.lit(5)).alias("chips")
    ).first()
    return result["chips"]


quadbin_tessellate_python_heavy_example_output = """
+----------------------------------------------+
|chips                                         |
+----------------------------------------------+
|[{5211790668774506495, [binary]}, {5212..., ..|
+----------------------------------------------+
... (4 chips: each quadbin cell paired with its clipped geometry WKB (SRID 4326))
"""


def quadbin_cellunion_python_heavy_example(spark):
    """Dissolve an array of quadbin cells into a single MultiPolygon EWKB (heavy quadbin tier).

    Reads the ``quadbin_cells`` setup view (cell = 5233961839712272383, SF at z10)
    and passes the k=1 kring (9 cells) to ``quadbin_cellunion``.  Returns BINARY
    EWKB with embedded SRID 4326.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(
        qx.quadbin_cellunion(qx.quadbin_kring(f.col("cell"), f.lit(1))).alias(
            "union_geom"
        )
    ).first()
    return result["union_geom"]


quadbin_cellunion_python_heavy_example_output = """
+-----------+
|union_geom |
+-----------+
|[binary]   |
+-----------+
... (EWKB binary — MultiPolygon dissolving the SF z10 kring, SRID 4326)
"""


def quadbin_cellunion_agg_python_heavy_example(spark):
    """Aggregate quadbin cells per group into a single MultiPolygon EWKB (heavy quadbin tier).

    Reads the ``quadbin_kring_cells`` setup view (9 rows: cells from the k=1
    kring around the SF z10 cell).  Groups all 9 cells under region ``'R1'``
    and dissolves them into one EWKB MultiPolygon.  Returns BINARY (SRID 4326).

    Both the light and heavy tiers return ``BINARY`` — there is no struct
    divergence for ``quadbin_cellunion_agg`` (unlike the BNG aggregators).
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    df = spark.table("quadbin_kring_cells")
    df_keyed = df.withColumn("region", f.lit("R1"))
    result = (
        df_keyed.groupBy("region")
        .agg(qx.quadbin_cellunion_agg(f.col("cell")).alias("coverage"))
        .first()
    )
    return result["coverage"]


quadbin_cellunion_agg_python_heavy_example_output = """
+------+--------+
|region|coverage|
+------+--------+
|R1    |[binary]|
+------+--------+
... (BINARY EWKB — dissolved coverage of all 9 kring cells around SF z10, SRID 4326)
"""


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


def custom_grid_python_heavy_example(spark):
    """Construct a custom regular grid descriptor (heavy gridx.custom tier).

    Calls ``cx.custom_grid`` directly on a one-row DataFrame to build the BNG-like
    grid spec: origin (0, 0), extent (1000000, 1000000), 2 splits per axis per level,
    1km root cells, EPSG:27700.  Returns the STRUCT descriptor passed to all other
    ``gbx_custom_*`` functions.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    base = spark.sql("SELECT 1 AS dummy")
    result = base.select(
        cx.custom_grid(
            f.lit(0),
            f.lit(1000000),
            f.lit(0),
            f.lit(1000000),
            f.lit(2),
            f.lit(1000),
            f.lit(1000),
            f.lit(27700),
        ).alias("grid")
    ).first()
    return result["grid"]


custom_grid_python_heavy_example_output = """
+----------------------------------------------+
|grid                                          |
+----------------------------------------------+
|{0, 1000000, 0, 1000000, 2, 1000, 1000, 27700}|
+----------------------------------------------+
"""


def custom_pointascell_python_heavy_example(spark):
    """Index a WKT point into a custom grid at a given resolution (heavy gridx.custom tier).

    Reads the ``custom_grids`` setup view (``point = 'POINT(530000 180000)'`` and
    ``grid`` struct, EPSG:27700 BNG-like grid, 1km root cells).  Resolution 5 maps
    the easting/northing to cell ``360287970373976640``.  Returns BIGINT.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    df = spark.table("custom_grids")
    result = df.select(
        cx.custom_pointascell(f.col("point"), f.col("grid"), f.lit(5)).alias("cell")
    ).first()
    return result["cell"]


custom_pointascell_python_heavy_example_output = """
+--------------------+
|cell                |
+--------------------+
|360287970373976640  |
+--------------------+
"""


def custom_cellaswkb_python_heavy_example(spark):
    """Return the WKB footprint polygon of a custom grid cell (heavy gridx.custom tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640`` at resolution 5,
    ``grid`` struct).  The cell is a 31.25m × 31.25m square at (530000, 180000)
    in EPSG:27700 — returned as plain WKB (no SRID).
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    df = spark.table("custom_grids")
    result = df.select(
        cx.custom_cellaswkb(f.col("cell"), f.col("grid")).alias("geom")
    ).first()
    return result["geom"]


custom_cellaswkb_python_heavy_example_output = """
+--------+
|geom    |
+--------+
|[binary]|
+--------+
... (WKB binary — 31.25m × 31.25m custom grid cell footprint polygon)
"""


def custom_cellaswkt_python_heavy_example(spark):
    """Return the WKT footprint polygon of a custom grid cell (heavy gridx.custom tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640``,
    ``grid`` struct).  Returns the cell boundary as a WKT POLYGON in the
    grid's CRS (EPSG:27700 for this example).
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    df = spark.table("custom_grids")
    result = df.select(
        cx.custom_cellaswkt(f.col("cell"), f.col("grid")).alias("wkt")
    ).first()
    return result["wkt"]


custom_cellaswkt_python_heavy_example_output = """
+----------------------------------------------------------------------------------------------------+
|wkt                                                                                                 |
+----------------------------------------------------------------------------------------------------+
|POLYGON ((530031.25 180000, 530031.25 180031.25, 530000 180031.25, 530000 180000, 530031.25 180000))|
+----------------------------------------------------------------------------------------------------+
"""


def custom_centroid_python_heavy_example(spark):
    """Return the centroid of a custom grid cell as a WKB POINT (heavy gridx.custom tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640``,
    ``grid`` struct).  Returns the center of the 31.25m × 31.25m cell as plain
    WKB (no SRID).  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    df = spark.table("custom_grids")
    result = df.select(
        cx.custom_centroid(f.col("cell"), f.col("grid")).alias("centroid")
    ).first()
    return result["centroid"]


custom_centroid_python_heavy_example_output = """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (WKB binary — POINT at the center of the 31.25m × 31.25m custom grid cell)
"""


def custom_polyfill_python_heavy_example(spark):
    """Fill a BNG polygon with custom grid cells at resolution 1 (500m cells) — heavy gridx.custom tier.

    Reads the ``custom_grids`` setup view for the ``grid`` struct and uses the
    3km × 3km BNG polygon centred on London.  At resolution 1 (500m cells),
    the polygon is covered by exactly 36 cells (6 per side).
    Returns ``ARRAY<BIGINT>``.  Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    df = spark.table("custom_grids")
    result = df.select(
        cx.custom_polyfill(f.lit(_CUSTOM_BNG_POLY_WKT), f.col("grid"), f.lit(1)).alias(
            "cells"
        )
    ).first()
    return result["cells"]


custom_polyfill_python_heavy_example_output = """
+---------------------------------------------+
|cells                                        |
+---------------------------------------------+
|[72057594038644994, ..., (36 cells at res=1)]|
+---------------------------------------------+
... (36 BIGINT cell IDs — 500m cells covering the 3km × 3km BNG polygon at resolution 1)
"""


def custom_kring_python_heavy_example(spark):
    """Return the 3×3 neighbourhood (k=1) around a custom grid cell (heavy gridx.custom tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640`` at resolution 5,
    ``grid`` struct).  At k=1, the filled neighbourhood contains 9 cells
    (center plus 8 surrounding).  Returns ``ARRAY<BIGINT>``.
    Identical to the lightweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    df = spark.table("custom_grids")
    result = df.select(
        cx.custom_kring(f.col("cell"), f.col("grid"), f.lit(1)).alias("ring")
    ).first()
    return result["ring"]


custom_kring_python_heavy_example_output = """
+-------------------------------------------+
|ring                                       |
+-------------------------------------------+
|[360287970373976640, ..., (9 cells at k=1)]|
+-------------------------------------------+
... (9 BIGINT cell IDs — the 3×3 neighbourhood including center cell at resolution 5)
"""
