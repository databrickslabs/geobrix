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


def bng_cellfill_python_heavy_example(spark):
    """Grouped aggregate: fill a NULL BNG cell from ring-1 neighbours (heavy bng tier).

    Inline data: London 100m cell ``TQ300800`` (NULL) surrounded by four ring-1
    neighbours each with value 5.0.  After fill the center equals 5.0 (mean).

    The **heavy** tier returns ``ARRAY<STRUCT<cellid STRING, value DOUBLE>>``.
    The light tier returns BINARY — a genuine divergence.

    Note: ``bx.register(spark)`` is called explicitly to ensure the heavy
    STRUCT-returning tier is active (pygx would register the BINARY variant).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.bng import functions as bx  # noqa: PLC0415

    bx.register(spark)
    df = spark.createDataFrame(
        [
            (1, "TQ300800", None),
            (1, "TQ299800", 5.0),
            (1, "TQ301800", 5.0),
            (1, "TQ300799", 5.0),
            (1, "TQ300801", 5.0),
        ],
        ["region", "cellid", "value"],
    )
    result = (
        df.groupBy("region")
        .agg(
            bx.bng_cellfill(f.col("cellid"), f.col("value"), 1, "mean", 2.0).alias("filled")
        )
        .first()
    )
    return result["filled"]


bng_cellfill_python_heavy_example_output = """
+------+---------------------------------------------------+
|region|filled                                             |
+------+---------------------------------------------------+
|1     |[{TQ300800, 5.0}, {TQ299800, 5.0}, ...(5 entries)]|
+------+---------------------------------------------------+
... (ARRAY<STRUCT<cellid STRING, value DOUBLE>> — heavy tier; center TQ300800 filled to 5.0)
"""


def quadbin_cellfill_python_heavy_example(spark):
    """Grouped aggregate: fill a NULL quadbin cell from ring-1 neighbours (heavy quadbin tier).

    Inline data: London z=10 center cell (NULL) plus two ring-1 neighbours with value 5.0.
    After fill the center equals 5.0 (mean).

    The **heavy** tier returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.
    The light tier returns BINARY — a genuine divergence.

    Note: ``qx.register(spark)`` is called to ensure the heavy tier is active.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qx  # noqa: PLC0415

    qx.register(spark)
    df = spark.createDataFrame(
        [
            (1, 5234261469560717311, None),
            (1, 5234261469560848383, 5.0),
            (1, 5234261469560586239, 5.0),
        ],
        ["region", "cellid", "value"],
    )
    result = (
        df.groupBy("region")
        .agg(
            qx.quadbin_cellfill(f.col("cellid"), f.col("value"), 1, "mean", 2.0).alias("filled")
        )
        .first()
    )
    return result["filled"]


quadbin_cellfill_python_heavy_example_output = """
+------+-------------------------------------------------------------+
|region|filled                                                       |
+------+-------------------------------------------------------------+
|1     |[{5234261469560717311, 5.0}, {5234261469560848383, 5.0}, ...]|
+------+-------------------------------------------------------------+
... (ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> — heavy tier; center cell filled to 5.0)
"""


def h3_cellfill_python_heavy_example(spark):
    """Grouped aggregate: fill a NULL H3 cell from ring-1 neighbours (heavy h3 tier).

    Inline data: London res-8 center H3 cell ``612934495919669247`` (NULL) plus two
    ring-1 neighbours with value 5.0.  After fill the center equals 5.0 (mean).

    The **heavy** tier returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.
    The light tier returns BINARY — a genuine divergence.

    Note: ``hx.register(spark)`` is called to load the Scala ``H3_CellFill``
    implementation (registers the heavy STRUCT-returning UDF).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.h3 import functions as hx  # noqa: PLC0415

    hx.register(spark)
    df = spark.createDataFrame(
        [
            (1, 612934495919669247, None),
            (1, 612934495863046143, 5.0),
            (1, 612934495900794879, 5.0),
        ],
        ["region", "cellid", "value"],
    )
    result = (
        df.groupBy("region")
        .agg(
            hx.h3_cellfill(f.col("cellid"), f.col("value"), 1, "mean", 2.0).alias("filled")
        )
        .first()
    )
    return result["filled"]


h3_cellfill_python_heavy_example_output = """
+------+----------------------------------------------------------+
|region|filled                                                    |
+------+----------------------------------------------------------+
|1     |[{612934495919669247, 5.0}, {612934495863046143, 5.0}, ...]|
+------+----------------------------------------------------------+
... (ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> — heavy tier; center H3 cell filled to 5.0)
"""


def custom_cellfill_python_heavy_example(spark):
    """Grouped aggregate: fill a NULL custom-grid cell from ring-1 neighbours (heavy custom tier).

    Reads the ``custom_grids`` setup view for the grid spec.  Uses
    ``custom_pointascell`` at resolution 0 to derive valid cell IDs for the
    center (NULL) and two ring-1 neighbours (value 5.0).  After fill the center
    equals 5.0 (mean).

    The **heavy** tier returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.
    The light tier returns BINARY — a genuine divergence.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415

    cx.register(spark)
    df_grid = spark.table("custom_grids")
    center_row = df_grid.select(
        cx.custom_pointascell(f.lit("POINT(530000 180000)"), f.col("grid"), f.lit(0)).alias("center"),
        cx.custom_pointascell(f.lit("POINT(531000 180000)"), f.col("grid"), f.lit(0)).alias("nbr1"),
        cx.custom_pointascell(f.lit("POINT(531000 181000)"), f.col("grid"), f.lit(0)).alias("nbr2"),
        f.col("grid"),
    ).first()
    center_id = center_row["center"]
    nbr1_id = center_row["nbr1"]
    nbr2_id = center_row["nbr2"]

    df = spark.createDataFrame(
        [
            (1, center_id, None),
            (1, nbr1_id, 5.0),
            (1, nbr2_id, 5.0),
        ],
        ["region", "cellid", "value"],
    )
    df = df.crossJoin(df_grid.select("grid"))
    result = (
        df.groupBy("region")
        .agg(
            cx.custom_cellfill(
                f.col("cellid"),
                f.col("value"),
                f.col("grid"),
                1,
                "mean",
                2.0,
            ).alias("filled")
        )
        .first()
    )
    return result["filled"]


custom_cellfill_python_heavy_example_output = """
+------+---------------------------------------------------------+
|region|filled                                                   |
+------+---------------------------------------------------------+
|1     |[{216172782113787048, 5.0}, {216172782113786967, 5.0}, ...]|
+------+---------------------------------------------------------+
... (ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> — heavy tier; center cell filled to 5.0)
"""


