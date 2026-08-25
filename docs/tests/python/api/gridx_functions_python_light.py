"""
Python code examples for the light (pygx) tier of GridX functions.

Single source of truth for the Python (light) tab in
docs/docs/api/gridx-functions.mdx. Consumed by the FunctionExamples
component (``pythonLight`` prop) and by the generate-function-info.py
``_TIER_SCANS`` binding detector.

All examples use the shared canonical fixtures from ``_fixtures.py``
so every function's four tabs show the SAME example — the same fixture,
operation, and argument values expressed in each tier's language.

Fixture view assignments (created by create_setup_views_gridx_light)
----------------------------------------------------------------------
``bng_cells``          — 1 row: cellid STRING = 'TQ3080'
                         Backs: bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid,
                                bng_kring, bng_kloop
``bng_cell_pairs``     — 1 row: cellid1 STRING, cellid2 STRING (adjacent cells)
                         Backs: bng_distance, bng_euclideandistance
``bng_points``         — 1 row: easting INT, northing INT, geom STRING (EPSG:27700)
                         Backs: bng_pointascell, bng_eastnorthasbng
``bng_polygons``       — 1 row: geom STRING (3km × 3km BNG polygon, EPSG:27700)
                         Backs: bng_geomkring, bng_geomkloop, bng_polyfill,
                                bng_tessellate, and the 5 *explode generators
``bng_chips``          — 9 rows: chip STRUCT<cellid STRING, core BOOLEAN, chip BINARY>
                         (from bng_tessellate of the BNG polygon at res=3)
                         Backs: bng_cellintersection, bng_cellunion,
                                bng_cellintersection_agg, bng_cellunion_agg
``quadbin_cells``      — 1 row: cell LONG = 5233961839712272383 (SF at z10)
                         Backs: quadbin_aswkb, quadbin_centroid, quadbin_resolution,
                                quadbin_kring, quadbin_cellunion
``quadbin_cell_pairs`` — 1 row: cell1 LONG, cell2 LONG (distance = 1 at z10)
                         Backs: quadbin_distance
``quadbin_polygons``   — 1 row: geom STRING (WGS84 polygon near origin)
                         Backs: quadbin_polyfill, quadbin_tessellate
``quadbin_kring_cells``— 9 rows: cell LONG (kring of SF-z10, k=1)
                         Backs: quadbin_cellunion_agg
``custom_grids``       — 1 row: grid STRUCT, cell LONG, point STRING
                         Backs: all 7 custom_* functions

Per-function examples (T2–T7 batches)
--------------------------------------
BNG scalar (T2):       bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid,
                       bng_distance, bng_euclideandistance,
                       bng_eastnorthasbng, bng_pointascell
BNG neighbourhood (T4): bng_kring, bng_kloop, bng_geomkring, bng_geomkloop,
                         bng_polyfill, bng_tessellate
BNG chip ops (T3):     bng_cellintersection, bng_cellunion,
                       bng_cellintersection_agg, bng_cellunion_agg
BNG explode (T5):      bng_kringexplode, bng_kloopexplode,
                       bng_geomkringexplode, bng_geomkloopexplode,
                       bng_tessellateexplode  (SQL LATERAL only for light tier)
Quadbin (T6):          quadbin_pointascell, quadbin_aswkb, quadbin_centroid,
                       quadbin_resolution, quadbin_distance, quadbin_kring,
                       quadbin_polyfill, quadbin_tessellate,
                       quadbin_cellunion, quadbin_cellunion_agg
Custom (T7):           custom_grid, custom_pointascell, custom_cellaswkb,
                       custom_cellaswkt, custom_centroid,
                       custom_polyfill, custom_kring

Light-tier UDTF invocation form
---------------------------------
The 5 BNG ``*explode`` functions (``bng_kringexplode``, ``bng_kloopexplode``,
``bng_geomkringexplode``, ``bng_geomkloopexplode``, ``bng_tessellateexplode``)
are Python UDTFs in the lightweight tier — they have **no Python DataFrame
Column form** (calling them as Column expressions raises ``NotImplementedError``).
The Python (light) tab for these functions uses::

    spark.sql("SELECT t.* FROM <view>, LATERAL gbx_bng_<fn>(...) t")

This is the same invocation as the SQL tab, driven via Python. SQL ``LATERAL``
works for both tiers; the DataFrame Column form is SQL-only for these generators.
"""

try:
    from databricks.labs.gbx.pygx import functions as gx
except ImportError:
    gx = None


# ---------------------------------------------------------------------------
# Shared helpers — imported from _fixtures.py
# ---------------------------------------------------------------------------


def _get_bng_cells_df(spark):
    from ._fixtures import bng_cells_df  # noqa: PLC0415

    return bng_cells_df(spark)


def _get_bng_cell_pairs_df(spark):
    from ._fixtures import bng_cell_pairs_df  # noqa: PLC0415

    return bng_cell_pairs_df(spark)


def _get_bng_coordinates_df(spark):
    from ._fixtures import bng_coordinates_df  # noqa: PLC0415

    return bng_coordinates_df(spark)


def _get_bng_polygons_df(spark):
    from ._fixtures import bng_polygons_df  # noqa: PLC0415

    return bng_polygons_df(spark)


def _get_bng_chips_df(spark):
    from ._fixtures import bng_chips_df  # noqa: PLC0415

    return bng_chips_df(spark)


def _get_quadbin_cells_df(spark):
    from ._fixtures import quadbin_cells_df  # noqa: PLC0415

    return quadbin_cells_df(spark)


def _get_quadbin_cell_pairs_df(spark):
    from ._fixtures import quadbin_cell_pairs_df  # noqa: PLC0415

    return quadbin_cell_pairs_df(spark)


def _get_quadbin_polygons_df(spark):
    from ._fixtures import quadbin_polygons_df  # noqa: PLC0415

    return quadbin_polygons_df(spark)


def _get_quadbin_kring_cells_df(spark):
    from ._fixtures import quadbin_kring_cells_df  # noqa: PLC0415

    return quadbin_kring_cells_df(spark)


def _get_custom_grid_df(spark):
    from ._fixtures import custom_grid_df  # noqa: PLC0415

    return custom_grid_df(spark)


# ---------------------------------------------------------------------------
# Setup example
# ---------------------------------------------------------------------------


def gridx_light_setup_example(spark):
    """Register the GridX lightweight (pygx) functions for this session.

    After this call, all ``gbx_bng_*``, ``gbx_quadbin_*``, and
    ``gbx_custom_*`` SQL functions and their Python Column equivalents
    are available. Both sub-packages (bng + quadbin + custom) are registered
    in a single call.
    """
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    gx.register(spark)


gridx_light_setup_example_output = (
    "GridX lightweight (pygx) registered: gbx_bng_*, gbx_quadbin_*, gbx_custom_*"
)


# ---------------------------------------------------------------------------
# Per-function examples — added by T2–T7 batches
# ---------------------------------------------------------------------------
# Naming convention:  def <base>_python_light_example(spark):
#                         ...
#                     <base>_python_light_example_output = "..."
#
# where <base> matches the SQL name without the gbx_ prefix, e.g.
#   bng_aswkb, bng_tessellate, quadbin_pointascell, custom_grid, ...


# ---------------------------------------------------------------------------
# T2: BNG codec + accessors
# bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid,
# bng_eastnorthasbng, bng_pointascell
#
# Fixture views:
#   bng_cells  — 1 row: cellid STRING = 'TQ3080'  (1km cell, central London)
#                Backs: bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid
#   bng_points — 1 row: easting INT = 530000, northing INT = 180000,
#                       geom STRING = 'POINT(530000 180000)' (EPSG:27700)
#                Backs: bng_eastnorthasbng, bng_pointascell
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


def bng_aswkb_python_light_example(spark):
    """Convert a BNG cell ID to Well-Known Binary (WKB) geometry (light pygx tier).

    Reads the ``bng_cells`` setup view (1 row: ``cellid = 'TQ3080'``, a 1km cell
    in central London, EPSG:27700).  Returns the WKB bytes for the cell footprint
    polygon — a 1km square in BNG eastings/northings with no embedded SRID.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cells_df(spark)
    result = df.select(gx.bng_aswkb(f.col("cellid")).alias("wkb")).first()
    return result["wkb"]


bng_aswkb_python_light_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (WKB binary)
"""


def bng_aswkt_python_light_example(spark):
    """Convert a BNG cell ID to Well-Known Text (WKT) geometry string (light pygx tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  Returns the WKT
    polygon string for the TQ3080 cell footprint in EPSG:27700 coordinates.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cells_df(spark)
    result = df.select(gx.bng_aswkt(f.col("cellid")).alias("wkt")).first()
    return result["wkt"]


bng_aswkt_python_light_example_output = """
+----------------------------------------------------------+
|wkt                                                       |
+----------------------------------------------------------+
|POLYGON ((531000 180000, 531000 181000, 530000 181000, ...|
+----------------------------------------------------------+
POLYGON ((531000 180000, 531000 181000, 530000 181000, 530000 180000, 531000 180000))
"""


def bng_cellarea_python_light_example(spark):
    """Return the area of a BNG cell in square kilometres (light pygx tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  TQ3080 is a
    1km × 1km cell at resolution 3 (integer index) — area = 1.0 sq km.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cells_df(spark)
    result = df.select(gx.bng_cellarea(f.col("cellid")).alias("area_km2")).first()
    return result["area_km2"]


bng_cellarea_python_light_example_output = """
+---------+
|area_km2 |
+---------+
|1.0      |
+---------+
"""


def bng_centroid_python_light_example(spark):
    """Return the centroid of a BNG cell as a WKB POINT (light pygx tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  The centroid of
    the 1km TQ3080 cell is POINT(530500 180500) in EPSG:27700 — returned as
    plain WKB (no SRID).  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cells_df(spark)
    result = df.select(gx.bng_centroid(f.col("cellid")).alias("centroid")).first()
    return result["centroid"]


bng_centroid_python_light_example_output = """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (WKB binary — POINT(530500 180500) in EPSG:27700)
"""


def bng_eastnorthasbng_python_light_example(spark):
    """Convert BNG easting/northing coordinates to a BNG cell reference (light pygx tier).

    Reads the ``bng_points`` setup view (easting=530000, northing=180000,
    EPSG:27700).  Resolution string ``'1km'`` (equivalent to integer index 3)
    maps the coordinates to cell ``'TQ3080'``.  Identical to the heavyweight
    output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_coordinates_df(spark)
    result = df.select(
        gx.bng_eastnorthasbng(f.col("easting"), f.col("northing"), f.lit("1km")).alias(
            "bng_cell"
        )
    ).first()
    return result["bng_cell"]


bng_eastnorthasbng_python_light_example_output = """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+
"""


def bng_pointascell_python_light_example(spark):
    """Convert a WKT point in BNG coordinates to a BNG cell reference (light pygx tier).

    Reads the ``bng_points`` setup view (``geom = 'POINT(530000 180000)'`` in
    EPSG:27700, central London).  Resolution ``'1km'`` → cell ``'TQ3080'``.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_coordinates_df(spark)
    result = df.select(
        gx.bng_pointascell(f.col("geom"), f.lit("1km")).alias("bng_cell")
    ).first()
    return result["bng_cell"]


bng_pointascell_python_light_example_output = """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+
"""


# ---------------------------------------------------------------------------
# T3: BNG distance + chip ops
# bng_distance, bng_euclideandistance, bng_cellintersection, bng_cellunion
#
# Fixture views:
#   bng_cell_pairs — 1 row: cellid1='TQ3080', cellid2='TQ3081' (adjacent, dist=1)
#                   Backs: bng_distance, bng_euclideandistance
#   bng_chips      — 9 rows: chip STRUCT<cellid,core,chip> from bng_tessellate
#                   Backs: bng_cellintersection, bng_cellunion
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


def bng_distance_python_light_example(spark):
    """Grid-step distance between two adjacent BNG cells (light pygx tier).

    Reads the ``bng_cell_pairs`` setup view (cellid1='TQ3080', cellid2='TQ3081',
    two adjacent 1km cells, EPSG:27700).  The two cells share an edge — distance
    = 1 grid step.  Returns LONG (grid steps, not metres).
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cell_pairs_df(spark)
    result = df.select(
        gx.bng_distance(f.col("cellid1"), f.col("cellid2")).alias("dist_steps")
    ).first()
    return result["dist_steps"]


bng_distance_python_light_example_output = """
+----------+
|dist_steps|
+----------+
|1         |
+----------+
"""


def bng_euclideandistance_python_light_example(spark):
    """Chebyshev grid-unit distance between two adjacent BNG cells (light pygx tier).

    Reads the ``bng_cell_pairs`` setup view (cellid1='TQ3080', cellid2='TQ3081').
    Returns LONG (grid units).  For adjacent cells at the same resolution,
    Chebyshev distance = 1.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cell_pairs_df(spark)
    result = df.select(
        gx.bng_euclideandistance(f.col("cellid1"), f.col("cellid2")).alias(
            "euclidean_dist"
        )
    ).first()
    return result["euclidean_dist"]


bng_euclideandistance_python_light_example_output = """
+--------------+
|euclidean_dist|
+--------------+
|1             |
+--------------+
"""


def bng_cellintersection_python_light_example(spark):
    """Intersect two BNG chip structs — returns the dissolved intersection chip (light pygx tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Filters for the core chip (TQ3080, fully
    interior: core=True, chip=None) and intersects it with itself.  The result is
    a STRUCT<cellid STRING, core BOOLEAN, chip BINARY>.  Chip inputs must come
    from ``bng_tessellate`` — passing plain cell-id strings raises ClassCastException.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_chips_df(spark)
    result = (
        df.filter(f.col("chip.cellid") == "TQ3080")
        .select(
            gx.bng_cellintersection(f.col("chip"), f.col("chip")).alias(
                "intersection_chip"
            )
        )
        .first()
    )
    return result["intersection_chip"]


bng_cellintersection_python_light_example_output = """
+--------------------+
|intersection_chip   |
+--------------------+
|{TQ3080, true, null}|
+--------------------+
"""


def bng_cellunion_python_light_example(spark):
    """Union two BNG chip structs — returns the dissolved union chip (light pygx tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Filters for the core chip (TQ3080, fully
    interior: core=True, chip=None) and unions it with itself.  The result is
    a STRUCT<cellid STRING, core BOOLEAN, chip BINARY>.  Chip inputs must come
    from ``bng_tessellate`` — passing plain cell-id strings raises ClassCastException.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_chips_df(spark)
    result = (
        df.filter(f.col("chip.cellid") == "TQ3080")
        .select(gx.bng_cellunion(f.col("chip"), f.col("chip")).alias("union_chip"))
        .first()
    )
    return result["union_chip"]


bng_cellunion_python_light_example_output = """
+--------------------+
|union_chip          |
+--------------------+
|{TQ3080, true, null}|
+--------------------+
"""


# ---------------------------------------------------------------------------
# T4: BNG neighbourhood/fill
# bng_kring, bng_kloop, bng_geomkring, bng_geomkloop, bng_polyfill,
# bng_tessellate
#
# Fixture views:
#   bng_cells    — 1 row: cellid STRING = 'TQ3080'
#                  Backs: bng_kring, bng_kloop
#   bng_polygons — 1 row: geom STRING (3km × 3km BNG polygon, EPSG:27700)
#                  Backs: bng_geomkring, bng_geomkloop, bng_polyfill,
#                         bng_tessellate
# All four tabs share ONE example per function (AGREE — no tier divergence).
# Geometry inputs MUST be EPSG:27700 eastings/northings; WGS84 coords yield
# empty arrays.
# ---------------------------------------------------------------------------


def bng_kring_python_light_example(spark):
    """Filled disk of BNG cells within k grid steps of a center cell (light pygx tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  At k=1 the result
    is the center cell plus the 8 surrounding 1km cells → 9 cells total.
    Returns ARRAY<STRING>.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cells_df(spark)
    result = df.select(gx.bng_kring(f.col("cellid"), f.lit(1)).alias("kring")).first()
    return result["kring"]


bng_kring_python_light_example_output = """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells: center TQ3080 plus 8 surrounding cells at k=1)
"""


def bng_kloop_python_light_example(spark):
    """Hollow ring of BNG cells at exactly k grid steps from a center cell (light pygx tier).

    Reads the ``bng_cells`` setup view (``cellid = 'TQ3080'``).  At k=1 the result
    is the 8 surrounding cells (excludes center) → 8 cells total.
    Returns ARRAY<STRING>.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_cells_df(spark)
    result = df.select(gx.bng_kloop(f.col("cellid"), f.lit(1)).alias("kloop")).first()
    return result["kloop"]


bng_kloop_python_light_example_output = """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (8 cells: hollow ring at k=1, center TQ3080 excluded)
"""


def bng_geomkring_python_light_example(spark):
    """Polyfill a BNG geometry then expand by k ring steps (light pygx tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polyfill covers 9 cells; k=1 expands by one ring →
    25 cells total.  Returns ARRAY<STRING>.  Identical to the heavyweight output
    (AGREE).  Geometry MUST be in EPSG:27700; WGS84 yields an empty array.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_polygons_df(spark)
    result = df.select(
        gx.bng_geomkring(f.col("geom"), f.lit(3), f.lit(1)).alias("kring")
    ).first()
    return result["kring"]


bng_geomkring_python_light_example_output = """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (25 cells: polyfill of BNG polygon expanded by k=1 ring)
"""


def bng_geomkloop_python_light_example(spark):
    """Return only the outer ring of BNG cells around a geometry (light pygx tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polyfill covers 9 cells; k=1 returns the 16 outer cells
    (hollow shell, excluding the interior 9).  Returns ARRAY<STRING>.  Identical
    to the heavyweight output (AGREE).  Geometry MUST be in EPSG:27700.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_polygons_df(spark)
    result = df.select(
        gx.bng_geomkloop(f.col("geom"), f.lit(3), f.lit(1)).alias("kloop")
    ).first()
    return result["kloop"]


bng_geomkloop_python_light_example_output = """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (16 cells: outer ring at k=1 around the BNG polygon polyfill)
"""


def bng_polyfill_python_light_example(spark):
    """Fill a geometry with all BNG cells at given resolution (light pygx tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polygon is covered by 9 cells.  Returns ARRAY<STRING>.
    Identical to the heavyweight output (AGREE).  Geometry MUST be in EPSG:27700;
    WGS84 yields an empty array.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_polygons_df(spark)
    result = df.select(gx.bng_polyfill(f.col("geom"), f.lit(3)).alias("cells")).first()
    return result["cells"]


bng_polyfill_python_light_example_output = """
+-----------------------------+
|cells                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells covering the 3km × 3km BNG polygon at 1km resolution)
"""


def bng_tessellate_python_light_example(spark):
    """Tessellate a geometry into BNG cells, splitting border cells at the boundary (light pygx tier).

    Reads the ``bng_polygons`` setup view (3km × 3km polygon in EPSG:27700).
    At res=3 (1km) the polygon produces 9 chips.  Center cell TQ3080 is a core
    chip (core=True, chip=None); the 8 border cells carry a WKB clipped polygon.
    Returns ARRAY<STRUCT<cellid STRING, core BOOLEAN, chip BINARY>>.
    Identical to the heavyweight output (AGREE).  Geometry MUST be in EPSG:27700.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_polygons_df(spark)
    result = df.select(
        gx.bng_tessellate(f.col("geom"), f.lit(3)).alias("chips")
    ).first()
    return result["chips"]


bng_tessellate_python_light_example_output = """
+--------------------------------------------+
|chips                                       |
+--------------------------------------------+
|[{TQ2979, false, [binary]}, {TQ3080, true,..|
+--------------------------------------------+
... (9 chips; TQ3080 is core (core=true, chip=null); border cells carry WKB clip geometry)
"""


# ---------------------------------------------------------------------------
# T5: BNG aggregators + explode UDTFs
# bng_cellintersection_agg, bng_cellunion_agg,
# bng_kringexplode, bng_kloopexplode, bng_geomkringexplode,
# bng_geomkloopexplode, bng_tessellateexplode
#
# Fixture views used:
#   bng_chips    — 9 rows: chip STRUCT<cellid,core,chip> from bng_tessellate
#                  Backs: bng_cellintersection_agg, bng_cellunion_agg
#   bng_cells    — 1 row: cellid STRING = 'TQ3080'
#                  Backs: bng_kringexplode, bng_kloopexplode
#   bng_polygons — 1 row: geom STRING (3km × 3km BNG polygon, EPSG:27700)
#                  Backs: bng_geomkringexplode, bng_geomkloopexplode,
#                         bng_tessellateexplode
#
# Aggregators: grouped-agg pandas_udf → BINARY (dissolved chip WKB).
#   Light tier returns BINARY; heavy tier returns STRUCT<cellid,core,chip>.
#   This is a GENUINE tier divergence (C4): use the :::warning callout in MDX.
#
# Explode UDTFs: SQL LATERAL is the ONLY invocation form for the light tier.
#   Calling these as Column expressions raises NotImplementedError.
#   The Python (light) tab drives the SAME LATERAL SQL via spark.sql(…).
# ---------------------------------------------------------------------------


def bng_cellintersection_agg_python_light_example(spark):
    """Aggregate intersection of chip structs per BNG cell (light pygx tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Groups by ``chip.cellid`` and intersects chips
    within each group.  Each group holds exactly one chip so the aggregate returns
    that chip's dissolved geometry.

    The light tier returns **BINARY** (dissolved chip WKB in EPSG:27700, no SRID).
    The heavy tier returns ``STRUCT<cellid, core, chip>`` — see the MDX :::warning.
    Chip inputs must come from ``bng_tessellate``; plain STRING cell IDs raise
    ``ClassCastException``.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_chips_df(spark)
    df_keyed = df.select(
        f.col("chip"),
        f.col("chip.cellid").alias("cellid"),
    )
    result = (
        df_keyed.groupBy("cellid")
        .agg(gx.bng_cellintersection_agg("chip").alias("common_chip"))
        .filter(f.col("cellid") == "TQ3080")
        .first()
    )
    return result["common_chip"]


bng_cellintersection_agg_python_light_example_output = """
+-------+------------+
|cellid |common_chip |
+-------+------------+
|TQ3080 |[binary]    |
+-------+------------+
... (BINARY — dissolved chip WKB for TQ3080; core cell → full cell polygon in EPSG:27700)
"""


def bng_cellunion_agg_python_light_example(spark):
    """Aggregate union of chip structs per BNG cell (light pygx tier).

    Reads the ``bng_chips`` setup view (9 chips from tessellating the 3km × 3km
    BNG polygon at resolution 3).  Groups by ``chip.cellid`` and dissolves chips
    within each group.  Each group holds exactly one chip so the aggregate returns
    that chip's dissolved geometry.

    The light tier returns **BINARY** (dissolved chip WKB in EPSG:27700, no SRID).
    The heavy tier returns ``STRUCT<cellid, core, chip>`` — see the MDX :::warning.
    Chip inputs must come from ``bng_tessellate``; plain STRING cell IDs raise
    ``ClassCastException``.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_bng_chips_df(spark)
    df_keyed = df.select(
        f.col("chip"),
        f.col("chip.cellid").alias("cellid"),
    )
    result = (
        df_keyed.groupBy("cellid")
        .agg(gx.bng_cellunion_agg("chip").alias("union_chip"))
        .filter(f.col("cellid") == "TQ3080")
        .first()
    )
    return result["union_chip"]


bng_cellunion_agg_python_light_example_output = """
+-------+----------+
|cellid |union_chip|
+-------+----------+
|TQ3080 |[binary]  |
+-------+----------+
... (BINARY — dissolved chip WKB for TQ3080; core cell → full cell polygon in EPSG:27700)
"""


def bng_kringexplode_python_light_example(spark):
    """Explode k-ring into one row per cell via SQL LATERAL (light pygx tier).

    The light UDTF (``gbx_bng_kringexplode``) has **no Python Column form** —
    calling it as a Column expression raises ``NotImplementedError``.  Use
    SQL ``LATERAL`` via ``spark.sql(…)``.  At k=1, 9 rows are returned (center
    TQ3080 plus 8 surrounding cells).  Each row carries a single ``cellid STRING``
    column.  Identical to the heavyweight SQL output (AGREE).
    """
    result = spark.sql("""
        SELECT t.*
        FROM (SELECT 'TQ3080' AS cellid) src,
        LATERAL gbx_bng_kringexplode(src.cellid, 1) t
        """)
    return result.collect()


bng_kringexplode_python_light_example_output = """
+------+
|cellid|
+------+
|TQ2979|
|TQ2980|
|TQ2981|
|TQ3079|
|TQ3080|
|TQ3081|
|TQ3179|
|TQ3180|
|TQ3181|
+------+
... (9 rows: center TQ3080 plus 8 surrounding cells at k=1)
"""


def bng_kloopexplode_python_light_example(spark):
    """Explode k-loop (hollow ring) into one row per cell via SQL LATERAL (light pygx tier).

    The light UDTF (``gbx_bng_kloopexplode``) has **no Python Column form** —
    calling it as a Column expression raises ``NotImplementedError``.  Use
    SQL ``LATERAL`` via ``spark.sql(…)``.  At k=1, 8 rows are returned (center
    TQ3080 is excluded).  Each row carries a single ``cellid STRING`` column.
    Identical to the heavyweight SQL output (AGREE).
    """
    result = spark.sql("""
        SELECT t.*
        FROM (SELECT 'TQ3080' AS cellid) src,
        LATERAL gbx_bng_kloopexplode(src.cellid, 1) t
        """)
    return result.collect()


bng_kloopexplode_python_light_example_output = """
+------+
|cellid|
+------+
|TQ2979|
|TQ2980|
|TQ2981|
|TQ3079|
|TQ3081|
|TQ3179|
|TQ3180|
|TQ3181|
+------+
... (8 rows: hollow ring at k=1, center TQ3080 excluded)
"""


def bng_geomkringexplode_python_light_example(spark):
    """Explode geometry k-ring into one row per cell via SQL LATERAL (light pygx tier).

    The light UDTF (``gbx_bng_geomkringexplode``) has **no Python Column form** —
    calling it as a Column expression raises ``NotImplementedError``.  Use
    SQL ``LATERAL`` via ``spark.sql(…)``.  Geometry MUST be in EPSG:27700
    (BNG eastings/northings) — WGS84 lon/lat yields empty results.
    At res=3 (1km), k=1: the 9-cell polyfill expands to 25 cells.
    Each row carries a single ``cellid STRING`` column.
    Identical to the heavyweight SQL output (AGREE).
    """
    df = _get_bng_polygons_df(spark)
    df.createOrReplaceTempView("_bng_polygons_tmp")
    result = spark.sql(
        "SELECT t.* FROM _bng_polygons_tmp src, "
        "LATERAL gbx_bng_geomkringexplode(src.geom, 3, 1) t"
    )
    return result.collect()


bng_geomkringexplode_python_light_example_output = """
+------+
|cellid|
+------+
|TQ2878|
|TQ2879|
|TQ2880|
|...   |
+------+
... (25 rows: polyfill of BNG polygon at res=3 expanded by k=1 ring)
"""


def bng_geomkloopexplode_python_light_example(spark):
    """Explode geometry k-loop (hollow ring) into one row per cell via SQL LATERAL (light pygx tier).

    The light UDTF (``gbx_bng_geomkloopexplode``) has **no Python Column form** —
    calling it as a Column expression raises ``NotImplementedError``.  Use
    SQL ``LATERAL`` via ``spark.sql(…)``.  Geometry MUST be in EPSG:27700
    (BNG eastings/northings) — WGS84 lon/lat yields empty results.
    At res=3 (1km), k=1: the outer hollow ring contains 16 cells.
    Each row carries a single ``cellid STRING`` column.
    Identical to the heavyweight SQL output (AGREE).
    """
    df = _get_bng_polygons_df(spark)
    df.createOrReplaceTempView("_bng_polygons_tmp")
    result = spark.sql(
        "SELECT t.* FROM _bng_polygons_tmp src, "
        "LATERAL gbx_bng_geomkloopexplode(src.geom, 3, 1) t"
    )
    return result.collect()


bng_geomkloopexplode_python_light_example_output = """
+------+
|cellid|
+------+
|TQ2878|
|TQ2879|
|TQ2880|
|...   |
+------+
... (16 rows: outer ring at k=1 around the BNG polygon polyfill)
"""


def bng_tessellateexplode_python_light_example(spark):
    """Explode tessellation into one row per chip via SQL LATERAL (light pygx tier).

    The light UDTF (``gbx_bng_tessellateexplode``) has **no Python Column form** —
    calling it as a Column expression raises ``NotImplementedError``.  Use
    SQL ``LATERAL`` via ``spark.sql(…)``.  Geometry MUST be in EPSG:27700
    (BNG eastings/northings) — WGS84 lon/lat yields empty results.
    At res=3 (1km): 9 rows, each with ``cellid STRING``, ``core BOOLEAN``,
    ``chip BINARY``.  The TQ3080 row has ``core=true, chip=null`` (full cell).

    Note: the heavy CollectionGenerator exposes only ``cellid`` via SQL LATERAL
    (``elementSchema`` bug); the light UDTF correctly returns all three fields
    and is the preferred form here.
    """
    df = _get_bng_polygons_df(spark)
    df.createOrReplaceTempView("_bng_polygons_tmp")
    result = spark.sql(
        "SELECT t.* FROM _bng_polygons_tmp src, "
        "LATERAL gbx_bng_tessellateexplode(src.geom, 3) t"
    )
    return result.collect()


bng_tessellateexplode_python_light_example_output = """
+------+-----+--------+
|cellid|core |chip    |
+------+-----+--------+
|TQ2979|false|[binary]|
|TQ2980|false|[binary]|
|TQ2981|false|[binary]|
|TQ3079|false|[binary]|
|TQ3080|true |null    |
|...   |...  |...     |
+------+-----+--------+
... (9 rows; TQ3080 is core (core=true, chip=null); border cells carry WKB clip geometry)
"""


# ---------------------------------------------------------------------------
# T6a: Quadbin codec + scalar
# quadbin_pointascell, quadbin_aswkb, quadbin_centroid,
# quadbin_resolution, quadbin_distance
#
# Fixture views:
#   quadbin_cells      — 1 row: cell LONG = 5233961839712272383 (SF at z10)
#                        Backs: quadbin_aswkb, quadbin_centroid, quadbin_resolution
#   quadbin_cell_pairs — 1 row: cell1 LONG, cell2 LONG (z10, distance=1)
#                        Backs: quadbin_distance
#
# quadbin_pointascell has no pre-existing coordinate fixture — uses an inline
# one-row DataFrame (lon/lat/zoom) consistent with all four tabs.
#
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


def quadbin_pointascell_python_light_example(spark):
    """Convert WGS84 lon/lat to a quadbin cell at a given zoom (light pygx tier).

    Creates an inline one-row DataFrame (lon=-122.4194, lat=37.7749, zoom=10)
    representing San Francisco at zoom 10.  Returns BIGINT cell ID
    ``5233961839712272383``.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.sql("SELECT -122.4194 AS lon, 37.7749 AS lat, 10 AS zoom")
    result = df.select(
        gx.quadbin_pointascell(f.col("lon"), f.col("lat"), f.col("zoom")).alias(
            "sf_cell"
        )
    ).first()
    return result["sf_cell"]


quadbin_pointascell_python_light_example_output = """
+-------------------+
|sf_cell            |
+-------------------+
|5233961839712272383|
+-------------------+
"""


def quadbin_aswkb_python_light_example(spark):
    """Return the quadbin cell footprint as EWKB geometry (light pygx tier).

    Reads the ``quadbin_cells`` setup view (1 row: ``cell = 5233961839712272383``,
    San Francisco at zoom 10).  Returns the four-corner cell boundary polygon as
    EWKB bytes with embedded SRID 4326.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(gx.quadbin_aswkb(f.col("cell")).alias("wkb")).first()
    return result["wkb"]


quadbin_aswkb_python_light_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (EWKB binary — quadbin cell footprint polygon, SRID 4326)
"""


def quadbin_centroid_python_light_example(spark):
    """Return the quadbin cell centroid as an EWKB POINT (light pygx tier).

    Reads the ``quadbin_cells`` setup view (``cell = 5233961839712272383``,
    San Francisco at zoom 10).  Returns the bbox-corner mean of the cell as an
    EWKB POINT with embedded SRID 4326.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(gx.quadbin_centroid(f.col("cell")).alias("centroid")).first()
    return result["centroid"]


quadbin_centroid_python_light_example_output = """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (EWKB binary — POINT at SF z10 cell centroid, SRID 4326)
"""


def quadbin_resolution_python_light_example(spark):
    """Return the resolution (zoom) of a quadbin cell (light pygx tier).

    Reads the ``quadbin_cells`` setup view (``cell = 5233961839712272383``,
    San Francisco at zoom 10).  Extracts the zoom level → returns ``10`` as INT.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(gx.quadbin_resolution(f.col("cell")).alias("z")).first()
    return result["z"]


quadbin_resolution_python_light_example_output = """
+--+
|z |
+--+
|10|
+--+
"""


def quadbin_distance_python_light_example(spark):
    """Chebyshev distance between two adjacent quadbin cells (light pygx tier).

    Reads the ``quadbin_cell_pairs`` setup view (1 row: ``cell1`` and ``cell2``
    — zoom-10 cells at (0.0, 0.0) and (0.0, 0.1), Chebyshev distance = 1).
    Returns INT.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_cell_pairs")
    result = df.select(
        gx.quadbin_distance(f.col("cell1"), f.col("cell2")).alias("d")
    ).first()
    return result["d"]


quadbin_distance_python_light_example_output = """
+--+
|d |
+--+
|1 |
+--+
"""


# ---------------------------------------------------------------------------
# T6b: Quadbin neighbourhood/union/agg
# quadbin_kring, quadbin_polyfill, quadbin_tessellate,
# quadbin_cellunion, quadbin_cellunion_agg
#
# Fixture views:
#   quadbin_cells      — 1 row: cell LONG = 5233961839712272383 (SF at z10)
#                        Backs: quadbin_kring, quadbin_cellunion
#   quadbin_polygons   — 1 row: geom STRING (WGS84 polygon near origin)
#                        Backs: quadbin_polyfill, quadbin_tessellate
#   quadbin_kring_cells— 9 rows: cell LONG (kring of SF-z10, k=1)
#                        Backs: quadbin_cellunion_agg
# All four tabs share ONE example per function (AGREE — no tier divergence).
# ---------------------------------------------------------------------------


def quadbin_kring_python_light_example(spark):
    """Return all quadbin cells within Chebyshev distance k of a cell (light pygx tier).

    Reads the ``quadbin_cells`` setup view (cell = 5233961839712272383, SF at
    zoom 10).  At k=1, returns the center cell plus the 8 surrounding cells →
    9 cells total.  Returns ARRAY<BIGINT>.  Identical to the heavyweight output
    (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(gx.quadbin_kring(f.col("cell"), f.lit(1)).alias("kring")).first()
    return result["kring"]


quadbin_kring_python_light_example_output = """
+-------------------------------------+
|kring                                |
+-------------------------------------+
|[5233961839712272383, ..., (9 cells)]|
+-------------------------------------+
... (9 cells: SF z10 center plus 8 surrounding cells at k=1)
"""


def quadbin_polyfill_python_light_example(spark):
    """Polyfill a WGS84 geometry with quadbin cells at a given zoom (light pygx tier).

    Reads the ``quadbin_polygons`` setup view (WGS84 polygon
    ``POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))`` near the origin).
    At zoom 5, the polygon is covered by 4 cells.  Returns ARRAY<BIGINT>.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_polygons")
    result = df.select(
        gx.quadbin_polyfill(f.col("geom"), f.lit(5)).alias("cells")
    ).first()
    return result["cells"]


quadbin_polyfill_python_light_example_output = """
+--------------------------+
|cells                     |
+--------------------------+
|[5211790668774506495, ...]|
+--------------------------+
... (4 cells covering the WGS84 polygon at zoom 5)
"""


def quadbin_tessellate_python_light_example(spark):
    """Tessellate a WGS84 geometry into quadbin cells with per-cell clip geometry (light pygx tier).

    Reads the ``quadbin_polygons`` setup view (WGS84 polygon near origin).
    At zoom 5, produces 4 chips — each a STRUCT<cell BIGINT, geom BINARY> where
    ``geom`` is the cell's clipped boundary as EWKB (SRID 4326).
    Returns ARRAY<STRUCT<cell BIGINT, geom BINARY>>.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_polygons")
    result = df.select(
        gx.quadbin_tessellate(f.col("geom"), f.lit(5)).alias("chips")
    ).first()
    return result["chips"]


quadbin_tessellate_python_light_example_output = """
+----------------------------------------------+
|chips                                         |
+----------------------------------------------+
|[{5211790668774506495, [binary]}, {5212..., ..|
+----------------------------------------------+
... (4 chips: each quadbin cell paired with its clipped geometry WKB (SRID 4326))
"""


def quadbin_cellunion_python_light_example(spark):
    """Dissolve an array of quadbin cells into a single MultiPolygon EWKB (light pygx tier).

    Reads the ``quadbin_cells`` setup view (cell = 5233961839712272383, SF at z10)
    and passes the k=1 kring (9 cells) to ``quadbin_cellunion``.  Returns BINARY
    EWKB with embedded SRID 4326.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_cells")
    result = df.select(
        gx.quadbin_cellunion(gx.quadbin_kring(f.col("cell"), f.lit(1))).alias(
            "union_geom"
        )
    ).first()
    return result["union_geom"]


quadbin_cellunion_python_light_example_output = """
+-----------+
|union_geom |
+-----------+
|[binary]   |
+-----------+
... (EWKB binary — MultiPolygon dissolving the SF z10 kring, SRID 4326)
"""


def quadbin_cellunion_agg_python_light_example(spark):
    """Aggregate quadbin cells per group into a single MultiPolygon EWKB (light pygx tier).

    Reads the ``quadbin_kring_cells`` setup view (9 rows: cells from the k=1
    kring around the SF z10 cell).  Groups all 9 cells under region ``'R1'``
    and dissolves them into one EWKB MultiPolygon.  Returns BINARY (SRID 4326).

    Both the light and heavy tiers return ``BINARY`` — there is no struct
    divergence for ``quadbin_cellunion_agg`` (unlike the BNG aggregators).
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = spark.table("quadbin_kring_cells")
    df_keyed = df.withColumn("region", f.lit("R1"))
    result = (
        df_keyed.groupBy("region")
        .agg(gx.quadbin_cellunion_agg("cell").alias("coverage"))
        .first()
    )
    return result["coverage"]


quadbin_cellunion_agg_python_light_example_output = """
+------+--------+
|region|coverage|
+------+--------+
|R1    |[binary]|
+------+--------+
... (BINARY EWKB — dissolved coverage of all 9 kring cells around SF z10, SRID 4326)
"""


# ---------------------------------------------------------------------------
# T7: Custom grid — all 7 functions
# custom_grid, custom_pointascell, custom_cellaswkb, custom_cellaswkt,
# custom_centroid, custom_polyfill, custom_kring
#
# Fixture view: custom_grids — 1 row:
#   grid STRUCT<...>  — BNG-like custom grid (0,1000000,0,1000000,2,1000,1000,27700)
#   cell LONG         = 360287970373976640  (POINT(530000 180000) at res=5)
#   point STRING      = 'POINT(530000 180000)' (EPSG:27700)
#
# All four tabs share ONE example per function (AGREE — no tier divergence).
# custom_grid demonstrates calling the constructor directly (like quadbin_pointascell).
# All other functions read from the custom_grids view for cross-tab consistency.
# ---------------------------------------------------------------------------

_BNG_POLY_WKT = (
    "POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))"
)


def custom_grid_python_light_example(spark):
    """Construct a custom regular grid descriptor (light pygx tier).

    Calls ``gx.custom_grid`` directly on a one-row DataFrame to build the BNG-like
    grid spec: origin (0, 0), extent (1000000, 1000000), 2 splits per axis per level,
    1km root cells, EPSG:27700.  Returns the STRUCT descriptor passed to all other
    ``gbx_custom_*`` functions.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    base = spark.sql("SELECT 1 AS dummy")
    result = base.select(
        gx.custom_grid(
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


custom_grid_python_light_example_output = """
+----------------------------------------------+
|grid                                          |
+----------------------------------------------+
|{0, 1000000, 0, 1000000, 2, 1000, 1000, 27700}|
+----------------------------------------------+
"""


def custom_pointascell_python_light_example(spark):
    """Index a WKT point into a custom grid at a given resolution (light pygx tier).

    Reads the ``custom_grids`` setup view (``point = 'POINT(530000 180000)'`` and
    ``grid`` struct, EPSG:27700 BNG-like grid, 1km root cells).  Resolution 5 maps
    the easting/northing to cell ``360287970373976640``.  Returns BIGINT.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_custom_grid_df(spark)
    result = df.select(
        gx.custom_pointascell(f.col("point"), f.col("grid"), f.lit(5)).alias("cell")
    ).first()
    return result["cell"]


custom_pointascell_python_light_example_output = """
+--------------------+
|cell                |
+--------------------+
|360287970373976640  |
+--------------------+
"""


def custom_cellaswkb_python_light_example(spark):
    """Return the WKB footprint polygon of a custom grid cell (light pygx tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640`` at resolution 5,
    ``grid`` struct).  The cell is a 31.25m × 31.25m square at (530000, 180000)
    in EPSG:27700 — returned as plain WKB (no SRID).
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_custom_grid_df(spark)
    result = df.select(
        gx.custom_cellaswkb(f.col("cell"), f.col("grid")).alias("geom")
    ).first()
    return result["geom"]


custom_cellaswkb_python_light_example_output = """
+--------+
|geom    |
+--------+
|[binary]|
+--------+
... (WKB binary — 31.25m × 31.25m custom grid cell footprint polygon)
"""


def custom_cellaswkt_python_light_example(spark):
    """Return the WKT footprint polygon of a custom grid cell (light pygx tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640``,
    ``grid`` struct).  Returns the cell boundary as a WKT POLYGON in the
    grid's CRS (EPSG:27700 for this example).
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_custom_grid_df(spark)
    result = df.select(
        gx.custom_cellaswkt(f.col("cell"), f.col("grid")).alias("wkt")
    ).first()
    return result["wkt"]


custom_cellaswkt_python_light_example_output = """
+----------------------------------------------------------------------------------------------------+
|wkt                                                                                                 |
+----------------------------------------------------------------------------------------------------+
|POLYGON ((530031.25 180000, 530031.25 180031.25, 530000 180031.25, 530000 180000, 530031.25 180000))|
+----------------------------------------------------------------------------------------------------+
"""


def custom_centroid_python_light_example(spark):
    """Return the centroid of a custom grid cell as a WKB POINT (light pygx tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640``,
    ``grid`` struct).  Returns the center of the 31.25m × 31.25m cell as plain
    WKB (no SRID).  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_custom_grid_df(spark)
    result = df.select(
        gx.custom_centroid(f.col("cell"), f.col("grid")).alias("centroid")
    ).first()
    return result["centroid"]


custom_centroid_python_light_example_output = """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (WKB binary — POINT at the center of the 31.25m × 31.25m custom grid cell)
"""


def custom_polyfill_python_light_example(spark):
    """Fill a BNG polygon with custom grid cells at resolution 1 (500m cells) — light pygx tier.

    Reads the ``custom_grids`` setup view for the ``grid`` struct and uses the
    3km × 3km BNG polygon centred on London.  At resolution 1 (500m cells),
    the polygon is covered by exactly 36 cells (6 per side).
    Returns ``ARRAY<BIGINT>``.  Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_custom_grid_df(spark)
    result = df.select(
        gx.custom_polyfill(f.lit(_BNG_POLY_WKT), f.col("grid"), f.lit(1)).alias("cells")
    ).first()
    return result["cells"]


custom_polyfill_python_light_example_output = """
+---------------------------------------------+
|cells                                        |
+---------------------------------------------+
|[72057594038644994, ..., (36 cells at res=1)]|
+---------------------------------------------+
... (36 BIGINT cell IDs — 500m cells covering the 3km × 3km BNG polygon at resolution 1)
"""


def custom_kring_python_light_example(spark):
    """Return the 3×3 neighbourhood (k=1) around a custom grid cell (light pygx tier).

    Reads the ``custom_grids`` setup view (``cell = 360287970373976640`` at resolution 5,
    ``grid`` struct).  At k=1, the filled neighbourhood contains 9 cells
    (center plus 8 surrounding).  Returns ``ARRAY<BIGINT>``.
    Identical to the heavyweight output (AGREE).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415

    df = _get_custom_grid_df(spark)
    result = df.select(
        gx.custom_kring(f.col("cell"), f.col("grid"), f.lit(1)).alias("ring")
    ).first()
    return result["ring"]


custom_kring_python_light_example_output = """
+-------------------------------------------+
|ring                                       |
+-------------------------------------------+
|[360287970373976640, ..., (9 cells at k=1)]|
+-------------------------------------------+
... (9 BIGINT cell IDs — the 3×3 neighbourhood including center cell at resolution 5)
"""
