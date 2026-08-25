"""
SQL examples for GridX (BNG) functions documentation.

All SQL examples are executable and tested. These are imported into the
documentation via CodeFromTest components to ensure single-copy pattern.
"""

# ============================================================================
# Conversion Functions - Convert BNG cells to standard formats
# ============================================================================


def bng_aswkb_sql_example():
    """Convert BNG cell to WKB format"""
    return """
SELECT gbx_bng_aswkb('TQ3080') as wkb_geom;
"""


def bng_aswkt_sql_example():
    """Convert BNG cell to WKT format"""
    return """
SELECT gbx_bng_aswkt('TQ3080') as wkt_geom;
"""


# ============================================================================
# Core Functions - Basic cell operations
# ============================================================================


def bng_cellarea_sql_example():
    """Get area of a BNG grid cell in square kilometres. TQ3080 is a 1km cell → 1.0 sq km."""
    return """
SELECT gbx_bng_cellarea('TQ3080') AS area_km2;
"""


def bng_centroid_sql_example():
    """Get centroid of BNG grid cell"""
    return """
SELECT gbx_bng_centroid('TQ3080') as centroid;
"""


def bng_distance_sql_example():
    """Grid-step distance between two BNG cells. Returns LONG (number of grid steps, not metres)."""
    return """
SELECT gbx_bng_distance('TQ3080', 'TQ3081') AS dist_steps;
"""


def bng_euclideandistance_sql_example():
    """Chebyshev grid-unit distance between two BNG cells. Returns LONG (grid units, not metres)."""
    return """
SELECT gbx_bng_euclideandistance('TQ3080', 'TQ3081') AS euclidean_dist;
"""


# ============================================================================
# Coordinate Conversion - Convert coordinates/points to BNG
# ============================================================================


def bng_eastnorthasbng_sql_example():
    """Convert OS Grid Reference (easting, northing) to BNG cell. Resolution: BNG index or string (e.g. '1km')."""
    return """
-- Convert OS Grid Reference coordinates (easting, northing); resolution '1km' or integer 3
SELECT gbx_bng_eastnorthasbng(530000, 180000, '1km') as bng_cell;
"""


def bng_pointascell_sql_example():
    """Convert point geometry to BNG cell. Point must be WKT in BNG coords (eastings, northings) or WKB; resolution '1km' or 3."""
    return """
-- Point in BNG coordinates (eastings, northings); resolution '1km' for 1 km cell
SELECT gbx_bng_pointascell('POINT(530000 180000)', '1km') AS bng_cell;
"""


# ============================================================================
# K-Ring Functions - Generate neighboring cells
# ============================================================================


def bng_kring_sql_example():
    """Filled disk of BNG cells within k grid steps of a center cell (inclusive).
    Returns ARRAY<STRING> — all cells at Chebyshev distance ≤ k.
    At k=1, center TQ3080 plus the 8 surrounding 1km cells → 9 cells total.
    """
    return """
SELECT gbx_bng_kring('TQ3080', 1) AS kring;
"""


def bng_kloop_sql_example():
    """Hollow ring of BNG cells at exactly k grid steps from a center cell.
    Returns ARRAY<STRING> — cells at Chebyshev distance = k (excludes center).
    At k=1, the 8 cells surrounding TQ3080 (no center) → 8 cells.
    """
    return """
SELECT gbx_bng_kloop('TQ3080', 1) AS kloop;
"""


def bng_kringexplode_sql_example():
    """Explode k-ring into one row per cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation for both tiers: the light UDTF
    (``gbx_bng_kringexplode``) and the heavy CollectionGenerator both accept
    this form.  Returns 9 rows (center TQ3080 + 8 neighbours) at k=1.
    Each row carries a single ``cellid STRING`` column.
    """
    return """
SELECT t.*
FROM (SELECT 'TQ3080' AS cellid) src,
LATERAL gbx_bng_kringexplode(src.cellid, 1) t;
"""


def bng_kloopexplode_sql_example():
    """Explode k-loop (hollow ring) into one row per cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation for both tiers: the light UDTF
    (``gbx_bng_kloopexplode``) and the heavy CollectionGenerator both accept
    this form.  Returns 8 rows (centre excluded) at k=1.
    Each row carries a single ``cellid STRING`` column.
    """
    return """
SELECT t.*
FROM (SELECT 'TQ3080' AS cellid) src,
LATERAL gbx_bng_kloopexplode(src.cellid, 1) t;
"""


def bng_geomkring_sql_example():
    """Polyfill a geometry at given BNG resolution then expand by k ring steps.
    Returns ARRAY<STRING> — all cells within Chebyshev distance k of the polyfill.
    Geometry must be in EPSG:27700 (BNG eastings/northings); WGS84 yields empty arrays.
    At res=3 (1km), k=1: polyfill 9 cells + 16 outer cells → 25 cells.
    """
    return """
SELECT gbx_bng_geomkring(
  'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))',
  3, 1
) AS kring;
"""


def bng_geomkloop_sql_example():
    """Polyfill a geometry at given BNG resolution then return only the outer ring.
    Returns ARRAY<STRING> — cells at exactly ring distance k (hollow shell).
    Geometry must be in EPSG:27700 (BNG eastings/northings); WGS84 yields empty arrays.
    At res=3 (1km), k=1: the 16 outer cells surrounding the 9-cell polyfill.
    """
    return """
SELECT gbx_bng_geomkloop(
  'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))',
  3, 1
) AS kloop;
"""


def bng_geomkringexplode_sql_example():
    """Explode geometry k-ring into one row per cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation for both tiers.  Geometry
    MUST be in EPSG:27700 (BNG eastings/northings) — WGS84 lon/lat yields
    empty results.  At res=3 (1km) with k=1, the 9-cell polyfill of the
    3km × 3km polygon expands to 25 cells.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))' AS geom) src,
LATERAL gbx_bng_geomkringexplode(src.geom, 3, 1) t;
"""


def bng_geomkloopexplode_sql_example():
    """Explode geometry k-loop (hollow ring) into one row per cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation for both tiers.  Geometry
    MUST be in EPSG:27700 (BNG eastings/northings) — WGS84 lon/lat yields
    empty results.  At res=3 (1km) with k=1, the outer hollow ring
    of the 3km × 3km polygon polyfill contains 16 cells.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))' AS geom) src,
LATERAL gbx_bng_geomkloopexplode(src.geom, 3, 1) t;
"""


# ============================================================================
# Tessellation Functions - Fill geometries with cells
# ============================================================================


def bng_polyfill_sql_example():
    """Fill a geometry with all BNG cells at given resolution.
    Returns ARRAY<STRING> — all cell IDs whose footprints overlap the geometry.
    Geometry must be in EPSG:27700 (BNG eastings/northings); WGS84 yields empty arrays.
    The 3km × 3km BNG polygon covers 9 cells at 1km resolution (res=3).
    """
    return """
SELECT gbx_bng_polyfill(
  'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))',
  3
) AS cells;
"""


def bng_tessellate_sql_example():
    """Tessellate a geometry into BNG cells, splitting border cells at the geometry boundary.
    Returns ARRAY<STRUCT<cellid STRING, core BOOLEAN, chip BINARY>>.
    Core cells are fully inside the geometry (chip=null); border cells carry a WKB clipped polygon.
    Geometry must be in EPSG:27700 (BNG eastings/northings); WGS84 yields empty arrays.
    """
    return """
SELECT gbx_bng_tessellate(
  'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))',
  3
) AS chips;
"""


def bng_tessellateexplode_sql_example():
    """Explode tessellation into one row per chip via SQL LATERAL (light-tier form).

    SQL LATERAL is the canonical invocation for both tiers.  Geometry
    MUST be in EPSG:27700 (BNG eastings/northings) — WGS84 lon/lat yields
    empty results.  The lightweight UDTF returns three columns per row:
    ``cellid STRING``, ``core BOOLEAN``, ``chip BINARY`` — 9 rows for
    the 3km × 3km BNG polygon at 1km resolution.

    Note: the heavyweight CollectionGenerator has an ``elementSchema`` bug
    (only ``cellid`` is visible via heavy SQL LATERAL); the light form
    correctly exposes all three fields and is shown here.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))' AS geom) src,
LATERAL gbx_bng_tessellateexplode(src.geom, 3) t;
"""


# ============================================================================
# Aggregator Functions - Aggregate multiple cells
# ============================================================================


def bng_cellintersection_sql_example():
    """Intersect two BNG chip structs. Inputs must be STRUCT<cellid,core,chip> from bng_tessellate.
    Returns STRUCT<cellid:STRING, core:BOOLEAN, chip:BINARY> — the dissolved intersection chip.
    Inline subquery form keeps gbx_bng_cellintersection as the first gbx_bng_ occurrence so
    DESCRIBE FUNCTION extracts a valid self-contained example.
    """
    return """
SELECT gbx_bng_cellintersection(chip, chip) AS intersection_chip
FROM (
  SELECT explode(
    gbx_bng_tessellate(
      'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))', 3
    )
  ) AS chip
) t
WHERE t.chip.cellid = 'TQ3080';
"""


def bng_cellunion_sql_example():
    """Union two BNG chip structs. Inputs must be STRUCT<cellid,core,chip> from bng_tessellate.
    Returns STRUCT<cellid:STRING, core:BOOLEAN, chip:BINARY> — the dissolved union chip.
    Inline subquery form keeps gbx_bng_cellunion as the first gbx_bng_ occurrence so
    DESCRIBE FUNCTION extracts a valid self-contained example.
    """
    return """
SELECT gbx_bng_cellunion(chip, chip) AS union_chip
FROM (
  SELECT explode(
    gbx_bng_tessellate(
      'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))', 3
    )
  ) AS chip
) t
WHERE t.chip.cellid = 'TQ3080';
"""


def bng_cellintersection_agg_sql_example():
    """Aggregate intersection of chip structs per BNG cell.

    Inputs must be STRUCT<cellid STRING, core BOOLEAN, chip BINARY> from
    ``bng_tessellate`` — passing plain STRING cell IDs throws ClassCastException.
    Groups the 9 tessellation chips by cellid; each group has exactly one chip,
    so the aggregate result equals that chip.  Filters for the core cell TQ3080
    (fully interior: core=true, chip=null) to match the Python and Scala tabs.

    Return type diverges by tier (see :::warning in the docs):
    - Heavyweight SQL: STRUCT<cellid, core, chip>  →  {TQ3080, true, null}
    - Lightweight SQL: BINARY (dissolved chip WKB in EPSG:27700)
    """
    return """
SELECT t.chip.cellid, gbx_bng_cellintersection_agg(t.chip) AS common_chip
FROM (
  SELECT explode(gbx_bng_tessellate(
    'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))', 3
  )) AS chip
) t
WHERE t.chip.cellid = 'TQ3080'
GROUP BY t.chip.cellid;
"""


def bng_cellunion_agg_sql_example():
    """Aggregate union of chip structs per BNG cell.

    Inputs must be STRUCT<cellid STRING, core BOOLEAN, chip BINARY> from
    ``bng_tessellate`` — passing plain STRING cell IDs throws ClassCastException.
    Groups the 9 tessellation chips by cellid; each group has exactly one chip,
    so the aggregate result equals that chip.  Filters for the core cell TQ3080
    (fully interior: core=true, chip=null) to match the Python and Scala tabs.

    Return type diverges by tier (see :::warning in the docs):
    - Heavyweight SQL: STRUCT<cellid, core, chip>  →  {TQ3080, true, null}
    - Lightweight SQL: BINARY (dissolved chip WKB in EPSG:27700)
    """
    return """
SELECT t.chip.cellid, gbx_bng_cellunion_agg(t.chip) AS union_chip
FROM (
  SELECT explode(gbx_bng_tessellate(
    'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))', 3
  )) AS chip
) t
WHERE t.chip.cellid = 'TQ3080'
GROUP BY t.chip.cellid;
"""


# =============================================================================
# EXAMPLE OUTPUT (show-type result for docs, same style as quick-start)
# =============================================================================

bng_aswkb_sql_example_output = """
+--------+
|wkb_geom|
+--------+
|[binary]|
+--------+
... (WKB binary)
"""

bng_aswkt_sql_example_output = """
+----------------------------------------------------------+
|wkt_geom                                                  |
+----------------------------------------------------------+
|POLYGON ((531000 180000, 531000 181000, 530000 181000, ...|
+----------------------------------------------------------+
POLYGON ((531000 180000, 531000 181000, 530000 181000, 530000 180000, 531000 180000))
"""

bng_cellarea_sql_example_output = """
+--------+
|area_km2|
+--------+
|1.0     |
+--------+
"""

bng_centroid_sql_example_output = """
+--------+
|centroid|
+--------+
|[binary]|
+--------+
... (WKB binary — POINT(530500 180500) in EPSG:27700)
"""

bng_distance_sql_example_output = """
+----------+
|dist_steps|
+----------+
|1         |
+----------+
"""

bng_euclideandistance_sql_example_output = """
+--------------+
|euclidean_dist|
+--------------+
|1             |
+--------------+
"""

bng_cellintersection_sql_example_output = """
+--------------------+
|intersection_chip   |
+--------------------+
|{TQ3080, true, null}|
+--------------------+
"""

bng_cellunion_sql_example_output = """
+--------------------+
|union_chip          |
+--------------------+
|{TQ3080, true, null}|
+--------------------+
"""

bng_eastnorthasbng_sql_example_output = """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+
"""

bng_pointascell_sql_example_output = """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+
"""

bng_kring_sql_example_output = """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells: center TQ3080 plus 8 surrounding cells at k=1)
"""

bng_kloop_sql_example_output = """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (8 cells: hollow ring at k=1, center TQ3080 excluded)
"""

bng_geomkring_sql_example_output = """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (25 cells: polyfill of BNG polygon expanded by k=1 ring)
"""

bng_geomkloop_sql_example_output = """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (16 cells: outer ring at k=1 around the BNG polygon polyfill)
"""

bng_polyfill_sql_example_output = """
+-----------------------------+
|cells                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells covering the 3km × 3km BNG polygon at 1km resolution)
"""

bng_tessellate_sql_example_output = """
+--------------------------------------------+
|chips                                       |
+--------------------------------------------+
|[{TQ2979, false, [binary]}, {TQ3080, true,..|
+--------------------------------------------+
... (9 chips; TQ3080 is core (core=true, chip=null); border cells carry WKB clip geometry)
"""


bng_cellintersection_agg_sql_example_output = """
# Heavyweight SQL (active tier = heavy/Scala) — STRUCT<cellid, core, chip>:
+------+--------------------+
|cellid|common_chip         |
+------+--------------------+
|TQ3080|{TQ3080, true, null}|
+------+--------------------+
... (core cell TQ3080: chip=null means the full cell polygon)

# Lightweight SQL (active tier = pygx) — BINARY (dissolved chip WKB in EPSG:27700):
+------+-----------+
|cellid|common_chip|
+------+-----------+
|TQ3080|[binary]   |
+------+-----------+
... (core cell TQ3080: [binary] is the WKB of the full TQ3080 polygon)
"""

bng_cellunion_agg_sql_example_output = """
# Heavyweight SQL (active tier = heavy/Scala) — STRUCT<cellid, core, chip>:
+------+--------------------+
|cellid|union_chip          |
+------+--------------------+
|TQ3080|{TQ3080, true, null}|
+------+--------------------+
... (core cell TQ3080: chip=null means the full cell polygon)

# Lightweight SQL (active tier = pygx) — BINARY (dissolved chip WKB in EPSG:27700):
+------+----------+
|cellid|union_chip|
+------+----------+
|TQ3080|[binary]  |
+------+----------+
... (core cell TQ3080: [binary] is the WKB of the full TQ3080 polygon)
"""


# ============================================================================
# Quadbin (CARTO v0) — 9 grid-math functions
# ============================================================================


def quadbin_pointascell_sql_example():
    """Convert lon/lat (EPSG:4326) to a quadbin cell at a given zoom (0..26)."""
    return """
SELECT gbx_quadbin_pointascell(-122.4194, 37.7749, 10) as sf_cell;
"""


def quadbin_aswkb_sql_example():
    """Return the quadbin cell footprint as EWKB (SRID=4326).

    San Francisco at zoom 10 (SF cell = gbx_quadbin_pointascell(-122.4194, 37.7749, 10)).
    Matches the quadbin_cells fixture used by the Python and Scala tabs.
    """
    return """
SELECT gbx_quadbin_aswkb(gbx_quadbin_pointascell(-122.4194, 37.7749, 10)) AS wkb;
"""


def quadbin_centroid_sql_example():
    """Return the quadbin cell centroid as EWKB POINT (SRID=4326).

    San Francisco at zoom 10 (same cell as quadbin_aswkb).  Centroid is the
    mean of the cell's four corner coordinates, returned as EWKB POINT (SRID 4326).
    Matches the quadbin_cells fixture used by the Python and Scala tabs.
    """
    return """
SELECT gbx_quadbin_centroid(gbx_quadbin_pointascell(-122.4194, 37.7749, 10)) AS centroid;
"""


def quadbin_resolution_sql_example():
    """Return the resolution (zoom 0..26) of a quadbin cell.

    San Francisco at zoom 10 — resolution is 10.
    Matches the quadbin_cells fixture used by the Python and Scala tabs.
    """
    return """
SELECT gbx_quadbin_resolution(gbx_quadbin_pointascell(-122.4194, 37.7749, 10)) AS z;
"""


def quadbin_polyfill_sql_example():
    """Polyfill a geometry's bbox with quadbin cells at a given zoom (0..20).

    Uses a raw WKT literal — no ST_GeomFromText (avoids DBR dependency).
    WGS84 polygon ``(-1,-1) → (1,1)`` at zoom 5 → 4 cells.
    """
    return """
SELECT gbx_quadbin_polyfill('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))', 5) AS cells;
"""


def quadbin_kring_sql_example():
    """Return all cells within Chebyshev distance k of a quadbin cell (inclusive).

    Uses the canonical SF z10 fixture cell (lon=-122.4194, lat=37.7749, zoom=10
    → cell 5233961839712272383) — same input as the Python and Scala tabs.
    At k=1, returns center plus 8 surrounding cells → 9 cells total.
    """
    return """
SELECT gbx_quadbin_kring(gbx_quadbin_pointascell(-122.4194, 37.7749, 10), 1) AS kring;
"""


def quadbin_tessellate_sql_example():
    """Tessellate a geometry into quadbin cells; returns array of struct(cell, geom).

    Uses a raw WKT literal — no ST_GeomFromText (avoids DBR dependency).
    WGS84 polygon ``(-1,-1) → (1,1)`` at zoom 5 → 4 chips, each carrying
    the per-cell clipped geometry as EWKB (SRID 4326).
    """
    return """
SELECT gbx_quadbin_tessellate('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))', 5) AS chips;
"""


def quadbin_cellunion_sql_example():
    """Union an ARRAY<BIGINT> of quadbin cells to a single MultiPolygon EWKB.

    Uses the canonical SF z10 fixture cell (lon=-122.4194, lat=37.7749, zoom=10
    → cell 5233961839712272383) — same input as the Python and Scala tabs.
    Dissolves the k=1 kring (9 cells) into one EWKB MultiPolygon (SRID 4326).
    """
    return """
SELECT gbx_quadbin_cellunion(
  gbx_quadbin_kring(gbx_quadbin_pointascell(-122.4194, 37.7749, 10), 1)
) AS union_geom;
"""


def quadbin_cellunion_agg_sql_example():
    """Aggregate quadbin cells per group into a single MultiPolygon EWKB.

    Inline subquery generates 9 cells (k=1 kring around the SF z10 cell) under
    a single region key, then dissolves them.  Uses the canonical SF z10 fixture
    cell (lon=-122.4194, lat=37.7749, zoom=10 → 5233961839712272383) — same
    input as the Python and Scala tabs.  The first ``gbx_quadbin_`` token is
    ``gbx_quadbin_cellunion_agg`` so DESCRIBE FUNCTION extracts this example.
    Both light and heavy tiers return BINARY EWKB (SRID 4326).
    """
    return """
SELECT region, gbx_quadbin_cellunion_agg(cell) AS coverage
FROM (
  SELECT 'R1' AS region,
         explode(gbx_quadbin_kring(gbx_quadbin_pointascell(-122.4194, 37.7749, 10), 1)) AS cell
) t
GROUP BY region;
"""


quadbin_cellunion_agg_sql_example_output = """
+------+--------+
|region|coverage|
+------+--------+
|R1    |[binary]|
+------+--------+
... (BINARY EWKB — dissolved coverage of all 9 kring cells around SF z10, SRID 4326)
"""


def quadbin_distance_sql_example():
    """Chebyshev distance between two quadbin cells at the same resolution.

    Two adjacent zoom-10 cells: (0.0, 0.0) and (0.0, 0.1) — Chebyshev distance = 1.
    Using 0.1 lat diff (not 0.0001) ensures a non-zero, non-degenerate result.
    Matches the quadbin_cell_pairs fixture used by the Python and Scala tabs.
    """
    return """
SELECT gbx_quadbin_distance(
    gbx_quadbin_pointascell(0.0, 0.0, 10),
    gbx_quadbin_pointascell(0.0, 0.1, 10)
) AS d;
"""


quadbin_pointascell_sql_example_output = """
+-------------------+
|sf_cell            |
+-------------------+
|5233961839712272383|
+-------------------+
"""

quadbin_aswkb_sql_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (EWKB binary — quadbin cell footprint polygon, SRID 4326)
"""

quadbin_centroid_sql_example_output = """
+--------+
|centroid|
+--------+
|[binary]|
+--------+
... (EWKB binary — POINT at SF z10 cell centroid, SRID 4326)
"""

quadbin_resolution_sql_example_output = """
+--+
|z |
+--+
|10|
+--+
"""

quadbin_distance_sql_example_output = """
+-+
|d|
+-+
|1|
+-+
"""

quadbin_kring_sql_example_output = """
+-------------------------------------+
|kring                                |
+-------------------------------------+
|[5233961839712272383, ..., (9 cells)]|
+-------------------------------------+
... (9 cells: SF z10 center plus 8 surrounding cells at k=1)
"""

quadbin_polyfill_sql_example_output = """
+--------------------------+
|cells                     |
+--------------------------+
|[5211790668774506495, ...]|
+--------------------------+
... (4 cells covering the WGS84 polygon at zoom 5)
"""

quadbin_tessellate_sql_example_output = """
+----------------------------------------------+
|chips                                         |
+----------------------------------------------+
|[{5211790668774506495, [binary]}, {5212..., ..|
+----------------------------------------------+
... (4 chips: each quadbin cell paired with its clipped geometry WKB (SRID 4326))
"""

quadbin_cellunion_sql_example_output = """
+----------+
|union_geom|
+----------+
|[binary]  |
+----------+
... (EWKB binary — MultiPolygon dissolving the SF z10 kring, SRID 4326)
"""


# ============================================================================
# Custom Grid — user-defined regular grid functions
# ============================================================================


def custom_grid_sql_example():
    """Define a user-specified regular grid from origin, extent, resolution, and SRID."""
    return """
SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid;
"""


def custom_pointascell_sql_example():
    """Index a WKT point into a user-defined regular grid at a given resolution."""
    return """
SELECT gbx_custom_pointascell('POINT(530000 180000)', gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 5) AS cell;
"""


def custom_cellaswkb_sql_example():
    """Return the WKB footprint polygon of a custom grid cell."""
    return """
SELECT gbx_custom_cellaswkb(360287970373976640, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)) AS geom;
"""


def custom_cellaswkt_sql_example():
    """Return the WKT footprint polygon of a custom grid cell."""
    return """
SELECT gbx_custom_cellaswkt(360287970373976640, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)) AS wkt;
"""


def custom_centroid_sql_example():
    """Return the centroid of a custom grid cell as a WKB point."""
    return """
SELECT gbx_custom_centroid(360287970373976640, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)) AS centroid;
"""


def custom_polyfill_sql_example():
    """Fill a geometry with custom grid cells at the given resolution (res=1, 500m cells)."""
    return """
SELECT gbx_custom_polyfill('POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))', gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 1) AS cells;
"""


def custom_kring_sql_example():
    """Return all custom grid cells within k steps of a center cell (k=1, 3x3 neighbourhood)."""
    return """
SELECT gbx_custom_kring(360287970373976640, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 1) AS ring;
"""


custom_grid_sql_example_output = """
+----------------------------------------------+
|grid                                          |
+----------------------------------------------+
|{0, 1000000, 0, 1000000, 2, 1000, 1000, 27700}|
+----------------------------------------------+
"""

custom_pointascell_sql_example_output = """
+------------------+
|cell              |
+------------------+
|360287970373976640|
+------------------+
"""

custom_cellaswkb_sql_example_output = """
+--------+
|geom    |
+--------+
|[binary]|
+--------+
... (WKB binary — 31.25m × 31.25m custom grid cell footprint polygon)
"""

custom_cellaswkt_sql_example_output = """
+----------------------------------------------------------------------------------------------------+
|wkt                                                                                                 |
+----------------------------------------------------------------------------------------------------+
|POLYGON ((530031.25 180000, 530031.25 180031.25, 530000 180031.25, 530000 180000, 530031.25 180000))|
+----------------------------------------------------------------------------------------------------+
"""

custom_centroid_sql_example_output = """
+--------+
|centroid|
+--------+
|[binary]|
+--------+
... (WKB binary — POINT at the center of the 31.25m × 31.25m custom grid cell)
"""

custom_polyfill_sql_example_output = """
+---------------------------------------------+
|cells                                        |
+---------------------------------------------+
|[72057594038644994, ..., (36 cells at res=1)]|
+---------------------------------------------+
... (36 BIGINT cell IDs — 500m cells covering the 3km × 3km BNG polygon at resolution 1)
"""

custom_kring_sql_example_output = """
+-------------------------------------------+
|ring                                       |
+-------------------------------------------+
|[360287970373976640, ..., (9 cells at k=1)]|
+-------------------------------------------+
... (9 BIGINT cell IDs — the 3×3 neighbourhood including center cell at resolution 5)
"""
