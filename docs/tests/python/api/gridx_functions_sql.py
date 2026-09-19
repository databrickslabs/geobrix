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
    At res=3 (1km), k=1: the boundary band (8 cells for this grid-aligned 3×3)
    plus the 16-cell outer ring → 24 cells; boundary-out does not fill the
    interior centre.
    The optional mode parameter controls how boundary cells are classified;
    'boundary-out' (default) expands outward from the geometry edge.
    """
    return """
SELECT gbx_bng_geomkring(
  'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))',
  3, 1, 'boundary-out'
) AS kring;
"""


def bng_geomkloop_sql_example():
    """Polyfill a geometry at given BNG resolution then return only the outer ring.
    Returns ARRAY<STRING> — cells at exactly ring distance k (hollow shell).
    Geometry must be in EPSG:27700 (BNG eastings/northings); WGS84 yields empty arrays.
    At res=3 (1km), k=1: the 16 outer cells surrounding the 9-cell polyfill.
    The optional mode parameter controls boundary classification (default 'boundary-out').
    """
    return """
SELECT gbx_bng_geomkloop(
  'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))',
  3, 1, 'boundary-out'
) AS kloop;
"""


def bng_geomkringexplode_sql_example():
    """Explode geometry k-ring into one row per cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation for both tiers.  Geometry
    MUST be in EPSG:27700 (BNG eastings/northings) — WGS84 lon/lat yields
    empty results.  At res=3 (1km) with k=1, the boundary-out band of the
    3km × 3km polygon expands by one ring to 24 cells (interior centre not filled).
    The optional mode parameter controls boundary classification (default 'boundary-out').
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))' AS geom) src,
LATERAL gbx_bng_geomkringexplode(src.geom, 3, 1, 'boundary-out') t;
"""


def bng_geomkloopexplode_sql_example():
    """Explode geometry k-loop (hollow ring) into one row per cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation for both tiers.  Geometry
    MUST be in EPSG:27700 (BNG eastings/northings) — WGS84 lon/lat yields
    empty results.  At res=3 (1km) with k=1, the outer hollow ring
    of the 3km × 3km polygon polyfill contains 16 cells.
    The optional mode parameter controls boundary classification (default 'boundary-out').
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))' AS geom) src,
LATERAL gbx_bng_geomkloopexplode(src.geom, 3, 1, 'boundary-out') t;
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
... (24 cells: boundary band expanded by k=1 ring; interior centre not filled)
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


def quadbin_kloop_sql_example():
    """Return the hollow ring of quadbin cells at EXACTLY Chebyshev distance k.

    Uses the canonical SF z10 fixture cell (lon=-122.4194, lat=37.7749, zoom=10
    → cell 5233961839712272383) — same input as the Python and Scala tabs.
    At k=1, returns the 8-cell ring (center excluded); at k=0, returns [seed].
    """
    return """
SELECT gbx_quadbin_kloop(gbx_quadbin_pointascell(-122.4194, 37.7749, 10), 1) AS kloop;
"""


def quadbin_geomkring_sql_example():
    """Polyfill a WGS84 geometry at given zoom then expand by k ring steps.

    Returns ARRAY<BIGINT> — all quadbin cells within Chebyshev distance k
    of the geometry's covering set. At zoom=12, k=1, the small NYC box
    (~0.04° × 0.04°) polyfill cells plus one outer ring are returned.
    """
    return """
SELECT gbx_quadbin_geomkring(
  'POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))',
  12, 1, 'boundary-out'
) AS kring;
"""


def quadbin_geomkloop_sql_example():
    """Polyfill a WGS84 geometry at given zoom then return only the outer ring.

    Returns ARRAY<BIGINT> — cells at exactly ring distance k (hollow shell).
    At zoom=12, k=1, returns the outer ring cells surrounding the polyfill.
    """
    return """
SELECT gbx_quadbin_geomkloop(
  'POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))',
  12, 1, 'boundary-out'
) AS kloop;
"""


def quadbin_geomkringexplode_sql_example():
    """Explode geometry k-ring into one row per BIGINT cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation. At zoom=12 with k=1, the
    covering polyfill of the NYC box expands outward by one ring.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))' AS geom) src,
LATERAL gbx_quadbin_geomkringexplode(src.geom, 12, 1, 'boundary-out') t;
"""


def quadbin_geomkloopexplode_sql_example():
    """Explode geometry k-loop (hollow ring) into one row per BIGINT cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation. At zoom=12 with k=1, returns
    the hollow outer ring cells of the NYC box polyfill.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))' AS geom) src,
LATERAL gbx_quadbin_geomkloopexplode(src.geom, 12, 1, 'boundary-out') t;
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

quadbin_kloop_sql_example_output = """
+----------------+
|kloop           |
+----------------+
|[..., (8 cells)]|
+----------------+
... (8 cells: hollow ring at k=1, SF z10 center excluded)
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


def custom_kloop_sql_example():
    """Return the hollow ring of custom grid cells at EXACTLY k steps from the center cell.

    Uses the canonical res-5 cell 360287970373976640 and the same 1 km grid from the
    other custom-grid examples.  At k=1, returns the 8-cell ring (center excluded);
    k=0 returns [center].
    """
    return """
SELECT gbx_custom_kloop(360287970373976640, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 1) AS kloop;
"""


def custom_distance_sql_example():
    """Chebyshev distance (in grid steps) between two custom grid cells.

    Uses gbx_custom_pointascell to obtain two adjacent cells at resolution 0
    (cell_size=1000, so a 1000-unit step in X separates them by exactly 1 grid step).
    The first gbx_custom_distance token is the example captured by DESCRIBE FUNCTION.
    """
    return """
SELECT gbx_custom_distance(
    gbx_custom_pointascell('POINT(530000 180000)', gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 0),
    gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700),
    gbx_custom_pointascell('POINT(531000 180000)', gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 0)
) AS dist;
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

custom_kloop_sql_example_output = """
+-----------------------+
|kloop                  |
+-----------------------+
|[..., (8 cells at k=1)]|
+-----------------------+
... (8 BIGINT cell IDs — hollow ring at k=1, center cell excluded)
"""

custom_distance_sql_example_output = """
+----+
|dist|
+----+
|1   |
+----+
... (Chebyshev grid distance between two cells 1 step apart in X at resolution 0)
"""


def custom_geomkring_sql_example():
    """Polyfill a geometry in the custom grid CRS then expand by k ring steps.

    Returns ARRAY<BIGINT> — all custom grid cells within Chebyshev distance k
    of the geometry's covering set.  Uses the canonical 1 km grid from the
    other custom-grid examples; the 4 600-unit box covers ~25 cells at res 0,
    and k=1 adds the surrounding outer ring.
    """
    return """
SELECT gbx_custom_geomkring(
  'POLYGON((530200 180200, 534800 180200, 534800 184800, 530200 184800, 530200 180200))',
  gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700),
  0, 1, 'boundary-out'
) AS kring;
"""


def custom_geomkloop_sql_example():
    """Polyfill a geometry in the custom grid CRS then return only the outer ring.

    Returns ARRAY<BIGINT> — cells at exactly ring distance k (hollow shell).
    At res=0, k=1, returns the outer ring cells surrounding the polyfill.
    """
    return """
SELECT gbx_custom_geomkloop(
  'POLYGON((530200 180200, 534800 180200, 534800 184800, 530200 184800, 530200 180200))',
  gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700),
  0, 1, 'boundary-out'
) AS kloop;
"""


def custom_geomkringexplode_sql_example():
    """Explode geometry k-ring (custom grid) into one row per BIGINT cell via SQL LATERAL.

    SQL LATERAL is the canonical invocation.  At res=0 with k=1, the covering
    polyfill of the box expands outward by one ring; each cell emitted as a row.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((530200 180200, 534800 180200, 534800 184800, 530200 184800, 530200 180200))' AS geom,
             gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid) src,
LATERAL gbx_custom_geomkringexplode(src.geom, src.grid, 0, 1, 'boundary-out') t;
"""


def custom_geomkloopexplode_sql_example():
    """Explode geometry k-loop (custom grid hollow ring) into one row per BIGINT cell.

    SQL LATERAL is the canonical invocation.  At res=0, k=1, returns the hollow
    outer ring cells surrounding the geometry's polyfill.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((530200 180200, 534800 180200, 534800 184800, 530200 184800, 530200 180200))' AS geom,
             gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid) src,
LATERAL gbx_custom_geomkloopexplode(src.geom, src.grid, 0, 1, 'boundary-out') t;
"""


custom_geomkring_sql_example_output = """
+--------------------------------------------+
|kring                                       |
+--------------------------------------------+
|[..., (cells within k=1 ring of 4.6 km box)]|
+--------------------------------------------+
... (ARRAY<BIGINT> — polyfill covering set plus one outer ring at resolution 0)
"""

custom_geomkloop_sql_example_output = """
+--------------------------------------------+
|kloop                                       |
+--------------------------------------------+
|[..., (outer ring cells, polyfill excluded)]|
+--------------------------------------------+
... (ARRAY<BIGINT> — hollow outer ring at k=1, polyfill cells excluded)
"""

custom_geomkringexplode_sql_example_output = """
+-----------+
|cellid     |
+-----------+
|...(BIGINT)|
+-----------+
... (one row per BIGINT cell ID in the k=1 ring of the geometry)
"""

custom_geomkloopexplode_sql_example_output = """
+-----------+
|cellid     |
+-----------+
|...(BIGINT)|
+-----------+
... (one row per BIGINT cell ID in the k=1 hollow outer ring)
"""


# ============================================================================
# Cell-fill grouped aggregators — fill NULL (covered-but-missing) cells from
# valid neighbours in the same group.  All four grids.
# ============================================================================


def bng_cellfill_sql_example():
    """Fill NULL BNG cells from valid ring-1 neighbours using mean interpolation.

    Inline grid: London 100 m cell TQ300800 (NULL, to be filled) surrounded by
    four ring-1 neighbours each with value 5.0.  With k=1 and method='mean' the
    NULL center is filled with the unweighted mean of its neighbours (5.0).

    ``gbx_bng_cellfill`` is a grouped aggregator: the call must appear inside a
    GROUP BY query.  BNG cell IDs are STRING.  The function returns BINARY on the
    light tier and ARRAY<STRUCT<cellid STRING, value DOUBLE>> on the heavy tier.
    """
    return """
SELECT region,
       gbx_bng_cellfill(cellid, value, 1, 'mean', 2.0) AS filled
FROM (
  VALUES
    (1, 'TQ300800', CAST(NULL AS DOUBLE)),
    (1, 'TQ299800', 5.0),
    (1, 'TQ301800', 5.0),
    (1, 'TQ300799', 5.0),
    (1, 'TQ300801', 5.0)
) AS t(region, cellid, value)
GROUP BY region;
"""


bng_cellfill_sql_example_output = """
+------+--------+
|region|filled  |
+------+--------+
|1     |[binary]|
+------+--------+
... (BINARY — decoded: TQ300800 filled to 5.0; ring-1 neighbours unchanged)
"""


def quadbin_cellfill_sql_example():
    """Fill NULL Quadbin cells from valid ring-1 neighbours using mean interpolation.

    The first sub-select contributes the London z=10 center cell with value NULL;
    the UNION adds its ring-1 neighbours (via ``gbx_quadbin_kring``) with value 5.0.
    With k=1 and method='mean' the NULL center is filled with 5.0.

    ``gbx_quadbin_cellfill`` is a grouped aggregator: the call must appear inside a
    GROUP BY query.  Cell IDs are BIGINT.  Returns BINARY (light) or
    ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> (heavy).
    """
    return """
SELECT region,
       gbx_quadbin_cellfill(cellid, value, 1, 'mean', 2.0) AS filled
FROM (
  SELECT 1 AS region,
         gbx_quadbin_pointascell(-0.1, 51.5, 10) AS cellid,
         CAST(NULL AS DOUBLE) AS value
  UNION ALL
  SELECT 1 AS region, cell AS cellid, 5.0 AS value
  FROM (
    SELECT explode(gbx_quadbin_kring(gbx_quadbin_pointascell(-0.1, 51.5, 10), 1)) AS cell
  )
) t
GROUP BY region;
"""


quadbin_cellfill_sql_example_output = """
+------+--------+
|region|filled  |
+------+--------+
|1     |[binary]|
+------+--------+
... (BINARY — decoded: London z10 center cell filled to 5.0; ring-1 neighbours unchanged)
"""


def custom_cellfill_sql_example():
    """Fill NULL custom-grid cells from valid ring-1 neighbours using mean interpolation.

    Inline grid: center cell 216172782113787048 (res=3, 500km×500km block in a
    0..1e6 × 0..1e6 grid) with value NULL, surrounded by two ring-1 neighbours
    with value 5.0.  With k=1 and method='mean' the NULL center is filled to 5.0.

    ``gbx_custom_cellfill`` is a grouped aggregator; the grid spec (third arg) must
    be supplied as a ``gbx_custom_grid(...)`` struct.  Returns BINARY (light) or
    ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> (heavy).
    """
    return """
SELECT region,
       gbx_custom_cellfill(cellid, value,
         gbx_custom_grid(0, 1000000, 0, 1000000, 2, 100000, 100000, 27700),
         1, 'mean', 2.0) AS filled
FROM (
  VALUES
    (1, 216172782113787048L, CAST(NULL AS DOUBLE)),
    (1, 216172782113786967L, 5.0),
    (1, 216172782113787127L, 5.0)
) AS t(region, cellid, value)
GROUP BY region;
"""


custom_cellfill_sql_example_output = """
+------+--------+
|region|filled  |
+------+--------+
|1     |[binary]|
+------+--------+
... (BINARY — decoded: center cell 216172782113787048 filled to 5.0; neighbours unchanged)
"""


# ============================================================================
# H3 Cell-Fill (both tiers)
#
# gbx_h3_cellfill is a grouped aggregator: interpolates NULL cells from
# valid ring-k neighbours.  Returns BINARY (light) or
# ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> (heavy).
# ============================================================================


def h3_cellfill_sql_example():
    """Fill NULL H3 cells from valid ring-1 neighbours using mean interpolation.

    Inline data: London res-8 center cell 612934495919669247 (NULL, to be
    filled) plus two ring-1 neighbours each carrying value 5.0.  With k=1
    and method='mean' the NULL center is filled with the mean of its
    neighbours (5.0).

    ``gbx_h3_cellfill`` is a grouped aggregator: the call must appear inside a
    GROUP BY query.  H3 cell IDs are BIGINT.  Returns BINARY (light) or
    ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> (heavy).
    """
    return """
SELECT region,
       gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0) AS filled
FROM (
  VALUES
    (1, 612934495919669247L, CAST(NULL AS DOUBLE)),
    (1, 612934495863046143L, 5.0),
    (1, 612934495900794879L, 5.0)
) AS t(region, cellid, value)
GROUP BY region;
"""


h3_cellfill_sql_example_output = """
+------+--------+
|region|filled  |
+------+--------+
|1     |[binary]|
+------+--------+
... (BINARY — decoded: center H3 cell 612934495919669247 filled to 5.0; neighbours unchanged)
"""


# ============================================================================
# H3 Geometry-Aware K-Ring/K-Loop Functions (light-only)
#
# gbx_h3_geomkring / gbx_h3_geomkloop take a geometry directly (WKB BINARY
# or WKT STRING) and compute the polyfill + dilation entirely via the h3
# library (polygon_to_cells_experimental + grid_disk).  No Databricks product
# functions are required — these run locally and on any Databricks cluster.
# ============================================================================


def h3_geomkring_sql_example():
    """Geometry-aware H3 k-ring from a geometry (WKB BINARY or WKT STRING).

    Returns ARRAY<BIGINT> — all H3 cells within k dilation steps of the geometry's
    covering set.  Self-contained: the h3 library performs both the polyfill
    (polygon_to_cells_experimental) and the neighbour walk (grid_disk), so no
    Databricks product functions are required.  mode controls boundary classification.
    """
    return """
SELECT gbx_h3_geomkring(
  'POLYGON((-73.99 40.71, -73.99 40.75, -73.95 40.75, -73.95 40.71, -73.99 40.71))',
  9, 1, 'boundary-out'
) AS kring;
"""


def h3_geomkloop_sql_example():
    """Geometry-aware H3 k-loop (hollow shell) from a geometry (WKB BINARY or WKT STRING).

    Returns ARRAY<BIGINT> — H3 cells at exactly k dilation steps from the geometry's
    covering set (hollow ring, no interior cells).  Self-contained via the h3 library.
    """
    return """
SELECT gbx_h3_geomkloop(
  'POLYGON((-73.99 40.71, -73.99 40.75, -73.95 40.75, -73.95 40.71, -73.99 40.71))',
  9, 1, 'boundary-out'
) AS kloop;
"""


def h3_geomkringexplode_sql_example():
    """Explode geometry-aware H3 k-ring into one row per cell via SQL LATERAL.

    Each row yields one BIGINT H3 cell id.  SQL LATERAL is the only invocation
    path for this streaming UDTF (no Python Column form).  Self-contained:
    no Databricks product functions required.
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((-73.99 40.71, -73.99 40.75, -73.95 40.75, -73.95 40.71, -73.99 40.71))' AS geom) src,
LATERAL gbx_h3_geomkringexplode(src.geom, 9, 1, 'boundary-out') t;
"""


def h3_geomkloopexplode_sql_example():
    """Explode geometry-aware H3 k-loop (hollow ring) into one row per cell via SQL LATERAL.

    Each row yields one BIGINT H3 cell id at exactly k steps.  SQL LATERAL is the
    only invocation path for this streaming UDTF (no Python Column form).
    """
    return """
SELECT t.*
FROM (SELECT 'POLYGON((-73.99 40.71, -73.99 40.75, -73.95 40.75, -73.95 40.71, -73.99 40.71))' AS geom) src,
LATERAL gbx_h3_geomkloopexplode(src.geom, 9, 1, 'boundary-out') t;
"""


# ---------------------------------------------------------------------------
# Expected-output panels for the geometry-aware explode + quadbin/h3 ring/loop
# examples (illustrative; product h3_*/quadbin cell math is data-dependent).
# ---------------------------------------------------------------------------
quadbin_geomkring_sql_example_output = """
+----------------------------------------+
|kring                                   |
+----------------------------------------+
|[..., (cells within k=1 ring at res 12)]|
+----------------------------------------+
... (ARRAY<BIGINT> — quadbin covering set plus one outer ring)
"""
quadbin_geomkloop_sql_example_output = """
+--------------------------------------------+
|kloop                                       |
+--------------------------------------------+
|[..., (outer ring cells, polyfill excluded)]|
+--------------------------------------------+
... (ARRAY<BIGINT> — hollow quadbin outer ring at k=1)
"""
h3_geomkring_sql_example_output = """
+------------------------------------+
|kring                               |
+------------------------------------+
|[617733151020810239, ..., (n cells)]|
+------------------------------------+
... (ARRAY<BIGINT> — H3 res-9 covering cells of NYC box expanded by k=1 ring)
"""
h3_geomkloop_sql_example_output = """
+------------------------------------+
|kloop                               |
+------------------------------------+
|[617733151020810239, ..., (n cells)]|
+------------------------------------+
... (ARRAY<BIGINT> — outer hollow ring at k=1, interior covering cells excluded)
"""
quadbin_geomkringexplode_sql_example_output = """
+-----------+
|cellid     |
+-----------+
|...(BIGINT)|
+-----------+
... (one row per BIGINT cell ID in the k=1 ring of the geometry)
"""
quadbin_geomkloopexplode_sql_example_output = """
+-----------+
|cellid     |
+-----------+
|...(BIGINT)|
+-----------+
... (one row per BIGINT cell ID in the k=1 hollow outer ring)
"""
h3_geomkringexplode_sql_example_output = """
+------------------+
|cellid            |
+------------------+
|617733151020810239|
|...               |
+------------------+
... (one row per BIGINT H3 cell ID in the k=1 ring around the NYC polygon at res 9)
"""
h3_geomkloopexplode_sql_example_output = """
+------------------+
|cellid            |
+------------------+
|617733151020810239|
|...               |
+------------------+
... (one row per BIGINT H3 cell ID in the k=1 hollow outer ring around the NYC polygon)
"""
bng_geomkringexplode_sql_example_output = """
+-----------+
|cellid     |
+-----------+
|...(STRING)|
+-----------+
... (one row per STRING BNG cell ID in the k=1 ring of the geometry)
"""
bng_geomkloopexplode_sql_example_output = """
+-----------+
|cellid     |
+-----------+
|...(STRING)|
+-----------+
... (one row per STRING BNG cell ID in the k=1 hollow outer ring)
"""
