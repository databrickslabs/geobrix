"""
SQL examples for VectorX functions documentation.

Used by the function-info generator and by docs via CodeFromTest.
"""


def st_legacyaswkb_sql_example():
    """Convert a legacy Mosaic geometry struct to standard WKB (SQL).

    Reads the ``legacy_geoms`` setup view — one row with a Mosaic InternalGeometry
    struct encoding POINT(13, 42): ``typeId=1``, ``srid=0``,
    ``boundaries=[[[13.0, 42.0]]]``, ``holes=[]``.  Returns the plain WKB bytes
    for POINT(13, 42); the output carries no embedded SRID.
    """
    return """
SELECT gbx_st_legacyaswkb(geom_legacy) AS wkb FROM legacy_geoms;
"""


st_legacyaswkb_sql_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (WKB binary)
"""


def st_asmvt_sql_example():
    """Aggregate tile-local features into a Mapbox Vector Tile (MVT) protobuf blob (SQL).

    Reads the two-row ``mvt_features`` fixture view (tile-local WKB POINT(100, 100) and
    POINT(200, 200) in pixel space, both in tile z=0/x=0/y=0) and aggregates all features
    in the same tile into one MVT blob. The ``GROUP BY z, x, y`` step is required; it tells
    the aggregator which tile's blob to build.
    """
    return """
SELECT z, x, y, gbx_st_asmvt(geom_wkb, attrs, 'layer') AS mvt
FROM mvt_features
GROUP BY z, x, y;
"""


st_asmvt_sql_example_output = """
+-+-+-+--------+
|z|x|y|mvt     |
+-+-+-+--------+
|0|0|0|[binary]|
+-+-+-+--------+
... (MVT binary)
"""


def st_asmvt_pyramid_sql_example():
    """Explode one WGS-84 feature into per-tile (z, x, y, mvt_bytes) rows (SQL, heavy form).

    A single WGS-84 POINT(0, 0) — at the intersection of the equator and prime meridian —
    is fed into the pyramid generator at zoom levels 0–2.  The generator clips and
    re-encodes the feature for every intersecting tile, yielding 3 rows: one per zoom level
    ((0,0,0), (1,1,1), (2,2,2)).

    Note: The heavy tier uses ``LATERAL VIEW ... AS tile`` (Hive-style); the lightweight
    tier uses ``LATERAL ... t`` (SQL standard).  Both forms accept the same arguments.
    """
    return """
WITH feats AS (
    SELECT unhex('010100000000000000000000000000000000000000') AS geom_wkb,
           named_struct('name', 'origin', 'id', 1L) AS attrs
)
SELECT t.tile.z AS z, t.tile.x AS x, t.tile.y AS y, t.tile.mvt_bytes AS mvt_bytes
FROM feats
LATERAL VIEW gbx_st_asmvt_pyramid(geom_wkb, attrs, 0, 2, 'layer', 4096) t AS tile;
"""


st_asmvt_pyramid_sql_example_output = """
+-+-+-+---------+
|z|x|y|mvt_bytes|
+-+-+-+---------+
|0|0|0|[binary] |
|1|1|1|[binary] |
|2|2|2|[binary] |
+-+-+-+---------+
... (MVT binary — one row per intersecting tile across zoom levels 0–2)
"""


def st_triangulate_sql_example():
    """Build a Delaunay triangulation from mass-point geometries (SQL).

    Reads the ``tin_survey`` fixture view — one row with ``pts ARRAY<BINARY>``
    (4 WKB POINT Z forming a 10×10 m square with elevations 0, 0, 10, 5 m) and
    ``bl ARRAY<BINARY>`` (empty breaklines array).  Constrained Delaunay
    triangulation of a quadrilateral produces exactly 2 triangle polygons.
    ``mode='constrained'`` is available in both tiers; ``mode='conforming'``
    (inserts Steiner points) is heavyweight-only.
    Uses ``LATERAL VIEW`` (Hive-style generator syntax); the lightweight tier
    supports the SQL-standard ``LATERAL`` form.
    """
    return """
SELECT t.triangle
FROM tin_survey
LATERAL VIEW gbx_st_triangulate(pts, bl, 0, 0, 'NONENCROACHING', 'constrained') t AS triangle
"""


st_triangulate_sql_example_output = """
+--------+
|triangle|
+--------+
|[binary]|
|[binary]|
+--------+
... (WKB binary — 2 Delaunay triangle polygons)
"""


def st_interpolateelevationbbox_sql_example():
    """Interpolate elevation on a regular grid covering a bounding box from a TIN (SQL).

    Reads the ``tin_survey`` fixture view (same 4-corner POINT Z fixture as
    ``st_triangulate``).  Samples the TIN on a 3×3 grid spanning the full 10×10 m
    extent (0–10 in both X and Y, SRID=0).  All 9 cell centres fall inside the TIN
    convex hull, so the generator emits 9 rows.
    ``mode='constrained'`` is available in both tiers; ``mode='conforming'``
    (inserts Steiner points) is heavyweight-only.
    Uses ``LATERAL VIEW`` (Hive-style generator syntax); the lightweight tier
    supports the SQL-standard ``LATERAL`` form.
    """
    return """
SELECT t.elevation_point
FROM tin_survey
LATERAL VIEW gbx_st_interpolateelevationbbox(pts, bl, 0, 0, 'NONENCROACHING', 0, 0, 10, 10, 3, 3, 0, 'constrained') t AS elevation_point
"""


st_interpolateelevationbbox_sql_example_output = """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 elevation grid, 9 rows)
"""


def st_interpolateelevationgeom_sql_example():
    """Interpolate elevation on a grid anchored to a geometry origin (SQL).

    Reads the ``tin_survey`` fixture view (same 4-corner POINT Z fixture).  The
    grid is anchored to POINT(0, 10) — top-left corner of the TIN extent — with
    3 columns, 3 rows, and cell sizes of 3.0 m × 3.0 m (negative Y steps
    downward per raster convention).  All 9 cell centres fall inside the TIN hull
    and the SRID of the output points is 0 (plain WKB origin carries no SRID).
    ``mode='constrained'`` is available in both tiers; ``mode='conforming'``
    (inserts Steiner points) is heavyweight-only.
    Uses ``LATERAL VIEW`` (Hive-style generator syntax); the lightweight tier
    supports the SQL-standard ``LATERAL`` form.
    """
    return """
SELECT t.elevation_point
FROM tin_survey
LATERAL VIEW gbx_st_interpolateelevationgeom(pts, bl, 0, 0, 'NONENCROACHING', unhex('010100000000000000000000000000000000002440'), 3, 3, 3, -3, 'constrained') t AS elevation_point
"""


st_interpolateelevationgeom_sql_example_output = """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 origin-anchored grid, 9 rows)
"""


def st_crs_sql_example():
    """Return the canonical CRS authority string for a geometry's embedded SRID (SQL).

    Reads the integer SRID from an EWKB or EWKT geometry. The fixture view
    ``vector_geoms`` has one row: ``geom STRING = 'SRID=4326;POINT (13 42)'``.
    Returns NULL for plain WKB/WKT with no embedded SRID; ESRI-range codes come
    back as ``ESRI:<n>`` (not ``EPSG:<n>``).
    """
    return """
SELECT gbx_st_crs(geom) AS crs FROM vector_geoms;
"""


st_crs_sql_example_output = """
+---------+
|crs      |
+---------+
|EPSG:4326|
+---------+
"""


def st_setcrs_sql_example():
    """Stamp a CRS on a geometry without reprojecting (SQL).

    The fixture view ``vector_geoms`` has one row: ``geom STRING =
    'SRID=4326;POINT (13 42)'``. In SQL the result is always BINARY (EWKB),
    whichever encoding the geometry argument arrived in.
    """
    return """
SELECT gbx_st_setcrs(geom, 'EPSG:4326') AS stamped FROM vector_geoms;
"""


st_setcrs_sql_example_output = """
+--------+
|stamped |
+--------+
|[binary]|
+--------+
... (EWKB binary — coordinates preserved, SRID=4326 embedded)
"""


def st_transformcrs_sql_example():
    """Reproject a geometry to a target CRS (SQL).

    The fixture view ``vector_geoms`` has one row: ``geom STRING =
    'SRID=4326;POINT (13 42)'`` — POINT(13, 42) is at 13°E 42°N (central
    Italy), inside UTM zone 33N's area of use (12°E–18°E), so reprojection to
    EPSG:32633 is in-domain and returns a non-null result. In SQL the result is
    always BINARY.
    """
    return """
SELECT gbx_st_transformcrs(geom, 'EPSG:32633') AS utm33n FROM vector_geoms;
"""


st_transformcrs_sql_example_output = """
+--------+
|utm33n  |
+--------+
|[binary]|
+--------+
... (EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)
"""


# ---------------------------------------------------------------------------
# Antimeridian family — st_shiftlongitude, st_wrapx, st_split
# Light-only (pyvx tier); register pyvx before running these SQL examples.
# All three functions accept WKT/EWKT strings or WKB binary input.
# ---------------------------------------------------------------------------


def st_shiftlongitude_sql_example():
    """Shift longitude from [-180,180] to [0,360] (SQL, light pyvx tier).

    Demonstrates shifting a polygon near the western antimeridian from
    negative-longitude space into [0,360] space, making it contiguous for
    splitting at x=180.  Inline WKT string input — no ST_GeomFromText wrapper
    needed because ``gbx_st_shiftlongitude`` accepts WKT/EWKT strings directly.
    Output is BINARY (WKB).
    """
    return """
SELECT gbx_st_shiftlongitude('POLYGON((-170 -10, -150 -10, -150 10, -170 10, -170 -10))') AS shifted
"""


st_shiftlongitude_sql_example_output = """
+--------+
|shifted |
+--------+
|[binary]|
+--------+
... (WKB binary — polygon in [0,360] longitude space, x coordinates shifted to [190,210])
"""


def st_wrapx_sql_example():
    """Wrap X coordinates back into [-180,180] (SQL, light pyvx tier).

    POINT(190, 10) is in [0,360] longitude space; ``wrap_x_origin=180`` with
    ``wrap_direction=-360`` maps any x ≥ 180 back by -360, yielding POINT(-170, 10).
    Inline WKT string input — no ST_AsBinary wrapper needed.  Output is BINARY (WKB).
    """
    return """
SELECT gbx_st_wrapx('POINT(190 10)', 180, -360) AS wrapped
"""


st_wrapx_sql_example_output = """
+--------+
|wrapped |
+--------+
|[binary]|
+--------+
... (WKB binary — POINT(-170, 10): x=190 wrapped back by -360)
"""


def st_split_sql_example():
    """Split a polygon by the 180° meridian; returns a GEOMETRYCOLLECTION (SQL, light pyvx tier).

    The input polygon straddles the antimeridian (x range 170 to 190); the
    blade is the 180° meridian linestring.  Both inputs are inline WKT strings.
    Output is BINARY (WKB) representing a two-piece GeometryCollection — one
    polygon on each side of the cut.
    """
    return """
SELECT gbx_st_split('POLYGON((170 -10, 190 -10, 190 10, 170 10, 170 -10))', 'LINESTRING(180 -90, 180 90)') AS pieces
"""


st_split_sql_example_output = """
+--------+
|pieces  |
+--------+
|[binary]|
+--------+
... (WKB binary — GeometryCollection with 2 polygon pieces split at x=180)
"""


def antimeridian_pattern_sql_example():
    """Full antimeridian-normalization pattern: shift → split → dump → wrap → union (SQL).

    Antimeridian-crossing polygons cannot be directly aggregated or compared by
    most spatial engines without normalization.  The input polygon straddles the
    180° antimeridian in standard [-180,180] space: vertices at 170°E and 170°W
    (-170°).  This pattern runs on Databricks Runtime 17.3+ where the built-in
    ``ST_Dump``/``ST_XMax``/``ST_Union``/``ST_Multi`` functions are available:

    1. ``gbx_st_shiftlongitude`` — shift [-180,180] → [0,360] so the polygon
       becomes contiguous (x ∈ [170, 190]) and cuttable at x=180.
    2. ``gbx_st_split`` — cut at the 180° meridian into a GeometryCollection.
    3. ``ST_Dump`` — explode the collection into individual polygon rows.
    4. ``CASE WHEN ST_XMax(piece) > 180 THEN gbx_st_wrapx(…) ELSE ST_AsBinary(piece) END``
       — wrap only the eastern piece (x ≥ 180) back to [-180, 0]; leave the
       western piece (all x ≤ 180) unchanged.  Applying wrapx uniformly would
       move the western piece's x=180 edge to x=-180, producing a 350°-wide
       polygon instead of a clean 10° strip.
    5. ``ST_Union`` / ``ST_Multi`` — reassemble into a clean MULTIPOLYGON.
    """
    return """
WITH raw AS (
  SELECT 'POLYGON((170 -10, -170 -10, -170 10, 170 10, 170 -10))' AS geom
),
pieces AS (
  SELECT (ST_Dump(ST_GeomFromWKB(
           gbx_st_split(gbx_st_shiftlongitude(geom),
                        'LINESTRING(180 -90, 180 90)')))).geom AS piece
  FROM raw
),
parts AS (
  SELECT CASE
           WHEN ST_XMax(piece) > 180
             THEN gbx_st_wrapx(ST_AsBinary(piece), 180, -360)
           ELSE ST_AsBinary(piece)
         END AS geom
  FROM pieces)
SELECT ST_AsText(ST_Multi(ST_Union(ST_GeomFromWKB(geom)))) AS normalized FROM parts;
"""


antimeridian_pattern_sql_example_output = """
+--------------------+
|normalized          |
+--------------------+
|MULTIPOLYGON (...)  |
+--------------------+
... (WKT — two polygons, one on each side of the 180° antimeridian)
"""


# ---------------------------------------------------------------------------
# Geometry validity family — st_makevalid, st_explainvalidity
# Light-only (pyvx tier). These functions are pure-Python UDFs — they have
# no heavyweight Scala equivalent and no JAR dependency.
# Input: inline WKT string (accepted by pyvx parse_geom via gbx UDFs).
# Output: BINARY (st_makevalid) / STRING/JSON (st_explainvalidity).
# NOTE: examples use only gbx_st_* functions — no product ST_* (ST_IsValid,
# ST_GeomFromText) because the doc-test Spark session is vanilla Spark 4.0.0
# with no DBR spatial built-ins. Product-gated conditional patterns are shown
# in the docs prose as illustrative real-cluster usage (precedent: this page's
# antimeridian section keeps ST_Dump / ST_GeomFromWKB in prose, not executed).
# ---------------------------------------------------------------------------


def st_makevalid_sql_example():
    """Repair an invalid geometry to OGC-SFS validity (SQL, light pyvx tier).

    A self-intersecting bowtie polygon — ``POLYGON((0 0,1 1,1 0,0 1,0 0))`` —
    is fed to ``gbx_st_makevalid``.  The default level ``linework`` nodes the
    self-intersection into a valid geometry.  Input is an inline WKT string
    accepted directly by the pyvx UDF (no ``ST_AsBinary`` wrapper needed).
    Output is BINARY (WKB).
    """
    return """
SELECT gbx_st_makevalid('POLYGON((0 0,1 1,1 0,0 1,0 0))') AS clean
"""


st_makevalid_sql_example_output = """
+--------+
|clean   |
+--------+
|[binary]|
+--------+
... (WKB binary — repaired geometry; bowtie becomes a valid multi-polygon)
"""


def st_explainvalidity_sql_example():
    """Diagnose SFS validity as JSON {valid, reason, code, location} (SQL, light pyvx tier).

    A self-intersecting bowtie polygon is diagnosed; the JSON shows
    ``valid=false``, the GEOS reason string (``Self-intersection[0.5 0.5]``),
    ``code=10`` (the stable self-intersection SFS violation code), and
    ``location=POINT(0.5 0.5)`` (the coordinate where the violation occurs).
    Input is an inline WKT string; output is a JSON STRING.
    """
    return """
SELECT gbx_st_explainvalidity('POLYGON((0 0,1 1,1 0,0 1,0 0))') AS detail
"""


st_explainvalidity_sql_example_output = """
+----------------------------------------------------------------------+
|detail                                                                |
+----------------------------------------------------------------------+
|{"valid": false, "reason": "Self-intersection[0.5 0.5]", "code": 10,|
+----------------------------------------------------------------------+
... (JSON string — {valid, reason, code, location} for SFS validity diagnosis)
"""


# ---------------------------------------------------------------------------
# Geometry cleaning family — st_simplifypreservetopology, st_removerepeatedpoints,
#                            st_reduceprecision, st_node, st_snap
# Light-only (pyvx tier). All 5 are pure-Python UDFs — no JAR dependency.
# Input: inline WKT string (accepted by pyvx parse_geom via gbx UDFs).
# Output: BINARY (WKB). Examples use only gbx_st_* — no product ST_* because
# the doc-test Spark session is vanilla Spark 4.0.0 with no DBR spatial built-ins.
# ---------------------------------------------------------------------------


def st_simplifypreservetopology_sql_example():
    """Simplify a polygon while preserving topology (SQL, light pyvx tier).

    A near-collinear polygon with an extra near-collinear vertex on its left edge —
    ``POLYGON((0 0,0 5,0.001 8,0 10,10 10,10 0,0 0))`` — is simplified with a
    tolerance of 1.0.  The topology-preserving Douglas-Peucker algorithm drops the
    near-collinear vertex (0.001, 8) without collapsing or splitting the polygon.
    Input is an inline WKT string; output is BINARY (WKB).
    """
    return """
SELECT gbx_st_simplifypreservetopology('POLYGON((0 0,0 5,0.001 8,0 10,10 10,10 0,0 0))', 1.0) AS simplified
"""


st_simplifypreservetopology_sql_example_output = """
+--------+
|simplified|
+--------+
|[binary]|
+--------+
... (WKB binary — simplified polygon with near-collinear vertex removed, topology preserved)
"""


def st_removerepeatedpoints_sql_example():
    """Remove consecutive duplicate vertices from a linestring (SQL, light pyvx tier).

    A linestring with repeated coordinates — ``LINESTRING(0 0,0 0,1 1,1 1,2 2)`` —
    has its duplicate consecutive vertices removed.  The default ``tolerance=0.0``
    removes only exact duplicates; a positive tolerance also removes near-duplicates
    within that distance.  Input is an inline WKT string; output is BINARY (WKB).
    """
    return """
SELECT gbx_st_removerepeatedpoints('LINESTRING(0 0,0 0,1 1,1 1,2 2)') AS deduped
"""


st_removerepeatedpoints_sql_example_output = """
+--------+
|deduped |
+--------+
|[binary]|
+--------+
... (WKB binary — linestring with duplicate consecutive vertices removed: LINESTRING(0 0,1 1,2 2))
"""


def st_reduceprecision_sql_example():
    """Snap coordinates to a precision grid (SQL, light pyvx tier).

    ``POINT(1.234 5.678)`` is snapped to a grid of size 1.0 — each coordinate
    is rounded to the nearest integer grid line.  Also known as snap-to-grid
    (``ST_SnapToGrid`` in PostGIS).  Uses ``mode="valid_output"`` so the result
    remains valid.  Input is an inline WKT string; output is BINARY (WKB).
    """
    return """
SELECT gbx_st_reduceprecision('POINT(1.234 5.678)', 1.0) AS snapped
"""


st_reduceprecision_sql_example_output = """
+--------+
|snapped |
+--------+
|[binary]|
+--------+
... (WKB binary — POINT(1.0, 6.0): coordinates snapped to nearest 1.0 grid lines)
"""


def st_node_sql_example():
    """Node linework: split at all self-intersections (SQL, light pyvx tier).

    A self-intersecting figure-eight linestring — ``LINESTRING(0 0,10 10,0 10,10 0)``
    — crosses itself at (5, 5).  ``gbx_st_node`` splits the linework at every
    intersection and returns a ``MultiLineString`` with each resulting segment as a
    separate sub-line.  Input is an inline WKT string; output is BINARY (WKB).
    """
    return """
SELECT gbx_st_node('LINESTRING(0 0,10 10,0 10,10 0)') AS noded
"""


st_node_sql_example_output = """
+--------+
|noded   |
+--------+
|[binary]|
+--------+
... (WKB binary — MultiLineString: figure-eight split into clean segments at the self-intersection)
"""


def st_snap_sql_example():
    """Snap geometry vertices onto a reference within a tolerance (SQL, light pyvx tier).

    A linestring that misses a reference line by 0.4 units —
    ``LINESTRING(0 0.4,10 0.4)`` — is snapped onto the reference
    ``LINESTRING(0 0,10 0)`` with a tolerance of 0.5.  The near-miss vertices
    (y=0.4) snap onto the reference (y=0), aligning the two geometries.
    Both inputs are inline WKT strings; output is BINARY (WKB).
    """
    return """
SELECT gbx_st_snap('LINESTRING(0 0.4,10 0.4)', 'LINESTRING(0 0,10 0)', 0.5) AS snapped
"""


st_snap_sql_example_output = """
+--------+
|snapped |
+--------+
|[binary]|
+--------+
... (WKB binary — linestring with near-miss vertices snapped onto the reference at y=0)
"""
