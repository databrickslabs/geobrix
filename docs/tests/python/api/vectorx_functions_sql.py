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
