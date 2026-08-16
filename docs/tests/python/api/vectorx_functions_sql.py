"""
SQL examples for VectorX functions documentation.

Used by the function-info generator and by docs via CodeFromTest.
"""


def st_legacyaswkb_sql_example():
    """Convert legacy Mosaic geometry to WKB (SQL). Requires table with geom_legacy column."""
    return """
SELECT gbx_st_legacyaswkb(geom_legacy) AS wkb FROM legacy_table;
"""


st_legacyaswkb_sql_example_output = """
+--------+
|wkb     |
+--------+
|[BINARY]|
+--------+
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
+---+---+---+---------+
|  z|  x|  y|      mvt|
+---+---+---+---------+
|  0|  0|  0|[binary] |
+---+---+---+---------+
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
+---+---+---+-----------+
|  z|  x|  y|  mvt_bytes|
+---+---+---+-----------+
|  0|  0|  0|  [binary] |
|  1|  1|  1|  [binary] |
|  2|  2|  2|  [binary] |
+---+---+---+-----------+
... (MVT binary — one row per intersecting tile across zoom levels 0–2)
"""


def st_triangulate_sql_example():
    """Build a Delaunay triangulation from mass-point and breakline geometries (SQL).

    Accepts a column of mass-point geometries (`masspoints`), a column of breakline
    geometries (`breaklines`), a snap tolerance, a minimum triangle area, a
    conforming-mesh strategy, and a triangulation `mode` (`'constrained'` default,
    available in both tiers; `'conforming'` is heavyweight-only). Returns one
    triangle geometry per row.
    """
    return """
SELECT gbx_st_triangulate(masspoints, breaklines, 0.01, 0.01, 'NONENCROACHING', 'constrained') AS triangle FROM survey;
"""


st_triangulate_sql_example_output = """
+--------+
|triangle|
+--------+
|[BINARY]|
+--------+
"""


def st_interpolateelevationbbox_sql_example():
    """Interpolate elevation on a regular grid covering a bounding box from a TIN (SQL).

    Builds a triangulated irregular network from mass points and breaklines, then
    samples it on a grid of `cols x rows` cells within the specified bounding box
    (xmin, ymin, xmax, ymax) in the given SRID. A trailing `mode` argument selects
    the triangulation strategy (`'constrained'` default, both tiers; `'conforming'`
    heavyweight-only). Returns one point-with-Z geometry per grid cell.
    """
    return """
SELECT gbx_st_interpolateelevationbbox(masspoints, breaklines, 0.0, 0.01, 'NONENCROACHING', 530000, 180000, 531000, 181000, 100, 100, 27700, 'constrained') AS elev_point FROM survey;
"""


st_interpolateelevationbbox_sql_example_output = """
+----------+
|elev_point|
+----------+
|[BINARY]  |
+----------+
"""


def st_interpolateelevationgeom_sql_example():
    """Interpolate elevation at locations derived from a geometry's bounding box (SQL).

    Builds a triangulated irregular network from mass points and breaklines, then
    samples it on a grid anchored to the bounding box of the supplied geometry.
    `cell_width` and `cell_height` control the grid resolution (negative height
    steps downward). A trailing `mode` argument selects the triangulation strategy
    (`'constrained'` default, both tiers; `'conforming'` heavyweight-only). Returns
    one point-with-Z geometry per grid cell.
    """
    return """
SELECT gbx_st_interpolateelevationgeom(masspoints, breaklines, 0.0, 0.01, 'NONENCROACHING', ST_Point(530000, 181000), 100, 100, 10.0, -10.0, 'constrained') AS elev_point FROM survey;
"""


st_interpolateelevationgeom_sql_example_output = """
+----------+
|elev_point|
+----------+
|[BINARY]  |
+----------+
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
(EWKB binary — coordinates preserved, SRID=4326 embedded)
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
(EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)
"""
