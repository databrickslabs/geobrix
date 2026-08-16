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
    """Aggregate features into a Mapbox Vector Tile (MVT) protobuf blob (SQL).

    The view `features` here is a 2-row sample with WKB geometries (`POINT(0.1, 0.1)`
    and `POINT(0.5, 0.5)`) and a `(name, id)` attribute struct. Real pipelines would
    `GROUP BY z, x, y` after composing tile-local coordinates upstream.
    """
    return """
WITH features AS (
    SELECT unhex('01010000009A9999999999B93F9A9999999999B93F') AS geom_wkb,
           named_struct('name', 'a', 'id', 1L) AS attrs
    UNION ALL SELECT unhex('0101000000000000000000E03F000000000000E03F'),
           named_struct('name', 'b', 'id', 2L)
)
SELECT length(gbx_st_asmvt(geom_wkb, attrs, 'layer1')) AS mvt_bytes_len FROM features;
"""


def st_asmvt_pyramid_sql_example():
    """Explode one feature into one row per intersecting (z, x, y) tile, encoded as MVT (SQL).

    The view `features` here is a single polygon (WKB for a rectangle spanning lon -30..+30,
    lat 10..20). At z=2 the polygon straddles the prime meridian (tiles x=1 and x=2 in the
    y=1 row), so the generator emits 2 rows. Output struct column `t.tile` carries
    `(z, x, y, mvt_bytes)`; pipe the bytes into `gbx_pmtiles_agg` for vector publishing.
    """
    return """
WITH features AS (
    SELECT unhex('010300000001000000050000000000000000003EC000000000000024400000000000003E4000000000000024400000000000003E4000000000000034400000000000003EC000000000000034400000000000003EC00000000000002440') AS geom_wkb,
           named_struct('name', 'region-a', 'id', 1L) AS attrs
)
SELECT t.tile.z AS z, length(t.tile.mvt_bytes) AS mvt_bytes_len
FROM features
LATERAL VIEW gbx_st_asmvt_pyramid(geom_wkb, attrs, 2, 2, 'regions') t AS tile;
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
