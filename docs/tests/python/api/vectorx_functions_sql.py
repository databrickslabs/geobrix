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
    """Return the canonical CRS authority string embedded in a geometry's SRID (SQL).

    Reads the integer SRID from an EWKB or EWKT geometry and classifies it using
    the authoritative PROJ code sets. Returns NULL for plain WKB / WKT with no
    embedded SRID. ESRI codes (e.g. 54008) are returned as ``ESRI:<n>``, not
    ``EPSG:<n>``, per the authoritative classification rule.

    Geometry input accepts WKB / EWKB (BINARY) and WKT / EWKT (STRING); the EWKT
    literals below are the most readable way to show a carried SRID.
    """
    return """
SELECT gbx_st_crs('SRID=4326;POINT (11 42)')  AS wgs84,
       gbx_st_crs('SRID=54008;POINT (11 42)') AS sinusoidal,
       gbx_st_crs('POINT (11 42)')            AS no_srid;
"""


st_crs_sql_example_output = """
+---------+----------+-------+
|wgs84    |sinusoidal|no_srid|
+---------+----------+-------+
|EPSG:4326|ESRI:54008|NULL   |
+---------+----------+-------+
"""


def st_setcrs_sql_example():
    """Stamp a CRS on a geometry without reprojecting (SQL).

    Assigns the EPSG or ESRI integer SRID to the geometry, leaving coordinates
    untouched. In SQL the result is always BINARY (EWKB), whichever encoding the
    geometry argument arrived in — read the stamped SRID back with ``gbx_st_crs``.

    A CRS with no integer authority code (raw WKT, PROJ4, or a non-numeric code
    such as ``OGC:CRS84``) is rejected at execution time, because a geometry SRID
    must be an integer.
    """
    return """
SELECT gbx_st_crs(gbx_st_setcrs('POINT (11 42)', 'EPSG:4326'))   AS stamped_wgs84,
       gbx_st_crs(gbx_st_setcrs('POINT (11 42)', 'ESRI:54008'))  AS stamped_esri,
       gbx_st_crs(gbx_st_setcrs(
           unhex('010100000000000000000026400000000000004540'), 32633))
           AS stamped_from_wkb;
"""


st_setcrs_sql_example_output = """
+-------------+------------+----------------+
|stamped_wgs84|stamped_esri|stamped_from_wkb|
+-------------+------------+----------------+
|EPSG:4326    |ESRI:54008  |EPSG:32633      |
+-------------+------------+----------------+
"""


def st_transformcrs_sql_example():
    """Reproject a geometry to a target CRS (SQL).

    Reprojects from the geometry's embedded SRID (EWKB / EWKT) or, for a plain
    SRID-less geometry, from an explicit third ``source_crs`` argument. In SQL the
    result is always BINARY.

    The carried SRID follows the **target**: a target with an integer authority
    code (``EPSG:n`` / ``ESRI:n``) stamps ``n``, while a target with none (raw WKT
    or PROJ4) reprojects the coordinates and clears the now-stale SRID — so
    ``gbx_st_crs`` returns NULL for that column. When no source CRS is resolvable
    at all the geometry is returned unchanged rather than erroring.
    """
    return """
SELECT gbx_st_crs(gbx_st_transformcrs('SRID=4326;POINT (11 42)', 'EPSG:32633'))
           AS to_utm33n,
       gbx_st_crs(gbx_st_transformcrs('POINT (11 42)', 'ESRI:54008', 'EPSG:4326'))
           AS to_sinusoidal,
       gbx_st_crs(gbx_st_transformcrs('SRID=4326;POINT (11 42)',
           '+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs')) AS to_proj4;
"""


st_transformcrs_sql_example_output = """
+----------+-------------+--------+
|to_utm33n |to_sinusoidal|to_proj4|
+----------+-------------+--------+
|EPSG:32633|ESRI:54008   |NULL    |
+----------+-------------+--------+
"""
