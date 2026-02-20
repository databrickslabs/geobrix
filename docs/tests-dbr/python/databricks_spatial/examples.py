"""
Databricks Spatial examples: GEOMETRY, GEOGRAPHY, ST functions, H3.

Requires Databricks Runtime 17.1+ (or Databricks SQL with ST/H3 support).
Single source of truth for docs/docs/databricks-spatial.mdx.
Tested by: docs/tests-dbr/python/databricks_spatial/test_examples.py

References:
- GEOMETRY: https://docs.databricks.com/aws/en/sql/language-manual/data-types/geometry-type
- GEOGRAPHY: https://docs.databricks.com/aws/en/sql/language-manual/data-types/geography-type
- ST functions: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-st-geospatial-functions
- H3 functions: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-h3-geospatial-functions
"""

# -----------------------------------------------------------------------------
# GEOMETRY type (Euclidean X,Y; SRID or ANY)
# -----------------------------------------------------------------------------

GEOMETRY_CREATE_FROM_WKT = """-- Create GEOMETRY from WKT (SRID 0 or specify)
SELECT st_geomfromtext('POINT(1 2)') AS geom;
SELECT st_geomfromtext('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 4326) AS geom;"""

GEOMETRY_CREATE_FROM_GEOJSON = """-- Create GEOMETRY from GeoJSON (default SRID 4326)
SELECT st_geomfromgeojson('{"type":"Point","coordinates":[[1,2]]}') AS geom;
SELECT to_geometry('{"type":"Point","coordinates":[[1,2]]}') AS geom;"""

GEOMETRY_EXPORT = """-- Export GEOMETRY to WKB, GeoJSON, WKT
SELECT hex(st_asbinary(st_geomfromtext('POINT(1 2)'))) AS wkb_hex;
SELECT st_asgeojson(st_geomfromtext('POINT(1 2)')) AS geojson;
SELECT st_astext(st_geomfromtext('POINT(1 2)')) AS wkt;"""

# -----------------------------------------------------------------------------
# GEOGRAPHY type (lat/lon; SRID 4326 only)
# -----------------------------------------------------------------------------

GEOGRAPHY_CREATE = """-- Create GEOGRAPHY from WKT/GeoJSON (SRID 4326)
SELECT st_geogfromtext('POINT(1 2)') AS geog;
SELECT st_geogfromgeojson('{"type":"Point","coordinates":[[1,2]]}') AS geog;
SELECT to_geography('{"type":"Point","coordinates":[[1,2]]}') AS geog;"""

GEOGRAPHY_EXPORT = """-- Export GEOGRAPHY to GeoJSON, WKT
SELECT st_asgeojson(st_geogfromtext('POINT(1 2)')) AS geojson;
SELECT st_astext(st_geogfromtext('POINT(1 2)')) AS wkt;"""

# -----------------------------------------------------------------------------
# ST functions (measurements, predicates, constructors)
# -----------------------------------------------------------------------------

ST_IMPORT_PYTHON = """# In Databricks Runtime notebooks: import ST functions
from pyspark.databricks.sql import functions as dbf

# Use dbf.st_* for GEOMETRY/GEOGRAPHY
# Example: dbf.st_area(col("geometry")), dbf.st_intersects(...)"""

ST_MEASUREMENTS = """-- Area, length, centroid, distance
SELECT st_area(st_geomfromtext('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')) AS area;
SELECT st_centroid(st_geomfromtext('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')) AS center;
SELECT st_distance(
  st_geomfromtext('POINT(0 0)'),
  st_geomfromtext('POINT(3 4)')
) AS dist;"""

ST_PREDICATES = """-- Spatial predicates: intersects, contains, within
SELECT st_intersects(
  st_geomfromtext('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
  st_geomfromtext('POINT(1 1)')
) AS intersects;
SELECT st_contains(
  st_geomfromtext('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'),
  st_geomfromtext('POINT(1 1)')
) AS contains;"""

ST_CONSTRUCTORS = """-- Construct point, buffer, convex hull
SELECT st_point(1, 2) AS pt;
SELECT st_buffer(st_geomfromtext('POINT(0 0)'), 0.5) AS buffered;
SELECT st_convexhull(st_geomfromtext('MULTIPOINT(0 0, 1 0, 1 1)')) AS hull;"""

# -----------------------------------------------------------------------------
# H3 functions (grid indexing)
# -----------------------------------------------------------------------------

H3_IMPORT_PYTHON = """# In Databricks Runtime notebooks: import H3 functions
from pyspark.databricks.sql import functions as dbf

# Use dbf.h3_* for H3 cell IDs (BIGINT or STRING)
# Example: dbf.h3_longlatash3(lon, lat, resolution)"""

H3_POINT_INDEX = """-- Index a point to H3 cell (resolution 7 ~5 km²)
SELECT h3_longlatash3(-73.99, 40.71, 7) AS h3_cell;
SELECT h3_longlatash3string(-73.99, 40.71, 7) AS h3_str;"""

H3_POLYFILL = """-- Polyfill: get H3 cells contained by a polygon
SELECT h3_polyfillash3(
  st_geogfromgeojson('{"type":"Polygon","coordinates":[[[-74.02,40.70],[-73.98,40.70],[-73.98,40.74],[-74.02,40.74],[-74.02,40.70]]]}'),
  7
) AS cells;"""

H3_NEIGHBORS = """-- K-ring: cells within distance k of a cell
SELECT h3_kring(h3_longlatash3(-73.99, 40.71, 7), 1) AS ring;
SELECT h3_hexring(h3_longlatash3(-73.99, 40.71, 7), 1) AS hex_ring;"""

H3_BOUNDARY = """-- Get H3 cell boundary as WKT/GeoJSON
SELECT h3_boundaryaswkt(h3_longlatash3(-73.99, 40.71, 7)) AS boundary_wkt;
SELECT h3_centerasgeojson(h3_longlatash3(-73.99, 40.71, 7)) AS center_geojson;"""


# -----------------------------------------------------------------------------
# Test helpers (run on DBR; skip if st_* / h3_* not available)
# -----------------------------------------------------------------------------

def run_geometry_create_from_wkt(spark):
    """Run GEOMETRY create from WKT example. Skips if DBR ST not available."""
    try:
        df = spark.sql("SELECT st_geomfromtext('POINT(1 2)') AS geom")
        return df.collect()
    except Exception as e:
        if "st_geomfromtext" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            raise RuntimeError("DBR not available")
        raise


def run_geography_create(spark):
    """Run GEOGRAPHY create example. Skips if DBR ST not available."""
    try:
        df = spark.sql("SELECT st_geogfromtext('POINT(1 2)') AS geog")
        return df.collect()
    except Exception as e:
        if "st_geogfromtext" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            raise RuntimeError("DBR not available")
        raise


def run_st_measurements(spark):
    """Run ST measurement example."""
    try:
        df = spark.sql(
            "SELECT st_area(st_geomfromtext('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')) AS area"
        )
        return df.collect()
    except Exception as e:
        if "st_area" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            raise RuntimeError("DBR not available")
        raise


def run_h3_point_index(spark):
    """Run H3 point index example. Skips if DBR H3 not available."""
    try:
        df = spark.sql("SELECT h3_longlatash3(-73.99, 40.71, 7) AS h3_cell")
        return df.collect()
    except Exception as e:
        if "h3_longlatash3" in str(e) or "UNRESOLVED_ROUTINE" in str(e):
            raise RuntimeError("DBR not available")
        raise
