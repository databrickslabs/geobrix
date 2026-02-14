"""
SQL API Reference Examples - Single source of truth for docs/docs/api/sql.mdx.

All snippets are string constants. Tests in test_sql_api.py validate registration
and compile/execute where possible. Use sample-data paths per cursor rules.
"""

# Sample data base (mounted Volumes)
SAMPLE_DATA_BASE = "/Volumes/main/default/geobrix_samples/geobrix-examples"
SAMPLE_NYC_RASTERS = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_NYC_SHAPEFILE = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"
SAMPLE_NYC_GEOJSON = f"{SAMPLE_DATA_BASE}/nyc/taxi-zones/nyc_taxi_zones.geojson"

# =============================================================================
# REGISTRATION (shown in docs)
# =============================================================================

REGISTER_PYTHON = """from databricks.labs.gbx.rasterx import functions as rx
from databricks.labs.gbx.gridx.bng import functions as bx
from databricks.labs.gbx.vectorx.jts.legacy import functions as vx

rx.register(spark)
bx.register(spark)
vx.register(spark)"""

REGISTER_SCALA = """import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}

rx.register(spark)
bx.register(spark)
vx.register(spark)"""

# =============================================================================
# SQL SNIPPETS (one constant per doc block)
# =============================================================================

SQL_LIST_FUNCTIONS = """-- List all GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_*';

-- List RasterX functions
SHOW FUNCTIONS LIKE 'gbx_rst_*';

-- List GridX functions
SHOW FUNCTIONS LIKE 'gbx_bng_*';

-- List VectorX functions
SHOW FUNCTIONS LIKE 'gbx_st_*';"""

SQL_DESCRIBE = """-- Get function details
DESCRIBE FUNCTION gbx_rst_boundingbox;

-- Get extended information
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;"""

SQL_READ_AND_QUERY_RASTERS = """-- Read rasters (use your sample data path)
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif`;

-- Extract metadata
SELECT
    path,
    gbx_rst_boundingbox(tile) as bbox,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height,
    gbx_rst_numbands(tile) as num_bands,
    gbx_rst_metadata(tile) as metadata
FROM rasters;"""

SQL_FILTER_RASTERS = """-- Filter by dimensions
SELECT *
FROM rasters
WHERE gbx_rst_width(tile) > 1000
  AND gbx_rst_height(tile) > 1000;

-- Filter by band count
SELECT *
FROM rasters
WHERE gbx_rst_numbands(tile) >= 3;"""

SQL_RASTER_TRANSFORMATIONS = """-- Clip raster (geometry as WKT; GeoBrix does not accept st_geomfromtext)
SELECT
    path,
    gbx_rst_clip(tile, 'POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))', true) as clipped_tile
FROM rasters;

-- Create raster catalog
CREATE OR REPLACE TABLE raster_catalog AS
SELECT
    path,
    gbx_rst_boundingbox(tile) as bounds,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height,
    gbx_rst_numbands(tile) as bands,
    gbx_rst_metadata(tile) as metadata
FROM rasters;"""

SQL_BNG_CELL_OPERATIONS = """-- Calculate cell area (returns square kilometres)
SELECT gbx_bng_cellarea('TQ') as area_km2;

-- Example with full cell id
SELECT
    'TQ3080' as grid,
    gbx_bng_cellarea('TQ3080') as area_km2;"""

SQL_BNG_POINT_TO_CELL = """-- Convert points to BNG cells (point as WKT; GeoBrix does not accept st_point)
CREATE OR REPLACE TEMP VIEW uk_points_bng AS
SELECT
    location_id,
    latitude,
    longitude,
    gbx_bng_pointascell(concat('POINT(', cast(longitude as string), ' ', cast(latitude as string), ')'), 1000) as bng_cell_1km,
    gbx_bng_pointascell(concat('POINT(', cast(longitude as string), ' ', cast(latitude as string), ')'), 100) as bng_cell_100m
FROM uk_locations;

SELECT * FROM uk_points_bng;"""

SQL_BNG_SPATIAL_AGGREGATION = """-- Aggregate by BNG cell (point as WKT; GeoBrix does not accept st_point)
CREATE OR REPLACE TABLE bng_aggregated AS
SELECT
    gbx_bng_pointascell(concat('POINT(', cast(longitude as string), ' ', cast(latitude as string), ')'), 1000) as bng_cell,
    COUNT(*) as point_count,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp,
    MIN(temperature) as min_temp
FROM weather_stations
WHERE country = 'GB'
GROUP BY bng_cell;

SELECT * FROM bng_aggregated ORDER BY point_count DESC LIMIT 10;"""

SQL_BNG_MULTI_RESOLUTION = """-- Analyze at multiple resolutions (location column must be WKT or WKB)
CREATE OR REPLACE VIEW multi_resolution AS
SELECT
    location_id,
    gbx_bng_pointascell(location, 10000) as bng_10km,
    gbx_bng_pointascell(location, 1000) as bng_1km,
    gbx_bng_pointascell(location, 100) as bng_100m
FROM locations;

-- Count by resolution
SELECT '10km' as resolution, COUNT(DISTINCT bng_10km) as cell_count FROM multi_resolution
UNION ALL
SELECT '1km', COUNT(DISTINCT bng_1km) FROM multi_resolution
UNION ALL
SELECT '100m', COUNT(DISTINCT bng_100m) FROM multi_resolution;"""

SQL_VECTORX_MIGRATION_WORKFLOW = """-- Full table migration
CREATE OR REPLACE TABLE migrated_features AS
SELECT
    feature_id,
    properties,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_mosaic_table;

-- Validate results
SELECT
    COUNT(*) as total,
    COUNT(geometry) as with_geometry,
    COUNT(CASE WHEN st_isvalid(geometry) THEN 1 END) as valid_geometries
FROM migrated_features;"""

SQL_VECTORX_SPATIAL_FUNCTIONS = """-- Convert and analyze
CREATE OR REPLACE VIEW features_analyzed AS
SELECT
    feature_id,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry,
    st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as area,
    st_length(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as perimeter,
    st_centroid(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as centroid
FROM legacy_features;

SELECT * FROM features_analyzed WHERE area > 1000;"""

SQL_READ_GEOJSON = """-- Read GeoJSON (use your path)
CREATE OR REPLACE TEMP VIEW features AS
SELECT * FROM geojson_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/taxi-zones/nyc_taxi_zones.geojson`;

-- Query with geometry
SELECT
    name,
    type,
    st_area(st_geomfromwkb(geom_0)) as area
FROM features
WHERE st_area(st_geomfromwkb(geom_0)) > 1000;"""

SQL_READ_GEOPACKAGE = """-- Read GeoPackage (note: specify layer in Python/Scala first)
-- gpkg = spark.read.format("gpkg").option("layerName", "buildings").load("/data/city.gpkg")
-- gpkg.createOrReplaceTempView("buildings")

SELECT
    building_id,
    name,
    st_area(st_geomfromwkb(shape)) as floor_area,
    st_centroid(st_geomfromwkb(shape)) as center_point
FROM buildings
WHERE st_area(st_geomfromwkb(shape)) > 500;"""

# =============================================================================
# EXAMPLE OUTPUT (for docs "Example output" blocks via CodeFromTest outputConstant)
# Matches quick-start style; update when paths or output change.
# =============================================================================

SQL_LIST_FUNCTIONS_output = """
+----------------------+
|function              |
+----------------------+
|gbx_rst_asformat      |
|gbx_rst_avg           |
|gbx_rst_bandmetadata  |
|gbx_rst_boundingbox   |
|...                   |
|gbx_bng_cellarea      |
|gbx_bng_pointascell    |
|...                   |
|gbx_st_legacyaswkb    |
|...                   |
+----------------------+
"""

SQL_DESCRIBE_output = """
Function: gbx_rst_boundingbox
Type: SCALAR
Input: (tile BINARY)
Returns: STRUCT<...>
"""

SQL_READ_AND_QUERY_RASTERS_output = """
+--------------------+------------------+-----+------+---------+--------+
|path                |bbox              |width|height|num_bands|metadata|
+--------------------+------------------+-----+------+---------+--------+
|.../nyc_sentinel2...|POLYGON ((-74....)|10980|10980 |1        |{...}   |
+--------------------+------------------+-----+------+---------+--------+
"""

SQL_FILTER_RASTERS_output = """
+--------------------+----+
|path                |tile|
+--------------------+----+
|...                 |... |
+--------------------+----+
"""

SQL_RASTER_TRANSFORMATIONS_output = """
-- Clip: same columns with clipped_tile
-- Catalog table created; query returns path, bounds, width, height, bands, metadata
+--------------------+------------------+-----+------+-----+--------+
|path                |bounds            |width|height|bands|metadata|
+--------------------+------------------+-----+------+-----+--------+
|...                 |POLYGON ((...))   |10980|10980 |1    |{...}   |
+--------------------+------------------+-----+------+-----+--------+
"""

SQL_BNG_CELL_OPERATIONS_output = """
+----------+
|area_km2  |
+----------+
|1.0       |
+----------+

+------+----------+
|grid |area_km2  |
+------+----------+
|TQ3080|1.0       |
+------+----------+
"""

SQL_BNG_POINT_TO_CELL_output = """
+-----------+----------+---------+------------+-------------+
|location_id|latitude  |longitude|bng_cell_1km|bng_cell_100m|
+-----------+----------+---------+------------+-------------+
|1          |51.5074   |-0.1278  |TQ 31 SW    |TQ 308 105   |
|...        |...       |...      |...         |...          |
+-----------+----------+---------+------------+-------------+
"""

SQL_BNG_SPATIAL_AGGREGATION_output = """
+----------+-----------+---------+---------+---------+----------+
|bng_cell  |point_count|avg_temp |max_temp |min_temp |...       |
+----------+-----------+---------+---------+---------+----------+
|TQ 31 SW  |42        |14.2     |28.1     |0.5      |...        |
|...       |...       |...      |...      |...      |...        |
+----------+-----------+---------+---------+---------+----------+
"""

SQL_BNG_MULTI_RESOLUTION_output = """
+-----------+----------+-----+
|resolution |cell_count|
+-----------+----------+-----+
|10km       |...       |
|1km        |...       |
|100m       |...       |
+-----------+----------+-----+
"""

SQL_VECTORX_MIGRATION_WORKFLOW_output = """
+-----+--------------+-----------------+
|total|with_geometry |valid_geometries |
+-----+--------------+-----------------+
|1000 |1000          |998              |
+-----+--------------+-----------------+
"""

SQL_VECTORX_SPATIAL_FUNCTIONS_output = """
+----------+--------------------+------+--------------------+
|feature_id|geometry            |area  |centroid            |
+----------+--------------------+------+--------------------+
|1         |POLYGON ((...))     |1234.5|POINT (...)         |
|...       |...                 |...   |...                 |
+----------+--------------------+------+--------------------+
"""

SQL_READ_GEOJSON_output = """
+----+----+------+
|name|type|area  |
+----+----+------+
|... |... |1234.5|
|... |... |...   |
+----+----+------+
"""

SQL_READ_GEOPACKAGE_output = """
+-----------+----+----------+--------------------+
|building_id|name|floor_area|center_point        |
+-----------+----+----------+--------------------+
|1          |... |5000.0    |POINT (...)         |
|...        |... |...       |...                 |
+-----------+----+----------+--------------------+
"""

# =============================================================================
# PYTHON HELPER (for tests / registration validation)
# =============================================================================

def register_functions_python(spark):
    """Register GeoBrix functions via Python for SQL use."""
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx

    rx.register(spark)
    bx.register(spark)
    vx.register(spark)
    return rx, bx, vx


if __name__ == "__main__":
    print("✓ All SQL API examples defined")
