---
sidebar_position: 4
---

# SQL API Reference

GeoBrix functions can be used directly in SQL after registration, providing a familiar interface for SQL users.

## Registration

SQL functions must be registered via Python or Scala before use:

### Via Python

```python
from databricks.labs.gbx.rasterx import functions as rx
from databricks.labs.gbx.gridx.bng import functions as bx
from databricks.labs.gbx.vectorx import functions as vx

rx.register(spark)
bx.register(spark)
vx.register(spark)
```

### Via Scala

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import com.databricks.labs.gbx.vectorx.{functions => vx}

rx.register(spark)
bx.register(spark)
vx.register(spark)
```

## Function Naming

All GeoBrix SQL functions use the `gbx_` prefix:

- **RasterX**: `gbx_rst_*`
- **GridX/BNG**: `gbx_bng_*`
- **VectorX**: `gbx_st_*`

## Listing Functions

```sql
-- List all GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_*';

-- List RasterX functions
SHOW FUNCTIONS LIKE 'gbx_rst_*';

-- List GridX functions
SHOW FUNCTIONS LIKE 'gbx_bng_*';

-- List VectorX functions
SHOW FUNCTIONS LIKE 'gbx_st_*';
```

## Describing Functions

```sql
-- Get function details
DESCRIBE FUNCTION gbx_rst_boundingbox;

-- Get extended information
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;
```

## RasterX SQL Functions

### Reading and Querying Rasters

```sql
-- Read rasters
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`/data/rasters`;

-- Extract metadata
SELECT
    path,
    gbx_rst_boundingbox(tile) as bbox,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height,
    gbx_rst_numbands(tile) as num_bands,
    gbx_rst_metadata(tile) as metadata
FROM rasters;
```

### Filtering Rasters

```sql
-- Filter by dimensions
SELECT *
FROM rasters
WHERE gbx_rst_width(tile) > 1000
  AND gbx_rst_height(tile) > 1000;

-- Filter by band count
SELECT *
FROM rasters
WHERE gbx_rst_numbands(tile) >= 3;
```

### Raster Transformations

```sql
-- Clip raster
SELECT
    path,
    gbx_rst_clip(
        tile,
        st_geomfromtext('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')
    ) as clipped_tile
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
FROM rasters;
```

## GridX SQL Functions (BNG)

### Cell Operations

```sql
-- Calculate cell area
SELECT gbx_bng_cellarea('TQ', 1000) as area_sqm;

-- Different precisions
SELECT
    'TQ' as grid,
    gbx_bng_cellarea('TQ', 10000) as area_10km,
    gbx_bng_cellarea('TQ', 1000) as area_1km,
    gbx_bng_cellarea('TQ', 100) as area_100m;
```

### Point to Cell Conversion

```sql
-- Convert points to BNG cells
CREATE OR REPLACE TEMP VIEW uk_points_bng AS
SELECT
    location_id,
    latitude,
    longitude,
    gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell_1km,
    gbx_bng_pointtocell(st_point(longitude, latitude), 100) as bng_cell_100m
FROM uk_locations;

SELECT * FROM uk_points_bng;
```

### Spatial Aggregation

```sql
-- Aggregate by BNG cell
CREATE OR REPLACE TABLE bng_aggregated AS
SELECT
    gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
    COUNT(*) as point_count,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp,
    MIN(temperature) as min_temp
FROM weather_stations
WHERE country = 'GB'
GROUP BY bng_cell;

SELECT * FROM bng_aggregated ORDER BY point_count DESC LIMIT 10;
```

### Multi-Resolution Analysis

```sql
-- Analyze at multiple resolutions
CREATE OR REPLACE VIEW multi_resolution AS
SELECT
    location_id,
    gbx_bng_pointtocell(location, 10000) as bng_10km,
    gbx_bng_pointtocell(location, 1000) as bng_1km,
    gbx_bng_pointtocell(location, 100) as bng_100m
FROM locations;

-- Count by resolution
SELECT '10km' as resolution, COUNT(DISTINCT bng_10km) as cell_count FROM multi_resolution
UNION ALL
SELECT '1km', COUNT(DISTINCT bng_1km) FROM multi_resolution
UNION ALL
SELECT '100m', COUNT(DISTINCT bng_100m) FROM multi_resolution;
```

## VectorX SQL Functions

### Legacy Geometry Conversion

```sql
-- Convert legacy Mosaic geometries
CREATE OR REPLACE TEMP VIEW converted_geometries AS
SELECT
    feature_id,
    properties,
    gbx_st_legacyaswkb(mosaic_geom) as wkb_geom,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_mosaic_table;

SELECT * FROM converted_geometries;
```

### Migration Workflow

```sql
-- Full table migration
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
FROM migrated_features;
```

### Using with Databricks Spatial Functions

```sql
-- Convert and analyze
CREATE OR REPLACE VIEW features_analyzed AS
SELECT
    feature_id,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry,
    st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as area,
    st_length(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as perimeter,
    st_centroid(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as centroid
FROM legacy_features;

SELECT * FROM features_analyzed WHERE area > 1000;
```

## Reading Data with SQL

### Shapefile

```sql
-- Read shapefile
CREATE OR REPLACE TEMP VIEW shapes AS
SELECT * FROM shapefile.`/data/boundaries.shp`;

-- Convert geometry
CREATE OR REPLACE VIEW shapes_geom AS
SELECT
    *,
    st_geomfromwkb(geom_0) as geometry
FROM shapes;

SELECT
    name,
    st_area(geometry) as area,
    st_centroid(geometry) as center
FROM shapes_geom;
```

### GeoJSON

```sql
-- Read GeoJSON
CREATE OR REPLACE TEMP VIEW features AS
SELECT * FROM geojson.`/data/features.geojson`;

-- Query with geometry
SELECT
    name,
    type,
    st_area(st_geomfromwkb(geom_0)) as area
FROM features
WHERE st_area(st_geomfromwkb(geom_0)) > 1000;
```

### GeoPackage

```sql
-- Read GeoPackage (note: specify layer in Python/Scala first)
-- gpkg = spark.read.format("gpkg").option("layerName", "buildings").load("/data/city.gpkg")
-- gpkg.createOrReplaceTempView("buildings")

SELECT
    building_id,
    name,
    st_area(st_geomfromwkb(shape)) as floor_area,
    st_centroid(st_geomfromwkb(shape)) as center_point
FROM buildings
WHERE st_area(st_geomfromwkb(shape)) > 500;
```

## Complex Queries

### Spatial Join

```sql
-- Join based on spatial relationship
SELECT
    p.parcel_id,
    p.owner,
    z.zone_name,
    z.zone_type
FROM
    (SELECT *, st_geomfromwkb(geom_0) as geometry FROM parcels) p
JOIN
    (SELECT *, st_geomfromwkb(geom_0) as geometry FROM zones) z
    ON st_contains(z.geometry, st_centroid(p.geometry));
```

### Spatial Aggregation

```sql
-- Aggregate by zone
SELECT
    z.zone_name,
    COUNT(p.parcel_id) as parcel_count,
    SUM(st_area(p.geometry)) as total_area
FROM
    (SELECT *, st_geomfromwkb(geom_0) as geometry FROM parcels) p
JOIN
    (SELECT *, st_geomfromwkb(geom_0) as geometry FROM zones) z
    ON st_contains(z.geometry, p.geometry)
GROUP BY z.zone_name;
```

### Multi-Source Analysis

```sql
-- Combine raster and vector
WITH raster_catalog AS (
    SELECT
        path,
        gbx_rst_boundingbox(tile) as raster_bounds
    FROM gdal.`/data/rasters`
),
vector_features AS (
    SELECT
        feature_id,
        st_geomfromwkb(geom_0) as geometry
    FROM shapefile.`/data/features.shp`
)
SELECT
    r.path,
    v.feature_id,
    st_intersects(v.geometry, r.raster_bounds) as intersects
FROM raster_catalog r
CROSS JOIN vector_features v
WHERE st_intersects(v.geometry, r.raster_bounds);
```

## Creating Tables and Views

### Permanent Tables

```sql
-- Create catalog table
CREATE OR REPLACE TABLE catalog.schema.raster_metadata AS
SELECT
    path,
    gbx_rst_boundingbox(tile) as bounds,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height,
    gbx_rst_numbands(tile) as bands
FROM gdal.`/data/rasters`;

-- Query table
SELECT * FROM catalog.schema.raster_metadata
WHERE width > 1000;
```

### Temporary Views

```sql
-- Create temp view
CREATE OR REPLACE TEMP VIEW processed_features AS
SELECT
    feature_id,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry,
    st_area(st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))) as area
FROM legacy_data;

-- Use in session
SELECT * FROM processed_features WHERE area > 5000;
```

## Performance Tips

### Filter Early

```sql
-- Good: Filter before processing
SELECT
    path,
    gbx_rst_metadata(tile) as metadata
FROM rasters
WHERE path LIKE '%2024%';

-- Less efficient: Process then filter
-- SELECT * FROM (
--     SELECT path, gbx_rst_metadata(tile) as metadata
--     FROM rasters
-- ) WHERE path LIKE '%2024%';
```

### Use Views for Reusability

```sql
-- Create view once
CREATE OR REPLACE VIEW raster_catalog AS
SELECT
    path,
    gbx_rst_boundingbox(tile) as bounds,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height
FROM gdal.`/data/rasters`;

-- Query multiple times
SELECT * FROM raster_catalog WHERE width > 1000;
SELECT * FROM raster_catalog WHERE height > 1000;
```

### Partition Tables

```sql
-- Create partitioned table
CREATE TABLE catalog.schema.features
PARTITIONED BY (region)
AS
SELECT
    *,
    st_geomfromwkb(geom_0) as geometry,
    region
FROM shapefile.`/data/features.shp`;

-- Efficient regional queries
SELECT * FROM catalog.schema.features
WHERE region = 'northeast';
```

## Next Steps

- [Python API Reference](./python.md)
- [Scala API Reference](./scala.md)
- [Examples](../examples/overview.md)
- [Package Documentation](../packages/overview.md)

