---
sidebar_position: 4
---

# Shapefile Reader

The Shapefile reader provides support for reading ESRI Shapefile format, one of the most common geospatial vector data formats.

![Shapefile Reader](../../../resources/images/readers/shapefile_reader.png)

## Format Name

`shapefile`

## Overview

This is a named OGR Reader that sets `driverName` to "[ESRI Shapefile](https://gdal.org/en/stable/drivers/vector/shapefile.html)". It provides convenient access to shapefile data with optimized defaults.

## Supported File Types

GDAL auto-associates the following extensions:

- **`.shp`** - Standard shapefile (requires .shx, .dbf side-car files)
- **`.shz`** - ZIP files containing all components of a single layer
- **`.shp.zip`** - ZIP files containing one or several layers
- **`.zip`** - With the named reader, generic ZIP files with shapefiles

## Basic Usage

```python
# Read shapefile(s)
df = spark.read.format("shapefile").load("/path/to/shapefiles")

df.show()
```

### Scala

```scala
// Read shapefile(s)
val df = spark.read.format("shapefile").load("/path/to/shapefiles")

df.show()
```

### SQL

```sql
-- Read shapefiles
CREATE OR REPLACE TEMP VIEW shapes AS
SELECT * FROM shapefile.`/path/to/shapefiles`;

SELECT * FROM shapes;
```

## Output Schema

The output maintains attribute columns and adds three columns for geometry:

```
root
 |-- geom_0: binary (geometry in WKB format)
 |-- geom_0_srid: integer (spatial reference ID)
 |-- geom_0_srid_proj: string (projection definition)
 |-- <attribute_columns>: various types
```

### Example Schema

```python
df = spark.read.format("shapefile").load("/data/sample.shp")
df.printSchema()

# Output:
# root
#  |-- geom_0: binary (nullable = true)
#  |-- geom_0_srid: integer (nullable = true)
#  |-- geom_0_srid_proj: string (nullable = true)
#  |-- ID: long (nullable = true)
#  |-- NAME: string (nullable = true)
#  |-- POPULATION: long (nullable = true)
```

## Options

All [OGR reader options](./ogr.md#options) are available:

- `chunkSize` - Records per chunk (default: "10000")
- `asWKB` - Output as WKB vs WKT (default: "true")

```python
# Adjust chunk size for performance
df = spark.read.format("shapefile") \
    .option("chunkSize", "50000") \
    .load("/path/to/large/shapefile")
```

## Usage Examples

### Example 1: Read Single Shapefile

```python
# Read a single shapefile
buildings = spark.read.format("shapefile").load("/data/buildings.shp")

# Show attributes
buildings.select("ID", "NAME", "HEIGHT", "geom_0_srid").show()
```

### Example 2: Read Directory of Shapefiles

```python
# Read all shapefiles in a directory
all_shapes = spark.read.format("shapefile").load("/data/vector/")

# Show distinct file sources
all_shapes.select("geom_0_srid").distinct().show()
```

### Example 3: Read Zipped Shapefiles

```python
# Read from ZIP files
zipped = spark.read.format("shapefile").load("/data/shapes.zip")

# Or directory of ZIP files
multi_zipped = spark.read.format("shapefile").load("/data/zipped_shapefiles/")

zipped.show()
```

### Example 4: Convert to Databricks GEOMETRY

```python
from pyspark.sql.functions import expr

# Read shapefile
df = spark.read.format("shapefile").load("/data/boundaries.shp")

# Convert to GEOMETRY type
geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Use Databricks ST functions
result = geometry_df.select(
    "NAME",
    "geometry",
    expr("st_area(geometry)").alias("area_sqm"),
    expr("st_length(geometry)").alias("perimeter_m"),
    expr("st_centroid(geometry)").alias("center_point")
)

result.show()
```

### Example 5: Spatial Query

```python
from pyspark.sql.functions import expr

# Read shapefiles
parcels = spark.read.format("shapefile").load("/data/parcels.shp")
zones = spark.read.format("shapefile").load("/data/zones.shp")

# Convert to GEOMETRY
parcels_geom = parcels.select(
    "parcel_id",
    expr("st_geomfromwkb(geom_0)").alias("parcel_geom")
)

zones_geom = zones.select(
    "zone_name",
    expr("st_geomfromwkb(geom_0)").alias("zone_geom")
)

# Spatial join
result = parcels_geom.join(
    zones_geom,
    expr("st_intersects(parcel_geom, zone_geom)")
)

result.select("parcel_id", "zone_name").show()
```

## SQL Examples

### Read and Query

```sql
-- Create view from shapefile
CREATE OR REPLACE TEMP VIEW buildings AS
SELECT
    *,
    st_geomfromwkb(geom_0) as geometry
FROM shapefile.`/data/buildings.shp`;

-- Query with spatial functions
SELECT
    building_id,
    building_name,
    st_area(geometry) as floor_area,
    st_centroid(geometry) as center_point
FROM buildings
WHERE st_area(geometry) > 1000;
```

### Spatial Join in SQL

```sql
-- Read both shapefiles
CREATE OR REPLACE TEMP VIEW properties AS
SELECT *, st_geomfromwkb(geom_0) as geometry
FROM shapefile.`/data/properties.shp`;

CREATE OR REPLACE TEMP VIEW flood_zones AS
SELECT *, st_geomfromwkb(geom_0) as geometry
FROM shapefile.`/data/flood_zones.shp`;

-- Find properties in flood zones
SELECT
    p.property_id,
    p.address,
    f.zone_level,
    f.risk_category
FROM properties p
JOIN flood_zones f
    ON st_intersects(p.geometry, f.geometry);
```

## Working with Projections

### Check Projection

```python
# Read shapefile
df = spark.read.format("shapefile").load("/data/parcels.shp")

# Check SRID and projection
df.select("geom_0_srid", "geom_0_srid_proj").distinct().show(truncate=False)
```

### Reproject Data

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import expr
rx.register(spark)

# Read shapefile
df = spark.read.format("shapefile").load("/data/state_plane.shp")

# Convert to GEOMETRY and reproject
reprojected = df.select(
    "*",
    expr("st_transform(st_geomfromwkb(geom_0), 'EPSG:' || geom_0_srid, 'EPSG:4326')").alias("wgs84_geom")
)

reprojected.show()
```

## Common Workflows

### Workflow 1: Shapefile to Delta Table

```python
from pyspark.sql.functions import expr

# Read shapefile
shapefile_df = spark.read.format("shapefile").load("/data/source.shp")

# Convert to GEOMETRY type
delta_df = shapefile_df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
).drop("geom_0", "geom_0_srid", "geom_0_srid_proj")

# Write to Delta Lake
delta_df.write.mode("overwrite").saveAsTable("catalog.schema.spatial_table")

# Optimize with Z-ordering
spark.sql("""
    OPTIMIZE catalog.schema.spatial_table
    ZORDER BY (geometry)
""")
```

### Workflow 2: Multi-File Processing

```python
from pyspark.sql.functions import input_file_name, expr

# Read all shapefiles in directory
all_files = spark.read.format("shapefile").load("/data/shapefiles/*.shp")

# Add source filename
with_source = all_files.withColumn("source_file", input_file_name())

# Process each file's features
processed = with_source.select(
    "source_file",
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

processed.show()
```

### Workflow 3: Attribute Filtering

```python
# Read shapefile
buildings = spark.read.format("shapefile").load("/data/buildings.shp")

# Filter by attributes
high_rise = buildings.filter("HEIGHT > 100")
commercial = buildings.filter("USE_TYPE = 'Commercial'")

# Save filtered results
high_rise.write.mode("overwrite").saveAsTable("high_rise_buildings")
commercial.write.mode("overwrite").saveAsTable("commercial_buildings")
```

## Performance Tips

### 1. Adjust Chunk Size

```python
# For large shapefiles
large_df = spark.read.format("shapefile") \
    .option("chunkSize", "100000") \
    .load("/data/large_shapefile.shp")
```

### 2. Partition Output

```python
# Partition by attribute for better query performance
df = spark.read.format("shapefile").load("/data/parcels.shp")

df.write.partitionBy("county_code").saveAsTable("parcels_by_county")
```

### 3. Cache Frequently Used Data

```python
# Cache converted geometries
shapes = spark.read.format("shapefile").load("/data/boundaries.shp")
shapes_cached = shapes.cache()

# Query multiple times
result1 = shapes_cached.filter("AREA > 1000")
result2 = shapes_cached.filter("TYPE = 'Park'")
```

## Troubleshooting

### Issue: Missing Side-Car Files

Shapefiles require .shx and .dbf files. Ensure all files are present:

```python
# Check files
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)
dbutils.fs.ls("/data/shapefile_folder/")
```

### Issue: Encoding Problems

Shapefiles may have encoding issues with special characters:

```python
# Read with encoding awareness
df = spark.read.format("shapefile").load("/data/international.shp")

# Check for encoding issues in attributes
df.select("NAME").show(truncate=False)
```

### Issue: Large File Performance

```python
# Split large shapefile reading
df = spark.read.format("shapefile") \
    .option("chunkSize", "10000") \
    .load("/data/large.shp")

# Repartition and cache
df.repartition(100).cache()
```

## Next Steps

- [Learn about VectorX](../packages/vectorx.md)
- [View API Reference](../api/overview.md)
- [Check Examples](../examples/overview.md)
- [Other Readers](./overview.md)

