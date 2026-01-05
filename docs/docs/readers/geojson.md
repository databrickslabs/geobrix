---
sidebar_position: 5
---

# GeoJSON Reader

The GeoJSON reader provides support for reading GeoJSON and GeoJSONSeq (newline-delimited GeoJSON) formats.

![GeoJSON Reader](../../../resources/images/readers/geojson_reader.png)

## Format Name

`geojson`

## Overview

This is a named OGR Reader that intelligently switches between [GeoJSON](https://gdal.org/en/stable/drivers/vector/geojson.html) and [GeoJSONSeq](https://gdal.org/en/stable/drivers/vector/geojsonseq.html) drivers based on the `multi` option.

## Supported Formats

- **GeoJSON** (.geojson, .json) - Standard GeoJSON format
- **GeoJSONSeq** (.geojsonl, .geojsons) - Newline-delimited GeoJSON (default)

## Basic Usage

```python
# Read GeoJSON files (uses GeoJSONSeq by default)
df = spark.read.format("geojson").load("/path/to/geojson")

df.show()
```

### Scala

```scala
// Read GeoJSON files
val df = spark.read.format("geojson").load("/path/to/geojson")

df.show()
```

### SQL

```sql
-- Read GeoJSON files
CREATE OR REPLACE TEMP VIEW features AS
SELECT * FROM geojson.`/path/to/geojson`;

SELECT * FROM features;
```

## Options

### `multi`

**Default:** `"true"`

Controls which GeoJSON driver to use:
- `"true"` → Uses [GeoJSONSeq](https://gdal.org/en/stable/drivers/vector/geojsonseq.html) driver (newline-delimited, better for large files)
- `"false"` → Uses [GeoJSON](https://gdal.org/en/stable/drivers/vector/geojson.html) driver (standard format)

```python
# Read standard GeoJSON
df = spark.read.format("geojson") \
    .option("multi", "false") \
    .load("/path/to/standard.geojson")

# Read GeoJSONSeq (default)
df = spark.read.format("geojson") \
    .option("multi", "true") \
    .load("/path/to/features.geojsonl")

# Or simply (multi=true is default)
df = spark.read.format("geojson").load("/path/to/features.geojsonl")
```

### Other Options

All [OGR reader options](./ogr.md#options) are available:

- `chunkSize` - Records per chunk (default: "10000")
- `asWKB` - Output as WKB vs WKT (default: "true")

## Output Schema

The output maintains attribute columns and adds three columns for geometry:

```
root
 |-- geom_0: binary (geometry in WKB format)
 |-- geom_0_srid: integer (spatial reference ID)
 |-- geom_0_srid_proj: string (projection definition)
 |-- <properties>: various types (GeoJSON properties)
```

### Example Schema

```python
df = spark.read.format("geojson").load("/data/sample.geojson")
df.printSchema()

# Output:
# root
#  |-- geom_0: binary (nullable = true)
#  |-- geom_0_srid: integer (nullable = true)
#  |-- geom_0_srid_proj: string (nullable = true)
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- type: string (nullable = true)
```

## Usage Examples

### Example 1: Read Standard GeoJSON

```python
# Read a FeatureCollection GeoJSON file
features = spark.read.format("geojson") \
    .option("multi", "false") \
    .load("/data/cities.geojson")

features.select("name", "population", "geom_0_srid").show()
```

### Example 2: Read GeoJSONSeq (Newline-Delimited)

```python
# Read newline-delimited GeoJSON (better for large files)
features = spark.read.format("geojson").load("/data/features.geojsonl")

print(f"Loaded {features.count()} features")
```

### Example 3: Convert to Databricks GEOMETRY

```python
from pyspark.sql.functions import expr

# Read GeoJSON
df = spark.read.format("geojson").load("/data/boundaries.geojson")

# Convert to GEOMETRY type
geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Use Databricks ST functions
result = geometry_df.select(
    "name",
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_centroid(geometry)").alias("center")
)

result.show()
```

### Example 4: Read from API Response

```python
# If you've saved API response as GeoJSON
api_data = spark.read.format("geojson") \
    .option("multi", "false") \
    .load("/data/api_response.geojson")

api_data.show()
```

### Example 5: Read Directory of GeoJSON Files

```python
# Read all GeoJSON files in a directory
all_features = spark.read.format("geojson").load("/data/geojson_files/")

# Check distinct feature types
all_features.groupBy("type").count().show()
```

## SQL Examples

### Read and Query

```sql
-- Create view from GeoJSON
CREATE OR REPLACE TEMP VIEW places AS
SELECT
    *,
    st_geomfromwkb(geom_0) as geometry
FROM geojson.`/data/places.geojson`;

-- Query with spatial functions
SELECT
    name,
    category,
    st_area(geometry) as area,
    st_x(st_centroid(geometry)) as longitude,
    st_y(st_centroid(geometry)) as latitude
FROM places
WHERE category = 'park';
```

### Spatial Join

```sql
-- Read GeoJSON files
CREATE OR REPLACE TEMP VIEW points AS
SELECT *, st_geomfromwkb(geom_0) as geometry
FROM geojson.`/data/points.geojson`;

CREATE OR REPLACE TEMP VIEW polygons AS
SELECT *, st_geomfromwkb(geom_0) as geometry
FROM geojson.`/data/polygons.geojson`;

-- Spatial join
SELECT
    pt.name as point_name,
    poly.name as polygon_name
FROM points pt
JOIN polygons poly
    ON st_contains(poly.geometry, pt.geometry);
```

## GeoJSON vs GeoJSONSeq

### Standard GeoJSON

**Format:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [0, 0] },
      "properties": { "name": "Feature 1" }
    },
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [1, 1] },
      "properties": { "name": "Feature 2" }
    }
  ]
}
```

**Usage:**
```python
df = spark.read.format("geojson").option("multi", "false").load("/data/standard.geojson")
```

**Best for:**
- Small to medium files
- API responses
- Standard GeoJSON files

### GeoJSONSeq (Newline-Delimited)

**Format:**
```json
{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{"name":"Feature 1"}}
{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":{"name":"Feature 2"}}
```

**Usage:**
```python
df = spark.read.format("geojson").load("/data/features.geojsonl")
# or explicitly:
df = spark.read.format("geojson").option("multi", "true").load("/data/features.geojsonl")
```

**Best for:**
- Large files
- Streaming data
- Parallel processing
- Better Spark performance

## Common Workflows

### Workflow 1: GeoJSON to Delta Table

```python
from pyspark.sql.functions import expr

# Read GeoJSON
geojson_df = spark.read.format("geojson").load("/data/source.geojson")

# Convert to GEOMETRY type
delta_df = geojson_df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
).drop("geom_0", "geom_0_srid", "geom_0_srid_proj")

# Write to Delta Lake
delta_df.write.mode("overwrite").saveAsTable("catalog.schema.features")
```

### Workflow 2: Convert Shapefile to GeoJSON

```python
from pyspark.sql.functions import expr, to_json, struct

# Read shapefile
shapefile_df = spark.read.format("shapefile").load("/data/input.shp")

# Convert to GeoJSON structure
geojson_df = shapefile_df.select(
    to_json(struct(
        expr("'Feature'").alias("type"),
        expr("st_asgeojson(st_geomfromwkb(geom_0))").alias("geometry"),
        struct("*").alias("properties")
    )).alias("feature")
)

# Write as newline-delimited GeoJSON
geojson_df.write.mode("overwrite").text("/data/output.geojsonl")
```

### Workflow 3: Filter and Export

```python
from pyspark.sql.functions import expr

# Read GeoJSON
features = spark.read.format("geojson").load("/data/all_features.geojson")

# Convert to GEOMETRY
with_geom = features.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Filter by spatial criteria
filtered = with_geom.filter(
    expr("st_area(geometry) > 1000")
)

# Save to Delta
filtered.write.mode("overwrite").saveAsTable("large_features")
```

### Workflow 4: Aggregate GeoJSON Files

```python
from pyspark.sql.functions import input_file_name

# Read multiple GeoJSON files
all_files = spark.read.format("geojson").load("/data/geojson/*.geojson")

# Add source file tracking
with_source = all_files.withColumn("source", input_file_name())

# Aggregate by source
summary = with_source.groupBy("source").count()
summary.show()
```

## Performance Tips

### 1. Use GeoJSONSeq for Large Files

```python
# For large files, use GeoJSONSeq (default)
large_df = spark.read.format("geojson").load("/data/large.geojsonl")

# Better performance than standard GeoJSON
```

### 2. Adjust Chunk Size

```python
# For files with many features
df = spark.read.format("geojson") \
    .option("chunkSize", "50000") \
    .load("/data/many_features.geojsonl")
```

### 3. Partition Output

```python
# Partition by attribute
df = spark.read.format("geojson").load("/data/features.geojson")

df.write.partitionBy("category").saveAsTable("features_by_category")
```

## Troubleshooting

### Issue: Parsing Errors

```python
# Try specifying the format explicitly
df = spark.read.format("geojson") \
    .option("multi", "false") \
    .load("/data/problematic.geojson")
```

### Issue: Large Single File

```python
# Convert to newline-delimited for better performance
# Use standard tools to convert:
# jq -c '.features[]' input.geojson > output.geojsonl

# Then read with GeoBrix
df = spark.read.format("geojson").load("/data/output.geojsonl")
```

### Issue: Missing Properties

```python
# Check schema
df = spark.read.format("geojson").load("/data/features.geojson")
df.printSchema()

# Some properties may be nested
df.select("properties.*").show()
```

## Converting Between Formats

### To GeoJSON from Other Formats

```python
from pyspark.sql.functions import expr

# Read any format
df = spark.read.format("shapefile").load("/data/input.shp")

# Convert geometry to GeoJSON
geojson_geom = df.select(
    "*",
    expr("st_asgeojson(st_geomfromwkb(geom_0))").alias("geometry_json")
)

geojson_geom.show()
```

## Next Steps

- [Learn about VectorX](../packages/vectorx.md)
- [View API Reference](../api/overview.md)
- [Other Readers](./overview.md)
- [Examples](../examples/overview.md)

