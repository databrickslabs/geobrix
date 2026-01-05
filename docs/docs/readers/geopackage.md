---
sidebar_position: 6
---

# GeoPackage Reader

The GeoPackage reader provides support for reading OGC GeoPackage format, a modern SQLite-based geospatial format.

![GeoPackage Reader](../../../resources/images/readers/gpkg_reader.png)

## Format Name

`gpkg`

## Overview

This is a named OGR Reader that sets `driverName` to "[GPKG](https://gdal.org/en/stable/drivers/vector/gpkg.html)". GeoPackage is an open, standards-based, platform-independent, portable, self-describing format for transferring geospatial information.

## Key Features

- **Self-contained**: Single-file SQLite database
- **Multi-layer**: Can contain multiple vector layers and raster tiles
- **Attributes**: Full attribute support with data types
- **Spatial Index**: Built-in spatial indexing
- **Portable**: Cross-platform compatibility

## Basic Usage

```python
# Read GeoPackage
df = spark.read.format("gpkg").load("/path/to/file.gpkg")

df.show()
```

### Scala

```scala
// Read GeoPackage
val df = spark.read.format("gpkg").load("/path/to/file.gpkg")

df.show()
```

### SQL

```sql
-- Read GeoPackage
CREATE OR REPLACE TEMP VIEW features AS
SELECT * FROM gpkg.`/path/to/file.gpkg`;

SELECT * FROM features;
```

## Output Schema

The output maintains attribute columns and adds geometry columns. Note that GeoPackage may use different column names (often `shape` instead of `geom_0`):

```
root
 |-- shape: binary (geometry in WKB format)
 |-- shape_srid: integer (spatial reference ID)
 |-- shape_srid_proj: string (projection definition)
 |-- <attribute_columns>: various types
```

## Options

### Multi-Layer Support

GeoPackage files can contain multiple layers. Use these options to specify which layer to read:

```python
# Read specific layer by name
df = spark.read.format("gpkg") \
    .option("layerName", "buildings") \
    .load("/path/to/data.gpkg")

# Read specific layer by index (0-based)
df = spark.read.format("gpkg") \
    .option("layerN", "1") \
    .load("/path/to/data.gpkg")
```

### Other Options

All [OGR reader options](./ogr.md#options) are available:

- `chunkSize` - Records per chunk (default: "10000")
- `asWKB` - Output as WKB vs WKT (default: "true")
- `layerName` - Specific layer to read
- `layerN` - Layer index to read (0-based)

## Usage Examples

### Example 1: Read Single Layer GeoPackage

```python
# Read GeoPackage (reads first/default layer)
buildings = spark.read.format("gpkg").load("/data/city.gpkg")

# Show attributes (note: geometry column may be named 'shape')
buildings.select("building_id", "name", "height", "shape_srid").show()
```

### Example 2: Read Specific Layer

```python
# GeoPackage with multiple layers
# Read buildings layer
buildings = spark.read.format("gpkg") \
    .option("layerName", "buildings") \
    .load("/data/city.gpkg")

# Read roads layer
roads = spark.read.format("gpkg") \
    .option("layerName", "roads") \
    .load("/data/city.gpkg")

# Read parcels layer
parcels = spark.read.format("gpkg") \
    .option("layerName", "parcels") \
    .load("/data/city.gpkg")

buildings.show()
roads.show()
parcels.show()
```

### Example 3: Convert to Databricks GEOMETRY

```python
from pyspark.sql.functions import expr

# Read GeoPackage
df = spark.read.format("gpkg").load("/data/boundaries.gpkg")

# Convert to GEOMETRY type (check actual column name: might be 'shape' or 'geom_0')
geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(shape)").alias("geometry")
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

### Example 4: Read from Cloud Storage

```python
# Read from S3
s3_gpkg = spark.read.format("gpkg").load("s3://bucket/path/data.gpkg")

# Read from Azure Blob Storage
azure_gpkg = spark.read.format("gpkg") \
    .load("wasbs://container@account.blob.core.windows.net/data.gpkg")

# Read from Unity Catalog Volume
volume_gpkg = spark.read.format("gpkg") \
    .load("/Volumes/catalog/schema/volume/data.gpkg")

s3_gpkg.show()
```

### Example 5: Read Multiple GeoPackages

```python
# Read multiple GeoPackage files
all_data = spark.read.format("gpkg").load("/data/geopackages/*.gpkg")

# Show count from each file
from pyspark.sql.functions import input_file_name

with_source = all_data.withColumn("source", input_file_name())
with_source.groupBy("source").count().show(truncate=False)
```

## SQL Examples

### Read and Query

```sql
-- Create view from GeoPackage
CREATE OR REPLACE TEMP VIEW parcels AS
SELECT
    *,
    st_geomfromwkb(shape) as geometry
FROM gpkg.`/data/parcels.gpkg`;

-- Query with spatial functions
SELECT
    parcel_id,
    owner,
    st_area(geometry) as area_sqm,
    st_perimeter(geometry) as perimeter_m
FROM parcels
WHERE st_area(geometry) > 5000;
```

### Multi-Layer Query

```sql
-- Read different layers from same GeoPackage
-- Note: You need to specify layer in Python/Scala first, then register as views

-- In Python first:
-- buildings = spark.read.format("gpkg").option("layerName", "buildings").load("/data/city.gpkg")
-- buildings.createOrReplaceTempView("buildings")
-- 
-- roads = spark.read.format("gpkg").option("layerName", "roads").load("/data/city.gpkg")
-- roads.createOrReplaceTempView("roads")

-- Then in SQL:
SELECT
    b.building_name,
    r.road_name,
    st_distance(
        st_geomfromwkb(b.shape),
        st_geomfromwkb(r.shape)
    ) as distance_m
FROM buildings b
CROSS JOIN roads r
WHERE st_distance(
    st_geomfromwkb(b.shape),
    st_geomfromwkb(r.shape)
) < 100;
```

## Working with Multiple Layers

### List Layers in GeoPackage

```python
# Use GDAL/OGR command line tools or Python GDAL bindings
# In notebook:
import subprocess

result = subprocess.run(
    ['ogrinfo', '-al', '-so', '/path/to/data.gpkg'],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### Read All Layers

```python
# Define layer names (you need to know them beforehand or query with ogrinfo)
layer_names = ["buildings", "roads", "parcels", "zones"]

# Read each layer
layers = {}
for layer_name in layer_names:
    layers[layer_name] = spark.read.format("gpkg") \
        .option("layerName", layer_name) \
        .load("/data/city.gpkg")

# Access each layer
buildings_df = layers["buildings"]
roads_df = layers["roads"]
```

## Common Workflows

### Workflow 1: GeoPackage to Delta Lake

```python
from pyspark.sql.functions import expr

# Read GeoPackage layer
gpkg_df = spark.read.format("gpkg") \
    .option("layerName", "features") \
    .load("/data/source.gpkg")

# Convert to GEOMETRY type
delta_df = gpkg_df.select(
    "*",
    expr("st_geomfromwkb(shape)").alias("geometry")
).drop("shape", "shape_srid", "shape_srid_proj")

# Write to Delta Lake
delta_df.write.mode("overwrite").saveAsTable("catalog.schema.features")

# Optimize
spark.sql("""
    OPTIMIZE catalog.schema.features
    ZORDER BY (geometry)
""")
```

### Workflow 2: Multi-Layer Processing

```python
from pyspark.sql.functions import expr, lit

# Read multiple layers and combine
buildings = spark.read.format("gpkg") \
    .option("layerName", "buildings") \
    .load("/data/city.gpkg") \
    .withColumn("layer_type", lit("building"))

roads = spark.read.format("gpkg") \
    .option("layerName", "roads") \
    .load("/data/city.gpkg") \
    .withColumn("layer_type", lit("road"))

# Standardize schema and union
from pyspark.sql.functions import col

buildings_std = buildings.select(
    col("building_id").alias("feature_id"),
    col("name"),
    col("layer_type"),
    expr("st_geomfromwkb(shape)").alias("geometry")
)

roads_std = roads.select(
    col("road_id").alias("feature_id"),
    col("name"),
    col("layer_type"),
    expr("st_geomfromwkb(shape)").alias("geometry")
)

# Combine
all_features = buildings_std.union(roads_std)
all_features.show()
```

### Workflow 3: Spatial Analysis

```python
from pyspark.sql.functions import expr

# Read GeoPackage
parcels = spark.read.format("gpkg") \
    .option("layerName", "parcels") \
    .load("/data/city.gpkg")

# Add geometry and spatial attributes
analyzed = parcels.select(
    "*",
    expr("st_geomfromwkb(shape)").alias("geometry")
).select(
    "parcel_id",
    "owner",
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_perimeter(geometry)").alias("perimeter"),
    expr("st_centroid(geometry)").alias("centroid"),
    expr("st_envelope(geometry)").alias("bbox")
)

# Save results
analyzed.write.mode("overwrite").saveAsTable("parcel_analysis")
```

## Performance Tips

### 1. Read Specific Layers

```python
# Don't read default layer if you need a specific one
df = spark.read.format("gpkg") \
    .option("layerName", "specific_layer") \
    .load("/data/large.gpkg")
```

### 2. Adjust Chunk Size

```python
# For large layers
df = spark.read.format("gpkg") \
    .option("layerName", "large_layer") \
    .option("chunkSize", "50000") \
    .load("/data/data.gpkg")
```

### 3. Cache Frequently Used Layers

```python
# Cache layer data
layer_df = spark.read.format("gpkg") \
    .option("layerName", "important_layer") \
    .load("/data/data.gpkg")

layer_df.cache()
```

## Advantages of GeoPackage

1. **Single File**: No side-car files to manage
2. **Multiple Layers**: Store related datasets together
3. **Full Data Types**: Rich attribute type support
4. **Spatial Index**: Built-in R-tree spatial index
5. **Standards-Based**: OGC standard format
6. **Modern**: Better than Shapefile for most use cases

## Troubleshooting

### Issue: Layer Not Found

```python
# Check layer name spelling and case
df = spark.read.format("gpkg") \
    .option("layerName", "Buildings") \
    .load("/data/city.gpkg")
```

### Issue: Geometry Column Name

```python
# GeoPackage may use 'shape', 'geom', or 'geometry' as column name
df = spark.read.format("gpkg").load("/data/file.gpkg")
df.columns  # Check actual column names

# Adjust accordingly
from pyspark.sql.functions import expr
geometry_df = df.select("*", expr("st_geomfromwkb(shape)").alias("geometry"))
```

### Issue: Large File Performance

```python
# Increase chunk size and repartition
df = spark.read.format("gpkg") \
    .option("chunkSize", "100000") \
    .load("/data/large.gpkg")

df = df.repartition(100)
```

## Next Steps

- [Learn about VectorX](../packages/vectorx.md)
- [View API Reference](../api/overview.md)
- [Other Readers](./overview.md)
- [Examples](../examples/overview.md)

