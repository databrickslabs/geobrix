---
sidebar_position: 1
---

# Readers Overview

GeoBrix provides automatic Spark readers for various geospatial file formats. These readers are automatically registered when the GeoBrix JAR is on the classpath.

## Available Readers

GeoBrix supports reading both raster and vector geospatial formats through GDAL/OGR:

### Raster Readers

| Format | Reader Name | Description |
|--------|-------------|-------------|
| **GDAL** | `gdal` | Generic GDAL raster reader supporting GeoTIFF and other formats |

### Vector Readers

| Format | Reader Name | Description |
|--------|-------------|-------------|
| **OGR** | `ogr` | Generic OGR vector reader for various formats |
| **Shapefile** | `shapefile` | ESRI Shapefile format (.shp, .shz, .shp.zip) |
| **GeoJSON** | `geojson` | GeoJSON and GeoJSONSeq formats |
| **GeoPackage** | `gpkg` | OGC GeoPackage format (.gpkg) |
| **File GeoDatabase** | `file_gdb` | ESRI File Geodatabase format |

## Common Features

All GeoBrix readers share common characteristics:

### Automatic Registration

Readers are automatically registered when the JAR is loaded:

```python
# No explicit registration needed
df = spark.read.format("shapefile").load("/path/to/files")
```

### Distributed Reading

All readers support distributed parallel reading:
- Files are automatically distributed across Spark executors
- Large files can be split for parallel processing
- Multi-threading support for efficient data loading

### Schema Inference

Schemas are automatically inferred from the files:
- Attribute columns are preserved
- Geometry columns are added with metadata
- Data types are automatically mapped

### Geometry Output

Vector readers output geometry in a consistent format:
- **Primary geometry column**: `geom_0` (or feature-specific name like `SHAPE`)
- **SRID column**: `geom_0_srid` - Spatial Reference ID
- **Projection column**: `geom_0_srid_proj` - Projection definition (Proj4 or WKT)

## Basic Usage Pattern

All readers follow the same basic pattern:

```python
# Generic pattern
df = (
    spark
    .read
    .format("<reader_name>")
    .option("<option_name>", "<option_value>")
    .load("<path>")
)

df.show()
```

### Examples

```python
# Read GeoTIFF
rasters = spark.read.format("gdal").load("/data/geotiffs")

# Read Shapefile
shapes = spark.read.format("shapefile").load("/data/shapefiles")

# Read GeoJSON
geojson = spark.read.format("geojson").load("/data/geojson")

# Read GeoPackage
gpkg = spark.read.format("gpkg").load("/data/packages")
```

## Path Specifications

All readers support flexible path specifications:

### Single File

```python
df = spark.read.format("shapefile").load("/path/to/file.shp")
```

### Directory

```python
# Reads all compatible files in directory
df = spark.read.format("shapefile").load("/path/to/directory")
```

### Wildcard Patterns

```python
# Read specific files
df = spark.read.format("gdal").load("/path/to/*.tif")
```

### Cloud Storage

```python
# S3
df = spark.read.format("shapefile").load("s3://bucket/path/to/shapefiles")

# Azure Blob Storage
df = spark.read.format("gdal").load("wasbs://container@account.blob.core.windows.net/path")

# Google Cloud Storage
df = spark.read.format("geojson").load("gs://bucket/path/to/geojson")

# Databricks DBFS
df = spark.read.format("gpkg").load("dbfs:/path/to/geopackages")

# Unity Catalog Volumes
df = spark.read.format("shapefile").load("/Volumes/catalog/schema/volume/shapefiles")
```

## Output Schema

### Raster Reader Output

```
root
 |-- path: string (path to file)
 |-- tile: binary (raster tile data)
 |-- metadata: map (raster metadata)
```

### Vector Reader Output

```
root
 |-- geom_0: binary (geometry in WKB format)
 |-- geom_0_srid: integer (spatial reference ID)
 |-- geom_0_srid_proj: string (projection definition)
 |-- <attribute_columns>: various types (feature attributes)
```

## Common Options

### Raster Options

- `sizeInMB` - Split threshold for large files (default: "16")
- `filterRegex` - Filter files by regex pattern (default: ".*")
- `driverName` - Specific GDAL driver to use

### Vector Options

- `driverName` - Specific OGR driver to use
- `chunkSize` - Records per chunk for multi-threading (default: "10000")
- `layerN` - Layer index for multi-layer formats (default: "0")
- `layerName` - Layer name for multi-layer formats
- `asWKB` - Output as WKB vs WKT (default: "true")

## Converting to Databricks Spatial Types

GeoBrix readers output WKB format. To use Databricks built-in spatial types:

```python
from pyspark.sql.functions import expr

# Read with GeoBrix
df = spark.read.format("shapefile").load("/path/to/shapefiles")

# Convert to GEOMETRY type
geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Use Databricks ST functions
result = geometry_df.select(
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_centroid(geometry)").alias("centroid")
)
```

### SQL Conversion

```sql
-- Create view with converted geometries
CREATE OR REPLACE TEMP VIEW shapes AS
SELECT
    *,
    st_geomfromwkb(geom_0) as geometry
FROM shapefile.`/path/to/shapefiles`;

-- Use Databricks ST functions
SELECT
    feature_id,
    st_area(geometry) as area,
    st_length(geometry) as perimeter
FROM shapes;
```

## Performance Tips

### 1. Use Appropriate Options

```python
# For large rasters, adjust split size
rasters = spark.read.format("gdal").option("sizeInMB", "32").load("/data/large_rasters")

# For large vector files, adjust chunk size
vectors = spark.read.format("shapefile").option("chunkSize", "50000").load("/data/large_shapes")
```

### 2. Filter Early

```python
# Use filterRegex to read only specific files
df = spark.read.format("gdal").option("filterRegex", ".*_2024_.*\\.tif").load("/data/all_rasters")
```

### 3. Partition Data

```python
# Partition by a spatial attribute
df = spark.read.format("shapefile").load("/data/shapes")
df.repartition("region").write.partitionBy("region").saveAsTable("shapes_by_region")
```

### 4. Cache Frequently Used Data

```python
# Cache converted geometries
df = spark.read.format("geojson").load("/data/boundaries")
geometry_df = df.select("*", expr("st_geomfromwkb(geom_0)").alias("geometry"))
geometry_df.cache()
```

## Multi-Format Workflows

Combine multiple readers in a single workflow:

```python
# Read different formats
rasters = spark.read.format("gdal").load("/data/rasters")
shapefiles = spark.read.format("shapefile").load("/data/vectors")
geojson = spark.read.format("geojson").load("/data/boundaries")

# Process each format
raster_catalog = rasters.select("path", "metadata")
vector_features = shapefiles.select("*", expr("st_geomfromwkb(geom_0)").alias("geometry"))
boundaries = geojson.select("*", expr("st_geomfromwkb(geom_0)").alias("geometry"))

# Combine or join as needed
```

## Troubleshooting

### Issue: Files Not Found

```python
# Check file paths
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)
dbutils.fs.ls("/path/to/check")
```

### Issue: Driver Not Recognized

```python
# Explicitly specify driver
df = spark.read.format("ogr").option("driverName", "ESRI Shapefile").load("/path")
```

### Issue: Large File Performance

```python
# Adjust split size for rasters
df = spark.read.format("gdal").option("sizeInMB", "8").load("/path")

# Adjust chunk size for vectors
df = spark.read.format("shapefile").option("chunkSize", "5000").load("/path")
```

### Issue: Geometry Conversion Errors

```python
# Validate geometries after reading
from pyspark.sql.functions import expr

df = spark.read.format("shapefile").load("/path")
validated = df.select(
    "*",
    expr("st_isvalid(st_geomfromwkb(geom_0))").alias("is_valid")
)
validated.filter("is_valid = false").show()
```

## Next Steps

Learn about specific readers:
- [GDAL Reader](./gdal.md) - Raster data
- [OGR Reader](./ogr.md) - Generic vector data
- [Shapefile Reader](./shapefile.md) - ESRI Shapefiles
- [GeoJSON Reader](./geojson.md) - GeoJSON files
- [GeoPackage Reader](./geopackage.md) - GeoPackage files
- [File GeoDatabase Reader](./filegdb.md) - ESRI File Geodatabases

