---
sidebar_position: 3
---

# OGR Reader

The OGR reader provides generic support for reading vector data formats through the OGR library. This is the base reader that powers all vector format readers in GeoBrix.

## Format Name

`ogr`

## Overview

The OGR reader is a generic vector data reader that can handle any format supported by OGR/GDAL. While GeoBrix provides named readers for common formats (Shapefile, GeoJSON, GeoPackage, etc.), you can use the OGR reader directly for any supported format.

## Supported Formats

The OGR reader supports [all OGR vector drivers](https://gdal.org/drivers/vector/index.html), including:

- **ESRI Shapefile** (.shp)
- **GeoJSON** (.geojson, .json)
- **GeoPackage** (.gpkg)
- **File Geodatabase** (.gdb)
- **KML** (.kml)
- **GML** (.gml)
- **CSV** with geometry (.csv)
- **PostgreSQL/PostGIS**
- **And 80+ more formats**

## Basic Usage

```python
# Read with auto-detected driver
df = spark.read.format("ogr").load("/path/to/vector/files")

df.show()
```

## Options

### `driverName`

**Default:** Auto-detected from file extension

Explicitly specify the OGR driver to use.

```python
# Explicitly use Shapefile driver
df = spark.read.format("ogr") \
    .option("driverName", "ESRI Shapefile") \
    .load("/path/to/shapefiles")

# Use GeoJSON driver
df = spark.read.format("ogr") \
    .option("driverName", "GeoJSON") \
    .load("/path/to/geojson")
```

### `chunkSize`

**Default:** `"10000"`

Number of records for multi-threading per file reading.

```python
# Increase chunk size for large features
df = spark.read.format("ogr") \
    .option("chunkSize", "50000") \
    .load("/path/to/large/file")

# Decrease for more parallelism
df = spark.read.format("ogr") \
    .option("chunkSize", "5000") \
    .load("/path/to/files")
```

### `layerN`

**Default:** `"0"`

Layer index for multi-layer formats (0-based).

```python
# Read second layer
df = spark.read.format("ogr") \
    .option("layerN", "1") \
    .load("/path/to/multi_layer.gpkg")
```

### `layerName`

**Default:** `""` (empty)

Layer name for multi-layer formats.

```python
# Read specific layer by name
df = spark.read.format("ogr") \
    .option("layerName", "buildings") \
    .load("/path/to/geodatabase.gdb")
```

### `asWKB`

**Default:** `"true"`

Output geometry as WKB (Well-Known Binary) vs WKT (Well-Known Text).

```python
# Output as WKT instead of WKB
df = spark.read.format("ogr") \
    .option("asWKB", "false") \
    .load("/path/to/vectors")
```

## Output Schema

```
root
 |-- geom_0: binary (geometry in WKB format)
 |-- geom_0_srid: integer (spatial reference ID)
 |-- geom_0_srid_proj: string (projection definition)
 |-- <attribute_1>: <type> (feature attributes...)
 |-- <attribute_2>: <type>
 |-- ...
```

## Usage Examples

### Example 1: Read with Specific Driver

```python
# Read KML files
df = spark.read.format("ogr") \
    .option("driverName", "KML") \
    .load("/path/to/file.kml")

df.select("Name", "Description", "geom_0_srid").show()
```

### Example 2: Read Multi-Layer Format

```python
# Read specific layer from GeoPackage
buildings = spark.read.format("ogr") \
    .option("layerName", "buildings") \
    .load("/path/to/data.gpkg")

roads = spark.read.format("ogr") \
    .option("layerName", "roads") \
    .load("/path/to/data.gpkg")

buildings.show()
roads.show()
```

### Example 3: Adjust Performance

```python
# Read large shapefile with custom chunk size
large_file = spark.read.format("ogr") \
    .option("driverName", "ESRI Shapefile") \
    .option("chunkSize", "100000") \
    .load("/path/to/large_shapefile.shp")

print(f"Loaded {large_file.count()} features")
```

### Example 4: Convert to Databricks Types

```python
from pyspark.sql.functions import expr

# Read and convert to GEOMETRY type
df = spark.read.format("ogr").load("/path/to/vectors")

geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Use Databricks spatial functions
result = geometry_df.select(
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_centroid(geometry)").alias("centroid")
)

result.show()
```

## Working with Different Formats

### KML Files

```python
kml_df = spark.read.format("ogr") \
    .option("driverName", "KML") \
    .load("/path/to/file.kml")

kml_df.show()
```

### GML Files

```python
gml_df = spark.read.format("ogr") \
    .option("driverName", "GML") \
    .load("/path/to/file.gml")

gml_df.show()
```

### CSV with Geometry

```python
csv_df = spark.read.format("ogr") \
    .option("driverName", "CSV") \
    .load("/path/to/points.csv")

csv_df.show()
```

### Database Connections

```python
# PostGIS (requires connection string)
postgis_df = spark.read.format("ogr") \
    .option("driverName", "PostgreSQL") \
    .load("PG:host=localhost dbname=gis user=postgres")
```

## Performance Tuning

### Chunk Size Optimization

```python
# For files with small features
df = spark.read.format("ogr") \
    .option("chunkSize", "50000") \
    .load("/path/to/points")

# For files with large/complex features
df = spark.read.format("ogr") \
    .option("chunkSize", "1000") \
    .load("/path/to/complex_polygons")
```

### Parallel Reading

```python
# Read multiple files in parallel
df = spark.read.format("ogr").load("/path/to/directory/*.shp")

# Repartition for processing
df.repartition(100).write.saveAsTable("processed_vectors")
```

## Named Readers vs OGR

For common formats, use the named readers for convenience:

```python
# These are equivalent:
df1 = spark.read.format("ogr").option("driverName", "ESRI Shapefile").load("/path")
df2 = spark.read.format("shapefile").load("/path")

# Named readers set appropriate defaults
df3 = spark.read.format("geojson").load("/path")  # Sets GeoJSONSeq by default
```

## Next Steps

- [Shapefile Reader](./shapefile.md)
- [GeoJSON Reader](./geojson.md)
- [GeoPackage Reader](./geopackage.md)
- [File GeoDatabase Reader](./filegdb.md)

