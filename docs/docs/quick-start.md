---
sidebar_position: 3
---

# Quick Start

This guide will help you get started with GeoBrix quickly after installation.

## Prerequisites

Make sure you have [installed GeoBrix](./installation.md) on your Databricks cluster.

## Register Functions

To get up and running with PySpark bindings and SQL function registration in a cluster, execute the following:

:::note
You do not need to register functions if you are only using the included readers.
:::

### Python/PySpark

```python
from databricks.labs.gbx.rasterx import functions as rx

# Register RasterX functions with Spark
rx.register(spark)
```

### Scala

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}

// Register RasterX functions with Spark
rx.register(spark)
```

## List Available Functions

Once registered, you can list all available GeoBrix functions.

### SQL

```sql
-- List all RasterX functions
SHOW FUNCTIONS LIKE 'gbx_rst_*';

-- List all GridX functions
SHOW FUNCTIONS LIKE 'gbx_bng_*';

-- List all VectorX functions
SHOW FUNCTIONS LIKE 'gbx_st_*';

-- List ALL GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_*';
```

![Show Functions](../../resources/images/quickstart/show_funcs.png)

## Describe Functions

Get detailed information about any function:

```sql
-- Get function description
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;
```

![Function Description](../../resources/images/quickstart/func_descrip.png)

## Basic Examples

### Reading Geospatial Data

#### Read GeoTiff Files

```python
# Read GeoTiff raster files
df = (
    spark
    .read
    .format("gdal")
    .load("/path/to/geotiff/files")
)

df.show()
```

#### Read Shapefiles

```python
# Read shapefiles
df = (
    spark
    .read
    .format("shapefile")
    .load("/path/to/shapefiles")
)

df.show()
```

#### Read GeoJSON

```python
# Read GeoJSON files
df = (
    spark
    .read
    .format("geojson")
    .option("multi", "false")
    .load("/path/to/geojson/files")
)

df.show()
```

### Using RasterX Functions

```python
from databricks.labs.gbx.rasterx import functions as rx

# Register functions
rx.register(spark)

# Read raster data
raster_df = spark.read.format("gdal").load("/path/to/rasters")

# Get bounding box of rasters
result = raster_df.select(
    rx.rst_boundingbox("tile").alias("bbox")
)

result.show()
```

### Using GridX Functions (BNG)

```python
from databricks.labs.gbx.gridx.bng import functions as bx

# Register BNG functions
bx.register(spark)

# Calculate cell area
df = spark.sql("""
    SELECT gbx_bng_cellarea('TQ', 1000) as area
""")

df.show()
```

### Using VectorX Functions

```python
from databricks.labs.gbx.vectorx import functions as vx

# Register VectorX functions
vx.register(spark)

# Convert legacy Mosaic geometry to WKB
df = spark.sql("""
    SELECT gbx_st_legacyaswkb(legacy_geom) as wkb_geom
    FROM legacy_table
""")

df.show()
```

## Working with SQL

After registering functions, you can use them directly in SQL:

```sql
-- Read a shapefile and query it
CREATE OR REPLACE TEMP VIEW my_shapes AS
SELECT * FROM shapefile.`/path/to/shapefiles`;

-- Use VectorX functions
SELECT 
    shape_id,
    gbx_st_legacyaswkb(geom_0) as geometry_wkb
FROM my_shapes;

-- Read GeoTiff and use RasterX functions
CREATE OR REPLACE TEMP VIEW my_rasters AS
SELECT * FROM gdal.`/path/to/geotiffs`;

SELECT
    tile_id,
    gbx_rst_boundingbox(tile) as bbox,
    gbx_rst_metadata(tile) as metadata
FROM my_rasters;
```

## Converting to Databricks Spatial Types

GeoBrix Beta outputs WKB or WKT formats. To use Databricks built-in spatial types and functions:

```python
# Read shapefile
df = spark.read.format("shapefile").load("/path/to/shapefiles")

# Convert WKB to Databricks GEOMETRY type
from pyspark.sql.functions import expr

geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Now you can use built-in ST functions
result = geometry_df.select(
    "geometry",
    expr("st_area(geometry)").alias("area"),
    expr("st_length(geometry)").alias("length")
)

result.show()
```

### SQL Conversion

```sql
-- Read shapefile
CREATE OR REPLACE TEMP VIEW shapes AS
SELECT * FROM shapefile.`/path/to/shapefiles`;

-- Convert to GEOMETRY type
CREATE OR REPLACE TEMP VIEW shapes_with_geom AS
SELECT 
    *,
    st_geomfromwkb(geom_0) as geometry
FROM shapes;

-- Use built-in spatial functions
SELECT
    shape_id,
    st_area(geometry) as area,
    st_centroid(geometry) as centroid,
    st_envelope(geometry) as envelope
FROM shapes_with_geom;
```

## Example Notebooks

For more comprehensive examples, see the [Examples](./examples/overview.md) section, which includes:

- Raster processing workflows
- Vector data analysis
- Grid indexing with BNG
- Data format conversions
- Integration with Databricks spatial types

## Common Patterns

### Pattern 1: Read → Process → Convert

```python
# 1. Read data with GeoBrix reader
df = spark.read.format("shapefile").load("/data/shapes")

# 2. Process with GeoBrix functions
from databricks.labs.gbx.vectorx import functions as vx
vx.register(spark)

# 3. Convert to Databricks types for further analysis
result = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)
```

### Pattern 2: Multi-Format Reading

```python
# Read different formats
geotiffs = spark.read.format("gdal").load("/data/rasters")
shapefiles = spark.read.format("shapefile").load("/data/vectors")
geojson = spark.read.format("geojson").load("/data/json")
geopackage = spark.read.format("gpkg").load("/data/packages")
```

### Pattern 3: Batch Processing

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Process multiple rasters in parallel
rasters = spark.read.format("gdal").load("/data/many_rasters")

results = rasters.select(
    "path",
    rx.rst_boundingbox("tile").alias("bbox"),
    rx.rst_metadata("tile").alias("metadata"),
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height")
)

results.write.mode("overwrite").saveAsTable("raster_catalog")
```

## Next Steps

- Explore [RasterX](./packages/rasterx.md) for raster processing
- Learn about [GridX](./packages/gridx.md) for grid indexing
- Check out [VectorX](./packages/vectorx.md) for vector operations
- Review [Readers](./readers/overview.md) documentation
- Browse the [API Reference](./api/overview.md)

