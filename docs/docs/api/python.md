---
sidebar_position: 3
---

# Python API Reference

GeoBrix provides Python bindings through PySpark, offering Pythonic access to all GeoBrix functionality.

## Installation

Ensure the GeoBrix wheel is installed on your cluster:

```python
# Verify installation
import databricks.labs.gbx
print("GeoBrix installed successfully")
```

## Import Patterns

### RasterX

```python
from databricks.labs.gbx.rasterx import functions as rx

# Register functions
rx.register(spark)

# Use functions
df = rasters.select(rx.rst_boundingbox("tile"))
```

### GridX (BNG)

```python
from databricks.labs.gbx.gridx.bng import functions as bx

# Register functions
bx.register(spark)

# Use functions
df = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000)")
```

### VectorX

```python
from databricks.labs.gbx.vectorx import functions as vx

# Register functions
vx.register(spark)

# Use functions
df = legacy_data.select(vx.st_legacyaswkb("mosaic_geom"))
```

## RasterX Functions

### Accessor Functions

Get raster properties and metadata.

#### `rst_boundingbox(tile)`

Get the bounding box of a raster.

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

rasters = spark.read.format("gdal").load("/data/rasters")
bbox_df = rasters.select(
    "path",
    rx.rst_boundingbox("tile").alias("bbox")
)
```

#### `rst_width(tile)`

Get raster width in pixels.

```python
width_df = rasters.select(rx.rst_width("tile").alias("width"))
```

#### `rst_height(tile)`

Get raster height in pixels.

```python
height_df = rasters.select(rx.rst_height("tile").alias("height"))
```

#### `rst_numbands(tile)`

Get number of bands in raster.

```python
bands_df = rasters.select(rx.rst_numbands("tile").alias("num_bands"))
```

#### `rst_metadata(tile)`

Get raster metadata.

```python
metadata_df = rasters.select(rx.rst_metadata("tile").alias("metadata"))
```

#### `rst_srid(tile)`

Get spatial reference identifier.

```python
srid_df = rasters.select(rx.rst_srid("tile").alias("srid"))
```

### Transformation Functions

Transform and manipulate rasters.

#### `rst_clip(tile, geometry)`

Clip raster by geometry.

```python
from pyspark.sql.functions import expr

clipped = rasters.select(
    rx.rst_clip(
        "tile",
        expr("st_geomfromtext('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')")
    ).alias("clipped_tile")
)
```

#### `rst_resample(tile, width, height)`

Resample raster to new dimensions.

```python
resampled = rasters.select(
    rx.rst_resample("tile", 1024, 1024).alias("resampled_tile")
)
```

### Complete Example

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import expr

# Register functions
rx.register(spark)

# Read rasters
rasters = spark.read.format("gdal").load("/data/satellite")

# Extract metadata and process
result = rasters.select(
    "path",
    rx.rst_boundingbox("tile").alias("bbox"),
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_numbands("tile").alias("bands"),
    rx.rst_metadata("tile").alias("metadata")
).filter(
    "width > 1000 AND height > 1000"
)

result.write.mode("overwrite").saveAsTable("raster_catalog")
```

## GridX Functions

### BNG Functions

British National Grid functions.

#### `bng_cellarea(grid_letter, precision)`

Calculate area of a BNG grid cell.

```python
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)

# Calculate cell area
area = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area_sqm")
area.show()
```

#### `bng_pointtocell(point, precision)`

Convert point to BNG grid cell.

```python
from pyspark.sql.functions import expr

points = spark.table("uk_locations")
bng_cells = points.select(
    "location_id",
    expr("gbx_bng_pointtocell(st_point(longitude, latitude), 1000)").alias("bng_cell")
)
```

### Complete Example

```python
from databricks.labs.gbx.gridx.bng import functions as bx
from pyspark.sql.functions import expr, count

# Register functions
bx.register(spark)

# Aggregate points by BNG cell
result = spark.sql("""
    SELECT
        gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
        COUNT(*) as point_count,
        AVG(value) as avg_value
    FROM measurements
    WHERE country = 'GB'
    GROUP BY bng_cell
""")

result.write.mode("overwrite").saveAsTable("bng_aggregated")
```

## VectorX Functions

### Conversion Functions

#### `st_legacyaswkb(legacy_geometry)`

Convert legacy Mosaic geometry to WKB.

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

# Register functions
vx.register(spark)

# Convert legacy geometries
legacy = spark.table("legacy_mosaic_table")
converted = legacy.select(
    "feature_id",
    vx.st_legacyaswkb("mosaic_geom").alias("wkb_geom")
)

# Convert to Databricks GEOMETRY type
geometry_df = converted.select(
    "feature_id",
    "wkb_geom",
    expr("st_geomfromwkb(wkb_geom)").alias("geometry")
)

geometry_df.write.mode("overwrite").saveAsTable("converted_features")
```

### Complete Example

```python
from databricks.labs.gbx.vectorx import functions as vx
from pyspark.sql.functions import expr

# Register functions
vx.register(spark)

# Full migration workflow
legacy_table = spark.table("legacy_mosaic_geometries")

# Convert and validate
migrated = legacy_table.select(
    "*",
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
).select(
    "feature_id",
    "properties",
    "geometry",
    expr("st_isvalid(geometry)").alias("is_valid"),
    expr("st_area(geometry)").alias("area")
).filter("is_valid = true")

# Save to Delta
migrated.write.mode("overwrite").saveAsTable("migrated_features")
```

## Using with DataFrames

### Select Operations

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Single function
result = df.select(rx.rst_boundingbox("tile"))

# Multiple functions
result = df.select(
    rx.rst_boundingbox("tile").alias("bbox"),
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height")
)

# With column renaming
result = df.select(
    "path",
    rx.rst_metadata("tile").alias("raster_metadata")
)
```

### Filter Operations

```python
# Filter based on GeoBrix function results
result = df.filter(
    rx.rst_width("tile") > 1000
)

# Complex filters
result = df.filter(
    (rx.rst_width("tile") > 1000) &
    (rx.rst_height("tile") > 1000) &
    (rx.rst_numbands("tile") >= 3)
)
```

### WithColumn Operations

```python
# Add new columns
result = df.withColumn("bbox", rx.rst_boundingbox("tile"))
result = df.withColumn("width", rx.rst_width("tile"))
result = df.withColumn("height", rx.rst_height("tile"))

# Chain operations
result = (
    df
    .withColumn("bbox", rx.rst_boundingbox("tile"))
    .withColumn("width", rx.rst_width("tile"))
    .withColumn("height", rx.rst_height("tile"))
)
```

## Using with SQL

After registration, functions are available in SQL:

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Create temp view
rasters = spark.read.format("gdal").load("/data/rasters")
rasters.createOrReplaceTempView("rasters")

# Use in SQL
result = spark.sql("""
    SELECT
        path,
        gbx_rst_boundingbox(tile) as bbox,
        gbx_rst_width(tile) as width,
        gbx_rst_height(tile) as height
    FROM rasters
    WHERE gbx_rst_width(tile) > 1000
""")
```

## Type Hints and IDE Support

GeoBrix functions work with PySpark's type system:

```python
from pyspark.sql import DataFrame
from databricks.labs.gbx.rasterx import functions as rx

def process_rasters(df: DataFrame) -> DataFrame:
    """
    Process rasters and extract metadata.
    
    Args:
        df: DataFrame with 'tile' column
        
    Returns:
        DataFrame with extracted metadata
    """
    rx.register(df.sparkSession)
    
    return df.select(
        "path",
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height")
    )
```

## Error Handling

```python
from databricks.labs.gbx.rasterx import functions as rx

try:
    rx.register(spark)
    result = df.select(rx.rst_boundingbox("tile"))
    result.show()
except Exception as e:
    print(f"Error processing rasters: {e}")
```

## Next Steps

- [SQL API Reference](./sql.md)
- [Scala API Reference](./scala.md)
- [Examples](../examples/overview.md)
- [Package Documentation](../packages/overview.md)

