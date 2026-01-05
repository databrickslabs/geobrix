---
sidebar_position: 2
---

# GDAL Reader

The GDAL reader provides support for reading raster data formats through the GDAL library. The primary focus is on GeoTIFF files, but any GDAL-supported raster format can be attempted.

![GDAL Reader](../../../resources/images/readers/gdal_reader.png)

## Format Name

`gdal`

## Supported Formats

The GDAL reader primarily focuses on **GeoTIFF** but supports any format available through GDAL drivers:

- **GeoTIFF** (.tif, .tiff) - Primary support ⭐
- **NetCDF** (.nc)
- **HDF** (.hdf, .h5)
- **GRIB** (.grb, .grib2)
- **JPEG2000** (.jp2)
- **PNG** (.png)
- **IMG** (.img)
- **ENVI** (.hdr, .bin)
- [Many more GDAL raster formats](https://gdal.org/en/stable/drivers/raster/index.html)

## Basic Usage

```python
# Read GeoTIFF files
df = spark.read.format("gdal").load("/path/to/geotiffs")

df.show()
```

### Scala

```scala
// Read GeoTIFF files
val df = spark.read.format("gdal").load("/path/to/geotiffs")

df.show()
```

### SQL

```sql
-- Read GeoTIFF files
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`/path/to/geotiffs`;

SELECT * FROM rasters;
```

## Options

### `sizeInMB`

**Default:** `"16"`

Split threshold for large files. Files larger than this size will be split for parallel processing.

```python
# Increase split threshold for larger tiles
df = spark.read.format("gdal").option("sizeInMB", "32").load("/path/to/large_rasters")

# Decrease for smaller tiles (more parallelism)
df = spark.read.format("gdal").option("sizeInMB", "8").load("/path/to/rasters")
```

### `filterRegex`

**Default:** `".*"` (all files)

Filter files by regex pattern when reading from a directory.

```python
# Read only files from 2024
df = spark.read.format("gdal").option("filterRegex", ".*_2024_.*\\.tif").load("/data/all_years")

# Read specific satellite scenes
df = spark.read.format("gdal").option("filterRegex", "LC08.*\\.tif").load("/data/landsat")
```

### `driverName`

**Default:** Auto-detected from file extension

Explicitly specify the GDAL driver to use.

```python
# Explicitly use GeoTIFF driver
df = spark.read.format("gdal").option("driverName", "GTiff").load("/path/to/files")

# Use NetCDF driver
df = spark.read.format("gdal").option("driverName", "NetCDF").load("/path/to/netcdf")

# Use HDF5 driver
df = spark.read.format("gdal").option("driverName", "HDF5").load("/path/to/hdf")
```

## Output Schema

```
root
 |-- path: string (path to the source file)
 |-- tile: binary (raster tile data)
 |-- metadata: map<string, string> (raster metadata)
```

### Schema Example

```python
df = spark.read.format("gdal").load("/data/sample.tif")
df.printSchema()

# Output:
# root
#  |-- path: string (nullable = true)
#  |-- tile: binary (nullable = true)
#  |-- metadata: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
```

## Usage Examples

### Example 1: Read Single GeoTIFF

```python
# Read a single GeoTIFF file
df = spark.read.format("gdal").load("/data/elevation.tif")

df.select("path", "metadata").show(truncate=False)
```

### Example 2: Read Directory of GeoTIFFs

```python
# Read all GeoTIFF files in a directory
df = spark.read.format("gdal").load("/data/satellite_imagery/")

# Check how many files were loaded
print(f"Loaded {df.count()} raster tiles")
```

### Example 3: Read with Filtering

```python
# Read only specific files
df = spark.read.format("gdal") \
    .option("filterRegex", ".*_B[0-9]+\\.tif") \
    .load("/data/landsat_scene")

# Show file paths
df.select("path").distinct().show(truncate=False)
```

### Example 4: Read Large Rasters with Custom Split Size

```python
# Read large rasters with 64MB tiles
large_rasters = spark.read.format("gdal") \
    .option("sizeInMB", "64") \
    .load("/data/large_elevation_models")

large_rasters.show()
```

### Example 5: Read from Cloud Storage

```python
# Read from S3
s3_rasters = spark.read.format("gdal").load("s3://bucket/path/to/rasters/*.tif")

# Read from Azure Blob Storage
azure_rasters = spark.read.format("gdal") \
    .load("wasbs://container@account.blob.core.windows.net/rasters/")

# Read from Unity Catalog Volume
volume_rasters = spark.read.format("gdal") \
    .load("/Volumes/catalog/schema/volume_name/rasters/")
```

## Working with Raster Data

### Extract Metadata

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read rasters
rasters = spark.read.format("gdal").load("/data/rasters")

# Extract raster properties
metadata = rasters.select(
    "path",
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_numbands("tile").alias("num_bands"),
    rx.rst_boundingbox("tile").alias("bbox"),
    rx.rst_metadata("tile").alias("metadata")
)

metadata.show(truncate=False)
```

### Process Rasters

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import expr
rx.register(spark)

# Read and process
rasters = spark.read.format("gdal").load("/data/input")

# Clip to area of interest
clipped = rasters.select(
    "path",
    rx.rst_clip(
        "tile",
        expr("st_geomfromtext('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')")
    ).alias("clipped_tile")
)

clipped.show()
```

### Create Raster Catalog

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read all rasters
rasters = spark.read.format("gdal").load("/data/satellite/")

# Build catalog
catalog = rasters.select(
    "path",
    rx.rst_boundingbox("tile").alias("bounds"),
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_numbands("tile").alias("bands"),
    rx.rst_srid("tile").alias("crs"),
    rx.rst_metadata("tile").alias("metadata")
)

# Save as Delta table
catalog.write.mode("overwrite").saveAsTable("raster_catalog")
```

## Performance Tuning

### Optimize Split Size

Choose `sizeInMB` based on your raster sizes and cluster:

```python
# Small rasters (< 10MB): Use default or smaller
df = spark.read.format("gdal").option("sizeInMB", "8").load("/data/small_tiles")

# Medium rasters (10-100MB): Use default
df = spark.read.format("gdal").load("/data/medium_rasters")

# Large rasters (> 100MB): Use larger split size
df = spark.read.format("gdal").option("sizeInMB", "64").load("/data/large_rasters")
```

### Parallel Processing

```python
# Read and repartition for processing
rasters = spark.read.format("gdal").load("/data/rasters")

# Repartition to match cluster size
num_executors = spark.sparkContext.defaultParallelism
rasters_partitioned = rasters.repartition(num_executors)

# Process in parallel
processed = rasters_partitioned.select(
    "path",
    # Your processing here
)
```

### Caching Strategy

```python
# Cache raster catalog for repeated queries
catalog = spark.read.format("gdal").load("/data/rasters")
catalog.cache()

# Query catalog multiple times
landsat_scenes = catalog.filter("path like '%LC08%'")
sentinel_scenes = catalog.filter("path like '%S2%'")
```

## Common Use Cases

### Use Case 1: Satellite Imagery Catalog

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read all satellite imagery
imagery = spark.read.format("gdal") \
    .option("filterRegex", ".*\\.(tif|TIF)") \
    .load("/data/satellite/")

# Create searchable catalog
catalog = imagery.select(
    "path",
    rx.rst_boundingbox("tile").alias("footprint"),
    rx.rst_metadata("tile").alias("metadata"),
    rx.rst_numbands("tile").alias("bands")
)

# Extract acquisition date from metadata
from pyspark.sql.functions import col
catalog = catalog.withColumn(
    "acquisition_date",
    col("metadata").getItem("ACQUISITION_DATE")
)

catalog.write.mode("overwrite").saveAsTable("satellite_catalog")
```

### Use Case 2: Elevation Model Processing

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read elevation models
dems = spark.read.format("gdal").load("/data/dems/")

# Calculate statistics
stats = dems.select(
    "path",
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_boundingbox("tile").alias("extent")
)

stats.show()
```

### Use Case 3: Multi-Temporal Analysis

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import regexp_extract
rx.register(spark)

# Read time series of rasters
time_series = spark.read.format("gdal") \
    .option("filterRegex", ".*_NDVI_.*\\.tif") \
    .load("/data/time_series/")

# Extract date from filename
time_series = time_series.withColumn(
    "date",
    regexp_extract("path", r"(\d{8})", 1)
)

# Build temporal catalog
catalog = time_series.select(
    "date",
    "path",
    rx.rst_boundingbox("tile").alias("extent")
)

catalog.orderBy("date").show()
```

## Troubleshooting

### Issue: Driver Not Found

```python
# Explicitly specify driver
df = spark.read.format("gdal") \
    .option("driverName", "GTiff") \
    .load("/path/to/files")
```

### Issue: Files Too Large

```python
# Reduce split size for better parallelism
df = spark.read.format("gdal") \
    .option("sizeInMB", "8") \
    .load("/path/to/large/files")
```

### Issue: Memory Issues

```python
# Process in smaller batches
df = spark.read.format("gdal") \
    .option("sizeInMB", "16") \
    .load("/path/to/files")

# Don't cache large raster data
df.select("path", "metadata").cache()  # Only cache metadata
```

## Integration with RasterX

The GDAL reader works seamlessly with RasterX functions:

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read -> Process -> Save pipeline
result = (
    spark.read.format("gdal")
    .load("/data/input")
    .select(
        "path",
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_metadata("tile").alias("metadata")
    )
)

result.write.mode("overwrite").saveAsTable("raster_metadata")
```

## Next Steps

- [Learn about RasterX functions](../packages/rasterx.md)
- [View API Reference](../api/overview.md)
- [Check Examples](../examples/overview.md)

