---
sidebar_position: 2
---

# RasterX

![RasterX](../../../resources/images/RasterX.png)

RasterX is GeoBrix's raster data processing package, providing comprehensive tools for working with raster datasets such as satellite imagery, elevation models, and other gridded spatial data.

## Overview

RasterX is a refactor and improvement of Mosaic raster functions. Since Databricks product does not (yet) support anything built-in specifically for raster processing, RasterX provides a "fully" gap-filling capability for raster operations on the Databricks platform.

## Key Features

- **GDAL-Powered**: Leverages GDAL for robust raster format support
- **Distributed Processing**: Built on Spark for scalable raster operations
- **Multiple Format Support**: GeoTIFF, NetCDF, and other GDAL-supported formats
- **Metadata Extraction**: Comprehensive raster metadata access
- **Raster Operations**: Clipping, resampling, transformations
- **Band Operations**: Multi-band raster support

## Function Categories

### Accessors

Functions to access raster properties and metadata:

- `gbx_rst_boundingbox` - Get the bounding box of a raster
- `gbx_rst_width` - Get raster width in pixels
- `gbx_rst_height` - Get raster height in pixels
- `gbx_rst_numbands` - Get number of bands
- `gbx_rst_metadata` - Extract raster metadata
- `gbx_rst_srid` - Get spatial reference identifier
- `gbx_rst_geotransform` - Get the geotransform parameters

### Constructors

Functions to create or load rasters:

- `gbx_rst_fromfile` - Load raster from file path
- `gbx_rst_fromcontent` - Create raster from binary content
- `gbx_rst_makeemptyraster` - Create empty raster with specifications

### Transformations

Functions to transform rasters:

- `gbx_rst_clip` - Clip raster by geometry
- `gbx_rst_resample` - Resample raster to different resolution
- `gbx_rst_reproject` - Reproject raster to different CRS
- `gbx_rst_merge` - Merge multiple rasters
- `gbx_rst_subdivide` - Subdivide large rasters

### Grid Operations

Functions for grid-based operations:

- `gbx_rst_pixelcount` - Count pixels in raster
- `gbx_rst_pixelasgridcell` - Convert pixels to grid cells
- `gbx_rst_rastertogridavg` - Grid-based averaging
- `gbx_rst_rastertogridmin` - Grid-based minimum
- `gbx_rst_rastertogridmax` - Grid-based maximum

### Band Operations

Functions for working with raster bands:

- `gbx_rst_bandmetadata` - Get band metadata
- `gbx_rst_getband` - Extract specific band
- `gbx_rst_setband` - Set band values

### Aggregations

Functions for aggregating raster data:

- `gbx_rst_combineavg` - Average multiple rasters
- `gbx_rst_merge_agg` - Merge rasters with aggregation

### Generators

Functions to generate raster data:

- `gbx_rst_tessellate` - Tessellate raster into tiles
- `gbx_rst_separatebands` - Separate multi-band raster

## Usage Examples

### Python/PySpark

```python
from databricks.labs.gbx.rasterx import functions as rx

# Register RasterX functions
rx.register(spark)

# Read raster files
raster_df = spark.read.format("gdal").load("/path/to/geotiffs")

# Get bounding box
bbox_df = raster_df.select(
    rx.rst_boundingbox("tile").alias("bbox")
)

# Get metadata
metadata_df = raster_df.select(
    "path",
    rx.rst_width("tile").alias("width"),
    rx.rst_height("tile").alias("height"),
    rx.rst_numbands("tile").alias("num_bands"),
    rx.rst_metadata("tile").alias("metadata")
)

metadata_df.show()

# Clip raster by geometry
from pyspark.sql.functions import expr

clipped_df = raster_df.select(
    rx.rst_clip("tile", expr("st_geomfromtext('POLYGON((...))')")).alias("clipped_tile")
)
```

### Scala

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}

// Register functions
rx.register(spark)

// Read raster files
val rasterDf = spark.read.format("gdal").load("/path/to/geotiffs")

// Get metadata
val metadataDf = rasterDf.select(
  col("path"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_numbands(col("tile")).alias("num_bands")
)

metadataDf.show()
```

### SQL

```sql
-- Register functions first in Python/Scala notebook
-- Then use in SQL

-- Read raster data
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`/path/to/geotiffs`;

-- Extract metadata
SELECT
    path,
    gbx_rst_boundingbox(tile) as bbox,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height,
    gbx_rst_numbands(tile) as num_bands,
    gbx_rst_metadata(tile) as metadata
FROM rasters;

-- Clip raster
SELECT
    path,
    gbx_rst_clip(
        tile,
        st_geomfromtext('POLYGON((-122.5 37.5, -122.5 38.5, -121.5 38.5, -121.5 37.5, -122.5 37.5))')
    ) as clipped_tile
FROM rasters;
```

## Common Workflows

### Workflow 1: Raster Cataloging

Create a catalog of raster files with metadata:

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read all rasters
rasters = spark.read.format("gdal").load("/data/satellite_imagery")

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

### Workflow 2: Raster Processing Pipeline

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import expr
rx.register(spark)

# Read rasters
rasters = spark.read.format("gdal").load("/data/input")

# Process: clip, resample, extract statistics
processed = rasters.select(
    "path",
    rx.rst_clip("tile", expr("st_geomfromwkt(aoi_wkt)")).alias("clipped")
).select(
    "path",
    rx.rst_resample("clipped", 30, 30).alias("resampled")
).select(
    "path",
    "resampled",
    rx.rst_metadata("resampled").alias("output_metadata")
)

# Write results
processed.write.mode("overwrite").format("delta").save("/data/processed")
```

### Workflow 3: Multi-Band Analysis

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read multi-band raster (e.g., Landsat)
landsat = spark.read.format("gdal").load("/data/landsat")

# Separate bands
bands = landsat.select(
    "path",
    rx.rst_separatebands("tile").alias("bands")
)

# Extract individual bands
red_band = bands.select(
    "path",
    rx.rst_getband("bands", 3).alias("red")
)

nir_band = bands.select(
    "path",
    rx.rst_getband("bands", 4).alias("nir")
)
```

## Performance Considerations

### Raster Tiling

For large rasters, use tiling to improve parallelism:

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read large raster
large_raster = spark.read.format("gdal").option("sizeInMB", "16").load("/data/large.tif")

# Tessellate into smaller tiles
tiles = large_raster.select(
    rx.rst_tessellate("tile", 256).alias("small_tile")
)

# Process tiles in parallel
processed_tiles = tiles.select(
    # Your processing here
    rx.rst_boundingbox("small_tile").alias("tile_bounds")
)
```

### Memory Management

- Use `sizeInMB` option when reading to control tile size
- Subdivide large rasters before processing
- Cache intermediate results for iterative operations

### Optimization Tips

1. **Partition your data**: Use appropriate partitioning for large raster datasets
2. **Use filters**: Filter by bounding box before expensive operations
3. **Broadcast small geometries**: When clipping, broadcast small geometries
4. **Repartition**: After reading, repartition to match cluster size

## Supported Formats

RasterX supports all GDAL-compatible raster formats, including:

- **GeoTIFF** (.tif, .tiff) - Primary focus
- **NetCDF** (.nc)
- **HDF** (.hdf, .h5)
- **GRIB** (.grb, .grib2)
- **JPEG2000** (.jp2)
- **PNG** (.png)
- **And many more** (see [GDAL Raster Drivers](https://gdal.org/drivers/raster/index.html))

## Integration with Databricks

### Delta Lake

Save processed rasters to Delta Lake:

```python
# Save raster metadata to Delta
catalog.write.mode("overwrite").format("delta").saveAsTable("raster_metadata")

# Save binary raster data
rasters.write.mode("overwrite").format("delta").save("/data/rasters_delta")
```

### Unity Catalog

Register raster tables in Unity Catalog:

```python
# Write to Unity Catalog
catalog.write.mode("overwrite").saveAsTable("catalog.schema.raster_catalog")
```

## Known Limitations

- Direct output to Databricks spatial types planned for future releases
- Some legacy Mosaic functions not yet ported (e.g., `rst_dtmfromgeoms`)
- Focus on GeoTIFF; other formats available but less tested

## Next Steps

- [View API Reference](../api/overview.md)
- [Check Examples](../examples/overview.md)
- [Learn about Readers](../readers/gdal.md)

