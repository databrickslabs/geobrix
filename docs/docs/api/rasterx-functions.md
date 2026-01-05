---
sidebar_position: 5
---

# RasterX Function Reference

Complete reference for all RasterX functions with detailed descriptions, parameters, return values, and examples.

## Accessor Functions

Functions to access raster properties and metadata.

### rst_boundingbox

Get the bounding box of a raster tile.

**Signature:**
```scala
rst_boundingbox(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Geometry representing the bounding box of the raster

**Examples:**

```python
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

rasters = spark.read.format("gdal").load("/data/rasters")
bbox_df = rasters.select(
    "path",
    rx.rst_boundingbox("tile").alias("bbox")
)
```

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
rx.register(spark)

val rasters = spark.read.format("gdal").load("/data/rasters")
val bboxDf = rasters.select(
  col("path"),
  rx.rst_boundingbox(col("tile")).alias("bbox")
)
```

```sql
SELECT
    path,
    gbx_rst_boundingbox(tile) as bbox
FROM gdal.`/data/rasters`;
```

---

### rst_width

Get the width of a raster tile in pixels.

**Signature:**
```scala
rst_width(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Integer representing the width in pixels

**Examples:**

```python
width_df = rasters.select(rx.rst_width("tile").alias("width"))
```

```sql
SELECT gbx_rst_width(tile) as width FROM rasters;
```

---

### rst_height

Get the height of a raster tile in pixels.

**Signature:**
```scala
rst_height(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Integer representing the height in pixels

**Examples:**

```python
height_df = rasters.select(rx.rst_height("tile").alias("height"))
```

```sql
SELECT gbx_rst_height(tile) as height FROM rasters;
```

---

### rst_numbands

Get the number of bands in a raster tile.

**Signature:**
```scala
rst_numbands(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Integer representing the number of bands

**Examples:**

```python
bands_df = rasters.select(rx.rst_numbands("tile").alias("num_bands"))
```

```sql
SELECT gbx_rst_numbands(tile) as bands FROM rasters;
```

---

### rst_metadata

Get metadata from a raster tile.

**Signature:**
```scala
rst_metadata(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Map of String to String containing raster metadata

**Examples:**

```python
metadata_df = rasters.select(rx.rst_metadata("tile").alias("metadata"))
```

```sql
SELECT gbx_rst_metadata(tile) as metadata FROM rasters;
```

---

### rst_srid

Get the spatial reference identifier (SRID) of a raster.

**Signature:**
```scala
rst_srid(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Integer SRID value

**Examples:**

```python
srid_df = rasters.select(rx.rst_srid("tile").alias("srid"))
```

```sql
SELECT gbx_rst_srid(tile) as srid FROM rasters;
```

---

### rst_geotransform

Get the geotransform parameters of a raster.

**Signature:**
```scala
rst_geotransform(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Array of geotransform coefficients

**Examples:**

```python
gt_df = rasters.select(rx.rst_geotransform("tile").alias("geotransform"))
```

---

### rst_bandmetadata

Get metadata for a specific band.

**Signature:**
```scala
rst_bandmetadata(tile: Column, bandIndex: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `bandIndex` - Integer band index (1-based)

**Returns:**
- Map of String to String containing band metadata

**Examples:**

```python
band_meta = rasters.select(
    rx.rst_bandmetadata("tile", lit(1)).alias("band1_metadata")
)
```

---

### rst_pixelcount

Get the total number of pixels in a raster.

**Signature:**
```scala
rst_pixelcount(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Long value representing total pixel count

**Examples:**

```python
pixel_count = rasters.select(rx.rst_pixelcount("tile").alias("pixel_count"))
```

---

## Constructor Functions

Functions to create or load rasters.

### rst_fromfile

Load a raster from a file path.

**Signature:**
```scala
rst_fromfile(path: Column): Column
```

**Parameters:**
- `path` - String column containing file path

**Returns:**
- Binary raster tile data

**Examples:**

```python
raster = spark.sql("""
    SELECT gbx_rst_fromfile('/data/sample.tif') as tile
""")
```

---

### rst_fromcontent

Create a raster from binary content.

**Signature:**
```scala
rst_fromcontent(content: Column): Column
```

**Parameters:**
- `content` - Binary column containing raster data

**Returns:**
- Binary raster tile data

---

### rst_makeemptyraster

Create an empty raster with specified dimensions.

**Signature:**
```scala
rst_makeemptyraster(width: Column, height: Column, bands: Column): Column
```

**Parameters:**
- `width` - Integer width in pixels
- `height` - Integer height in pixels  
- `bands` - Integer number of bands

**Returns:**
- Binary raster tile data

---

## Transformation Functions

Functions to transform and manipulate rasters.

### rst_clip

Clip a raster by a geometry.

**Signature:**
```scala
rst_clip(tile: Column, geometry: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `geometry` - Geometry column for clipping boundary

**Returns:**
- Binary raster tile clipped to geometry

**Examples:**

```python
from pyspark.sql.functions import expr

clipped = rasters.select(
    rx.rst_clip(
        "tile",
        expr("st_geomfromtext('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')")
    ).alias("clipped_tile")
)
```

```sql
SELECT
    path,
    gbx_rst_clip(
        tile,
        st_geomfromtext('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')
    ) as clipped
FROM rasters;
```

---

### rst_resample

Resample a raster to different dimensions.

**Signature:**
```scala
rst_resample(tile: Column, width: Column, height: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `width` - Target width in pixels
- `height` - Target height in pixels

**Returns:**
- Binary resampled raster tile

**Examples:**

```python
resampled = rasters.select(
    rx.rst_resample("tile", lit(1024), lit(1024)).alias("resampled")
)
```

```sql
SELECT gbx_rst_resample(tile, 512, 512) as resampled FROM rasters;
```

---

### rst_reproject

Reproject a raster to a different coordinate reference system.

**Signature:**
```scala
rst_reproject(tile: Column, targetSRID: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `targetSRID` - Target SRID/CRS code

**Returns:**
- Binary reprojected raster tile

**Examples:**

```python
reprojected = rasters.select(
    rx.rst_reproject("tile", lit(4326)).alias("wgs84_tile")
)
```

---

### rst_subdivide

Subdivide a large raster into smaller tiles.

**Signature:**
```scala
rst_subdivide(tile: Column, tileWidth: Column, tileHeight: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `tileWidth` - Width of subdivided tiles
- `tileHeight` - Height of subdivided tiles

**Returns:**
- Array of binary raster tiles

---

### rst_merge

Merge multiple rasters into one.

**Signature:**
```scala
rst_merge(tiles: Column): Column
```

**Parameters:**
- `tiles` - Array column of raster tiles

**Returns:**
- Binary merged raster tile

---

## Grid Functions

Functions for grid-based raster operations.

### rst_pixelasgridcell

Convert raster pixels to grid cells.

**Signature:**
```scala
rst_pixelasgridcell(tile: Column, gridResolution: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `gridResolution` - Grid resolution

**Returns:**
- Array of grid cells with values

---

### rst_rastertogridavg

Aggregate raster values by grid cells using average.

**Signature:**
```scala
rst_rastertogridavg(tile: Column, gridResolution: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `gridResolution` - Grid resolution

**Returns:**
- Array of grid cells with average values

---

### rst_rastertogridmin

Aggregate raster values by grid cells using minimum.

**Signature:**
```scala
rst_rastertogridmin(tile: Column, gridResolution: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `gridResolution` - Grid resolution

**Returns:**
- Array of grid cells with minimum values

---

### rst_rastertogridmax

Aggregate raster values by grid cells using maximum.

**Signature:**
```scala
rst_rastertogridmax(tile: Column, gridResolution: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `gridResolution` - Grid resolution

**Returns:**
- Array of grid cells with maximum values

---

## Band Functions

Functions for working with raster bands.

### rst_getband

Extract a specific band from a raster.

**Signature:**
```scala
rst_getband(tile: Column, bandIndex: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `bandIndex` - Integer band index (1-based)

**Returns:**
- Binary single-band raster tile

**Examples:**

```python
red_band = rasters.select(
    rx.rst_getband("tile", lit(1)).alias("red")
)
```

---

### rst_setband

Set values for a specific band.

**Signature:**
```scala
rst_setband(tile: Column, bandIndex: Column, values: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `bandIndex` - Integer band index (1-based)
- `values` - Array of values to set

**Returns:**
- Binary raster tile with updated band

---

### rst_separatebands

Separate a multi-band raster into individual bands.

**Signature:**
```scala
rst_separatebands(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Array of binary single-band raster tiles

**Examples:**

```python
bands = rasters.select(
    rx.rst_separatebands("tile").alias("bands")
)
```

---

## Aggregation Functions

Functions for aggregating raster data.

### rst_combineavg

Combine multiple rasters using average aggregation.

**Signature:**
```scala
rst_combineavg(tiles: Column): Column
```

**Parameters:**
- `tiles` - Array of raster tiles

**Returns:**
- Binary raster tile with averaged values

---

### rst_merge_agg

Aggregate function to merge rasters.

**Signature:**
```scala
rst_merge_agg(tile: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data

**Returns:**
- Binary merged raster tile

**Usage:**
```python
merged = rasters.groupBy("region").agg(
    rx.rst_merge_agg("tile").alias("merged_tile")
)
```

---

## Generator Functions

Functions to generate or tessellate rasters.

### rst_tessellate

Tessellate a raster into smaller tiles.

**Signature:**
```scala
rst_tessellate(tile: Column, tileSize: Column): Column
```

**Parameters:**
- `tile` - Binary column containing raster tile data
- `tileSize` - Size of tiles in pixels

**Returns:**
- Array of binary raster tiles

**Examples:**

```python
tiles = rasters.select(
    rx.rst_tessellate("tile", lit(256)).alias("small_tiles")
)
```

---

## Complete Example

Here's a comprehensive workflow using multiple RasterX functions:

```python
from databricks.labs.gbx.rasterx import functions as rx
from pyspark.sql.functions import expr, lit

# Register functions
rx.register(spark)

# Read rasters
rasters = spark.read.format("gdal").load("/data/satellite")

# Process pipeline
result = (
    rasters
    # Extract metadata
    .select(
        "path",
        "tile",
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_numbands("tile").alias("bands"),
        rx.rst_srid("tile").alias("srid")
    )
    # Filter by size
    .filter("width > 1000 AND height > 1000")
    # Clip to area of interest
    .select(
        "path",
        rx.rst_clip(
            "tile",
            expr("st_geomfromtext('POLYGON((...))' )")
        ).alias("clipped")
    )
    # Resample
    .select(
        "path",
        rx.rst_resample("clipped", lit(512), lit(512)).alias("resampled")
    )
    # Extract bands
    .select(
        "path",
        rx.rst_separatebands("resampled").alias("bands")
    )
)

# Save results
result.write.mode("overwrite").saveAsTable("processed_rasters")
```

## Next Steps

- [GridX Function Reference](./gridx-functions.md)
- [VectorX Function Reference](./vectorx-functions.md)
- [RasterX Package Documentation](../packages/rasterx.md)

