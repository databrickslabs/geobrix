---
sidebar_position: 2
---

# Scala API Reference

GeoBrix is natively written in Scala, providing the most direct and performant access to its functionality.

## Import Patterns

### RasterX

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}

// Register functions
rx.register(spark)

// Use functions
val df = rasters.select(rx.rst_boundingbox(col("tile")))
```

### GridX (BNG)

```scala
import com.databricks.labs.gbx.gridx.bng.{functions => bx}

// Register functions
bx.register(spark)

// Use functions
val df = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000)")
```

### VectorX

```scala
import com.databricks.labs.gbx.vectorx.{functions => vx}

// Register functions
vx.register(spark)

// Use functions
val df = legacyData.select(vx.st_legacyaswkb(col("mosaic_geom")))
```

## RasterX Functions

### Accessor Functions

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Register functions
rx.register(spark)

// Read rasters
val rasters = spark.read.format("gdal").load("/data/rasters")

// Extract metadata
val metadata = rasters.select(
  col("path"),
  rx.rst_boundingbox(col("tile")).alias("bbox"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_numbands(col("tile")).alias("num_bands"),
  rx.rst_metadata(col("tile")).alias("metadata")
)

metadata.show()
```

### Transformation Functions

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)

val rasters = spark.read.format("gdal").load("/data/rasters")

// Clip raster
val clipped = rasters.select(
  col("path"),
  rx.rst_clip(
    col("tile"),
    expr("st_geomfromtext('POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))')")
  ).alias("clipped_tile")
)

// Resample raster
val resampled = rasters.select(
  col("path"),
  rx.rst_resample(col("tile"), lit(1024), lit(1024)).alias("resampled_tile")
)
```

### Complete Example

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Register functions
rx.register(spark)

// Read rasters
val rasters = spark.read.format("gdal").load("/data/satellite")

// Extract metadata and filter
val catalog = rasters.select(
  col("path"),
  rx.rst_boundingbox(col("tile")).alias("bbox"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_numbands(col("tile")).alias("bands"),
  rx.rst_metadata(col("tile")).alias("metadata")
).filter(
  col("width") > 1000 && col("height") > 1000
)

// Write to Delta
catalog.write.mode("overwrite").saveAsTable("raster_catalog")
```

## GridX Functions

### BNG Functions

```scala
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Register functions
bx.register(spark)

// Calculate cell area
val area = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area_sqm")
area.show()

// Convert points to BNG cells
val points = spark.table("uk_locations")
val bngCells = points.select(
  col("location_id"),
  expr("gbx_bng_pointtocell(st_point(longitude, latitude), 1000)").alias("bng_cell")
)

bngCells.show()
```

### Complete Example

```scala
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Register functions
bx.register(spark)

// Aggregate by BNG cell
val aggregated = spark.sql("""
  SELECT
    gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
    COUNT(*) as point_count,
    AVG(value) as avg_value
  FROM measurements
  WHERE country = 'GB'
  GROUP BY bng_cell
""")

aggregated.write.mode("overwrite").saveAsTable("bng_aggregated")
```

## VectorX Functions

### Conversion Functions

```scala
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Register functions
vx.register(spark)

// Convert legacy geometries
val legacy = spark.table("legacy_mosaic_table")
val converted = legacy.select(
  col("feature_id"),
  vx.st_legacyaswkb(col("mosaic_geom")).alias("wkb_geom")
)

// Convert to Databricks GEOMETRY type
val geometryDf = converted.select(
  col("feature_id"),
  col("wkb_geom"),
  expr("st_geomfromwkb(wkb_geom)").alias("geometry")
)

geometryDf.write.mode("overwrite").saveAsTable("converted_features")
```

### Complete Example

```scala
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Register functions
vx.register(spark)

// Full migration workflow
val legacyTable = spark.table("legacy_mosaic_geometries")

// Convert and validate
val migrated = legacyTable
  .select(
    col("*"),
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
  )
  .select(
    col("feature_id"),
    col("properties"),
    col("geometry"),
    expr("st_isvalid(geometry)").alias("is_valid"),
    expr("st_area(geometry)").alias("area")
  )
  .filter(col("is_valid") === true)

// Save to Delta
migrated.write.mode("overwrite").saveAsTable("migrated_features")
```

## DataFrame Operations

### Select Operations

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)

// Single function
val result = df.select(rx.rst_boundingbox(col("tile")))

// Multiple functions
val result = df.select(
  rx.rst_boundingbox(col("tile")).alias("bbox"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height")
)

// With additional columns
val result = df.select(
  col("path"),
  rx.rst_metadata(col("tile")).alias("raster_metadata")
)
```

### Filter Operations

```scala
// Filter based on function results
val result = df.filter(rx.rst_width(col("tile")) > 1000)

// Complex filters
val result = df.filter(
  rx.rst_width(col("tile")) > 1000 &&
  rx.rst_height(col("tile")) > 1000 &&
  rx.rst_numbands(col("tile")) >= 3
)
```

### WithColumn Operations

```scala
// Add new columns
val result = df
  .withColumn("bbox", rx.rst_boundingbox(col("tile")))
  .withColumn("width", rx.rst_width(col("tile")))
  .withColumn("height", rx.rst_height(col("tile")))
```

## Using with SQL

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}

// Register functions
rx.register(spark)

// Create temp view
val rasters = spark.read.format("gdal").load("/data/rasters")
rasters.createOrReplaceTempView("rasters")

// Use in SQL
val result = spark.sql("""
  SELECT
    path,
    gbx_rst_boundingbox(tile) as bbox,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height
  FROM rasters
  WHERE gbx_rst_width(tile) > 1000
""")

result.show()
```

## Type Safety

Scala provides compile-time type safety:

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

def processRasters(spark: SparkSession, df: DataFrame): DataFrame = {
  rx.register(spark)
  
  df.select(
    col("path"),
    rx.rst_boundingbox(col("tile")).alias("bbox"),
    rx.rst_width(col("tile")).alias("width"),
    rx.rst_height(col("tile")).alias("height")
  )
}

// Usage
val rasters = spark.read.format("gdal").load("/data/rasters")
val processed = processRasters(spark, rasters)
```

## Error Handling

```scala
import com.databricks.labs.gbx.rasterx.{functions => rx}
import scala.util.{Try, Success, Failure}

Try {
  rx.register(spark)
  val result = df.select(rx.rst_boundingbox(col("tile")))
  result.show()
} match {
  case Success(_) => println("Processing successful")
  case Failure(exception) => println(s"Error: ${exception.getMessage}")
}
```

## Performance Tips

### Use Column References

```scala
import org.apache.spark.sql.functions.col

// Good: Use col() references
val result = df.select(rx.rst_boundingbox(col("tile")))

// Avoid: String column names in Scala
// val result = df.select(rx.rst_boundingbox("tile"))
```

### Batch Operations

```scala
// Process multiple operations at once
val result = df.select(
  col("path"),
  rx.rst_boundingbox(col("tile")).alias("bbox"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_metadata(col("tile")).alias("metadata")
)
```

### Cache Intermediate Results

```scala
// Cache expensive operations
val enriched = df
  .withColumn("bbox", rx.rst_boundingbox(col("tile")))
  .withColumn("metadata", rx.rst_metadata(col("tile")))
  .cache()

// Use multiple times
val filtered1 = enriched.filter(col("width") > 1000)
val filtered2 = enriched.filter(col("bands") > 3)
```

## Next Steps

- [Python API Reference](./python.md)
- [SQL API Reference](./sql.md)
- [Examples](../examples/overview.md)
- [Package Documentation](../packages/overview.md)

