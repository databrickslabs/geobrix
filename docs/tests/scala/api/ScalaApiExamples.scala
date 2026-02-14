/*
 * Scala API Reference Examples - Single source of truth for docs/docs/api/scala.mdx.
 *
 * Each val holds the exact snippet shown in the docs. Validated at compile time by
 * ScalaApiExamplesDocTest. Package docs.tests.scala.api so Maven compiles this with
 * the doc test sources.
 */
package docs.tests.scala.api

object ScalaApiExamples {

  /** Register all packages (used in docs/docs/api/overview.mdx - Scala section) */
  val RegisterAllPackages: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}

// Register each package
rx.register(spark)
bx.register(spark)
vx.register(spark)
""".trim

  val RegisterAllPackages_output: String =
    """
RasterX, GridX, and VectorX functions registered (gbx_rst_*, gbx_bng_*, gbx_st_*).
""".trim

  val RegisterRasterX: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}

// Register functions
rx.register(spark)

// Use functions
val df = rasters.select(rx.rst_boundingbox(col("tile")))
""".trim

  val RegisterGridX: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}

// Register functions
bx.register(spark)

// Use functions
val df = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000)")
""".trim

  val RegisterVectorX: String =
    """
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}

// Register functions
vx.register(spark)

// Use functions
val df = legacyData.select(vx.st_legacyaswkb(col("mosaic_geom")))
""".trim

  val RasterXAccessorFunctions: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Register functions
rx.register(spark)

// Read rasters (sample data path)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")

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
""".trim

  val RasterXTransformationFunctions: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)

val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")

// Clip raster with WKT geometry (GeoBrix accepts WKT or WKB; cutlineAllTouched = true)
val clipWkt = "POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"
val clipped = rasters.select(
  col("path"),
  rx.rst_clip(col("tile"), lit(clipWkt), lit(true)).alias("clipped_tile")
)
""".trim

  val RasterXCompleteExample: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Register functions
rx.register(spark)

// Read rasters (sample data path)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")

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
""".trim

  val GridXBNGFunctions: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Register functions
bx.register(spark)

// Calculate cell area
val area = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area_sqm")
area.show()

// Convert points to BNG cells (point as WKT; GeoBrix does not accept st_point)
val points = spark.table("uk_locations")
val bngCells = points.select(
  col("location_id"),
  expr("gbx_bng_pointascell(concat('POINT(', cast(longitude as string), ' ', cast(latitude as string), ')'), 1000)").alias("bng_cell")
)

bngCells.show()
""".trim

  // GridXCompleteExample: see docs/tests/scala/api/GridXCompleteExample.snippet (point as WKT)

  val VectorXConversionFunctions: String =
    """
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}
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
""".trim

  // VectorXCompleteExample: see docs/tests-dbr/scala/api/VectorXCompleteExample.snippet (DBR st_* only)

  // =========================================================================
  // Example output (for docs "Example output" blocks via CodeFromTest outputConstant)
  // =========================================================================

  val RegisterRasterX_output: String =
    """
df: DataFrame with bbox column from tile
""".trim

  val RegisterGridX_output: String =
    """
df: DataFrame with area_sqm (e.g. 1000000.0)
""".trim

  val RegisterVectorX_output: String =
    """
df: DataFrame with wkb geometry column
""".trim

  val RasterXAccessorFunctions_output: String =
    """
+--------------------+------------------+-----+------+---------+--------+
|path                |bbox              |width|height|num_bands|metadata|
+--------------------+------------------+-----+------+---------+--------+
|.../nyc_sentinel2...|POLYGON ((-74....)|10980|10980 |1        |{...}   |
+--------------------+------------------+-----+------+---------+--------+
""".trim

  val RasterXTransformationFunctions_output: String =
    """
+--------------------+------------+
|path                |clipped_tile|
+--------------------+------------+
|...                 |[BINARY]    |
+--------------------+------------+
""".trim

  val RasterXCompleteExample_output: String =
    """
Table raster_catalog created with path, bbox, width, height, bands, metadata
""".trim

  val GridXBNGFunctions_output: String =
    """
+---------+
|area_sqm |
+---------+
|1000000.0|
+---------+

+-----------+----------+
|location_id|bng_cell  |
+-----------+----------+
|1          |TQ 31 SW  |
|...        |...       |
+-----------+----------+
""".trim

  val VectorXConversionFunctions_output: String =
    """
Table converted_features: feature_id, wkb_geom, geometry columns
""".trim

}
