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

  // =========================================================================
  // RasterX per-function examples (proof subset for tabbed docs)
  // =========================================================================

  val rst_avg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_avg(col("tile")).alias("band_averages"))
result.show()
""".trim

  val rst_avg_scala_example_output: String =
    """
+-------------+
|band_averages|
+-------------+
|[5.5]        |
+-------------+
""".trim

  val rst_boundingbox_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_boundingbox(col("tile")).alias("bbox"))
result.show(truncate = false)
""".trim

  val rst_boundingbox_scala_example_output: String =
    """
+-----------------------------------------------+
|bbox                                           |
+-----------------------------------------------+
|POLYGON ((-74... 40..., -73... 40..., ...))    |
+-----------------------------------------------+
""".trim

  val rst_numbands_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_numbands(col("tile")).alias("num_bands"))
result.show()
""".trim

  val rst_numbands_scala_example_output: String =
    """
+---------+
|num_bands|
+---------+
|1        |
+---------+
""".trim

  val rst_width_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_width(col("tile")).alias("width"))
result.show()
""".trim

  val rst_width_scala_example_output: String =
    """
+-----+
|width|
+-----+
|4    |
+-----+
""".trim

  val rst_bandmetadata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_bandmetadata(col("tile"), lit(1)).alias("band_meta"))
result.show(truncate = false)
""".trim

  val rst_bandmetadata_scala_example_output: String =
    """
+---------+
|band_meta|
+---------+
|{}       |
+---------+
""".trim

  val rst_format_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_format(col("tile")).alias("format"))
result.show()
""".trim

  val rst_format_scala_example_output: String =
    """
+------+
|format|
+------+
|GTiff |
+------+
""".trim

  val rst_georeference_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_georeference(col("tile")).alias("georeference"))
result.show(truncate = false)
""".trim

  val rst_georeference_scala_example_output: String =
    """
+------------------------------------------------------------+
|                                                georeference|
+------------------------------------------------------------+
|{scaleX -> 0.5, scaleY -> -0.5, upperLeftY -> 50.0, skewX...|
+------------------------------------------------------------+
""".trim

  val rst_getnodata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_getnodata(col("tile")).alias("nodata"))
result.show()
""".trim

  val rst_getnodata_scala_example_output: String =
    """
+---------+
|nodata   |
+---------+
|[-9999.0]|
+---------+
""".trim

  val rst_getsubdataset_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Multi-layer formats such as NetCDF expose each variable as a subdataset.
// rst_getsubdataset extracts one layer by name and returns it as a new tile.
val rasters = spark.read.format("netcdf_gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_climate.nc")
val result = rasters.select(
  rx.rst_width(rx.rst_getsubdataset(col("tile"), lit("temperature"))).alias("width")
)
result.show()
""".trim

  val rst_getsubdataset_scala_example_output: String =
    """
+-----+
|width|
+-----+
|4    |
+-----+
""".trim

  val rst_height_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_height(col("tile")).alias("height"))
result.show()
""".trim

  val rst_height_scala_example_output: String =
    """
+------+
|height|
+------+
|3     |
+------+
""".trim

  val rst_max_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_max(col("tile")).alias("band_max"))
result.show()
""".trim

  val rst_max_scala_example_output: String =
    """
+--------+
|band_max|
+--------+
|[11.0]  |
+--------+
""".trim

  val rst_median_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_median(col("tile")).alias("band_median"))
result.show()
""".trim

  val rst_median_scala_example_output: String =
    """
+-----------+
|band_median|
+-----------+
|[5.5]      |
+-----------+
""".trim

  val rst_memsize_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_memsize(col("tile")).alias("memsize"))
result.show()
""".trim

  val rst_memsize_scala_example_output: String =
    """
+-------+
|memsize|
+-------+
|432    |
+-------+
""".trim

  val rst_metadata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_metadata(col("tile")).alias("metadata"))
result.show(truncate = false)
""".trim

  val rst_metadata_scala_example_output: String =
    """
+--------------------------------------------------+
|                                          metadata|
+--------------------------------------------------+
|{driver -> GTiff, crs -> EPSG:4326, count -> 1,...|
+--------------------------------------------------+
""".trim

  val rst_min_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_min(col("tile")).alias("band_min"))
result.show()
""".trim

  val rst_min_scala_example_output: String =
    """
+--------+
|band_min|
+--------+
|[0.0]   |
+--------+
""".trim

  val rst_pixelcount_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_pixelcount(col("tile")).alias("pixel_count"))
result.show()
""".trim

  val rst_pixelcount_scala_example_output: String =
    """
+-----------+
|pixel_count|
+-----------+
|[12]       |
+-----------+
""".trim

  val rst_pixelheight_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_pixelheight(col("tile")).alias("pixel_height"))
result.show()
""".trim

  val rst_pixelheight_scala_example_output: String =
    """
+------------+
|pixel_height|
+------------+
|0.5         |
+------------+
""".trim

  val rst_pixelwidth_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_pixelwidth(col("tile")).alias("pixel_width"))
result.show()
""".trim

  val rst_pixelwidth_scala_example_output: String =
    """
+-----------+
|pixel_width|
+-----------+
|0.5        |
+-----------+
""".trim

  val rst_rotation_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_rotation(col("tile")).alias("rotation"))
result.show()
""".trim

  val rst_rotation_scala_example_output: String =
    """
+--------+
|rotation|
+--------+
|0.0     |
+--------+
""".trim

  val rst_scalex_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(
  rx.rst_scalex(col("tile")).alias("scale_x"),
  rx.rst_scaley(col("tile")).alias("scale_y")
)
result.show()
""".trim

  val rst_scalex_scala_example_output: String =
    """
+-------+-------+
|scale_x|scale_y|
+-------+-------+
|0.5    |-0.5   |
+-------+-------+
""".trim

  val rst_scaley_scala_example: String = rst_scalex_scala_example
  val rst_scaley_scala_example_output: String = rst_scalex_scala_example_output

  val rst_skewx_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(
  rx.rst_skewx(col("tile")).alias("skew_x"),
  rx.rst_skewy(col("tile")).alias("skew_y")
)
result.show()
""".trim

  val rst_skewx_scala_example_output: String =
    """
+------+------+
|skew_x|skew_y|
+------+------+
|0.0   |0.0   |
+------+------+
""".trim

  val rst_skewy_scala_example: String = rst_skewx_scala_example
  val rst_skewy_scala_example_output: String = rst_skewx_scala_example_output

  val rst_srid_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_srid(col("tile")).alias("srid"))
result.show()
""".trim

  val rst_srid_scala_example_output: String =
    """
+----+
|srid|
+----+
|4326|
+----+
""".trim

  val rst_crs_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_crs(col("tile")).alias("crs"))
result.show(truncate = false)
""".trim

  val rst_crs_scala_example_output: String =
    """
+------------------------------------+
|crs                                 |
+------------------------------------+
|EPSG:4326                           |
+------------------------------------+
""".trim

  val rst_subdatasets_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_subdatasets(col("tile")).alias("subdatasets"))
result.show()
""".trim

  val rst_subdatasets_scala_example_output: String =
    """
+-----------+
|subdatasets|
+-----------+
|{}         |
+-----------+
""".trim

  val rst_summary_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_summary(col("tile")).alias("summary"))
result.show(truncate = false)
""".trim

  val rst_summary_scala_example_output: String =
    """
+------------------------------------------------------------+
|                                                     summary|
+------------------------------------------------------------+
|{driverShortName: GTiff, size: [4, 3], coordinateSystem: ...|
+------------------------------------------------------------+
""".trim

  val rst_type_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_type(col("tile")).alias("band_types"))
result.show()
""".trim

  val rst_type_scala_example_output: String =
    """
+----------+
|band_types|
+----------+
|[Float32] |
+----------+
""".trim

  val rst_upperleftx_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(
  rx.rst_upperleftx(col("tile")).alias("upper_left_x"),
  rx.rst_upperlefty(col("tile")).alias("upper_left_y")
)
result.show()
""".trim

  val rst_upperleftx_scala_example_output: String =
    """
+------------+------------+
|upper_left_x|upper_left_y|
+------------+------------+
|10.0        |50.0        |
+------------+------------+
""".trim

  val rst_upperlefty_scala_example: String = rst_upperleftx_scala_example
  val rst_upperlefty_scala_example_output: String = rst_upperleftx_scala_example_output

  val rst_isempty_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_isempty(col("tile")).alias("is_empty"))
result.show()
""".trim

  val rst_isempty_scala_example_output: String =
    """
+--------+
|is_empty|
+--------+
|false   |
+--------+
""".trim

  val rst_tryopen_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_tryopen(col("tile")).alias("try_open"))
result.show()
""".trim

  val rst_tryopen_scala_example_output: String =
    """
+--------+
|try_open|
+--------+
|true    |
+--------+
""".trim

  val rst_histogram_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val result = rasters.select(rx.rst_histogram(col("tile")).alias("histogram"))
result.show(truncate = false)
""".trim

  val rst_histogram_scala_example_output: String =
    """
+--------------------------------------------------+
|                                         histogram|
+--------------------------------------------------+
|{band_1 -> [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,...|
+--------------------------------------------------+
""".trim

}
