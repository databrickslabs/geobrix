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

// Reads the multiband_rasters Setup view (rgb_nir_small.tif, 3 bands);
// the single-band sentinel2 tile is all-NoData for this function.
val df = spark.table("multiband_rasters")
val result = df.select(rx.rst_avg(col("tile")).alias("band_averages"))
result.show()
""".trim

  val rst_avg_scala_example_output: String =
    """
+------------------------------------+
|band_averages                       |
+------------------------------------+
|[83.59375, 153.125, 114.3125]       |
+------------------------------------+
""".trim

  val rst_boundingbox_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_boundingbox(col("tile")).alias("bbox"))
result.show(truncate = false)
""".trim

  val rst_boundingbox_scala_example_output: String =
    """
+----+
|bbox|
+----+
|[...|
+----+
(WKB binary — bounding POLYGON of the raster extent in EPSG:32618)
""".trim

  val rst_numbands_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands: red, NIR, green).
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_numbands(col("tile")).alias("num_bands"))
result.show()
""".trim

  val rst_numbands_scala_example_output: String =
    """
+---------+
|num_bands|
+---------+
|3        |
+---------+
""".trim

  val rst_width_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_width(col("tile")).alias("width"))
result.show()
""".trim

  val rst_width_scala_example_output: String =
    """
+-----+
|width|
+-----+
|236  |
+-----+
""".trim

  val rst_bandmetadata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif) which carries per-band GDAL metadata tags.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_bandmetadata(col("tile"), lit(1)).alias("band_meta"))
result.show(truncate = false)
""".trim

  val rst_bandmetadata_scala_example_output: String =
    """
+----------------------------------------------+
|band_meta                                     |
+----------------------------------------------+
|{name -> red, wavelength_nm -> 665, band_in...|
+----------------------------------------------+
""".trim

  val rst_format_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
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

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_georeference(col("tile")).alias("georeference"))
result.show(truncate = false)
""".trim

  val rst_georeference_scala_example_output: String =
    """
+--------------------------------------------------------------+
|georeference                                                  |
+--------------------------------------------------------------+
|{scaleX -> 10.0, scaleY -> -10.0, upperLeftX -> 2121950.0,... |
+--------------------------------------------------------------+
""".trim

  val rst_getnodata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_getnodata(col("tile")).alias("nodata"))
result.show()
""".trim

  val rst_getnodata_scala_example_output: String =
    """
+--------+
|nodata  |
+--------+
|[0.0]   |
+--------+
""".trim

  val rst_getsubdataset_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses committed CMIP5 NetCDF fixture (has time_bnds and prAdjust subdatasets).
// Subdatasets require a multi-layer format such as NetCDF.
// rst_width wraps the result to return a real scalar proving extraction.
val rasters = spark.table("netcdf_rasters")
val result = rasters.select(
  rx.rst_width(rx.rst_getsubdataset(col("tile"), lit("prAdjust"))).alias("width")
)
result.show()
""".trim

  val rst_getsubdataset_scala_example_output: String =
    """
+-----+
|width|
+-----+
|  720|
+-----+
(width of the extracted prAdjust subdataset — 720 pixels, 31 bands, 360 rows)
""".trim

  val rst_height_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_height(col("tile")).alias("height"))
result.show()
""".trim

  val rst_height_scala_example_output: String =
    """
+------+
|height|
+------+
|161   |
+------+
""".trim

  val rst_max_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands); single-band sentinel2 is all-NoData.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_max(col("tile")).alias("band_max"))
result.show()
""".trim

  val rst_max_scala_example_output: String =
    """
+---------------------+
|band_max             |
+---------------------+
|[119.0, 197.0, 148.0]|
+---------------------+
""".trim

  val rst_median_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands); single-band sentinel2 is all-NoData.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_median(col("tile")).alias("band_median"))
result.show()
""".trim

  val rst_median_scala_example_output: String =
    """
+---------------------+
|band_median          |
+---------------------+
|[85.0, 157.5, 111.5] |
+---------------------+
""".trim

  val rst_memsize_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_memsize(col("tile")).alias("memsize"))
result.show()
""".trim

  val rst_memsize_scala_example_output: String =
    """
+-------+
|memsize|
+-------+
|71749  |
+-------+
""".trim

  val rst_metadata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_metadata(col("tile")).alias("metadata"))
result.show(truncate = false)
""".trim

  val rst_metadata_scala_example_output: String =
    """
+--------------------------------------------------+
|metadata                                          |
+--------------------------------------------------+
|{driver -> GTiff, crs -> EPSG:32618, count -> 1,..|
+--------------------------------------------------+
""".trim

  val rst_min_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands); single-band sentinel2 is all-NoData.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_min(col("tile")).alias("band_min"))
result.show()
""".trim

  val rst_min_scala_example_output: String =
    """
+-------------------+
|band_min           |
+-------------------+
|[50.0, 102.0, 82.0]|
+-------------------+
""".trim

  val rst_pixelcount_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 8x8, no NoData); single-band sentinel2 returns [0].
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_pixelcount(col("tile")).alias("pixel_count"))
result.show()
""".trim

  val rst_pixelcount_scala_example_output: String =
    """
+------------+
|pixel_count |
+------------+
|[64, 64, 64]|
+------------+
""".trim

  val rst_pixelheight_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_pixelheight(col("tile")).alias("pixel_height"))
result.show()
""".trim

  val rst_pixelheight_scala_example_output: String =
    """
+------------+
|pixel_height|
+------------+
|10.0        |
+------------+
""".trim

  val rst_pixelwidth_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_pixelwidth(col("tile")).alias("pixel_width"))
result.show()
""".trim

  val rst_pixelwidth_scala_example_output: String =
    """
+-----------+
|pixel_width|
+-----------+
|10.0       |
+-----------+
""".trim

  val rst_rotation_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
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

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_scalex(col("tile")).alias("scale_x"))
result.show()
""".trim

  val rst_scalex_scala_example_output: String =
    """
+-------+
|scale_x|
+-------+
|10.0   |
+-------+
""".trim

  val rst_scaley_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_scaley(col("tile")).alias("scale_y"))
result.show()
""".trim

  val rst_scaley_scala_example_output: String =
    """
+-------+
|scale_y|
+-------+
|-10.0  |
+-------+
""".trim

  val rst_skewx_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_skewx(col("tile")).alias("skew_x"))
result.show()
""".trim

  val rst_skewx_scala_example_output: String =
    """
+------+
|skew_x|
+------+
|0.0   |
+------+
""".trim

  val rst_skewy_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_skewy(col("tile")).alias("skew_y"))
result.show()
""".trim

  val rst_skewy_scala_example_output: String =
    """
+------+
|skew_y|
+------+
|0.0   |
+------+
""".trim

  val rst_srid_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_srid(col("tile")).alias("srid"))
result.show()
""".trim

  val rst_srid_scala_example_output: String =
    """
+-----+
|srid |
+-----+
|32618|
+-----+
""".trim

  val rst_crs_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_crs(col("tile")).alias("crs"))
result.show(truncate = false)
""".trim

  val rst_crs_scala_example_output: String =
    """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
""".trim

  val rst_subdatasets_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses committed CMIP5 NetCDF fixture (has time_bnds and prAdjust subdatasets).
val rasters = spark.table("netcdf_rasters")
val result = rasters.select(rx.rst_subdatasets(col("tile")).alias("subdatasets"))
result.show(truncate = false)
""".trim

  val rst_subdatasets_scala_example_output: String =
    """
+------------------------------------------------------+
|subdatasets                                           |
+------------------------------------------------------+
|{SUBDATASET_1_NAME -> ..., SUBDATASET_1_DESC -> [31...|
+------------------------------------------------------+
(map with SUBDATASET_1_NAME/DESC for time_bnds and SUBDATASET_2_NAME/DESC for prAdjust)
""".trim

  val rst_summary_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands) which has real pixel data.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_summary(col("tile")).alias("summary"))
result.show(truncate = false)
""".trim

  val rst_summary_scala_example_output: String =
    """
+------------------------------------------------------------+
|summary                                                     |
+------------------------------------------------------------+
|{"driverShortName": "GTiff", "size": [8, 8], "coordinateS...|
+------------------------------------------------------------+
""".trim

  val rst_type_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands, UInt16).
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_type(col("tile")).alias("band_types"))
result.show()
""".trim

  val rst_type_scala_example_output: String =
    """
+-----------------------------+
|band_types                   |
+-----------------------------+
|[UInt16, UInt16, UInt16]     |
+-----------------------------+
""".trim

  val rst_upperleftx_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_upperleftx(col("tile")).alias("upper_left_x"))
result.show()
""".trim

  val rst_upperleftx_scala_example_output: String =
    """
+------------+
|upper_left_x|
+------------+
|2121950.0   |
+------------+
""".trim

  val rst_upperlefty_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_upperlefty(col("tile")).alias("upper_left_y"))
result.show()
""".trim

  val rst_upperlefty_scala_example_output: String =
    """
+---------------+
|upper_left_y   |
+---------------+
|-10790470.0    |
+---------------+
""".trim

  val rst_isempty_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif) which carries real pixel data.
// The single-band sentinel2 tile has NoData=0 with all pixels equal zero (isempty=true).
val rasters = spark.table("multiband_rasters")
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

// Uses multiband fixture (rgb_nir_small.tif — committed, always openable).
val rasters = spark.table("multiband_rasters")
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

// Uses multiband fixture (rgb_nir_small.tif, 3 bands) so histogram has entries per band.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_histogram(col("tile")).alias("histogram"))
result.show(truncate = false)
""".trim

  val rst_histogram_scala_example_output: String =
    """
+--------------------------------------------------+
|histogram                                         |
+--------------------------------------------------+
|{band_1 -> [1, 0, 0, ...], band_2 -> [1, 0, 1,... |
+--------------------------------------------------+
""".trim

  // ===========================================================================
  // Tile ops & constructors family (Scala)
  // ===========================================================================

  val rst_asformat_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_asformat(col("tile"), lit("GTiff")).alias("tile"))
result.show()
""".trim

  val rst_asformat_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(re-encoded tile in the requested GDAL format)
""".trim

  val rst_band_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands) to demonstrate band extraction.
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_band(col("tile"), lit(1)).alias("tile"))
result.show()
""".trim

  val rst_band_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single band extracted from the 3-band multiband fixture)
""".trim

  val rst_buildoverviews_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_buildoverviews(col("tile"), array(lit(2), lit(4))).alias("tile"))
result.show()
""".trim

  val rst_buildoverviews_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with internal overviews at levels [2, 4] embedded)
""".trim

  val rst_clip_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// WKT polygon in raster's native CRS (EPSG:32618); no SRID prefix = no reprojection.
// To clip with a WGS84 geometry use EWKT: "SRID=4326;POLYGON(...)".
val rasters = spark.table("rasters")
val clipGeom = "POLYGON((2121950 -10791280, 2123140 -10791280, 2123140 -10790470, 2121950 -10790470, 2121950 -10791280))"
val result = rasters.select(rx.rst_clip(col("tile"), lit(clipGeom), lit(true)).alias("tile"))
result.show()
""".trim

  val rst_clip_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(clipped tile; polygon is in the raster's native CRS (no SRID = no reprojection))
""".trim

  val rst_cog_convert_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_cog_convert(col("tile")).alias("tile"))
result.show()
""".trim

  val rst_cog_convert_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(COG tile; a COG is a valid GeoTIFF with tiled internal layout)
""".trim

  val rst_convolve_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val kernel = array(
  array(lit(0.0), lit(0.0), lit(0.0)),
  array(lit(0.0), lit(1.0), lit(0.0)),
  array(lit(0.0), lit(0.0), lit(0.0))
)
val result = rasters.select(rx.rst_convolve(col("tile"), kernel).alias("tile"))
result.show()
""".trim

  val rst_convolve_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(convolved tile; kernel is a 3x3 identity)
""".trim

  val rst_fillnodata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_fillnodata(col("tile"), lit(100.0), lit(0)).alias("tile"))
result.show()
""".trim

  val rst_fillnodata_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(filled tile; NoData holes searched within 100 pixels)
""".trim

  val rst_filter_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_filter(col("tile"), lit(3), lit("median")).alias("tile"))
result.show()
""".trim

  val rst_filter_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(filtered tile; 3x3 median filter applied)
""".trim

  val rst_fromcontent_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Constructor: binaryFile reader + rst_fromcontent is the canonical tier-agnostic pattern.
// binaryFile runs in Spark (holds the Volume credential) and works on any compute.
val binary = spark.read.format("binaryFile")
  .load("/Volumes/main/default/test-data/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
val tiles = binary.select(rx.rst_fromcontent(col("content"), lit("GTiff")).alias("tile"))
val result = tiles.select(rx.rst_format(col("tile")).alias("format"))
result.show()
""".trim

  val rst_fromcontent_scala_example_output: String =
    """
+------+
|format|
+------+
|GTiff |
+------+
(format of the tile loaded from binary content via binaryFile reader)
""".trim

  val rst_frombands_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Constructor: extract per-band tiles from the multiband fixture, then stack them back.
val rasters = spark.table("multiband_rasters")
val withBands = rasters.select(
  array(
    rx.rst_band(col("tile"), lit(1)),
    rx.rst_band(col("tile"), lit(2)),
    rx.rst_band(col("tile"), lit(3))
  ).alias("bands")
)
val result = withBands.select(rx.rst_frombands(col("bands")).alias("tile"))
result.show()
""".trim

  val rst_frombands_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(3 per-band tiles stacked back into a 3-band tile)
""".trim

  val rst_initnodata_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_initnodata(col("tile")).alias("tile"))
result.show()
""".trim

  val rst_initnodata_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with NoData initialized)
""".trim

  val rst_resample_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_resample(col("tile"), lit(2.0), lit("bilinear")).alias("tile"))
result.show()
""".trim

  val rst_resample_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(2x bilinear upsampled tile; source is 236x161 px)
""".trim

  val rst_resample_to_res_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_resample_to_res(col("tile"), lit(20.0), lit(20.0), lit("average")).alias("tile"))
result.show()
""".trim

  val rst_resample_to_res_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(downsampled tile; 10 m to 20 m resolution)
""".trim

  val rst_resample_to_size_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_resample_to_size(col("tile"), lit(100), lit(100), lit("near")).alias("tile"))
result.show()
""".trim

  val rst_resample_to_size_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(resampled tile forced to 100x100 pixels)
""".trim

  val rst_setcrs_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_crs(rx.rst_setcrs(col("tile"), lit("EPSG:32618"))).alias("crs"))
result.show()
""".trim

  val rst_setcrs_scala_example_output: String =
    """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
(CRS string after stamping; does NOT reproject — use rst_transformcrs to reproject)
""".trim

  val rst_setsrid_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_setsrid(col("tile"), lit(32618)).alias("tile"))
result.show()
""".trim

  val rst_setsrid_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with SRID stamped to 32618; does NOT reproject — use rst_transform to reproject)
""".trim

  val rst_threshold_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_threshold(col("tile"), lit(">"), lit(0.0)).alias("tile"))
result.show()
""".trim

  val rst_threshold_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(binary mask tile; pixels > 0.0 → 1, others → 0)
""".trim

  val rst_transform_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_transform(col("tile"), lit(4326)).alias("tile"))
result.show()
""".trim

  val rst_transform_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(reprojected tile; source EPSG:32618 (UTM Zone 18N) to EPSG:4326)
""".trim

  val rst_transformcrs_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_crs(rx.rst_transformcrs(col("tile"), lit("EPSG:3857"))).alias("crs"))
result.show()
""".trim

  val rst_transformcrs_scala_example_output: String =
    """
+----------+
|crs       |
+----------+
|EPSG:3857 |
+----------+
(CRS string of the reprojected tile; accepts authority codes, WKT, or PROJ4)
""".trim

  val rst_updatetype_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val rasters = spark.table("rasters")
val result = rasters.select(rx.rst_updatetype(col("tile"), lit("Float32")).alias("tile"))
result.show()
""".trim

  val rst_updatetype_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(type-converted tile; use rst_type to confirm the new data type)
""".trim

  // ===========================================================================
  // Aggregators family (Scala)
  // ===========================================================================

  val rst_combineavg_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Multi-tile fixture: load multiband tif and split to 3 per-band rows (same grid).
val mb = spark.read.format("gdal").load("src/test/resources/binary/geotiff-small/rgb_nir_small.tif")
val b1 = mb.select(rx.rst_band(col("tile"), lit(1)).alias("tile")).withColumn("region", lit("R1"))
val b2 = mb.select(rx.rst_band(col("tile"), lit(2)).alias("tile")).withColumn("region", lit("R1"))
val b3 = mb.select(rx.rst_band(col("tile"), lit(3)).alias("tile")).withColumn("region", lit("R1"))
val df = b1.union(b2).union(b3)
val result = df.groupBy("region").agg(rx.rst_combineavg_agg(col("tile")).alias("avg"))
result.show()
""".trim

  val rst_combineavg_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|avg                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_derivedband_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Multi-tile fixture: load multiband tif and split to 3 per-band rows.
val mb = spark.read.format("gdal").load("src/test/resources/binary/geotiff-small/rgb_nir_small.tif")
val b1 = mb.select(rx.rst_band(col("tile"), lit(1)).alias("tile")).withColumn("region", lit("R1"))
val b2 = mb.select(rx.rst_band(col("tile"), lit(2)).alias("tile")).withColumn("region", lit("R1"))
val b3 = mb.select(rx.rst_band(col("tile"), lit(3)).alias("tile")).withColumn("region", lit("R1"))
val df = b1.union(b2).union(b3)
val fn = "def fn(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n    out_ar[:] = in_ar[0]\n"
val result = df.groupBy("region").agg(rx.rst_derivedband_agg(col("tile"), fn, "fn").alias("derived"))
result.show()
""".trim

  val rst_derivedband_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|derived                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_frombands_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Multi-tile fixture: load multiband tif and split to 3 per-band rows with band_index.
val mb = spark.read.format("gdal").load("src/test/resources/binary/geotiff-small/rgb_nir_small.tif")
val b1 = mb.select(rx.rst_band(col("tile"), lit(1)).alias("tile")).withColumn("band_index", lit(1)).withColumn("region", lit("R1"))
val b2 = mb.select(rx.rst_band(col("tile"), lit(2)).alias("tile")).withColumn("band_index", lit(2)).withColumn("region", lit("R1"))
val b3 = mb.select(rx.rst_band(col("tile"), lit(3)).alias("tile")).withColumn("band_index", lit(3)).withColumn("region", lit("R1"))
val df = b1.union(b2).union(b3)
val result = df.groupBy("region").agg(rx.rst_frombands_agg(col("tile"), col("band_index")).alias("stacked"))
result.show()
""".trim

  val rst_frombands_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|stacked                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_merge_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Multi-tile fixture: load multiband tif and split to 3 per-band rows.
val mb = spark.read.format("gdal").load("src/test/resources/binary/geotiff-small/rgb_nir_small.tif")
val b1 = mb.select(rx.rst_band(col("tile"), lit(1)).alias("tile")).withColumn("region", lit("R1"))
val b2 = mb.select(rx.rst_band(col("tile"), lit(2)).alias("tile")).withColumn("region", lit("R1"))
val b3 = mb.select(rx.rst_band(col("tile"), lit(3)).alias("tile")).withColumn("region", lit("R1"))
val df = b1.union(b2).union(b3)
val result = df.groupBy("region").agg(rx.rst_merge_agg(col("tile")).alias("mosaic"))
result.show()
""".trim

  val rst_merge_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|mosaic                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_rasterize_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Multi-row fixture: 3 rows with WKB polygons + burn values over a 4x4 EPSG:4326 canvas.
spark.sql("CREATE OR REPLACE TEMP VIEW _rst_agg_src AS SELECT 'R1' AS region, unhex('010300000001000000050000000000000000000000000000000000000000000000000000000000000000001040000000000000104000000000000000000000000000001040000000000000104000000000000000000000000000000000') AS geom, 1.0 AS value UNION ALL SELECT 'R1', unhex('010300000001000000050000000000000000000000000000000000000000000000000000000000000000001040000000000000104000000000000000000000000000001040000000000000104000000000000000000000000000000000'), 2.0 UNION ALL SELECT 'R1', unhex('010300000001000000050000000000000000000000000000000000000000000000000000000000000000001040000000000000104000000000000000000000000000001040000000000000104000000000000000000000000000000000'), 3.0")
val df = spark.table("_rst_agg_src")
val result = df.groupBy("region").agg(
  rx.rst_rasterize_agg(col("geom"), col("value"), lit(0.0), lit(0.0), lit(4.0), lit(4.0), lit(8), lit(8), lit(4326)).alias("burned")
)
result.show()
""".trim

  val rst_rasterize_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|burned                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_gridfrompoints_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.{ByteBuffer, ByteOrder}

rx.register(spark)
// Multi-row fixture: 4 WKB POINT rows with scalar observations, [0,0,1,1] EPSG:4326 extent.
def mkPt(x: Double, y: Double): Array[Byte] = {
  val buf = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
  buf.put(1.toByte); buf.putInt(1); buf.putDouble(x); buf.putDouble(y); buf.array()
}
val schema = StructType(Seq(StructField("pt", BinaryType()), StructField("val", DoubleType()), StructField("region", StringType())))
val rows = Seq((mkPt(0.1,0.1),10.0,"R1"),(mkPt(0.9,0.1),20.0,"R1"),(mkPt(0.1,0.9),30.0,"R1"),(mkPt(0.9,0.9),40.0,"R1"))
val df = spark.createDataFrame(spark.sparkContext.parallelize(rows.map { case (p,v,r) => org.apache.spark.sql.Row(p,v,r) }), schema)
val result = df.groupBy("region").agg(
  rx.rst_gridfrompoints_agg(col("pt"), col("val"), lit(0.0), lit(0.0), lit(1.0), lit(1.0), lit(8), lit(8), lit(4326)).alias("idw")
)
result.show()
""".trim

  val rst_gridfrompoints_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|idw                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_dtmfromgeoms_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.{ByteBuffer, ByteOrder}

rx.register(spark)
// Multi-row fixture: 4 WKB POINT Z rows over [0,0,1,1] EPSG:4326.
def mkPtZ(x: Double, y: Double, z: Double): Array[Byte] = {
  val buf = ByteBuffer.allocate(29).order(ByteOrder.LITTLE_ENDIAN)
  buf.put(1.toByte); buf.putInt(1001); buf.putDouble(x); buf.putDouble(y); buf.putDouble(z); buf.array()
}
val schema = StructType(Seq(StructField("pt", BinaryType()), StructField("region", StringType())))
val rows = Seq((mkPtZ(0.1,0.1,100.0),"R1"),(mkPtZ(0.9,0.1,200.0),"R1"),(mkPtZ(0.1,0.9,150.0),"R1"),(mkPtZ(0.9,0.9,250.0),"R1"))
val df = spark.createDataFrame(spark.sparkContext.parallelize(rows.map { case (p,r) => org.apache.spark.sql.Row(p,r) }), schema)
val result = df.groupBy("region").agg(
  rx.rst_dtmfromgeoms_agg(col("pt"), lit(null).cast("array<binary>"), lit(0.0), lit(0.0), lit(0.0), lit(0.0), lit(1.0), lit(1.0), lit(8), lit(8), lit(4326)).alias("dtm")
)
result.show()
""".trim

  val rst_dtmfromgeoms_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|dtm                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_h3_rasterize_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

rx.register(spark)
// Multi-row fixture: 3 H3 resolution-9 cell ids (as BIGINT) with burn values.
val schema = StructType(Seq(
  StructField("cellid", LongType()), StructField("value", DoubleType()), StructField("region", StringType())))
// H3 res-9 cells near origin
val rows = Seq((617733151020810239L, 1.0, "R1"), (617733151021334527L, 2.0, "R1"), (617733151085035519L, 3.0, "R1"))
val df = spark.createDataFrame(spark.sparkContext.parallelize(rows.map(r => org.apache.spark.sql.Row(r._1,r._2,r._3))), schema)
val result = df.groupBy("region").agg(rx.rst_h3_rasterize_agg(col("cellid"), col("value")).alias("tile"))
result.show()
""".trim

  val rst_h3_rasterize_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_quadbin_rasterize_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.{functions => gx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

rx.register(spark)
gx.register(spark)
// Multi-row fixture: 3 quadbin zoom-12 cells near central London.
val schema = StructType(Seq(StructField("region", StringType()), StructField("lon", DoubleType()), StructField("lat", DoubleType()), StructField("val", DoubleType())))
val rows = Seq(("R1",-0.10,51.50,1.0),("R1",-0.11,51.51,2.0),("R1",-0.09,51.49,3.0))
val raw = spark.createDataFrame(spark.sparkContext.parallelize(rows.map(r => org.apache.spark.sql.Row(r._1,r._2,r._3,r._4))), schema)
raw.createOrReplaceTempView("_qb_src")
val df = spark.sql("SELECT region, gbx_quadbin_pointascell(lon, lat, 12) AS cellid, val AS value FROM _qb_src")
val result = df.groupBy("region").agg(rx.rst_quadbin_rasterize_agg(col("cellid"), col("value")).alias("tile"))
result.show()
""".trim

  val rst_quadbin_rasterize_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  val rst_bng_rasterize_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bng}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

rx.register(spark)
bng.register(spark)
// Multi-row fixture: 3 BNG 1km cells near central London (EPSG:27700).
val schema = StructType(Seq(StructField("region", StringType()), StructField("e", DoubleType()), StructField("n", DoubleType()), StructField("val", DoubleType())))
val rows = Seq(("R1",530000.0,180000.0,1.0),("R1",531000.0,181000.0,2.0),("R1",529000.0,179000.0,3.0))
val raw = spark.createDataFrame(spark.sparkContext.parallelize(rows.map(r => org.apache.spark.sql.Row(r._1,r._2,r._3,r._4))), schema)
raw.createOrReplaceTempView("_bng_src")
val df = spark.sql("SELECT region, gbx_bng_eastnorthasbng(e, n, 3) AS cellid, val AS value FROM _bng_src")
val result = df.groupBy("region").agg(rx.rst_bng_rasterize_agg(col("cellid"), col("value")).alias("tile"))
result.show()
""".trim

  val rst_bng_rasterize_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
""".trim

  // =========================================================================
  // Band-math examples (tabbed docs: 10 functions)
  // =========================================================================

  val rst_ndvi_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture (rgb_nir_small.tif, 3 bands: red=1, NIR=2, green=3)
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_ndvi(col("tile"), lit(1), lit(2)).alias("ndvi"))
result.show(truncate = false)
""".trim

  val rst_ndvi_scala_example_output: String =
    """
+-----------------------------------------------------------+
|ndvi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDVI raster: (NIR-Red)/(NIR+Red))
""".trim

  val rst_evi_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Uses multiband fixture with red, NIR, and green (as blue)
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_evi(col("tile"), lit(1), lit(2), lit(3)).alias("evi"))
result.show(truncate = false)
""".trim

  val rst_evi_scala_example_output: String =
    """
+-----------------------------------------------------------+
|evi                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band EVI raster: G*(NIR-Red)/(NIR+C1*Red-C2*Blue+L))
""".trim

  val rst_savi_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Soil-Adjusted Vegetation Index: uses red (band 1) and NIR (band 2)
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_savi(col("tile"), lit(1), lit(2)).alias("savi"))
result.show(truncate = false)
""".trim

  val rst_savi_scala_example_output: String =
    """
+-----------------------------------------------------------+
|savi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band SAVI raster: (NIR-Red)/(NIR+Red+L)*(1+L))
""".trim

  val rst_ndwi_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Normalized Difference Water Index: green (band 3) and NIR (band 2)
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_ndwi(col("tile"), lit(3), lit(2)).alias("ndwi"))
result.show(truncate = false)
""".trim

  val rst_ndwi_scala_example_output: String =
    """
+-----------------------------------------------------------+
|ndwi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDWI raster: (Green-NIR)/(Green+NIR))
""".trim

  val rst_nbr_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Normalized Burn Ratio: NIR (band 2) and green (band 3) as SWIR substitute
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_nbr(col("tile"), lit(2), lit(3)).alias("nbr"))
result.show(truncate = false)
""".trim

  val rst_nbr_scala_example_output: String =
    """
+-----------------------------------------------------------+
|nbr                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NBR raster: (NIR-SWIR)/(NIR+SWIR))
""".trim

  val rst_index_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Generic index dispatcher: computes NDVI via named formula with band map
val rasters = spark.table("multiband_rasters")
val bandMap = create_map(lit("red"), lit(1), lit("nir"), lit(2))
val result = rasters.select(rx.rst_index(col("tile"), lit("ndvi"), bandMap).alias("index"))
result.show(truncate = false)
""".trim

  val rst_index_scala_example_output: String =
    """
+-----------------------------------------------------------+
|index                                                      |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band index raster from named formula)
""".trim

  val rst_combineavg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Multi-row fixture: 3 single-band tiles (one per band from multiband GeoTIFF)
val schema = StructType(Seq(StructField("tile", BinaryType()), StructField("band_index", IntegerType()), StructField("region", StringType())))
// Load multiband, extract 3 bands, collect into array, then average
val multiband = spark.table("multiband_rasters")
val b1 = multiband.select(rx.rst_band(col("tile"), lit(1)).alias("tile")).withColumn("band_index", lit(1))
val b2 = multiband.select(rx.rst_band(col("tile"), lit(2)).alias("tile")).withColumn("band_index", lit(2))
val b3 = multiband.select(rx.rst_band(col("tile"), lit(3)).alias("tile")).withColumn("band_index", lit(3))
val bands = b1.union(b2).union(b3).withColumn("region", lit("R1"))
val result = bands.groupBy("region").agg(rx.rst_combineavg(collect_list("tile")).alias("combined"))
result.show(truncate = false)
""".trim

  val rst_combineavg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|combined                                                   |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(averaged combined raster)
""".trim

  val rst_derivedband_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Apply a GDAL VRT Python pixel-function that doubles band 1
val rasters = spark.table("multiband_rasters")
val pythonFunc = "def double(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n    out_ar[:] = in_ar[0] * 2\n"
val result = rasters.select(rx.rst_derivedband(col("tile"), lit(pythonFunc), lit("double")).alias("derived"))
result.show(truncate = false)
""".trim

  val rst_derivedband_scala_example_output: String =
    """
+-----------------------------------------------------------+
|derived                                                    |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(derived band raster from Python UDF)
""".trim

  val rst_mapalgebra_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Map algebra: scale band values by factor of 2
val rasters = spark.table("multiband_rasters")
val result = rasters.select(rx.rst_mapalgebra(array(col("tile")), lit("A * 2")).alias("scaled"))
result.show(truncate = false)
""".trim

  val rst_mapalgebra_scala_example_output: String =
    """
+-----------------------------------------------------------+
|scaled                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(result raster from map algebra: A * 2)
""".trim

  val rst_merge_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Multi-row fixture: 3 single-band tiles (one per band from multiband GeoTIFF)
val multiband = spark.table("multiband_rasters")
val b1 = multiband.select(rx.rst_band(col("tile"), lit(1)).alias("tile")).withColumn("band_index", lit(1))
val b2 = multiband.select(rx.rst_band(col("tile"), lit(2)).alias("tile")).withColumn("band_index", lit(2))
val b3 = multiband.select(rx.rst_band(col("tile"), lit(3)).alias("tile")).withColumn("band_index", lit(3))
val bands = b1.union(b2).union(b3).withColumn("region", lit("R1"))
val result = bands.groupBy("region").agg(rx.rst_merge(collect_list("tile")).alias("merged"))
result.show(truncate = false)
""".trim

  val rst_merge_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|merged                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(merged raster from aligned tiles)
""".trim

  // ============================================================================
  // Terrain Analysis Functions
  // ============================================================================

  val rst_slope_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_slope(col("tile"), lit("degrees"), lit(1.0), lit(1.0)).alias("slope"))
result.show(truncate = false)
""".trim

  val rst_slope_scala_example_output: String =
    """
+-----------------------------------------------------------+
|slope                                                      |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(slope in degrees; auto-scaled from raster CRS units)
""".trim

  val rst_aspect_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_aspect(col("tile"), lit(false), lit(false)).alias("aspect"))
result.show(truncate = false)
""".trim

  val rst_aspect_scala_example_output: String =
    """
+-----------------------------------------------------------+
|aspect                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(aspect in compass degrees: 0=N, 90=E, 180=S, 270=W)
""".trim

  val rst_hillshade_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_hillshade(col("tile"), lit(315.0), lit(45.0), lit(1.0)).alias("shade"))
result.show(truncate = false)
""".trim

  val rst_hillshade_scala_example_output: String =
    """
+-----------------------------------------------------------+
|shade                                                      |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(8-bit hillshade: NW azimuth, 45-degree altitude)
""".trim

  val rst_tri_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_tri(col("tile")).alias("tri"))
result.show(truncate = false)
""".trim

  val rst_tri_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tri                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TRI: mean absolute neighbour difference)
""".trim

  val rst_tpi_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_tpi(col("tile")).alias("tpi"))
result.show(truncate = false)
""".trim

  val rst_tpi_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tpi                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TPI: positive=ridge, negative=valley)
""".trim

  val rst_roughness_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_roughness(col("tile")).alias("roughness"))
result.show(truncate = false)
""".trim

  val rst_roughness_scala_example_output: String =
    """
+-----------------------------------------------------------+
|roughness                                                  |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(roughness: max absolute difference in 3x3 window)
""".trim

  val rst_color_relief_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_color_relief(col("tile"), lit("src/test/resources/binary/elevation/elevation.clr")).alias("rgba"))
result.show(truncate = false)
""".trim

  val rst_color_relief_scala_example_output: String =
    """
+-----------------------------------------------------------+
|rgba                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(4-band RGBA tile mapped via gdaldem color table)
""".trim

  val rst_proximity_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_proximity(col("tile"), lit(""), lit("PIXEL"), lit(100.0)).alias("distance"))
result.show(truncate = false)
""".trim

  val rst_proximity_scala_example_output: String =
    """
+-----------------------------------------------------------+
|distance                                                   |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(distance in pixels to nearest non-NoData, capped at 100)
""".trim

  val rst_contour_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_contour(col("tile"), array(), lit(50.0), lit(0.0), lit("elev")).alias("contours"))
result.show(truncate = false)
""".trim

  val rst_contour_scala_example_output: String =
    """
+--------------------------------------+
|contours                              |
+--------------------------------------+
|[{[BINARY], 100.0}, {[BINARY], 200.0}]|
+--------------------------------------+
(array of contour features: LineString + elevation value)
""".trim

  val rst_viewshed_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(
  rx.rst_viewshed(col("tile"), lit("POINT(-73.5 40.5)"), lit(100.0), lit(1.6), lit(5000.0)).alias("viewshed")
)
result.show(truncate = false)
""".trim

  val rst_viewshed_scala_example_output: String =
    """
+-----------------------------------------------------------+
|viewshed                                                   |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(binary visibility mask: 1=visible, 0=not visible)
""".trim

  val rst_sample_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val dem = spark.table("dem_rasters")
val result = dem.select(rx.rst_sample(col("tile"), lit("SRID=4326;POINT(-73.97 40.75)")).alias("sampled"))
result.show(truncate = false)
""".trim

  val rst_sample_scala_example_output: String =
    """
+--------+
|sampled |
+--------+
|[1234.5]|
+--------+
(array of sampled values, one per band)
""".trim

  val rst_gridfrompoints_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Create synthetic point cloud with WKB-encoded points and values
val points = spark.createDataFrame(Seq(
  (Array[Array[Byte]](/* WKB point 1 */, /* WKB point 2 */), Array[Double](100.0, 110.0))
)).toDF("points_wkb", "values")
val result = points.select(
  rx.rst_gridfrompoints(col("points_wkb"), col("values"), lit(0.0), lit(0.0),
    lit(1000.0), lit(1000.0), lit(256), lit(256), lit(32633)).alias("idw")
)
result.show(truncate = false)
""".trim

  val rst_gridfrompoints_scala_example_output: String =
    """
+-----------------------------------------------------------+
|idw                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(IDW-interpolated tile over specified extent)
""".trim

  val rst_dtmfromgeoms_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
// Create synthetic survey points with Z-valued WKB geometries
val survey = spark.createDataFrame(Seq(
  (Array[Array[Byte]](/* WKB point 1 */, /* WKB point 2 */), Array[Array[Byte]]())
)).toDF("points_wkb", "breaklines_wkb")
val result = survey.select(
  rx.rst_dtmfromgeoms(col("points_wkb"), col("breaklines_wkb"), lit(0.0), lit(0.01),
    lit(0.0), lit(0.0), lit(1000.0), lit(1000.0), lit(100), lit(100), lit(32633)).alias("tin")
)
result.show(truncate = false)
""".trim

  val rst_dtmfromgeoms_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tin                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TIN-interpolated DTM over specified extent and pixel count)
""".trim

  // ========================================================================
  // Coordinate Transforms & Tiling
  // ========================================================================

  val rst_rastertoworldcoord_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_rastertoworldcoord(col("tile"), lit(100), lit(80)).alias("world_coord"))
result.show(truncate = false)
""".trim

  val rst_rastertoworldcoord_scala_example_output: String =
    """
+-------------------+
|world_coord        |
+-------------------+
|{500980.0, ...}    |
+-------------------+
(struct with x: DOUBLE, y: DOUBLE)
""".trim

  val rst_rastertoworldcoordx_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_rastertoworldcoordx(col("tile"), lit(100), lit(80)).alias("easting"))
result.show(truncate = false)
""".trim

  val rst_rastertoworldcoordx_scala_example_output: String =
    """
+-------+
|easting|
+-------+
|500980 |
+-------+
"""

  val rst_rastertoworldcoordy_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_rastertoworldcoordy(col("tile"), lit(100), lit(80)).alias("northing"))
result.show(truncate = false)
""".trim

  val rst_rastertoworldcoordy_scala_example_output: String =
    """
+--------+
|northing|
+--------+
|4599220 |
+--------+
"""

  val rst_worldtorastercoord_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_worldtorastercoord(col("tile"), lit(554880), lit(4545120)).alias("pixel_coord"))
result.show(truncate = false)
""".trim

  val rst_worldtorastercoord_scala_example_output: String =
    """
+-----------+
|pixel_coord|
+-----------+
|{5490, ...}|
+-----------+
(struct with x: INT, y: INT)
"""

  val rst_worldtorastercoordx_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_worldtorastercoordx(col("tile"), lit(554880), lit(4545120)).alias("pixel_col"))
result.show(truncate = false)
""".trim

  val rst_worldtorastercoordx_scala_example_output: String =
    """
+---------+
|pixel_col|
+---------+
|5490     |
+---------+
"""

  val rst_worldtorastercoordy_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_worldtorastercoordy(col("tile"), lit(554880), lit(4545120)).alias("pixel_row"))
result.show(truncate = false)
""".trim

  val rst_worldtorastercoordy_scala_example_output: String =
    """
+---------+
|pixel_row|
+---------+
|5490     |
+---------+
"""

  val rst_to_webmercator_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_to_webmercator(col("tile")).alias("tile"))
result.show(truncate = false)
""".trim

  val rst_to_webmercator_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(reprojected to Web Mercator, EPSG:3857)
""".trim

  val rst_tilexyz_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_tilexyz(col("tile"), lit(12), lit(1234), lit(1523)).alias("png_bytes"))
result.show(truncate = false)
""".trim

  val rst_tilexyz_scala_example_output: String =
    """
+----------+
|png_bytes |
+----------+
|[BINARY]  |
+----------+
(PNG image bytes, 256×256 pixels)
"""

  val rst_xyzpyramid_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_xyzpyramid(col("tile"), lit(10), lit(12)).alias("tile_array"))
result.show(truncate = false)
""".trim

  val rst_xyzpyramid_scala_example_output: String =
    """
+----------+
|tile_array|
+----------+
|[tile, ...|
+----------+
(array of tile structs: [{z, x, y, bytes}, ...])
"""

  val rst_h3_tessellate_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_h3_tessellate(col("tile"), lit(7)).alias("h3_cells"))
result.show(truncate = false)
""".trim

  val rst_h3_tessellate_scala_example_output: String =
    """
+---+
|h3_|
+---+
|[{ |
+---+
(array of structs: [{cellid: LONG, raster: BINARY}, ...])
"""

  val h3_cell_bbox_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

rx.register(spark)
// H3 res-9 cell ids (as BIGINT); scalar bbox in EPSG:4326, centroids mode.
val schema = StructType(Seq(StructField("cellid", LongType())))
val rows = Seq(617733151020810239L, 617733151085035519L, 617733151021334527L)
val df = spark.createDataFrame(spark.sparkContext.parallelize(rows.map(org.apache.spark.sql.Row(_))), schema)
val result = df.select(col("cellid"), rx.gbx_h3_cell_bbox(col("cellid"), 4326, "centroids", 0).alias("bbox"))
result.show(truncate = false)
""".trim

  val h3_cell_bbox_scala_example_output: String =
    """
+------------------+------------------------------+
|cellid            |bbox                          |
+------------------+------------------------------+
|617733151020810239|{-74.02, 40.70, -74.01, 40.71}|
+------------------+------------------------------+
(STRUCT<xmin, ymin, xmax, ymax> per H3 cell, in EPSG:4326)
"""

  val rst_bng_tessellate_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_bng_tessellate(col("tile"), lit(3)).alias("bng_cells"))
result.show(truncate = false)
""".trim

  val rst_bng_tessellate_scala_example_output: String =
    """
+---+
|bng|
+---+
|[{ |
+---+
(array of tile structs per BNG cell; raster rewarped to EPSG:27700)
"""

  val rst_quadbin_tessellate_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_quadbin_tessellate(col("tile"), lit(12)).alias("qb_cells"))
result.show(truncate = false)
""".trim

  val rst_quadbin_tessellate_scala_example_output: String =
    """
+---+
|qb_|
+---+
|[{ |
+---+
(array of tile structs per quadbin cell, zoom 12)
"""

  // =========================================================================
  // Generator Functions (Scala)
  // =========================================================================

  val rst_retile_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_retile(col("tile"), lit(64), lit(64)).alias("tiles"))
result.show(truncate = false)
""".trim

  val rst_retile_scala_example_output: String =
    """
+-----+
|tiles|
+-----+
|[{0, |
+-----+
(array of tile structs: [{0, raster, path, metadata}, ...])
"""

  val rst_tooverlappingtiles_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_tooverlappingtiles(col("tile"), lit(64), lit(64), lit(8)).alias("tiles"))
result.show(truncate = false)
""".trim

  val rst_tooverlappingtiles_scala_example_output: String =
    """
+-----+
|tiles|
+-----+
|[{0, |
+-----+
(array of overlapping tile structs with 8% overlap)
"""

  val rst_separatebands_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_separatebands(col("tile")).alias("bands"))
result.show(truncate = false)
""".trim

  val rst_separatebands_scala_example_output: String =
    """
+-----+
|bands|
+-----+
|[{0, |
+-----+
(array of tile structs: one per band from the multiband raster)
"""

  val rst_polygonize_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_polygonize(col("tile")).alias("features"))
result.show(truncate = false)
""".trim

  val rst_polygonize_scala_example_output: String =
    """
+--------+
|features|
+--------+
|[{...}] |
+--------+
(array of {geom_wkb: binary (WKB), value: double})
"""

  val rst_maketiles_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("rasters")
val result = raster.select(rx.rst_maketiles(col("tile"), lit(1.0)).alias("tiles"))
result.show(truncate = false)
""".trim

  val rst_maketiles_scala_example_output: String =
    """
+-----+
|tiles|
+-----+
|[{0, |
+-----+
(array of tile structs per MB subdivision)
"""

  val rst_rasterize_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

rx.register(spark)
val wkt = "POLYGON((2 2, 8 2, 8 8, 2 8, 2 2))"
val result = spark.createDataFrame(Seq(
  (wkt,)
)).toDF("geom")
  .select(rx.rst_rasterize(
    col("geom"),
    lit(1.0),
    lit(0.0), lit(0.0), lit(10.0), lit(10.0),
    lit(10), lit(10),
    lit(4326)
  ).alias("tile"))
result.show(truncate = false)
""".trim

  val rst_rasterize_scala_example_output: String =
    """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(rasterized tile: pixels inside the polygon carry the burn value; outside = NoData)
"""

  // ============================================================================
  // H3 Rastertogrid Functions — Scala
  // ============================================================================

  val rst_h3_rastertogridavg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridavg(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridavg_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 123.45), ...), ...)
(Seq[Seq[Row]]: outer seq = bands, inner seq = cells per band)"""

  val rst_h3_rastertogridcount_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridcount(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridcount_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 256), ...), ...)
(pixel count per band×cell)"""

  val rst_h3_rastertogridmax_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridmax(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridmax_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 255.0), ...), ...)
(max value per band×cell)"""

  val rst_h3_rastertogridmin_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridmin(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridmin_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 0.0), ...), ...)
(min value per band×cell)"""

  val rst_h3_rastertogridmedian_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridmedian(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridmedian_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 120.5), ...), ...)
(median value per band×cell)"""

  val rst_h3_rastertogridsum_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridsum(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridsum_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 31563.0), ...), ...)
(sum of pixel values per band×cell)"""

  val rst_h3_rastertogridvariance_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridvariance(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridvariance_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 1245.5), ...), ...)
(variance per band×cell)"""

  val rst_h3_rastertogridstddev_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_h3_rastertogridstddev(col("tile"), lit(4)).alias("h3_grid")).first()
result.getAs[Seq[Seq[Row]]]("h3_grid")
""".trim

  val rst_h3_rastertogridstddev_scala_example_output: String =
    """Vector(Vector(Row(cellID: 599686042433355775, measure: 35.29), ...), ...)
(standard deviation per band×cell)"""

  // ============================================================================
  // Quadbin Rastertogrid Functions — Scala
  // ============================================================================

  val rst_quadbin_rastertogridavg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridavg(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridavg_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 123.45), ...), ...)
(Seq[Seq[Row]]: outer seq = bands, inner seq = cells per band)"""

  val rst_quadbin_rastertogridcount_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridcount(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridcount_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 256), ...), ...)
(pixel count per band×cell)"""

  val rst_quadbin_rastertogridmax_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridmax(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridmax_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 255.0), ...), ...)
(max value per band×cell)"""

  val rst_quadbin_rastertogridmin_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridmin(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridmin_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 0.0), ...), ...)
(min value per band×cell)"""

  val rst_quadbin_rastertogridmedian_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridmedian(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridmedian_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 120.5), ...), ...)
(median value per band×cell)"""

  val rst_quadbin_rastertogridsum_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridsum(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridsum_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 31563.0), ...), ...)
(sum of pixel values per band×cell)"""

  val rst_quadbin_rastertogridvariance_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridvariance(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridvariance_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 1245.5), ...), ...)
(variance per band×cell)"""

  val rst_quadbin_rastertogridstddev_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_quadbin_rastertogridstddev(col("tile"), lit(4)).alias("quadbin_grid")).first()
result.getAs[Seq[Seq[Row]]]("quadbin_grid")
""".trim

  val rst_quadbin_rastertogridstddev_scala_example_output: String =
    """Vector(Vector(Row(cellID: 12345, measure: 35.29), ...), ...)
(standard deviation per band×cell)"""

  // ============================================================================
  // BNG Rastertogrid Functions — Scala (reprojects to EPSG:27700; STRING cell ids)
  // ============================================================================

  val rst_bng_rastertogridavg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridavg(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridavg_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridcount_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridcount(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridcount_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridmax_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridmax(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridmax_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridmin_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridmin(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridmin_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridmedian_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridmedian(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridmedian_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridsum_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridsum(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridsum_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridvariance_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridvariance(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridvariance_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  val rst_bng_rastertogridstddev_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

val raster = spark.table("multiband_rasters")
val result = raster.select(rx.rst_bng_rastertogridstddev(col("tile"), lit(3)).alias("bng_grid")).first()
result.getAs[Seq[Seq[Row]]]("bng_grid")
""".trim

  val rst_bng_rastertogridstddev_scala_example_output: String =
    """Vector(Vector(Row(OW5575, ...), Row(OW5574, ...), ...), Vector(...), Vector(...))
(Seq[Seq[Row]] — outer per band, inner per BNG cell; cellID is a STRING grid-square label)"""

  // =========================================================================
  // VectorX CRS family — st_crs, st_setcrs, st_transformcrs
  // Fixture: ``vector_geoms`` view — 1 row: geom STRING = 'SRID=4326;POINT (13 42)'
  // =========================================================================

  val st_crs_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

vx.register(spark)
val df = spark.table("vector_geoms")
val result = df.select(vx.st_crs(col("geom")).alias("crs")).first()
result.getString(0)
""".trim

  val st_crs_scala_example_output: String =
    """
+---------+
|crs      |
+---------+
|EPSG:4326|
+---------+
""".trim

  val st_setcrs_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

vx.register(spark)
val df = spark.table("vector_geoms")
val result = df.select(vx.st_setcrs(col("geom"), "EPSG:4326").alias("stamped")).first()
result.getAs[Array[Byte]]("stamped")
""".trim

  val st_setcrs_scala_example_output: String =
    """
+---------+
|stamped  |
+---------+
|[binary] |
+---------+
(EWKB binary — coordinates preserved, SRID=4326 embedded)""".trim

  val st_transformcrs_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

vx.register(spark)
val df = spark.table("vector_geoms")
val result = df.select(vx.st_transformcrs(col("geom"), lit("EPSG:32633")).alias("utm33n")).first()
result.getAs[Array[Byte]]("utm33n")
""".trim

  val st_transformcrs_scala_example_output: String =
    """
+--------+
|utm33n  |
+--------+
|[binary]|
+--------+
(EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)""".trim

}
