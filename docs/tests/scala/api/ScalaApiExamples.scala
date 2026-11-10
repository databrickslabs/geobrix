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

// Registration is a Python step, once per session:
//   from databricks.labs.gbx import register; register(spark)
// The Scala functions below call the registered SQL names.
""".trim

  val RegisterAllPackages_output: String =
    """
RasterX, GridX, and VectorX functions registered (gbx_rst_*, gbx_bng_*, gbx_st_*).
""".trim

  val RegisterRasterX: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

// Use functions
val df = rasters.select(rx.rst_boundingbox(col("tile")))
""".trim

  val RegisterGridX: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

// Use functions
val df = spark.sql("SELECT gbx_bng_cellarea('TQ3080')")
""".trim

  val RegisterVectorX: String =
    """
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

// Use functions
val df = legacyData.select(vx.st_legacyaswkb(col("mosaic_geom")))
""".trim

  val RasterXAccessorFunctions: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

// Read rasters (sample data path)
val rasters = spark.read.format("binaryFile").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif").select(col("path"), call_function("gbx_rst_fromcontent", col("content"), lit("GTiff")).alias("tile"))

// Extract metadata
val metadata = rasters.select(
  col("path"),
  rx.rst_boundingbox(col("tile")).alias("bbox"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_numbands(col("tile")).alias("num_bands"),
  call_function("gbx_rst_metadata", col("tile")).alias("metadata")
)

metadata.show()
""".trim

  val RasterXTransformationFunctions: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)

val rasters = spark.read.format("binaryFile").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif").select(col("path"), call_function("gbx_rst_fromcontent", col("content"), lit("GTiff")).alias("tile"))

// Clip raster with WKT geometry (GeoBrix accepts WKT or WKB; cutlineAllTouched = true)
val clipWkt = "POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"
val clipped = rasters.select(
  col("path"),
  call_function("gbx_rst_clip", col("tile"), lit(clipWkt), lit(true)).alias("clipped_tile")
)
""".trim

  val RasterXCompleteExample: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

// Read rasters (sample data path)
val rasters = spark.read.format("binaryFile").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif").select(col("path"), call_function("gbx_rst_fromcontent", col("content"), lit("GTiff")).alias("tile"))

// Extract metadata and filter
val catalog = rasters.select(
  col("path"),
  rx.rst_boundingbox(col("tile")).alias("bbox"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_numbands(col("tile")).alias("bands"),
  call_function("gbx_rst_metadata", col("tile")).alias("metadata")
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

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

// Calculate cell area (square kilometres)
val area = spark.sql("SELECT gbx_bng_cellarea('TQ3080') as area_km2")
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

// Registration is a Python step:
//   from databricks.labs.gbx import register; register(spark)

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
df: DataFrame with area_km2 (e.g. 1.0)
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
|area_km2 |
+---------+
|1.0      |
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

  val rst_fromcontent_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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

  val rst_h3_rasterize_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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

  // ============================================================================
  // Terrain Analysis Functions
  // ============================================================================

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

  // ========================================================================
  // Coordinate Transforms & Tiling
  // ========================================================================

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

  val h3_cell_bbox_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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

  // =========================================================================
  // rst_custom_rasterize_agg
  //
  // Fixture: 3 rows of (region, custom-grid cell id BIGINT, burn value) at
  // resolution 1 (2km cells) inside a 4km London BNG extent.
  // Cell IDs are computed via gbx_custom_pointascell for centroids of 3
  // of the 4 resolution-1 cells.  Grouped by region, producing 1 tile.
  // =========================================================================

  val rst_custom_rasterize_agg_scala_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
// Custom grid: 4km BNG London square, root cell 4000m, splits=2 -> res 1 = four 2km cells.
val grid = call_function("gbx_custom_grid",
  lit(529000), lit(533000), lit(179000), lit(183000), lit(2), lit(4000), lit(4000), lit(27700))
// Compute three resolution-1 cell IDs (centroids of three 2km cells within the grid).
val schema = StructType(Seq(
  StructField("region", StringType()),
  StructField("cellid", LongType()),
  StructField("value", DoubleType())))
val cells = spark.sql(
  "SELECT 'R1' AS region, gbx_custom_pointascell('POINT(530000 180000)', " +
  "gbx_custom_grid(529000,533000,179000,183000,2,4000,4000,27700), 1) AS cellid, 1.0 AS value " +
  "UNION ALL SELECT 'R1', gbx_custom_pointascell('POINT(532000 180000)', " +
  "gbx_custom_grid(529000,533000,179000,183000,2,4000,4000,27700), 1), 2.0 " +
  "UNION ALL SELECT 'R1', gbx_custom_pointascell('POINT(530000 182000)', " +
  "gbx_custom_grid(529000,533000,179000,183000,2,4000,4000,27700), 1), 3.0")
val result = cells.groupBy("region").agg(
  rx.rst_custom_rasterize_agg(col("cellid"), col("value"), grid).alias("tile"))
result.show()
""".trim

  val rst_custom_rasterize_agg_scala_example_output: String =
    """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null; three 2km custom-grid cells burned)
""".trim

  // =========================================================================
  // VectorX TIN family — st_triangulate, st_interpolateelevationbbox,
  //                       st_interpolateelevationgeom
  //
  // Fixture: ``tin_survey`` view — 1 row: pts ARRAY<BINARY> (4 WKB POINT Z),
  //          bl ARRAY<BINARY> (empty).  The 4 points form a 10×10 m square:
  //          (0,0,z=0), (10,0,z=0), (10,10,z=10), (0,10,z=5) → 2 triangles.
  //
  // These are Generator Column functions in the heavyweight (vectorx) tier.
  // In the lightweight (pyvx) tier, invoke the registered UDTF via SQL LATERAL.
  // mode='constrained' (default) is both-tier; mode='conforming' is heavy-only.
  // =========================================================================

  val st_triangulate_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
val df = spark.table("tin_survey")
val result = df.select(
  vx.st_triangulate(col("pts"), col("bl"), lit(0), lit(0), lit("NONENCROACHING"), lit("constrained")).alias("triangle")
)
result.show()
""".trim

  val st_triangulate_scala_example_output: String =
    """
+--------+
|triangle|
+--------+
|[binary]|
|[binary]|
+--------+
... (WKB binary — 2 Delaunay triangle polygons)""".trim

  val st_interpolateelevationbbox_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
val df = spark.table("tin_survey")
val result = df.select(
  vx.st_interpolateelevationbbox(
    col("pts"), col("bl"),
    lit(0), lit(0), lit("NONENCROACHING"),
    lit(0), lit(0), lit(10), lit(10),
    lit(3), lit(3), lit(0),
    lit("constrained")
  ).alias("elevation_point")
)
result.show()
""".trim

  val st_interpolateelevationbbox_scala_example_output: String =
    """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 elevation grid, 9 rows)""".trim

  val st_interpolateelevationgeom_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
val origin = expr("unhex('010100000000000000000000000000000000002440')")
val df = spark.table("tin_survey")
val result = df.select(
  vx.st_interpolateelevationgeom(
    col("pts"), col("bl"),
    lit(0), lit(0), lit("NONENCROACHING"),
    origin, lit(3), lit(3), lit(3), lit(-3),
    lit("constrained")
  ).alias("elevation_point")
)
result.show()
""".trim

  val st_interpolateelevationgeom_scala_example_output: String =
    """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 origin-anchored grid, 9 rows)""".trim

  // =========================================================================
  // VectorX CRS family — st_crs, st_setcrs, st_transformcrs
  // Fixture: ``vector_geoms`` view — 1 row: geom STRING = 'SRID=4326;POINT (13 42)'
  // =========================================================================

  val st_crs_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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
... (EWKB binary — coordinates preserved, SRID=4326 embedded)""".trim

  val st_transformcrs_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.{functions => vx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
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
... (EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)""".trim

  // =========================================================================
  // VectorX Legacy Mosaic conversion — st_legacyaswkb
  // Fixture: ``legacy_geoms`` view — 1 row: geom_legacy STRUCT (Mosaic InternalGeometry)
  // Encodes POINT(13, 42): typeId=1, srid=0, boundaries=[[[13.0, 42.0]]], holes=[].
  // Import path (heavy): com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}
  // Output: plain WKB — no embedded SRID. Same function, same output bytes, both tiers.
  // =========================================================================

  val st_legacyaswkb_scala_example: String =
    """
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}
import org.apache.spark.sql.functions._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
val df = spark.table("legacy_geoms")
val result = df.select(vx.st_legacyaswkb(col("geom_legacy")).alias("wkb")).first()
result.getAs[Array[Byte]]("wkb")
""".trim

  val st_legacyaswkb_scala_example_output: String =
    """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (WKB binary)""".trim

  // =========================================================================
  // GridX per-function examples — T2: BNG codec + accessors
  // bng_aswkb, bng_aswkt, bng_cellarea, bng_centroid,
  // bng_eastnorthasbng, bng_pointascell
  //
  // Fixture views: bng_cells (cellid STRING = 'TQ3080'),
  //                bng_points (easting INT, northing INT, geom STRING)
  // All four tabs share ONE example per function (AGREE — no tier divergence).
  // =========================================================================

  val bng_aswkb_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cells view (cellid = 'TQ3080', 1km cell, central London)
val df = spark.table("bng_cells")
val result = df.select(bx.bng_aswkb(col("cellid")).alias("wkb"))
result.show()
""".trim

  val bng_aswkb_scala_example_output: String =
    """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (WKB binary)""".trim

  val bng_aswkt_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cells view (cellid = 'TQ3080')
val df = spark.table("bng_cells")
val result = df.select(bx.bng_aswkt(col("cellid")).alias("wkt"))
result.show(truncate = false)
""".trim

  val bng_aswkt_scala_example_output: String =
    """
+----------------------------------------------------------+
|wkt                                                       |
+----------------------------------------------------------+
|POLYGON ((530000 180000, 531000 180000, 531000 181000, ...|
+----------------------------------------------------------+
POLYGON ((530000 180000, 531000 180000, 531000 181000, 530000 181000, 530000 180000))""".trim

  val bng_cellarea_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cells view (cellid = 'TQ3080', a 1km cell → area = 1.0 sq km)
val df = spark.table("bng_cells")
val result = df.select(bx.bng_cellarea(col("cellid")).alias("area_km2"))
result.show()
""".trim

  val bng_cellarea_scala_example_output: String =
    """
+---------+
|area_km2 |
+---------+
|1.0      |
+---------+""".trim

  val bng_centroid_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cells view (cellid = 'TQ3080')
// Returns a WKB POINT at the cell centre (530500, 180500) in EPSG:27700
val df = spark.table("bng_cells")
val result = df.select(bx.bng_centroid(col("cellid")).alias("centroid"))
result.show()
""".trim

  val bng_centroid_scala_example_output: String =
    """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (WKB binary — POINT(530500 180500) in EPSG:27700)""".trim

  val bng_pointascell_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_points view (geom = 'POINT(530000 180000)' in EPSG:27700)
val df = spark.table("bng_points")
val result = df.select(
  bx.bng_pointascell(col("geom"), lit("1km")).alias("bng_cell")
)
result.show()
""".trim

  val bng_pointascell_scala_example_output: String =
    """
+--------+
|bng_cell|
+--------+
|TQ3080  |
+--------+""".trim

  // =========================================================================
  // GridX per-function examples — T3: BNG distance + chip ops
  // bng_distance, bng_euclideandistance, bng_cellintersection, bng_cellunion
  //
  // Fixture views: bng_cell_pairs (cellid1='TQ3080', cellid2='TQ3081'),
  //                bng_chips (9 chip structs from tessellation at res=3)
  // All four tabs share ONE example per function (AGREE — no tier divergence).
  // =========================================================================

  val bng_distance_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cell_pairs view (cellid1='TQ3080', cellid2='TQ3081', adjacent 1km cells)
// Returns LONG (grid steps, not metres): adjacent cells → dist_steps = 1
val df = spark.table("bng_cell_pairs")
val result = df.select(bx.bng_distance(col("cellid1"), col("cellid2")).alias("dist_steps"))
result.show()
""".trim

  val bng_distance_scala_example_output: String =
    """
+----------+
|dist_steps|
+----------+
|1         |
+----------+""".trim

  val bng_euclideandistance_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cell_pairs view (cellid1='TQ3080', cellid2='TQ3081')
// Returns LONG (Chebyshev grid units): adjacent cells → euclidean_dist = 1
val df = spark.table("bng_cell_pairs")
val result = df.select(
  bx.bng_euclideandistance(col("cellid1"), col("cellid2")).alias("euclidean_dist")
)
result.show()
""".trim

  val bng_euclideandistance_scala_example_output: String =
    """
+--------------+
|euclidean_dist|
+--------------+
|1             |
+--------------+""".trim

  val bng_cellintersection_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_chips view (9 chip structs from tessellating the BNG polygon at res=3)
// Filters for the core chip (TQ3080, core=true, chip=null) and intersects it with itself.
// Chip inputs must come from bng_tessellate — plain String cell IDs throw ClassCastException.
val df = spark.table("bng_chips").filter(col("chip.cellid") === "TQ3080")
val result = df.select(
  bx.bng_cellintersection(col("chip"), col("chip")).alias("intersection_chip")
)
result.show()
""".trim

  val bng_cellintersection_scala_example_output: String =
    """
+--------------------+
|intersection_chip   |
+--------------------+
|{TQ3080, true, null}|
+--------------------+""".trim

  val bng_cellunion_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_chips view (9 chip structs from tessellating the BNG polygon at res=3)
// Filters for the core chip (TQ3080, core=true, chip=null) and unions it with itself.
// Chip inputs must come from bng_tessellate — plain String cell IDs throw ClassCastException.
val df = spark.table("bng_chips").filter(col("chip.cellid") === "TQ3080")
val result = df.select(
  bx.bng_cellunion(col("chip"), col("chip")).alias("union_chip")
)
result.show()
""".trim

  val bng_cellunion_scala_example_output: String =
    """
+--------------------+
|union_chip          |
+--------------------+
|{TQ3080, true, null}|
+--------------------+""".trim

  // =========================================================================
  // GridX per-function examples — T4: BNG neighbourhood/fill
  // bng_kring, bng_kloop, bng_geomkring, bng_geomkloop, bng_polyfill,
  // bng_tessellate
  //
  // Fixture views: bng_cells (cellid STRING = 'TQ3080'),
  //                bng_polygons (geom STRING — 3km × 3km BNG polygon, EPSG:27700)
  // All four tabs share ONE example per function (AGREE — no tier divergence).
  // Geometry inputs MUST be EPSG:27700; WGS84 coords yield empty arrays.
  // =========================================================================

  val bng_kring_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cells view (cellid = 'TQ3080', 1km cell, central London)
// Returns ARRAY<STRING> — center plus 8 surrounding cells at k=1 → 9 cells
val df = spark.table("bng_cells")
val result = df.select(bx.bng_kring(col("cellid"), lit(1)).alias("kring"))
result.show(truncate = false)
""".trim

  val bng_kring_scala_example_output: String =
    """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells: center TQ3080 plus 8 surrounding cells at k=1)""".trim

  val bng_kloop_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_cells view (cellid = 'TQ3080', 1km cell, central London)
// Returns ARRAY<STRING> — the 8 surrounding cells at k=1 (center excluded)
val df = spark.table("bng_cells")
val result = df.select(bx.bng_kloop(col("cellid"), lit(1)).alias("kloop"))
result.show(truncate = false)
""".trim

  val bng_kloop_scala_example_output: String =
    """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (8 cells: hollow ring at k=1, center TQ3080 excluded)""".trim

  val bng_geomkring_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_polygons view (3km × 3km BNG polygon in EPSG:27700)
// EPSG:27700 coordinates required — WGS84 yields empty arrays
// res=3 polyfill → 9 cells; k=1 expansion → 25 cells total
val df = spark.table("bng_polygons")
val result = df.select(bx.bng_geomkring(col("geom"), lit(3), lit(1)).alias("kring"))
result.show(truncate = false)
""".trim

  val bng_geomkring_scala_example_output: String =
    """
+-----------------------------+
|kring                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (24 cells: boundary band expanded by k=1 ring; interior centre not filled)""".trim

  val bng_geomkloop_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_polygons view (3km × 3km BNG polygon in EPSG:27700)
// EPSG:27700 coordinates required — WGS84 yields empty arrays
// res=3 polyfill → 9 cells; k=1 outer ring → 16 cells (hollow shell)
val df = spark.table("bng_polygons")
val result = df.select(bx.bng_geomkloop(col("geom"), lit(3), lit(1)).alias("kloop"))
result.show(truncate = false)
""".trim

  val bng_geomkloop_scala_example_output: String =
    """
+-----------------------------+
|kloop                        |
+-----------------------------+
|[TQ2878, TQ2879, TQ2880, ...]|
+-----------------------------+
... (16 cells: outer ring at k=1 around the BNG polygon polyfill)""".trim

  val bng_polyfill_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_polygons view (3km × 3km BNG polygon in EPSG:27700)
// EPSG:27700 coordinates required — WGS84 yields empty arrays
// res=3 (1km cells): 9 cells cover the 3km × 3km polygon
val df = spark.table("bng_polygons")
val result = df.select(bx.bng_polyfill(col("geom"), lit(3)).alias("cells"))
result.show(truncate = false)
""".trim

  val bng_polyfill_scala_example_output: String =
    """
+-----------------------------+
|cells                        |
+-----------------------------+
|[TQ2979, TQ2980, TQ2981, ...]|
+-----------------------------+
... (9 cells covering the 3km × 3km BNG polygon at 1km resolution)""".trim

  val bng_tessellate_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Reads the bng_polygons view (3km × 3km BNG polygon in EPSG:27700)
// EPSG:27700 coordinates required — WGS84 yields empty arrays
// res=3 (1km): 9 chips; TQ3080 is core (core=true, chip=null); borders carry WKB clip
val df = spark.table("bng_polygons")
val result = df.select(bx.bng_tessellate(col("geom"), lit(3)).alias("chips"))
result.show(truncate = false)
""".trim

  val bng_tessellate_scala_example_output: String =
    """
+--------------------------------------------+
|chips                                       |
+--------------------------------------------+
|[{TQ2979, false, [binary]}, {TQ3080, true,..|
+--------------------------------------------+
... (9 chips; TQ3080 is core (core=true, chip=null); border cells carry WKB clip geometry)""".trim

  // =========================================================================
  // GridX per-function examples — T5: BNG aggregators
  // bng_cellintersection_agg, bng_cellunion_agg
  //
  // Fixture view: bng_chips (9 chip structs from tessellation at res=3)
  // DIVERGE: heavy returns STRUCT<cellid, core, chip>; light returns BINARY.
  // The 5 BNG *explode UDTFs have no Column form on any tier — Scala tab
  // is legitimately absent for those; only the 2 aggregators have Scala vals.
  // =========================================================================

  // =========================================================================
  // GridX per-function examples — T7: Custom grid — all 7 functions
  // custom_grid, custom_pointascell, custom_cellaswkb, custom_cellaswkt,
  // custom_centroid, custom_polyfill, custom_kring
  //
  // Fixture views: custom_grids (grid STRUCT + cell=360287970373976640 + point='POINT(530000 180000)')
  // custom_grid uses an inline one-row DataFrame; all others read custom_grids.
  // All four tabs share ONE example per function (AGREE — no tier divergence).
  // Cell IDs are BIGINT; geometry outputs are plain WKB with no SRID.
  // =========================================================================

  val custom_grid_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Define a BNG-like custom grid: EPSG:27700, 1km root cells, 2 splits/level
val df = spark.sql("SELECT 1 AS dummy")
val result = df.select(
  cx.custom_grid(
    lit(0), lit(1000000), lit(0), lit(1000000),
    lit(2), lit(1000), lit(1000), lit(27700)
  ).alias("grid")
)
result.show(truncate = false)
""".trim

  val custom_grid_scala_example_output: String =
    """
+----------------------------------------------+
|grid                                          |
+----------------------------------------------+
|{0, 1000000, 0, 1000000, 2, 1000, 1000, 27700}|
+----------------------------------------------+""".trim

  val custom_pointascell_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (point = 'POINT(530000 180000)', grid struct, EPSG:27700)
// Resolution 5 maps the easting/northing to cell 360287970373976640
val df = spark.table("custom_grids")
val result = df.select(
  cx.custom_pointascell(col("point"), col("grid"), lit(5)).alias("cell")
)
result.show()
""".trim

  val custom_pointascell_scala_example_output: String =
    """
+--------------------+
|cell                |
+--------------------+
|360287970373976640  |
+--------------------+""".trim

  val custom_cellaswkb_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (cell = 360287970373976640 at res=5, grid struct)
// Returns the 31.25m x 31.25m cell footprint polygon as plain WKB (no SRID)
val df = spark.table("custom_grids")
val result = df.select(cx.custom_cellaswkb(col("cell"), col("grid")).alias("geom"))
result.show()
""".trim

  val custom_cellaswkb_scala_example_output: String =
    """
+--------+
|geom    |
+--------+
|[binary]|
+--------+
... (WKB binary — 31.25m × 31.25m custom grid cell footprint polygon)""".trim

  val custom_cellaswkt_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (cell = 360287970373976640 at res=5, grid struct)
// Returns the cell boundary as a WKT POLYGON in EPSG:27700 coordinates
val df = spark.table("custom_grids")
val result = df.select(cx.custom_cellaswkt(col("cell"), col("grid")).alias("wkt"))
result.show(truncate = false)
""".trim

  val custom_cellaswkt_scala_example_output: String =
    """
+-------------------------------------------------------------------------------------------------+
|wkt                                                                                              |
+-------------------------------------------------------------------------------------------------+
|POLYGON ((530000 180000, 530031.25 180000, 530031.25 180031.25, 530000 180031.25, 530000 180000))|
+-------------------------------------------------------------------------------------------------+""".trim

  val custom_centroid_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (cell = 360287970373976640 at res=5, grid struct)
// Returns the center of the 31.25m x 31.25m cell as plain WKB (no SRID)
val df = spark.table("custom_grids")
val result = df.select(cx.custom_centroid(col("cell"), col("grid")).alias("centroid"))
result.show()
""".trim

  val custom_centroid_scala_example_output: String =
    """
+-----------+
|centroid   |
+-----------+
|[binary]   |
+-----------+
... (WKB binary — POINT at the center of the 31.25m × 31.25m custom grid cell)""".trim

  val custom_polyfill_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view for the grid struct.
// Fills the 3km x 3km BNG polygon with 500m cells at resolution 1 (36 cells).
val poly = "POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))"
val df = spark.table("custom_grids")
val result = df.select(cx.custom_polyfill(lit(poly), col("grid"), lit(1)).alias("cells"))
result.show(truncate = false)
""".trim

  val custom_polyfill_scala_example_output: String =
    """
+---------------------------------------------+
|cells                                        |
+---------------------------------------------+
|[72057594038644994, ..., (36 cells at res=1)]|
+---------------------------------------------+
... (36 BIGINT cell IDs — 500m cells covering the 3km × 3km BNG polygon at resolution 1)""".trim

  val custom_kring_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (cell = 360287970373976640 at res=5, grid struct)
// k=1 returns 9 cells: center plus 8 surrounding cells (3x3 neighbourhood)
val df = spark.table("custom_grids")
val result = df.select(cx.custom_kring(col("cell"), col("grid"), lit(1)).alias("ring"))
result.show(truncate = false)
""".trim

  val custom_kring_scala_example_output: String =
    """
+-------------------------------------------+
|ring                                       |
+-------------------------------------------+
|[360287970373976640, ..., (9 cells at k=1)]|
+-------------------------------------------+
... (9 BIGINT cell IDs — the 3×3 neighbourhood including center cell at resolution 5)""".trim

  // -------------------------------------------------------------------------
  // Custom-grid geometry-aware kring/kloop
  // -------------------------------------------------------------------------

  val custom_geomkring_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (grid struct + BNG-like 1km grid descriptor)
// Offset 3km × 3km BNG London polygon at res=1 (500m cells); k=1 → 55 cells
val poly = "POLYGON((529100 179100,529100 182100,532100 182100,532100 179100,529100 179100))"
val df = spark.table("custom_grids")
val result = df.select(cx.custom_geomkring(lit(poly), col("grid"), lit(1), lit(1)).alias("kring"))
result.show(truncate = false)
""".trim

  val custom_geomkring_scala_example_output: String =
    """
+-------------------------------------------+
|kring                                      |
+-------------------------------------------+
|[72057594038779906, ..., (55 cells at k=1)]|
+-------------------------------------------+
... (55 BIGINT cell IDs — covering cells of offset 3km polygon at res=1 (500m) expanded by k=1 ring)""".trim

  val custom_geomkloop_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (grid struct + BNG-like 1km grid descriptor)
// Offset 3km × 3km BNG London polygon at res=1 (500m cells); k=1 outer ring → 19 cells
val poly = "POLYGON((529100 179100,529100 182100,532100 182100,532100 179100,529100 179100))"
val df = spark.table("custom_grids")
val result = df.select(cx.custom_geomkloop(lit(poly), col("grid"), lit(1), lit(1)).alias("kloop"))
result.show(truncate = false)
""".trim

  val custom_geomkloop_scala_example_output: String =
    """
+-------------------------------------------+
|kloop                                      |
+-------------------------------------------+
|[72057594038779906, ..., (19 cells at k=1)]|
+-------------------------------------------+
... (19 BIGINT cell IDs — outer ring at k=1 around the offset 3km polygon at res=1 (500m))""".trim

  val custom_geomkringexplode_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (grid struct)
// A generator: select it directly and it expands to one cellid row per cell
val poly = "POLYGON((529100 179100,529100 182100,532100 182100,532100 179100,529100 179100))"
val df = spark.table("custom_grids")
df.select(cx.custom_geomkringexplode(lit(poly), col("grid"), lit(1), lit(1))).show(truncate = false)
""".trim

  val custom_geomkringexplode_scala_example_output: String =
    """
+------------------+
|cellid            |
+------------------+
|72057594038779906 |
|...               |
+------------------+
... (one BIGINT row per cell in the k=1 ring around the offset 3km polygon at res=1 (500m))""".trim

  val custom_geomkloopexplode_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (grid struct)
// A generator: select it directly and it expands to one cellid row per cell
val poly = "POLYGON((529100 179100,529100 182100,532100 182100,532100 179100,529100 179100))"
val df = spark.table("custom_grids")
df.select(cx.custom_geomkloopexplode(lit(poly), col("grid"), lit(1), lit(1))).show(truncate = false)
""".trim

  val custom_geomkloopexplode_scala_example_output: String =
    """
+------------------+
|cellid            |
+------------------+
|72057594038779906 |
|...               |
+------------------+
... (one BIGINT row per cell in the hollow outer ring at k=1 around the offset 3km polygon; 19 cells)""".trim

  // =========================================================================
  // Batch E: cellfill (grouped aggregate) + kloop / distance (scalar)
  // bng_cellfill, h3_cellfill, custom_cellfill
  // custom_kloop, custom_distance
  //
  // Fixture views: bng_cells (TQ3080), custom_grids
  // cellfill inline data: 1 NULL center + ring-1 neighbours at 5.0 → filled=5.0
  // =========================================================================

  val bng_cellfill_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Inline data: TQ300800 (NULL) + 4 ring-1 neighbours at 5.0 → TQ300800 filled to 5.0
// Heavy tier returns ARRAY<STRUCT<cellid STRING, value DOUBLE>>
val df = spark.createDataFrame(Seq(
  (1, "TQ300800", null.asInstanceOf[java.lang.Double]),
  (1, "TQ299800", 5.0: java.lang.Double),
  (1, "TQ301800", 5.0: java.lang.Double),
  (1, "TQ300799", 5.0: java.lang.Double),
  (1, "TQ300801", 5.0: java.lang.Double)
)).toDF("region", "cellid", "value")
val result = df.groupBy("region")
  .agg(bx.bng_cellfill(col("cellid"), col("value"), 1, "mean", 2.0).alias("filled"))
result.show(truncate = false)
""".trim

  val bng_cellfill_scala_example_output: String =
    """
+------+---------------------------------------------------+
|region|filled                                             |
+------+---------------------------------------------------+
|1     |[{TQ300800, 5.0}, {TQ299800, 5.0}, ...(5 entries)]|
+------+---------------------------------------------------+
... (ARRAY<STRUCT<cellid STRING, value DOUBLE>> — center TQ300800 filled to 5.0)""".trim

  val h3_cellfill_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.h3.{functions => hx}
import org.apache.spark.sql.functions._

// Inline data: London res-8 H3 center (NULL) + 2 ring-1 neighbours at 5.0
// Heavy tier returns ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>
val df = spark.createDataFrame(Seq(
  (1, 612934495919669247L, null.asInstanceOf[java.lang.Double]),
  (1, 612934495863046143L, 5.0: java.lang.Double),
  (1, 612934495900794879L, 5.0: java.lang.Double)
)).toDF("region", "cellid", "value")
val result = df.groupBy("region")
  .agg(hx.h3_cellfill(col("cellid"), col("value"), 1, "mean", 2.0).alias("filled"))
result.show(truncate = false)
""".trim

  val h3_cellfill_scala_example_output: String =
    """
+------+----------------------------------------------------------+
|region|filled                                                    |
+------+----------------------------------------------------------+
|1     |[{612934495919669247, 5.0}, {612934495863046143, 5.0}, ...]|
+------+----------------------------------------------------------+
... (ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> — center H3 cell filled to 5.0)""".trim

  val custom_cellfill_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view for the grid spec; inline cell data
// Heavy tier returns ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>
val dfGrid = spark.table("custom_grids")
val df = spark.createDataFrame(Seq(
  (1, 216172782113787048L, null.asInstanceOf[java.lang.Double]),
  (1, 216172782113786967L, 5.0: java.lang.Double),
  (1, 216172782113787127L, 5.0: java.lang.Double)
)).toDF("region", "cellid", "value").crossJoin(dfGrid.select("grid"))
val result = df.groupBy("region")
  .agg(cx.custom_cellfill(col("cellid"), col("value"), col("grid"), 1, "mean", 2.0).alias("filled"))
result.show(truncate = false)
""".trim

  val custom_cellfill_scala_example_output: String =
    """
+------+---------------------------------------------------------+
|region|filled                                                   |
+------+---------------------------------------------------------+
|1     |[{216172782113787048, 5.0}, {216172782113786967, 5.0}, ...]|
+------+---------------------------------------------------------+
... (ARRAY<STRUCT<cellid BIGINT, value DOUBLE>> — center custom cell filled to 5.0)""".trim

  val custom_kloop_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view (cell = 360287970373976640 at res=5, grid struct)
// At k=1 returns 8 cells: the hollow ring (center excluded)
val df = spark.table("custom_grids")
val result = df.select(cx.custom_kloop(col("cell"), col("grid"), lit(1)).alias("kloop"))
result.show(truncate = false)
""".trim

  val custom_kloop_scala_example_output: String =
    """
+-----------------------+
|kloop                  |
+-----------------------+
|[..., (8 cells at k=1)]|
+-----------------------+
... (8 BIGINT cell IDs — hollow ring at k=1, center cell excluded)""".trim

  val custom_distance_scala_example: String =
    """
import com.databricks.labs.gbx.gridx.custom.{functions => cx}
import org.apache.spark.sql.functions._

// Reads the custom_grids view for the grid spec
// Two adjacent cells at res=0 (1000m cells) → Chebyshev distance = 1
val df = spark.table("custom_grids")
val result = df.select(
  cx.custom_distance(
    cx.custom_pointascell(lit("POINT(530000 180000)"), col("grid"), lit(0)),
    col("grid"),
    cx.custom_pointascell(lit("POINT(531000 180000)"), col("grid"), lit(0))
  ).alias("dist")
)
result.show()
""".trim

  val custom_distance_scala_example_output: String =
    """
+----+
|dist|
+----+
|1   |
+----+
... (Chebyshev grid distance between two cells 1 step apart in X at resolution 0)""".trim

  // ===========================================================================
  // PMTiles family (Scala)
  // ===========================================================================

  val pmtiles_agg_scala_example: String =
    """
import com.databricks.labs.gbx.pmtiles.{functions => px}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Registration is a Python step: from databricks.labs.gbx import register; register(spark)
// 9 synthetic tiles at zoom level 2 (x in [0,2], y in [0,2]).
val rows = Seq(
  (2,0,0,"t00".getBytes),(2,0,1,"t01".getBytes),(2,0,2,"t02".getBytes),
  (2,1,0,"t10".getBytes),(2,1,1,"t11".getBytes),(2,1,2,"t12".getBytes),
  (2,2,0,"t20".getBytes),(2,2,1,"t21".getBytes),(2,2,2,"t22".getBytes))
val schema = StructType(Seq(
  StructField("z", IntegerType()), StructField("x", IntegerType()),
  StructField("y", IntegerType()), StructField("tile_bytes", BinaryType())))
val df = spark.createDataFrame(
  spark.sparkContext.parallelize(rows.map(r => org.apache.spark.sql.Row(r._1, r._2, r._3, r._4))),
  schema)
val result = df.agg(
  px.pmtiles_agg(col("tile_bytes"), col("z"), col("x"), col("y"),
    lit("{\"name\":\"my_tileset\"}")).alias("pmt"))
result.show()
""".trim

  val pmtiles_agg_scala_example_output: String =
    """
+------------------------------------------+
|pmt                                       |
+------------------------------------------+
|[50 4D 54 69 6C 65 73 03 ...]             |
+------------------------------------------+
(BINARY: PMTiles v3 archive — starts with magic bytes b'PMTiles' + version byte 3; contains 9 synthetic tiles at zoom 2)""".trim

  // ===========================================================================
  // SQL from a JVM job (docs/docs/installation.mdx)
  // ===========================================================================

  val jvm_job_dataframe_example: String =
    """
import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// The wrapper calls gbx_rst_width by name. A Python step ran gbx.register(spark)
// in this session, so the name resolves.
val widths = spark.table("main.geobrix.rasters")
  .select(col("path"), rx.rst_width(col("tile")).alias("width"))
widths.show()
""".trim

  val jvm_job_uc_example: String =
    """
// No registration. An admin ran gbx.uc.install("main", "geobrix", ...) once.
// A three-level name always runs the Unity Catalog function.
val ring = spark.sql("SELECT main.geobrix.gbx_bng_kring('TQ3080', 1) AS cells")
ring.show(truncate = false)
""".trim

}
