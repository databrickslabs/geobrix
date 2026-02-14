package tests.docs.scala.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * GeoTIFF Reader Examples - Single Source of Truth
  *
  * All Scala code examples shown in docs/docs/readers/gtiff.mdx are defined here.
  * Uses payload-only pattern: object constants for docs display, methods for test validation.
  */
object GTiffExamples {

  // Display constants (payload only) - shown in documentation
  val READ_GTIFF: String =
    """val df = spark.read.format("gtiff_gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")"""

  /** Example output for READ_GTIFF (displayed via outputConstant in gtiff.mdx). */
  val READ_GTIFF_output: String =
    """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                 |{...}|
+--------------------------------------------------+-----+"""

  val READ_WITH_OPTIONS: String =
    """val df = spark.read.format("gtiff_gdal")
      |  .option("readSubdatasets", "false")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")""".stripMargin

  val SQL_GTIFF: String =
    """SELECT * FROM gtiff_gdal.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif` LIMIT 10;"""

  // Test methods (validate logic) - used by ScalaTest
  def readGTiff(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"): DataFrame = {
    spark.read.format("gtiff_gdal").load(path)
  }

  def readWithOptions(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"): DataFrame = {
    spark.read.format("gtiff_gdal")
      .option("readSubdatasets", "false")
      .load(path)
  }
}
