package tests.docs.scala.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * GDAL Reader Examples - Single Source of Truth
  *
  * All Scala code examples shown in docs/docs/readers/gdal.mdx are defined here.
  * Uses payload-only pattern: object constants for docs display, methods for test validation.
  */
object GDALExamples {

  // Display constants (payload only) - shown in documentation
  val READ_GDAL: String =
    """val df = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")"""

  /** Example output for READ_GDAL (displayed via outputConstant in gdal.mdx). */
  val READ_GDAL_output: String =
    """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

  val READ_WITH_DRIVER: String =
    """val df = spark.read.format("gdal")
      |  .option("driver", "GTiff")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")""".stripMargin

  val SQL_GDAL: String =
    """SELECT * FROM gdal.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif` LIMIT 10;"""

  // Test methods (validate logic) - used by ScalaTest
  def readGDAL(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"): DataFrame = {
    spark.read.format("gdal").load(path)
  }

  def readWithDriver(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"): DataFrame = {
    spark.read.format("gdal")
      .option("driver", "GTiff")
      .load(path)
  }
}
