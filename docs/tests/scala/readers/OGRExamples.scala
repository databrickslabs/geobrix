package tests.docs.scala.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import tests.docs.scala.SampleDataPath

/**
  * OGR Reader Examples - Single Source of Truth
  *
  * All Scala code examples shown in docs/docs/readers/ogr.mdx are defined here.
  * Uses payload-only pattern: object constants for docs display, methods for test validation.
  */
object OGRExamples {

  // Display constants (payload only) - shown in documentation
  val READ_OGR: String =
    """val df = spark.read.format("ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson")"""

  /** Example output for READ_OGR (displayed via outputConstant in ogr.mdx). */
  val READ_OGR_output: String =
    """+--------------------+-----------+-----+
|geom_0              |geom_0_srid|...  |
+--------------------+-----------+-----+
|[BINARY]            |4326       |...  |
|...                 |...        |...  |
+--------------------+-----------+-----+"""

  val READ_WITH_DRIVER: String =
    """val df = spark.read.format("ogr")
      |  .option("driverName", "GeoJSON")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson")""".stripMargin

  val SQL_OGR: String =
    """SELECT * FROM ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson`;"""

  // Test methods (validate logic) - used by ScalaTest
  def readOGR(spark: SparkSession, path: String = SampleDataPath.nycBoroughs): DataFrame = {
    spark.read.format("ogr").load(path)
  }

  def readWithDriver(spark: SparkSession, path: String = SampleDataPath.nycBoroughs): DataFrame = {
    spark.read.format("ogr")
      .option("driverName", "GeoJSON")
      .load(path)
  }
}
