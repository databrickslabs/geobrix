package tests.docs.scala.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import tests.docs.scala.SampleDataPath

/**
  * Shapefile Reader Examples - Single Source of Truth
  *
  * All Scala code examples shown in docs/docs/readers/shapefile.mdx are defined here.
  * Uses payload-only pattern: object constants for docs display, methods for test validation.
  */
object ShapefileExamples {

  // Display constants (payload only) - shown in documentation
  val READ_SHAPEFILE: String =
    """val df = spark.read.format("shapefile_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")"""

  /** Example output for READ_SHAPEFILE (displayed via outputConstant in shapefile.mdx). */
  val READ_SHAPEFILE_output: String =
    """+--------------------+-----------+----+
|geom_0              |geom_0_srid|name|
+--------------------+-----------+----+
|[BINARY]            |4326       |... |
|...                 |...        |... |
+--------------------+-----------+----+"""

  val READ_WITH_OPTIONS: String =
    """val df = spark.read.format("shapefile_ogr")
      |  .option("chunkSize", "50000")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")""".stripMargin

  val SQL_SHAPEFILE: String =
    """SELECT * FROM shapefile_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip`;"""

  // Test methods (validate logic) - used by ScalaTest
  def readShapefile(spark: SparkSession, path: String = SampleDataPath.nycSubwayShp): DataFrame = {
    spark.read.format("shapefile_ogr").load(path)
  }

  def readWithOptions(spark: SparkSession, path: String = SampleDataPath.nycSubwayShp): DataFrame = {
    spark.read.format("shapefile_ogr")
      .option("chunkSize", "50000")
      .load(path)
  }
}
