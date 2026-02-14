package tests.docs.scala.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * GeoPackage Reader Examples - Single Source of Truth
  *
  * All Scala code examples shown in docs/docs/readers/geopackage.mdx are defined here.
  * Uses payload-only pattern: object constants for docs display, methods for test validation.
  */
object GeoPackageExamples {

  // Display constants (payload only) - shown in documentation
  val READ_GEOPACKAGE: String =
    """val df = spark.read.format("gpkg_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg")"""

  /** Example output for READ_GEOPACKAGE (displayed via outputConstant in geopackage.mdx). */
  val READ_GEOPACKAGE_output: String =
    """+--------------------+--------------+---------+
|shape               |shape_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |Manhattan|
|...                 |...           |...      |
+--------------------+--------------+---------+"""

  val READ_SPECIFIC_LAYER: String =
    """val boroughs = spark.read.format("gpkg_ogr")
      |  .option("layerName", "boroughs")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg")""".stripMargin

  val SQL_GEOPACKAGE: String =
    """SELECT * FROM gpkg_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg`;"""

  // Test methods (validate logic) - used by ScalaTest
  def readGeoPackage(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg"): DataFrame = {
    spark.read.format("gpkg_ogr").load(path)
  }

  def readSpecificLayer(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg", layer: String = "boroughs"): DataFrame = {
    spark.read.format("gpkg_ogr")
      .option("layerName", layer)
      .load(path)
  }
}
