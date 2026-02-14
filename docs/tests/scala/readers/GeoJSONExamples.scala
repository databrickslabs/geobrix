package tests.docs.scala.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * GeoJSON Reader Examples - Single Source of Truth
  *
  * All Scala code examples shown in docs/docs/readers/geojson.mdx are defined here.
  * Uses payload-only pattern: object constants for docs display, methods for test validation.
  */
object GeoJSONExamples {

  // Display constants (payload only) - shown in documentation
  val READ_GEOJSON: String =
    """val df = spark.read.format("geojson_ogr")
      |  .option("multi", "false")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson")""".stripMargin

  /** Example output for READ_GEOJSON (displayed via outputConstant in geojson.mdx). */
  val READ_GEOJSON_output: String =
    """+--------------------+-----------+---------+
|geom_0              |geom_0_srid|BoroName |
+--------------------+-----------+---------+
|[BINARY]            |4326       |Manhattan|
|...                 |...        |...      |
+--------------------+-----------+---------+"""

  val READ_GEOJSONSEQ: String =
    """val df = spark.read.format("geojson_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojsonl")""".stripMargin

  val SQL_GEOJSON: String =
    """SELECT * FROM geojson_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson`;"""

  // Test methods (validate logic) - used by ScalaTest
  def readGeoJSON(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"): DataFrame = {
    spark.read.format("geojson_ogr")
      .option("multi", "false")
      .load(path)
  }

  def readGeoJSONSeq(spark: SparkSession, path: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojsonl"): DataFrame = {
    spark.read.format("geojson_ogr").load(path)
  }
}
