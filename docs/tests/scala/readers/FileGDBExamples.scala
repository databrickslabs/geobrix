package com.databricks.labs.gbx.docs.readers

/**
 * File Geodatabase Reader Examples - Single Source of Truth
 *
 * All Scala code examples shown in docs/docs/readers/filegdb.mdx are imported from this object.
 */
object FileGDBExamples {

  val FILEGDB_ZIP_PATH: String = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip"

  // Display constants (payload only - extracted by CodeFromTest component)
  val READ_FILEGDB: String =
    """// Read File Geodatabase (.zip; sample data is distributed as NYC_Sample.gdb.zip)
      |val df = spark.read.format("file_gdb_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip")""".stripMargin

  /** Example output for READ_FILEGDB (displayed via outputConstant in filegdb.mdx). */
  val READ_FILEGDB_output: String =
    """+--------------------+--------------+---------+
|SHAPE               |SHAPE_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |...      |
|...                 |...           |...      |
+--------------------+--------------+---------+"""

  val READ_WITH_LAYER: String =
    """// Read specific feature class by name
      |val df = spark.read
      |  .format("file_gdb_ogr")
      |  .option("layerName", "NYC_Boroughs")
      |  .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip")""".stripMargin

  val SQL_FILEGDB: String =
    """-- Read File Geodatabase in SQL
      |SELECT * FROM file_gdb_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip` LIMIT 10;""".stripMargin

  // Test methods (validate logic)
  def readFileGDB()(implicit spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    spark.read.format("file_gdb_ogr").load(FILEGDB_ZIP_PATH)
  }

  def readWithLayer(layer: String = "NYC_Boroughs")(implicit spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    spark.read
      .format("file_gdb_ogr")
      .option("layerName", layer)
      .load(FILEGDB_ZIP_PATH)
  }
}
