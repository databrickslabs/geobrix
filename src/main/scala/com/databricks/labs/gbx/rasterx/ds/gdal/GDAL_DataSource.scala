package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Spark Data Source V2 provider for the "gdal" format: reads raster files (GeoTIFF, etc.) as
  * a DataFrame of tiles (source path, tile struct, metadata). Schema uses binary tile content
  * by default; actual reading is done by GDAL_Table/Partition/Reader.
  */
//noinspection ScalaUnusedSymbol
class GDAL_DataSource extends TableProvider with DataSourceRegister {

    /** Overrides TableProvider.inferSchema: fixed schema (source: String, tile: struct). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        StructType(
          Array(
            StructField("source", StringType),
            StructField("tile", RST_ExpressionUtil.tileDataType(BinaryType))
          )
        )
    }

    /** Overrides TableProvider.getTable: returns GDAL_Table with given schema and properties. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new GDAL_Table(schema, properties.asScala.toMap)
    }

    /** Overrides DataSourceRegister.shortName: returns "gdal". */
    override def shortName(): String = "gdal"

}
