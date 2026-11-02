package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
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
        GDAL_Table.ReadSchema
    }

    /** Overrides TableProvider.getTable: returns GDAL_Table with given schema and properties. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new GDAL_Table(schema, properties.asScala.toMap)
    }

    /** True so a write reaches GDAL_Table.newWriteBuilder (the retirement refusal) whatever the DataFrame's schema;
      * GDAL_Table.newScanBuilder then rejects a read given a schema other than the fixed one. */
    override def supportsExternalMetadata(): Boolean = true

    /** Overrides DataSourceRegister.shortName: returns "gdal". */
    override def shortName(): String = "gdal"

}
