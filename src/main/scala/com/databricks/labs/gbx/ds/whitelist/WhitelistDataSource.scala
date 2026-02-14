package com.databricks.labs.gbx.ds.whitelist

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Data source that exposes a single "did_read" column; used to whitelist paths or trigger
  * side effects (e.g. registration) via a trivial read. See WhitelistTable/WhitelistReader.
  */
class WhitelistDataSource extends TableProvider with DataSourceRegister {

    /** Overrides TableProvider.inferSchema: returns empty schema (no columns). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        StructType(Array(StructField("did_read", BooleanType)))
    }

    /** Overrides TableProvider.getTable: returns WhitelistTable. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new WhitelistTable(schema, properties.asScala.toMap)
    }

    /** Overrides DataSourceRegister.shortName: returns "whitelist_ds". */
    override def shortName(): String = "whitelist_ds"

}
