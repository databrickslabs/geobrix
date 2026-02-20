package com.databricks.labs.gbx.ds.register

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

/** TableProvider that runs a single scan to trigger side effects (e.g. GeoBrix function registration); shortName = register_ds. */
class RegisterDataSource extends TableProvider with DataSourceRegister {

    /** Overrides TableProvider.inferSchema: single column did_read (Boolean). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        StructType(Array(StructField("did_read", BooleanType)))
    }

    /** Overrides TableProvider.getTable: returns RegisterTable (scan runs RegisterBatch). */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new RegisterTable(schema, properties.asScala.toMap)
    }

    /** Overrides DataSourceRegister.shortName: returns "register_ds". */
    override def shortName(): String = "register_ds"

}
