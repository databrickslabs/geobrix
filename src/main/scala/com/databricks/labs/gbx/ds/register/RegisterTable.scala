package com.databricks.labs.gbx.ds.register

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.{MapHasAsScala, SetHasAsJava}

/** Table for register_ds: batch read only; scan runs RegisterBatch (e.g. to register GeoBrix functions). */
class RegisterTable(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    /** Overrides Table.name: returns "register_ds". */
    override def name(): String = "register_ds"

    /** Overrides Table.schema: returns the provided schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Overrides SupportsRead.newScanBuilder: builds scan that runs RegisterBatch (registration side effect). */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new RegisterBatch(schema, properties ++ options.asScala)
    }

    /** Overrides Table.capabilities: BATCH_READ only. */
    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
