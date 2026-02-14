package com.databricks.labs.gbx.ds.whitelist

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.{MapHasAsScala, SetHasAsJava}

/** Table that supports only batch read; used to whitelist paths or trigger side effects (e.g. function registration). */
class WhitelistTable(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    /** Overrides Table.name: returns "whitelist_ds". */
    override def name(): String = "whitelist_ds"

    /** Overrides Table.schema: returns the provided schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Overrides SupportsRead.newScanBuilder: builds scan that runs WhitelistBatch (no rows or side effect). */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new WhitelistBatch(schema, properties ++ options.asScala)
    }

    /** Overrides Table.capabilities: BATCH_READ only. */
    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
