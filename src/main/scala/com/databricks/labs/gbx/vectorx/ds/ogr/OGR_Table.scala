package com.databricks.labs.gbx.vectorx.ds.ogr

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** Spark Connector Table for OGR: batch read only, scan built via OGR_Batch. */
class OGR_Table(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    /** Overrides Table.name: returns "ogr". */
    override def name(): String = "ogr"

    /** Overrides Table.schema: returns the inferred read schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Overrides SupportsRead.newScanBuilder: builds scan that produces feature rows via OGR_Batch. */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new OGR_Batch(schema, properties ++ options.asScala)
    }

    /** Overrides Table.capabilities: BATCH_READ only. */
    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
