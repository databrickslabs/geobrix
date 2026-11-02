package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** Spark Connector Table for the GDAL data source: reads only (GDAL_Batch). Writing is the Python writers' job. */
class GDAL_Table(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    /** Overrides Table.name: returns "gdal". */
    override def name(): String = "gdal"

    /** Overrides Table.schema: returns the read schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Builds a scan that produces (source path, tile struct) rows via GDAL_Reader. */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new GDAL_Batch(schema, properties ++ options.asScala)
    }

    /** Overrides Table.capabilities: BATCH_READ only. */
    override def capabilities(): java.util.Set[TableCapability] =
        Set(TableCapability.BATCH_READ).asJava

}
