package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** Spark Connector Table for the GDAL data source: supports batch read (GDAL_Batch) and write (GDAL_WriteBuilder). */
class GDAL_Table(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead with SupportsWrite {

    /** Overrides Table.name: returns "gdal". */
    override def name(): String = "gdal"

    /** Overrides Table.schema: returns the read/write schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Builds a scan that produces (source path, tile struct) rows via GDAL_Reader. */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new GDAL_Batch(schema, properties ++ options.asScala)
    }

    /** Builds a write that consumes rows and writes rasters via GDAL_RowWriter. */
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        new GDAL_WriteBuilder(info.schema(), properties ++ info.options().asScala)
    }

    /** Overrides Table.capabilities: BATCH_READ and BATCH_WRITE. */
    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava

}
