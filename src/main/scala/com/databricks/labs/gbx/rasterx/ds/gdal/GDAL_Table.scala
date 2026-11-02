package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** Spark Connector Table for the GDAL data source: reads (GDAL_Batch). A write is refused with a `Fix:` naming the
  * Python writers. BATCH_WRITE and TRUNCATE are declared only so Spark reaches `newWriteBuilder`, where the refusal is,
  * rather than raising its bare "does not support batch write". */
class GDAL_Table(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead with SupportsWrite {

    /** Overrides Table.name: returns "gdal". */
    override def name(): String = "gdal"

    /** Overrides Table.schema: returns the read schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Builds a scan that produces (source path, tile struct) rows via GDAL_Reader. */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        // `supportsExternalMetadata` lets a write reach the refusal below, and Spark applies it to reads too: a schema
        // handed to `.schema(...)` arrives here instead of being rejected, so the read checks it against its own layout.
        if (schema.catalogString != GDAL_Table.ReadSchema.catalogString) {
            throw new IllegalArgumentException(GDAL_Table.schemaMismatchMsg(schema))
        }
        () => new GDAL_Batch(schema, properties ++ options.asScala)
    }

    /** Refuses every write: the Scala GDAL writers are retired and the Python writers serve every compute. */
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
        throw new UnsupportedOperationException(GDAL_Table.WriteRetiredMsg)

    /** Overrides Table.capabilities: BATCH_READ, plus the write capabilities that route a write to the refusal above. */
    override def capabilities(): java.util.Set[TableCapability] =
        Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE).asJava

}

object GDAL_Table {

    /** The read schema: source path and the tile struct (binary raster content). */
    val ReadSchema: StructType = StructType(
      Array(
        StructField("source", StringType),
        StructField("tile", RST_ExpressionUtil.tileDataType(BinaryType))
      )
    )

    /** The refusal for a read given a schema other than [[ReadSchema]]. */
    def schemaMismatchMsg(given: StructType): String =
        s"The 'gdal' and 'gtiff_gdal' readers have a fixed schema (${ReadSchema.simpleString}); " +
            s"a schema of ${given.simpleString} was given. Fix: drop .schema(...) and select or cast the columns after the read."

    /** The refusal for a write to `gdal` or `gtiff_gdal`. */
    val WriteRetiredMsg: String =
        "The 'gdal' and 'gtiff_gdal' formats are read-only: the Scala raster writers were retired in 0.6.0. " +
            "Fix: write with a Python writer: 'raster_gbx', 'gtiff_gbx' or 'cog_gbx', after " +
            "`from databricks.labs.gbx.ds.register import register; register(spark)`. See the Raster Writer docs page."
}
