package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/** BatchWrite for GDAL: creates GDAL_DataWriterFactory with path, nameCol, and ext from options. */
class GDAL_BatchWrite(schema: StructType, options: Map[String, String], expressionConfig: ExpressionConfig) extends BatchWrite {

    /** Builds a factory that creates GDAL_RowWriter per partition/task. */
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
        val root = options("path")
        val nameCol = options.get("nameCol")
        val ext = options.getOrElse("ext", "tif") // default to tif
        new GDAL_DataWriterFactory(schema, root, nameCol, ext, expressionConfig)
    }
    /** Overrides BatchWrite.commit: no-op (individual commits in GDAL_WriterMsg). */
    override def commit(messages: Array[WriterCommitMessage]): Unit = ()
    /** Overrides BatchWrite.abort: no-op. */
    override def abort(messages: Array[WriterCommitMessage]): Unit = ()

}
