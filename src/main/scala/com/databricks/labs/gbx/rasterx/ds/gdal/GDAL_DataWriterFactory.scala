package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

/** Factory that creates one GDAL_RowWriter per (partitionId, taskId) for writing tiles under root. */
class GDAL_DataWriterFactory(schema: StructType, root: String, nameCol: Option[String], ext: String, expressionConfig: ExpressionConfig)
    extends DataWriterFactory
      with Serializable {
    /** Overrides DataWriterFactory.createWriter: returns GDAL_RowWriter for this partition/task. */
    override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
        new GDAL_RowWriter(schema, root, nameCol, ext, partitionId, taskId, expressionConfig)
}
