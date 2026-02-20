package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{CheckpointManager, GDALManager}
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskFailureListener

import scala.util.Try

/**
  * Helpers for RasterX expressions: tile struct type, GDAL/checkpoint init, and iterator cleanup.
  *
  * Tile struct is (cellid, raster, metadata); raster type is String (path) or Binary (content).
  */
object RST_ExpressionUtil {

    /** DataType of the raster field (second field) of the tile struct for the given tile expression. */
    def rasterType(tileExpr: Expression): DataType = tileExpr.dataType.asInstanceOf[StructType].fields(1).dataType

    /** StructType for a tile with the given tile expression's raster type (cellid, raster, metadata). */
    def tileDataType(tileExpr: Expression): DataType = {
        val rasterDataType = rasterType(tileExpr)
        StructType(
          Seq(
            StructField("cellid", LongType, nullable = false),
            StructField("raster", rasterDataType, nullable = false),
            StructField("metadata", MapType(StringType, StringType), nullable = true)
          )
        )
    }

    /** StructType for a tile with the given raster DataType (cellid, raster, metadata). */
    def tileDataType(rdt: DataType): DataType = {
        StructType(
          Seq(
            StructField("cellid", LongType, nullable = false),
            StructField("raster", rdt, nullable = false),
            StructField("metadata", MapType(StringType, StringType), nullable = true)
          )
        )
    }

    /** Initialize NodeFileManager, GDAL, and CheckpointManager for this process (e.g. on executor). */
    def init(expressionConfig: ExpressionConfig): Unit = {
        NodeFileManager.init(expressionConfig.hConf)
        GDALManager.init(expressionConfig)
        CheckpointManager.init(expressionConfig)
    }

    /** Register task completion/failure listeners to close the given iterator (e.g. release resources). */
    def addCleanupListener(it: Iterator[_]): Unit = {
        val iter = it.asInstanceOf[AutoCloseable]
        Try {
            val tc = org.apache.spark.TaskContext.get()
            tc.addTaskCompletionListener[Unit](_ => iter.close())
            tc.addTaskFailureListener(new TaskFailureListener() {
                override def onTaskFailure(context: TaskContext, error: Throwable): Unit = iter.close()
            })
        }
    }

}
