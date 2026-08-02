package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/**
  * Wraps raster expression eval so failures produce an error row (with error_message in metadata)
  * instead of failing the task, unless ExpressionConfig.crashExpressions is true.
  */
object RST_ErrorHandler extends Logging {

    /** True if metadata contains RasterX error keys (e.g. from createErrorMetadata). */
    private def hasError(metadata: Map[String, String]): Boolean = {
        metadata.contains("error_message")
    }

    /** Builds metadata map with error message and class name for safeEval error rows. */
    private def createErrorMetadata(error: Throwable): Map[String, String] = {
        Map(
          "error_message" -> error.getMessage,
          "error_detail" -> error.getStackTrace.mkString("\n"),
          "gdal_error" -> org.gdal.gdal.gdal.GetLastErrorMsg()
        )
    }

    /** Run eval; on exception return a tile row with error metadata instead of throwing.
      * VirtualTileExceptions are always re-thrown — they indicate a virtual tile reaching a
      * heavyweight function and must surface as hard failures, not silent error rows.
      */
    def safeEval(eval: () => InternalRow, row: InternalRow, rasterType: DataType): InternalRow = {
        try {
            eval()
        } catch {
            case e: VirtualTileException => throw e
            case e: Throwable =>
                // Check if input already had error
                val (cellId, metadata) = Try { // just in case of malformed rows and unexpected errors
                    val (cellId, ds, metadata) = RasterSerializationUtil.rowToTile(row, rasterType)
                    RasterDriver.releaseDataset(ds)
                    (cellId, metadata)
                }.getOrElse((-1L, Map.empty[String, String]))
                if (hasError(metadata)) {
                    // Return input as-is since it already had error
                    row
                } else {
                    // Create new error row
                    val errorMetadata = createErrorMetadata(e)
                    RasterSerializationUtil.tileToRow((cellId, null, errorMetadata), rasterType, null)
                }
        }
    }

    /** Like safeEval for single row but for array of rows; returns first row with error metadata or propagates.
      * VirtualTileExceptions are always re-thrown — they indicate a virtual tile reaching a
      * heavyweight function and must surface as hard failures, not silent error rows.
      *
      * @param elementFieldCount the declared field count of each element struct (3 for v1, 8 for v2).
      *                          Passed from the expression's declared input schema.
      */
    def safeEval(eval: () => InternalRow, rows: ArrayData, rasterType: DataType, elementFieldCount: Int = 3): InternalRow = {
        try {
            eval()
        } catch {
            case e: VirtualTileException => throw e
            case e: Throwable =>
                // Check if input already had error
                val errorIdx = (0 until rows.numElements()).find { i =>
                    val row = rows.getStruct(i, elementFieldCount)
                    val metadata = Try { // just in case of malformed rows and unexpected errors
                        val (_, ds, metadata) = RasterSerializationUtil.rowToTile(row, rasterType)
                        RasterDriver.releaseDataset(ds)
                        metadata
                    }.getOrElse(Map.empty[String, String])
                    hasError(metadata)
                }
                if (errorIdx.nonEmpty) {
                    // Return the input row that already had the error
                    rows.getStruct(errorIdx.get, elementFieldCount)
                } else {
                    // Create new error row
                    val errorMetadata = createErrorMetadata(e)
                    RasterSerializationUtil.tileToRow((-1, null, errorMetadata), rasterType, null)
                }
        }
    }

    /** Runs eval; on exception returns null or throws if ExpressionConfig.crashExpressions is true.
      *
      * VirtualTileExceptions are always re-thrown regardless of crashExpressions: they indicate
      * a virtual tile reaching a heavyweight function and must surface as hard failures. Ordinary
      * per-row IllegalArgumentExceptions (e.g. non-Point geometry for rst_sample, bad EPSG code
      * for rst_transform) fall through to the null-tolerant path so they remain swallowable.
      */
    def safeEval(eval: () => Any, row: InternalRow, rasterType: DataType, conf: UTF8String): Any = {
        try {
            eval()
        } catch {
            case e: VirtualTileException =>
                // Virtual-tile guard must propagate — wrong API usage, not a per-row data error.
                throw e
            case t: Throwable =>
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                if (exprConf.crashExpressions) {
                    val (cellId, metadata) = Try { // just in case of malformed rows and unexpected errors
                        val (cellId, ds, metadata) = RasterSerializationUtil.rowToTile(row, rasterType)
                        RasterDriver.releaseDataset(ds)
                        (cellId, metadata)
                    }.getOrElse((-1L, Map.empty[String, String]))
                    val wrapped = new Error(s"""
                                       |Error during expression evaluation. Cell ID: $cellId
                                       |Metadata: $metadata
                                       |""".stripMargin)
                    // Chain the real cause (FileNotFound / IO / GDAL / ...) so it shows up as
                    // `Caused by:` instead of being lost behind the generic wrapper message.
                    wrapped.initCause(t)
                    throw wrapped
                }
                // Default (non-crashing) mode: log the swallowed failure so it isn't entirely
                // silent -- otherwise a null tile only surfaces downstream as an opaque
                // "Long cannot be cast to InternalRow" with no link back to the real error.
                logWarning(s"RasterX expression evaluation failed; returning null tile: ${t.getMessage}", t)
                null // swallow the error and return null for any eval
        }
    }

    /** Runs generator eval; on exception returns single row with error metadata.
      * VirtualTileExceptions are always re-thrown — they indicate a virtual tile reaching a
      * heavyweight function and must surface as hard failures, not silent error rows.
      */
    def safeEval(eval: () => IterableOnce[InternalRow], row: InternalRow, rasterType: DataType): IterableOnce[InternalRow] = {
        try {
            eval()
        } catch {
            case e: VirtualTileException => throw e
            case e: Throwable =>
                val cellId = Try { // just in case of malformed rows and unexpected errors
                    val (cellId, ds, _) = RasterSerializationUtil.rowToTile(row, rasterType)
                    RasterDriver.releaseDataset(ds)
                    cellId
                }.getOrElse(-1L)
                val errorMetadata = createErrorMetadata(e)
                Seq(RasterSerializationUtil.tileToRow((cellId, null, errorMetadata), rasterType, null))
        }
    }

}
