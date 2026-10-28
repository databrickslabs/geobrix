package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try
import scala.util.control.NonFatal

/** Raised by a generator's own argument guard (for example the `gbx_rst_xyzpyramid` tile-count cap) so the
  * message reaches the caller. Extends IllegalArgumentException, but unlike an ordinary per-row IAE it is
  * re-thrown by [[RST_ErrorHandler.safeEvalNullElement]] rather than degraded to a NULL row.
  */
final class GeneratorGuardException(msg: String) extends IllegalArgumentException(msg)

/**
  * Wraps raster expression eval so failures produce an error row (with error_message in metadata)
  * instead of failing the task, unless ExpressionConfig.crashExpressions is true.
  */
object RST_ErrorHandler extends Logging {

    /** True if metadata contains RasterX error keys (e.g. from createErrorMetadata). */
    private def hasError(metadata: Map[String, String]): Boolean = {
        metadata.contains("error_message")
    }

    /** Builds metadata map with error message and class name for safeEval error rows. The reason is under
      * `error_message` and under `last_error`, the key every other tile operation, the light tier and the
      * manager's error-tile contract use. */
    private def createErrorMetadata(error: Throwable): Map[String, String] = {
        Map(
          "error_message" -> error.getMessage,
          "last_error" -> Option(error.getMessage).getOrElse(error.getClass.getName),
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
      * @param elementFieldCount the declared field count of each element struct (3 for v1, 9 for v2).
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
      *
      * The tile generators declare ONE column, `tile`, so the error tile is wrapped in a one-field
      * row exactly like the rows of the success path. Returning the bare 9-field tile row hands Spark
      * a Long where it expects the struct and fails with a ClassCastException.
      *
      * @param tileRow
      *   the tile row the generator is evaluating (its tile argument, not the generator's input row);
      *   the error tile carries its cell id, read without opening the raster, or -1 when it has none.
      */
    def safeEval(eval: () => IterableOnce[InternalRow], tileRow: InternalRow, rasterType: DataType): IterableOnce[InternalRow] = {
        try {
            eval()
        } catch {
            case e: VirtualTileException => throw e
            case e: Throwable =>
                val errorMetadata = createErrorMetadata(e)
                val errorTile = RasterSerializationUtil.tileToRow((RasterSerializationUtil.cellIdOf(tileRow), null, errorMetadata), rasterType, null)
                Seq(InternalRow.fromSeq(Seq(errorTile)))
        }
    }

    /** Generator variant for generators whose single output column cannot carry an error tile
      * (the `(z, x, y, bytes)` struct of `gbx_rst_xyzpyramid` has no metadata map).
      *
      * A failure while reading or rendering the raster is a per-row data error: it emits ONE row whose
      * column is NULL, so the failure stays visible and auditable (`WHERE tile IS NULL`), and is logged.
      * Only two things are re-thrown: [[VirtualTileException]] (a virtual tile reached a heavyweight
      * function) and [[GeneratorGuardException]] (the generator's own argument guard, whose message must
      * reach the caller). Any other `IllegalArgumentException`, such as one raised while reading the tile,
      * is data-origin and degrades like every other failure here, as in the scalar overload
      * (see [[safeEval]] with a conf). Fatal errors (out of memory, ...) are never swallowed.
      */
    def safeEvalNullElement(eval: () => IterableOnce[InternalRow]): IterableOnce[InternalRow] = {
        try {
            eval()
        } catch {
            case e: VirtualTileException    => throw e
            case e: GeneratorGuardException => throw e
            case NonFatal(e) =>
                logWarning(s"RasterX generator evaluation failed; emitting one NULL row: ${e.getMessage}", e)
                Seq(InternalRow.fromSeq(Seq(null)))
        }
    }

}
