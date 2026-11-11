package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.operations.ProjDatabaseException
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/** Raised by a tile function when an argument is wrong for the tile it is given (a band index past the tile's last
  * band, a subdataset name the tile does not hold): the tile is only known once it is open, inside
  * [[RST_ErrorHandler.safeEval]], which re-throws this one so that it names the value, as every other wrong argument
  * does. (A wrong argument that needs no tile is checked before `safeEval`.)
  */
final class RasterArgumentException(msg: String) extends IllegalArgumentException(msg)

/**
  * Wraps raster expression eval so failures produce an error row (with error_message and last_error in metadata)
  * instead of failing the task, unless ExpressionConfig.crashExpressions is true.
  */
object RST_ErrorHandler extends Logging {

    /** `eval` with no other thread of this JVM in GeoBrix's netCDF / HDF GDAL work when `hdf` (a tile of such a file): the
      * whole expression, from the open to the release, because GDAL's netCDF driver reads a band's metadata outside its
      * own mutex and the payload's HDF5 is not thread-safe ([[GDALManager.withHdfLock]]; reentrant, and no Spark task is
      * waited on inside an expression). */
    private def underHdfLock[T](hdf: Boolean)(eval: () => T): T = GDALManager.withHdfLockIf(hdf)(eval())

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
            underHdfLock(RasterSerializationUtil.isHdfTile(row))(eval)
        } catch {
            case e: VirtualTileException => throw e
            case e: ProjDatabaseException => throw e
            case e: RasterArgumentException => throw e
            case NonFatal(e) =>
                // Fatal errors (out of memory, stack overflow) fail the task so it can be retried; they are not data.
                if (RasterSerializationUtil.isErrorTile(row)) {
                    // The input already is an error tile: it goes out unchanged, so the upstream diagnostic survives.
                    row
                } else {
                    // Create new error row; the cell id is read from the row, never by opening the raster.
                    val errorMetadata = createErrorMetadata(e)
                    RasterSerializationUtil.tileToRow((RasterSerializationUtil.cellIdOf(row), null, errorMetadata), rasterType, null)
                }
        }
    }

    /** Runs eval; on exception returns null or throws if ExpressionConfig.crashExpressions is true.
      *
      * VirtualTileExceptions are always re-thrown regardless of crashExpressions: they indicate
      * a virtual tile reaching a heavyweight function and must surface as hard failures. Ordinary
      * per-row IllegalArgumentExceptions (e.g. non-Point geometry for rst_sample, a bad EPSG code
      * in a reprojection) fall through to the null-tolerant path so they remain swallowable.
      */
    def safeEval(eval: () => Any, row: InternalRow, rasterType: DataType, conf: UTF8String): Any = {
        try {
            underHdfLock(RasterSerializationUtil.isHdfTile(row))(eval)
        } catch {
            case e: VirtualTileException =>
                // Virtual-tile guard must propagate — wrong API usage, not a per-row data error.
                throw e
            case e: ProjDatabaseException =>
                // PROJ's database is unreadable on this thread: the answer would be wrong, not NULL data.
                throw e
            case NonFatal(t) =>
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                if (exprConf.crashExpressions) {
                    // Cell id and metadata come from the row itself, never by opening the raster.
                    val cellId = RasterSerializationUtil.cellIdOf(row)
                    val metadata = RasterSerializationUtil.metadataOf(row)
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
      * A NULL tile has nothing to split: no rows, as `explode(NULL)` gives and as the light tier does. An error
      * tile in comes out as itself in the one-column row, so the upstream diagnostic survives; it is found
      * structurally and never opened, so no failure is needed to reach it.
      *
      * @param tileRow
      *   the tile row the generator is evaluating (its tile argument, not the generator's input row);
      *   the error tile carries its cell id, read without opening the raster, or -1 when it has none.
      */
    def safeEval(eval: () => IterableOnce[InternalRow], tileRow: InternalRow, rasterType: DataType): IterableOnce[InternalRow] = {
        if (tileRow == null) return Seq.empty
        if (RasterSerializationUtil.isErrorTile(tileRow)) return Seq(InternalRow.fromSeq(Seq(tileRow)))
        try {
            eval()
        } catch {
            case e: VirtualTileException => throw e
            case e: ProjDatabaseException => throw e
            case NonFatal(e) =>
                val errorMetadata = createErrorMetadata(e)
                val errorTile = RasterSerializationUtil.tileToRow((RasterSerializationUtil.cellIdOf(tileRow), null, errorMetadata), rasterType, null)
                Seq(InternalRow.fromSeq(Seq(errorTile)))
        }
    }

}
