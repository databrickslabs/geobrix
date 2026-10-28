package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/** Raised by a generator's own argument guard (for example the `gbx_rst_xyzpyramid` tile-count cap) so the
  * message reaches the caller. Extends IllegalArgumentException, but unlike an ordinary per-row IAE it is
  * re-thrown by [[RST_ErrorHandler.safeEvalNullElement]] rather than degraded to a NULL row.
  */
final class GeneratorGuardException(msg: String) extends IllegalArgumentException(msg)

/**
  * Wraps raster expression eval so failures produce an error row (with error_message and last_error in metadata)
  * instead of failing the task, unless ExpressionConfig.crashExpressions is true.
  */
object RST_ErrorHandler extends Logging {

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
            case NonFatal(e) =>
                // Check if an input already was an error tile (read structurally, without opening any raster)
                val errorIdx = (0 until rows.numElements()).find { i =>
                    RasterSerializationUtil.isErrorTile(rows.getStruct(i, elementFieldCount))
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
            case NonFatal(e) =>
                val errorMetadata = createErrorMetadata(e)
                val errorTile = RasterSerializationUtil.tileToRow((RasterSerializationUtil.cellIdOf(tileRow), null, errorMetadata), rasterType, null)
                Seq(InternalRow.fromSeq(Seq(errorTile)))
        }
    }

    /** What an aggregate answers for a group of nothing but error tiles: one error tile with the members' reasons
      * (distinct, in arrival order), so the upstream diagnostic survives the aggregate as it survives a scalar.
      * One member is that member as it is. NULL when any member is not an error tile, or the group is empty: a group
      * with a readable member is an ordinary one, and the aggregate drops the error tiles and says how many.
      *
      * @param members the aggregate's buffered tile rows
      */
    def errorTileOfGroup(members: scala.collection.Seq[Any], rasterType: DataType): InternalRow = {
        val rows = members.map(_.asInstanceOf[InternalRow])
        if (rows.isEmpty || !rows.forall(RasterSerializationUtil.isErrorTile)) null
        else if (rows.size == 1) rows.head
        else {
            val reasons = rows.flatMap { r =>
                val md = RasterSerializationUtil.metadataOf(r)
                md.get("last_error").orElse(md.get("error_message"))
            }.distinct
            val cellIds = rows.map(RasterSerializationUtil.cellIdOf).distinct
            val cellId = if (cellIds.size == 1) cellIds.head else -1L // several cell ids: none, as the readable groups do
            RasterSerializationUtil.tileToRow((cellId, null, Map("last_error" -> reasons.mkString("; "))), rasterType, null)
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
