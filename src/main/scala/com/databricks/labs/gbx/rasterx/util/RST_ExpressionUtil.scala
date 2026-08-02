package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskFailureListener

import scala.util.Try

/**
  * Helpers for RasterX expressions: tile struct type, GDAL init, and iterator cleanup.
  *
  * Tile struct is (cellid, raster, metadata); raster type is Binary (content only; String path-tiles are rejected).
  */
object RST_ExpressionUtil {

    /** DataType of the raster field (second field) of the tile struct for the given tile expression.
      * Throws [[IllegalArgumentException]] if the raster field is StringType (v1 path-tile), which
      * is not supported by the heavyweight tier.
      */
    def rasterType(tileExpr: Expression): DataType = {
        val rdt = tileExpr.dataType.asInstanceOf[StructType].fields(1).dataType
        rdt match {
            case StringType => throw new IllegalArgumentException(
                "Raster path-tiles (raster field as a String path) are not supported by the " +
                "heavyweight tier. Materialize the raster to bytes in the lightweight tier " +
                "(materialize=True, or write + read back) before passing it to a heavyweight function.")
            case other => other
        }
    }

    /**
      * Raster DataType inside an `ARRAY<tile>` expression, with a friendly
      * IllegalArgumentException when the caller actually passed a single tile.
      *
      * Used by the non-aggregating array-of-tiles functions
      * (`gbx_rst_combineavg`, `gbx_rst_merge`, `gbx_rst_frombands`,
      * `gbx_rst_mapalgebra`). Without this guard, callers who write
      * `gbx_rst_combineavg(tile)` instead of `gbx_rst_combineavg(collect_list(tile))`
      * or the aggregator variant get a raw `ClassCastException: StructType
      * cannot be cast to ArrayType` from inside Spark's CheckAnalysis,
      * which is hostile and untraceable from a notebook.
      *
      * `funcName` is the SQL-facing name surfaced in the error.
      * `aggHint` is an optional pointer to the aggregator companion
      * (e.g. "gbx_rst_combineavg_agg") for functions where the typical
      * mistake is reaching for the non-agg form when an aggregate across
      * rows was wanted.
      *
      * Note: Spark 4.0's `AnalysisException` no longer exposes a
      * `(String)` constructor (only the error-class form), so the error
      * is raised as `IllegalArgumentException` — still surfaces during
      * Catalyst analysis with the full message, and avoids depending on
      * Spark-internal error-class catalogs.
      */
    def arrayOfTileRasterType(
        funcName: String,
        tileExpr: Expression,
        aggHint: Option[String] = None
    ): DataType = tileExpr.dataType match {
        case ArrayType(StructType(fields), _) if fields.length >= 2 =>
            fields(1).dataType match {
                case StringType => throw new IllegalArgumentException(
                    "Raster path-tiles (raster field as a String path) are not supported by the " +
                    "heavyweight tier. Materialize the raster to bytes in the lightweight tier " +
                    "(materialize=True, or write + read back) before passing it to a heavyweight function.")
                case other => other
            }
        case other =>
            val aggSuggestion = aggHint
                .map(name => s" To aggregate the column across rows, use $name(tile).")
                .getOrElse("")
            throw new IllegalArgumentException(
                s"$funcName expects ARRAY<tile> (e.g. collect_list(tile) " +
                s"or array(t1, t2, ...)), but received ${other.simpleString}." +
                aggSuggestion
            )
    }

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

    /** Initialize NodeFileManager and GDAL for this process (e.g. on executor). */
    def init(expressionConfig: ExpressionConfig): Unit = {
        NodeFileManager.init(expressionConfig.hConf)
        GDALManager.init(expressionConfig)
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
