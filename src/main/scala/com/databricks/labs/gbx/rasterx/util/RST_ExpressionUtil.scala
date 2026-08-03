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

    /**
      * Field count of the element StructType inside an ARRAY&lt;tile&gt; expression.
      * Returns 3 for a v1 tile (3-field) or 8 for a v2 tile (8-field), derived from the
      * declared schema so it is automatically correct for whatever the input is.
      * Defaults to 3 (v1) if the expression type does not match ArrayType(StructType, _).
      */
    def arrayOfTileElementFieldCount(tileExpr: Expression): Int = tileExpr.dataType match {
        case ArrayType(st: StructType, _) => st.fields.length
        case _                            => 3
    }

    /** StructType for the window sub-struct (col_off, row_off, width, height). */
    val windowType: StructType = StructType(Seq(
        StructField("col_off", IntegerType, nullable = false),
        StructField("row_off", IntegerType, nullable = false),
        StructField("width", IntegerType, nullable = false),
        StructField("height", IntegerType, nullable = false)))

    /** Canonical v2 tile schema — 8 fields, matching the light-tier V2_TILE_SCHEMA byte-for-byte. */
    val v2TileType: StructType = StructType(Seq(
        StructField("cellid", LongType, nullable = false),
        StructField("raster", BinaryType, nullable = true),
        StructField("path", StringType, nullable = true),
        StructField("window", windowType, nullable = true),
        StructField("clip_polygon", BinaryType, nullable = true),
        StructField("clip_crs", StringType, nullable = true),
        StructField("crs", StringType, nullable = true),
        StructField("metadata", MapType(StringType, StringType), nullable = true)))

    /** StructType for a tile with the given tile expression's raster type (v2 8-field schema). */
    def tileDataType(tileExpr: Expression): DataType = v2TileType

    /** StructType for a tile with the given raster DataType (v2 8-field schema). */
    def tileDataType(rdt: DataType): DataType = v2TileType

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
