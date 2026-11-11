package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, TileBytes}
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._


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
    def rasterType(tile: Expression): DataType = {
        val rdt = tile.dataType.asInstanceOf[StructType].fields(1).dataType
        rdt match {
            case StringType => throw new IllegalArgumentException(
                "Raster path-tiles (raster field as a String path) are not supported by the " +
                "heavyweight tier. Materialize the raster to bytes in the lightweight tier " +
                "(materialize=True, or write + read back) before passing it to a heavyweight function.")
            case other => other
        }
    }

    /** StructType for the window sub-struct (col_off, row_off, width, height). */
    val windowType: StructType = StructType(Seq(
        StructField("col_off", IntegerType, nullable = false),
        StructField("row_off", IntegerType, nullable = false),
        StructField("width", IntegerType, nullable = false),
        StructField("height", IntegerType, nullable = false)))

    /** Canonical v2 tile schema — 9 fields, matching the light-tier V2_TILE_SCHEMA byte-for-byte.
      *
      * Field order invariant: ``path_mode`` sits immediately after ``path`` (grouping the two
      * path-provenance fields), and ``metadata`` is always the LAST field. */
    val v2TileType: StructType = StructType(Seq(
        StructField("cellid", LongType, nullable = false),
        StructField("raster", BinaryType, nullable = true),
        StructField("path", StringType, nullable = true),
        StructField("path_mode", StringType, nullable = true),
        StructField("window", windowType, nullable = true),
        StructField("clip_polygon", BinaryType, nullable = true),
        StructField("clip_crs", StringType, nullable = true),
        StructField("crs", StringType, nullable = true),
        StructField("metadata", MapType(StringType, StringType), nullable = true)))

    /** StructType for a tile with the given tile expression's raster type (v2 9-field schema). */
    def tileDataType(tile: Expression): DataType = v2TileType

    /** StructType for a tile with the given raster DataType (v2 9-field schema). */
    def tileDataType(rdt: DataType): DataType = v2TileType

    /** Initialize NodeFileManager (Hadoop config and copy retention) and GDAL for this process (e.g. on executor). */
    def init(expressionConfig: ExpressionConfig): Unit = {
        NodeFileManager.init(expressionConfig)
        GDALManager.init(expressionConfig)
        // the read cap of this task's tile reads (TileBytes, at RasterDriver.readFromBytes)
        TileBytes.useReadCap(expressionConfig.readCap)
    }

}
