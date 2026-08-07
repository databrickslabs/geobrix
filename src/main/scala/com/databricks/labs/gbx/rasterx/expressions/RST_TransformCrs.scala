package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{RasterProject, SpatialRefOps}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Reproject (warp) a raster tile to a CRS given as a STRING, resampling the pixel
  * grid. The string companion to `gbx_rst_transform` (which takes an EPSG int).
  *
  * The CRS argument follows the shared int-cast rule (see `SpatialRefOps.resolveCrs`):
  * an int-castable string (`"3857"`) → EPSG; any other string → GDAL's universal
  * parser (`EPSG:x` / `ESRI:x` / WKT / PROJ4). Unlike `gbx_rst_transform` this accepts
  * a non-EPSG target (ESRI/WKT) and does NOT require a positive EPSG code.
  */
case class RST_TransformCrs(
    tile: Expression,
    crsExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, crsExpr, ExpressionConfigExpr())
    // Pin crs as StringType so SQL string literals coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(tile.dataType, StringType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_TransformCrs.name
    override def replacement: Expression = invoke(RST_TransformCrs)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_TransformCrs extends WithExpressionInfo {

    def eval(row: InternalRow, crs: UTF8String, conf: UTF8String): InternalRow = eval(row, crs, conf, BinaryType)

    def eval(row: InternalRow, crs: UTF8String, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resultDs, metadata) = execute(ds, options, crs.toString)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, metadata), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          dt
        )

    /** Warp `ds` to the CRS resolved from `crs` (EPSG int-string, EPSG:/ESRI: auth,
      * WKT, or PROJ4). Accepts non-EPSG targets; RasterProject's WKT fallback handles
      * authority-less targets. Caller releases the returned Dataset. */
    def execute(ds: Dataset, options: Map[String, String], crs: String): (Dataset, Map[String, String]) = {
        val dstSR = SpatialRefOps.resolveCrs(crs)
        RasterProject.project(ds, options, dstSR)
    }

    override def name: String = "gbx_rst_transformcrs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_TransformCrs(c(0), c(1))

}
