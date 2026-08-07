package com.databricks.labs.gbx.rasterx.expressions.resample

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Resample a raster tile to an explicit ground resolution (`x_res`, `y_res`)
  * in source CRS units (e.g. metres for UTM, degrees for EPSG:4326).
  *
  * `gdalwarp -tr xRes yRes` chooses the output grid; output extent matches the
  * source bounding box adjusted to the new pixel size. CRS is preserved.
  * `algorithm` defaults to `"bilinear"`; see [[RST_ResampleHelper.AllowedAlgorithms]].
  */
case class RST_ResampleToRes(
    tile: Expression,
    xResExpr: Expression,
    yResExpr: Expression,
    algorithmExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tile, xResExpr, yResExpr, algorithmExpr, ExpressionConfigExpr())
    // Pin types so SQL decimal literals coerce to Double cleanly.
    override def inputTypes: Seq[DataType] =
        Seq(tile.dataType, DoubleType, DoubleType, StringType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_ResampleToRes.name
    override def replacement: Expression = invoke(RST_ResampleToRes)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_ResampleToRes extends WithExpressionInfo {

    def eval(
        row: InternalRow, xRes: Double, yRes: Double, algorithm: UTF8String, conf: UTF8String
    ): InternalRow = runDispatch(row, xRes, yRes, algorithm, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, xRes: Double, yRes: Double,
        algorithm: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val algStr = if (algorithm == null) "bilinear" else algorithm.toString
              val (resDs, resMtd) = execute(ds, options, xRes, yRes, algStr)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    def execute(
        ds: Dataset, options: Map[String, String], xRes: Double, yRes: Double, algorithm: String
    ): (Dataset, Map[String, String]) =
        RST_ResampleHelper.warpToRes(ds, options, xRes, yRes, algorithm)

    override def name: String = "gbx_rst_resample_to_res"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_ResampleToRes(c(0), c(1), c(2), Literal("bilinear"))
        case 4 => RST_ResampleToRes(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_resample_to_res takes 3 or 4 arguments " +
            s"(tile, x_res, y_res, [algorithm]); got $n"
        )
    }
}
