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
  * Resample a raster tile to an explicit output size `width_px x height_px`.
  *
  * Output extent and CRS match the source; only the pixel grid is changed.
  * `algorithm` defaults to `"bilinear"`; see [[RST_ResampleHelper.AllowedAlgorithms]].
  */
case class RST_ResampleToSize(
    tileExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    algorithmExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tileExpr, widthPxExpr, heightPxExpr, algorithmExpr, ExpressionConfigExpr())
    override def inputTypes: Seq[DataType] =
        Seq(tileExpr.dataType, IntegerType, IntegerType, StringType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_ResampleToSize.name
    override def replacement: Expression = invoke(RST_ResampleToSize)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_ResampleToSize extends WithExpressionInfo {

    // PySpark sends Python ints as LongType; offer both Int and Long overloads.
    def eval(
        row: InternalRow, widthPx: Int, heightPx: Int, algorithm: UTF8String, conf: UTF8String
    ): InternalRow = runDispatch(row, widthPx, heightPx, algorithm, conf, BinaryType)
    def eval(
        row: InternalRow, widthPx: Long, heightPx: Long, algorithm: UTF8String, conf: UTF8String
    ): InternalRow = runDispatch(row, widthPx.toInt, heightPx.toInt, algorithm, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, widthPx: Int, heightPx: Int,
        algorithm: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val algStr = if (algorithm == null) "bilinear" else algorithm.toString
              val (resDs, resMtd) = execute(ds, options, widthPx, heightPx, algStr)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    def execute(
        ds: Dataset, options: Map[String, String], widthPx: Int, heightPx: Int, algorithm: String
    ): (Dataset, Map[String, String]) =
        RST_ResampleHelper.warpToSize(ds, options, widthPx, heightPx, algorithm)

    override def name: String = "gbx_rst_resample_to_size"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_ResampleToSize(c(0), c(1), c(2), Literal("bilinear"))
        case 4 => RST_ResampleToSize(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_resample_to_size takes 3 or 4 arguments " +
            s"(tile, width_px, height_px, [algorithm]); got $n"
        )
    }
}
