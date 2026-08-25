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
  * Resample a raster tile by a multiplicative `factor`.
  *
  *   - `factor > 1` upsamples (more pixels)
  *   - `0 < factor < 1` downsamples (fewer pixels)
  *
  * `algorithm` is any gdalwarp `-r` value (default `"bilinear"`):
  * `near`, `bilinear`, `cubic`, `cubicspline`, `lanczos`, `average`, `mode`,
  * `max`, `min`, `med`, `q1`, `q3`.
  *
  * Output dimensions are `round(srcW * factor) x round(srcH * factor)`. The
  * source CRS and extent are preserved; only pixel density changes.
  */
case class RST_Resample(
    tile: Expression,
    factorExpr: Expression,
    algorithmExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, factorExpr, algorithmExpr, ExpressionConfigExpr())
    // Pin types so SQL decimal literals (e.g. ``2.0``) coerce to Double cleanly.
    override def inputTypes: Seq[DataType] = Seq(tile.dataType, DoubleType, StringType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Resample.name
    override def replacement: Expression = invoke(RST_Resample)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_Resample extends WithExpressionInfo {

    def eval(row: InternalRow, factor: Double, algorithm: UTF8String, conf: UTF8String): InternalRow =
        runDispatch(row, factor, algorithm, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, factor: Double, algorithm: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val algStr = if (algorithm == null) "bilinear" else algorithm.toString
              val (resDs, resMtd) = execute(ds, options, factor, algStr)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(
        ds: Dataset, options: Map[String, String], factor: Double, algorithm: String
    ): (Dataset, Map[String, String]) =
        RST_ResampleHelper.warpByFactor(ds, options, factor, algorithm)

    override def name: String = "gbx_rst_resample"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_Resample(c(0), c(1), Literal("bilinear"))
        case 3 => RST_Resample(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_resample takes 2 or 3 arguments (tile, factor, [algorithm]); got $n"
        )
    }
}
