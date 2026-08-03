package com.databricks.labs.gbx.rasterx.expressions.dem

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
  * Compute aspect (compass direction of slope) from a DEM tile via
  * `gdal.DEMProcessing("aspect")`.
  *
  *   - `trigonometric` (default false): if true, output trigonometric angles
  *     measured counterclockwise from east; if false, output compass angles
  *     measured clockwise from north (0=N, 90=E, 180=S, 270=W).
  *   - `zeroForFlat` (default false): if true, flat areas get value 0; if false,
  *     flat areas get -9999.
  *
  * Output is a single-band Float32 GTiff with aspect per pixel.
  */
case class RST_Aspect(
    tileExpr: Expression,
    trigonometricExpr: Expression,
    zeroForFlatExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, trigonometricExpr, zeroForFlatExpr, ExpressionConfigExpr())
    override def inputTypes: Seq[DataType] = Seq(tileExpr.dataType, BooleanType, BooleanType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Aspect.name
    override def replacement: Expression = invoke(RST_Aspect)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_Aspect extends WithExpressionInfo {

    def eval(row: InternalRow, trig: Boolean, zeroForFlat: Boolean, conf: UTF8String): InternalRow =
        runDispatch(row, trig, zeroForFlat, conf, BinaryType)

    private def runDispatch(row: InternalRow, trig: Boolean, zeroForFlat: Boolean, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, trig, zeroForFlat)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, trigonometric: Boolean, zeroForFlat: Boolean): (Dataset, Map[String, String]) = {
        val opts = scala.collection.mutable.Buffer.empty[String]
        if (trigonometric) opts += "-trigonometric"
        if (zeroForFlat) opts += "-zero_for_flat"
        RST_DEMProcessingHelper.process(ds, "aspect", opts.toSeq)
    }

    override def name: String = "gbx_rst_aspect"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Aspect(c(0), Literal(false), Literal(false))
        case 2 => RST_Aspect(c(0), c(1), Literal(false))
        case 3 => RST_Aspect(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_aspect takes 1 to 3 arguments (tile, [trigonometric, [zero_for_flat]]); got $n"
        )
    }

}
