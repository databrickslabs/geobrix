package com.databricks.labs.gbx.rasterx.expressions.dem

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Apply a color relief mapping to a DEM tile via
  * `gdal.DEMProcessing("color-relief")`.
  *
  *   - `colorTablePath`: path (FUSE-mounted Volume or local) to a color table
  *     file (gdaldem color file format: each line is `elevation R G B [A]`,
  *     or special values `nv`, `default`, `0%`, `100%`).
  *
  * Output is a 3- or 4-band Byte (uint8) GTiff (RGB or RGBA).
  */
case class RST_ColorRelief(
    tileExpr: Expression,
    colorTablePathExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, colorTablePathExpr, ExpressionConfigExpr())
    override def inputTypes: Seq[DataType] = Seq(tileExpr.dataType, StringType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_ColorRelief.name
    override def replacement: Expression = invoke(RST_ColorRelief)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

object RST_ColorRelief extends WithExpressionInfo {

    def eval(row: InternalRow, colorTablePath: UTF8String, conf: UTF8String): InternalRow =
        runDispatch(row, colorTablePath, conf, BinaryType)

    private def runDispatch(row: InternalRow, colorTablePath: UTF8String, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
              val ctp = if (colorTablePath == null) null else colorTablePath.toString
              val (resDs, resMtd) = execute(ds, ctp)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, colorTablePath: String): (Dataset, Map[String, String]) = {
        require(colorTablePath != null && colorTablePath.nonEmpty,
            "gbx_rst_color_relief: color_table_path is required")
        RST_DEMProcessingHelper.process(ds, "color-relief", Seq.empty, colorFilename = colorTablePath)
    }

    override def name: String = "gbx_rst_color_relief"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_ColorRelief(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_color_relief takes 2 arguments (tile, color_table_path); got $n"
        )
    }

}
