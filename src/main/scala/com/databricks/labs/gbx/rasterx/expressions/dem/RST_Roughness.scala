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
  * Compute terrain Roughness from a DEM tile via
  * `gdal.DEMProcessing("Roughness")`. Roughness is the largest inter-cell
  * difference of a central pixel and its 8 neighbours.
  *
  * Output is a single-band Float32 GTiff. No options.
  */
case class RST_Roughness(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def inputTypes: Seq[DataType] = Seq(tile.dataType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Roughness.name
    override def replacement: Expression = invoke(RST_Roughness)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

object RST_Roughness extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): InternalRow = runDispatch(row, conf, BinaryType)

    private def runDispatch(row: InternalRow, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset): (Dataset, Map[String, String]) =
        RST_DEMProcessingHelper.process(ds, "Roughness")

    override def name: String = "gbx_rst_roughness"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Roughness(c(0))
        case n => throw new IllegalArgumentException(s"gbx_rst_roughness takes 1 argument (tile); got $n")
    }

}
