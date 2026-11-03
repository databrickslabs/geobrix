package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.CombineStats
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Per-pixel/per-band SUM of the valid (non-NoData) values across an ARRAY of aligned tiles.
  * Inputs must share the same grid (see [[com.databricks.labs.gbx.rasterx.operations.RasterAlignment]]);
  * an all-NoData pixel is NoData in the output. Mirrors [[RST_CombineAvg]]. */
case class RST_CombineSum(
    tiles: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.arrayOfTileRasterType(RST_CombineSum.name, tiles)
    private lazy val elementFieldCountLit: Expression =
        Literal(RST_ExpressionUtil.arrayOfTileElementFieldCount(tiles), IntegerType)
    override def children: Seq[Expression] = Seq(tiles, ExpressionConfigExpr(), elementFieldCountLit)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_CombineSum.name
    override def replacement: Expression = invoke(RST_CombineSum)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for the array-of-tiles input. */
object RST_CombineSum extends WithExpressionInfo {

    def eval(row: ArrayData, conf: UTF8String): InternalRow = eval(row, conf, 3)

    def eval(array: ArrayData, conf: UTF8String, elementFieldCount: Int): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              RasterSerializationUtil.withMembers(array, elementFieldCount, "RST_CombineSum", exprConf.hConf)(execute)
          },
          array,
          BinaryType,
          elementFieldCount
        )

    def execute(tiles: Seq[(Long, Dataset, Map[String, String])]): (Long, Dataset, Map[String, String]) = {
        val cellID = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
        val (combinedRaster, mtd) = CombineStats.compute(tiles.map(_._2).toArray, tiles.head._3, "sum")
        (cellID, combinedRaster, mtd)
    }

    override def name: String = "gbx_rst_combinesum"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_CombineSum(c(0))

}
