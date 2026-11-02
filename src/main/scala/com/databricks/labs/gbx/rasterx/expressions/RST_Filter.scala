package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.KernelFilter
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression for applying NxN filter on a raster. */
case class RST_Filter(
    tile: Expression,
    kernelSizeExpr: Expression,
    operationExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, kernelSizeExpr, operationExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Filter.name
    override def replacement: Expression = invoke(RST_Filter)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Filter extends WithExpressionInfo {

    def eval(row: InternalRow, n: Int, operation: UTF8String, conf: UTF8String): InternalRow =
        eval(row, n, operation, conf, BinaryType)

    /** The operations, as the SQL function names them (any case). */
    val operations: Seq[String] = Seq("min", "max", "mean", "median")

    /** The window must be a positive odd integer and the operation one of [[operations]]; the failure names the
      * value. Returns the operation as KernelFilter spells it. */
    def requireArguments(n: Int, operation: String): String = {
        require(n >= 1 && n % 2 == 1, s"gbx_rst_filter: kernel_size must be a positive odd integer; got $n")
        val op = operation.toLowerCase
        require(operations.contains(op), s"gbx_rst_filter: unknown operation '$operation'; known: ${operations.mkString(", ")}")
        op
    }

    def eval(row: InternalRow, n: Int, operation: UTF8String, conf: UTF8String, rdt: DataType): InternalRow = {
        // A wrong argument fails the call (naming the value); only the tile's own failures become its error tile.
        val op = if (row != null && operation != null) requireArguments(n, operation.toString) else operation.toString
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, rdt)
              val result = execute(ds, n, op)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, result._1, result._2), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(result._1)
              res
          },
          row,
          rdt
        )
    }

    def execute(ds: Dataset, n: Int, operation: String): (Dataset, Map[String, String]) = {
        val res = KernelFilter.filter(ds, n, operation)
        res
    }

    override def name: String = "gbx_rst_filter"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Filter(c(0), c(1), c(2))
}
