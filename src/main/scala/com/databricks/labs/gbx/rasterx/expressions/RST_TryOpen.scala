package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/** Expression that returns the tile unchanged if it opens successfully, or an error tile row on failure. */
case class RST_TryOpen(
    tileExpr: Expression
) extends InvokedExpression {

    /** Raster DataType from the tile expression. */
    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def nullable: Boolean = true
    override def prettyName: String = RST_TryOpen.name
    override def replacement: Expression = rstInvoke(RST_TryOpen, rasterType)
    override def dataType: DataType = BooleanType
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_TryOpen extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): Boolean = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): Boolean = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): Boolean = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        Try {
            val ds = RasterSerializationUtil.rowToDS(row, rdt)
            RasterDriver.releaseDataset(ds)
            true
        }.toOption.isDefined
    }

    // execute not needed as in order to have an instance of ds the opening already succeeded
    // TODO: perhaps split into 2 executes String/Binary and avoid unified eval

    override def name: String = "gbx_rst_tryopen"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_TryOpen(c(0))
}
