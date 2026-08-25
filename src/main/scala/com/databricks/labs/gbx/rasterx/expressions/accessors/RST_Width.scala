package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Catalyst expression that evaluates to the raster width in pixels (GDAL GetRasterXSize). Case class holding tile; used as the catalyst node when gbx_rst_width(tile) is invoked in SQL or DataFrame API. */
case class RST_Width(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = true
    override def prettyName: String = RST_Width.name
    override def replacement: Expression = invoke(RST_Width)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Width extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): java.lang.Integer = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, dt: DataType): java.lang.Integer =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                res
            },
            row,
            dt,
            conf
          )
        ).map(v => java.lang.Integer.valueOf(v.asInstanceOf[Int])).orNull

    def execute(ds: Dataset): Int = ds.GetRasterXSize()

    override def name: String = "gbx_rst_width"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Width(c(0))

}
