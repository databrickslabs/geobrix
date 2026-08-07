package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the maximum raster value in each quadbin grid cell. */
case class RST_Quadbin_RasterToGridMax(
    tile: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", LongType), StructField("measure", DoubleType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_Quadbin_RasterToGridMax.name
    override def replacement: Expression = invoke(RST_Quadbin_RasterToGridMax)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and entry points for path/binary tile. */
object RST_Quadbin_RasterToGridMax extends WithExpressionInfo {

    def eval(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = doInvoke(row, resolution, conf, BinaryType)

    def eval(row: InternalRow, resolution: Long, conf: UTF8String): ArrayData = eval(row, resolution.toInt, conf)

    private def doInvoke(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() => RST_Quadbin_RasterToGrid.eval[Double](row, resolution, conf, rdt, this.execute), row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull

    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] = {
        val maxF = (values: ArrayBuffer[Double]) => values.max
        RST_Quadbin_RasterToGrid.execute(ds, resolution, maxF)
    }

    override def name: String = "gbx_rst_quadbin_rastertogridmax"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Quadbin_RasterToGridMax(c(0), c(1))

}
