package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the population standard deviation of raster values within each BNG grid cell. */
case class RST_BNG_RasterToGridStddev(tile: Expression, resolution: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(tile, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", StringType), StructField("measure", DoubleType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_BNG_RasterToGridStddev.name
    override def replacement: Expression = invoke(RST_BNG_RasterToGridStddev)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))
}

/** Companion: SQL name, builder, and entry points for path/binary tile. */
object RST_BNG_RasterToGridStddev extends WithExpressionInfo {

    def eval(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = doInvoke(row, resolution, conf, BinaryType)

    // Long overloads -- PySpark sends Python ints as LongType.
    def eval(row: InternalRow, resolution: Long, conf: UTF8String): ArrayData = eval(row, resolution.toInt, conf)

    // BNG string-key resolution ("1km" etc.) -- PySpark may send a UTF8String.
    def eval(row: InternalRow, resolution: UTF8String, conf: UTF8String): ArrayData = eval(row, BNG.getResolution(resolution), conf)

    private def doInvoke(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() => RST_BNG_RasterToGrid.eval[Double](row, resolution, conf, rdt, this.execute), row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull

    def execute(ds: Dataset, resolution: Int): Array[Array[(String, Double)]] = {
        // population stddev = sqrt(population variance), two-pass (see spec 3.2).
        val stdF = (values: ArrayBuffer[Double]) => {
            val n = values.length
            val m = values.sum / n
            math.sqrt(values.map(v => { val d = v - m; d * d }).sum / n)
        }
        RST_BNG_RasterToGrid.execute(ds, resolution, stdF)
    }

    override def name: String = "gbx_rst_bng_rastertogridstddev"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_BNG_RasterToGridStddev(c(0), c(1))

}
