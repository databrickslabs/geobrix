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

/** Returns the population variance of the raster values within each quadbin grid cell. */
case class RST_Quadbin_RasterToGridVariance(
    tileExpr: Expression,
    resolution: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", LongType), StructField("measure", DoubleType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_Quadbin_RasterToGridVariance.name
    override def replacement: Expression = rstInvoke(RST_Quadbin_RasterToGridVariance, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and entry points for path/binary tile. */
object RST_Quadbin_RasterToGridVariance extends WithExpressionInfo {

    def evalPath(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = doInvoke(row, resolution, conf, StringType)
    def evalBinary(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = doInvoke(row, resolution, conf, BinaryType)

    // Long overloads -- PySpark sends Python ints as LongType.
    def evalPath(row: InternalRow, resolution: Long, conf: UTF8String): ArrayData = evalPath(row, resolution.toInt, conf)
    def evalBinary(row: InternalRow, resolution: Long, conf: UTF8String): ArrayData = evalBinary(row, resolution.toInt, conf)

    private def doInvoke(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() => RST_Quadbin_RasterToGrid.eval[Double](row, resolution, conf, rdt, this.execute), row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull

    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] = {
        // population variance, two-pass (matches numpy ddof=0; see spec 3.2).
        val varF = (values: ArrayBuffer[Double]) => {
            val n = values.length
            val m = values.sum / n
            values.map(v => { val d = v - m; d * d }).sum / n
        }
        RST_Quadbin_RasterToGrid.execute(ds, resolution, varF)
    }

    override def name: String = "gbx_rst_quadbin_rastertogridvariance"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Quadbin_RasterToGridVariance(c(0), c(1))

}
