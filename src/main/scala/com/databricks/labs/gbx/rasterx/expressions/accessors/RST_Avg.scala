package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions._
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Expression that evaluates to the average pixel value per band (array of doubles);
 *  an all-nodata band (zero valid pixels) yields a NULL element. */
case class RST_Avg(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Avg.name
    override def replacement: PrettyInvoke = invoke(RST_Avg)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Avg extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, dt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                ArrayData.toArrayData(res)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset): Array[java.lang.Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) null
            else {
                val md = band.AsMDArray()
                val stats = md.GetStatistics()
                val res: java.lang.Double =
                    if (stats == null || stats.getValid_count == 0) null
                    else stats.getMean
                if (stats != null) stats.delete()
                md.delete()
                band.delete()
                res
            }
        }.toArray
    }

    override def name: String = "gbx_rst_avg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Avg(c(0))

}
