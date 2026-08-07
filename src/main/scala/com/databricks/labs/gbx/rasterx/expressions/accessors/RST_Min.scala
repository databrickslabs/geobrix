package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the min value per band of the raster. Returns null for bands with zero valid pixels. */
case class RST_Min(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Min.name
    override def replacement: Expression = invoke(RST_Min)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Min extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds)
                RasterDriver.releaseDataset(ds)
                ArrayData.toArrayData(res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset): Array[java.lang.Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            val res: java.lang.Double =
                if (band == null) null
                else if (BandAccessors.isEmpty(band)) null
                else java.lang.Double.valueOf(BandAccessors.getMinMax(band)._1)
            if (band != null) band.delete()
            res
        }.toArray
    }

    override def name: String = "gbx_rst_min"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Min(c(0))

}
