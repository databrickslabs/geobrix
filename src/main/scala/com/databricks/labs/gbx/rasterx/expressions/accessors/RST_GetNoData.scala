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

/** The expression for extracting the no data value of a raster. */
case class RST_GetNoData(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_GetNoData.name
    override def replacement: Expression = invoke(RST_GetNoData)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_GetNoData extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt, gated = false)
                val res = executeOrNull(ds)
                RasterDriver.releaseDataset(ds)
                if (res == null) null else ArrayData.toArrayData(res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    /** The NoData value of each band, NaN for a band that declares none; NULL when no band declares one (the
      * lightweight function's answer). A NoData value that is itself NaN counts as declared. */
    def executeOrNull(ds: Dataset): Array[Double] = {
        val declared = (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            val v    = Array.fill[java.lang.Double](1)(null)
            if (band != null) band.GetNoDataValue(v)
            Option(v(0)).map(_.doubleValue())
        }
        if (declared.forall(_.isEmpty)) null else declared.map(_.getOrElse(Double.NaN)).toArray
    }

    def execute(ds: Dataset): Array[Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) Double.NaN
            else BandAccessors.getNoDataValue(band)
        }.toArray
    }

    override def name: String = "gbx_rst_getnodata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_GetNoData(c(0))
}
