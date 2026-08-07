package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** The expression that initializes no data values of a raster. */
case class RST_InitNoData(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_InitNoData.name
    override def replacement: Expression = invoke(RST_InitNoData)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_InitNoData extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): InternalRow = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mdt) = RasterSerializationUtil.rowToTile(row, rdt)
              val (resultDs, newMdt) = execute(ds, mdt)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, newMdt), rdt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          rdt
        )

    def execute(ds: Dataset, options: Map[String, String]): (Dataset, Map[String, String]) = {
        val perBand = (1 to ds.GetRasterCount())
            .map { bandIndex =>
                val band = ds.GetRasterBand(bandIndex)
                GDAL.getNoDataConstant(band.getDataType)
            }
        // gdalwarp -dstnodata takes ONE argv (a single value applied to all bands,
        // or a space-separated per-band list). The command string is later split on
        // spaces by OperatorOptions.parseOptions, so a multi-value string must NOT
        // be quote-wrapped (the quotes survive the split and gdalwarp rejects the
        // stray-quote tokens). When all bands share one constant (uniform dtype, the
        // common case) emit a single unquoted value; that is one token and applies
        // to every band. (A genuinely mixed-dtype multi-band raster would still need
        // a per-band list, which the space-splitting command path cannot carry; that
        // is out of scope here and uniform-dtype is the norm for these rasters.)
        val noDataValues = perBand.distinct match {
            case Seq(single) => single.toString
            case _           => perBand.mkString(" ")
        }
        val cmd = s"""gdalwarp -dstnodata $noDataValues"""
        val uuid = java.util.UUID.randomUUID().toString
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resFile = s"/vsimem/initnodata_$uuid.$extension"
        GDALWarp.executeWarp(
          resFile,
          Array(ds),
          options,
          command = cmd
        )
    }

    override def name: String = "gbx_rst_initnodata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_InitNoData(c(0))

}
