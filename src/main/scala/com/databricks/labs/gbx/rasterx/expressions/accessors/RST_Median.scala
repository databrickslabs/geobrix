package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

/** Returns the median value per band of the raster, or null for a band with zero valid pixels. */
case class RST_Median(
    tileExpr: Expression
) extends InvokedExpression {

    /** Raster DataType from the tile expression. */
    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Median.name
    override def replacement: Expression = rstInvoke(RST_Median, rasterType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Median extends WithExpressionInfo {

    def evalPath(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, StringType)
    def evalBinary(row: InternalRow, conf: UTF8String): ArrayData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                val res = execute(ds, Map.empty)
                RasterDriver.releaseDataset(ds)
                ArrayData.toArrayData(res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset, options: Map[String, String]): Array[java.lang.Double] = {
        val outShortName = ds.GetDriver().getShortName
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(outShortName)
        val resultPath = s"/vsimem/rst_median_$uuid.$extension"
        val cmd = s"gdalwarp -r med -ts 1 1"
        val (resDs, _) = GDALWarp.executeWarp(resultPath, Array(ds), options, cmd)
        val medians: Array[java.lang.Double] = (1 to ds.GetRasterCount()).map { i =>
            val srcBand = ds.GetRasterBand(i)
            val srcMd = srcBand.AsMDArray()
            val srcStats = srcMd.GetStatistics()
            val validCount = if (srcStats == null) 0L else srcStats.getValid_count
            if (srcStats != null) srcStats.delete()
            srcMd.delete()
            srcBand.delete()
            val res: java.lang.Double = if (validCount == 0L) {
                // Band has no valid pixels — return null per reducer convention.
                null
            } else if (resDs != null) {
                // Warp succeeded: the single output pixel is the median.
                val md = resDs.GetRasterBand(i).AsMDArray()
                val stats = md.GetStatistics()
                val v: java.lang.Double =
                    if (stats != null && stats.getValid_count > 0) stats.getMax
                    else {
                        // Pre-computed stats absent on fresh /vsimem band; read pixel directly.
                        val pixelBuf = Array.ofDim[Double](1)
                        resDs.GetRasterBand(i).ReadRaster(0, 0, 1, 1, pixelBuf)
                        pixelBuf(0): java.lang.Double
                    }
                if (stats != null) stats.delete()
                md.delete()
                v
            } else {
                // Warp failed despite valid pixels (e.g. missing georef); compute median directly.
                val w = ds.GetRasterXSize()
                val h = ds.GetRasterYSize()
                val buf = Array.ofDim[Double](w * h)
                ds.GetRasterBand(i).ReadRaster(0, 0, w, h, buf)
                val nodataBuf = Array.ofDim[java.lang.Double](1)
                ds.GetRasterBand(i).GetNoDataValue(nodataBuf)
                val valid = if (nodataBuf(0) != null) buf.filter(_ != nodataBuf(0).doubleValue) else buf
                if (valid.isEmpty) null else valid.sorted.apply(valid.length / 2): java.lang.Double
            }
            res
        }.toArray
        if (resDs != null) resDs.delete()
        gdal.Unlink(resultPath)
        medians
    }

    override def name: String = "gbx_rst_median"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Median(c(0))

}
