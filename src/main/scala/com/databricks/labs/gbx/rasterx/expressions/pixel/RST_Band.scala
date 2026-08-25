package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, TranslateOptions, gdal}

import java.util.{Vector => JVector}

/**
  * Extract a single band from a multi-band raster as a new single-band tile.
  *
  * Equivalent to `gdal_translate -b <bandIndex> <src> <dst>`. `bandIndex` is
  * 1-based to match GDAL convention. The extracted tile preserves the source
  * CRS, GeoTransform, and pixel values; only the band count is reduced to 1.
  */
case class RST_Band(
    tile: Expression,
    bandIndexExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, bandIndexExpr, ExpressionConfigExpr())
    // Pin band_index as IntegerType so SQL integer literals coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(tile.dataType, IntegerType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Band.name
    override def replacement: Expression = invoke(RST_Band)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

object RST_Band extends WithExpressionInfo {

    def eval(row: InternalRow, bandIndex: Int, conf: UTF8String): InternalRow =
        runDispatch(row, bandIndex, conf, BinaryType)
    def eval (row: InternalRow, bandIndex: Long, conf: UTF8String): InternalRow =
        runDispatch(row, bandIndex.toInt, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, bandIndex: Int, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, options, bandIndex)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path — extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, options: Map[String, String], bandIndex: Int): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_Band.execute: source Dataset is null")
        val nBands = ds.GetRasterCount
        require(
            bandIndex >= 1 && bandIndex <= nBands,
            s"gbx_rst_band: band_index $bandIndex out of range [1..$nBands]"
        )
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val outPath = s"/vsimem/band_$uuid.tif"
        val opts = new JVector[String]()
        opts.add("-of")
        opts.add("GTiff")
        opts.add("-b")
        opts.add(bandIndex.toString)
        val tOpts = new TranslateOptions(opts)
        val result =
            try {
                gdal.Translate(outPath, ds, tOpts)
            } finally {
                tOpts.delete()
            }
        val errMsg = gdal.GetLastErrorMsg()
        if (result == null) {
            throw new RuntimeException(
                s"gbx_rst_band: gdal.Translate(-b $bandIndex) failed: " +
                  (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }
        result.FlushCache()

        val metadata = Map(
            "path" -> outPath,
            "driver" -> "GTiff",
            "extension" -> "tif",
            "last_command" -> s"gdal.Translate(-b $bandIndex)",
            "last_error" -> (if (errMsg == null) "" else errMsg),
            "all_parents" -> Option(ds.GetDescription()).getOrElse(""),
            "size" -> "-1",
            "format" -> "GTiff",
            "compression" -> "DEFLATE",
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (result, metadata)
    }

    override def name: String = "gbx_rst_band"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_Band(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_band takes 2 arguments (tile, band_index); got $n"
        )
    }

}
