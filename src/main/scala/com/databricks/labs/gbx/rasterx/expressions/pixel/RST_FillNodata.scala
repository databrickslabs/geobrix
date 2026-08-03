package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

import java.util.{Vector => JVector}

/**
  * Interpolate NoData pixels from their valid neighbours using `gdal.FillNodata`.
  *
  *   - `max_search_dist` (default 100): how far (in pixels) the algorithm
  *     searches for valid neighbour values to fill a NoData cell from.
  *   - `smoothing_iter` (default 0): number of 3x3 smoothing iterations applied
  *     after the fill pass.
  *
  * The operation is applied band-by-band to a GTiff copy of the input; pixel
  * data type, CRS, and extent are preserved. NoData detection uses each band's
  * declared NoData value (via the GDAL Java binding's `FillNodata` overload that
  * passes `null` as the mask, asking it to derive the mask from the band itself).
  */
case class RST_FillNodata(
    tileExpr: Expression,
    maxSearchDistExpr: Expression,
    smoothingIterExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tileExpr, maxSearchDistExpr, smoothingIterExpr, ExpressionConfigExpr()
    )
    // Pin max_search_dist as DoubleType (gdal.FillNodata takes a Double), and
    // smoothing_iter as IntegerType so SQL literals coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(
        tileExpr.dataType, DoubleType, IntegerType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_FillNodata.name
    override def replacement: Expression = invoke(RST_FillNodata)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_FillNodata extends WithExpressionInfo {

    def eval(row: InternalRow, maxSearchDist: Double, smoothingIter: Int, conf: UTF8String): InternalRow =
        runDispatch(row, maxSearchDist, smoothingIter, conf, BinaryType)
    def eval (row: InternalRow, maxSearchDist: Double, smoothingIter: Long, conf: UTF8String): InternalRow =
        runDispatch(row, maxSearchDist, smoothingIter.toInt, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, maxSearchDist: Double, smoothingIter: Int, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, options, maxSearchDist, smoothingIter)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Makes a writable GTiff copy of `ds` (FillNodata mutates in place), runs
      * the fill band-by-band, and returns the modified copy.
      */
    def execute(
        ds: Dataset, options: Map[String, String], maxSearchDist: Double, smoothingIter: Int
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_FillNodata.execute: source Dataset is null")
        require(
            maxSearchDist > 0.0 && !maxSearchDist.isNaN && !maxSearchDist.isInfinity,
            s"gbx_rst_fillnodata: max_search_dist must be > 0 and finite; got $maxSearchDist"
        )
        require(
            smoothingIter >= 0,
            s"gbx_rst_fillnodata: smoothing_iter must be >= 0; got $smoothingIter"
        )

        // Make a writable copy first; FillNodata mutates the band in place.
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val outPath = s"/vsimem/fillnodata_$uuid.$extension"
        val (outDs, mtd) = GDALTranslate.executeTranslate(outPath, ds, "gdal_translate", options)

        val nBands = outDs.GetRasterCount
        val gdalOpts = new JVector[String]()
        // Route GDAL's scratch X/Y index work files to the in-memory MEM driver instead of
        // the default GTiff temp file. The default writes a temp GTiff via
        // CPLGenerateTempFilename (CPL_TMPDIR/TMPDIR), which on some executors (e.g. a
        // Databricks cluster) fails with "Could not create Y index work file. Check driver
        // capabilities." MEM needs no filesystem and matches the /vsimem output. (GDAL >=
        // 3.7; harmlessly ignored on older builds.)
        gdalOpts.add("TEMP_FILE_DRIVER=MEM")
        var b = 1
        while (b <= nBands) {
            val band = outDs.GetRasterBand(b)
            // mask = null asks GDAL to derive the mask from the band's NoData value.
            val rc = gdal.FillNodata(band, null, maxSearchDist, smoothingIter, gdalOpts, null)
            if (rc != 0) {
                val errMsg = gdal.GetLastErrorMsg()
                throw new RuntimeException(
                    s"gbx_rst_fillnodata: gdal.FillNodata failed on band $b (rc=$rc): " +
                      (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
                )
            }
            band.FlushCache()
            b += 1
        }
        outDs.FlushCache()
        (outDs, mtd)
    }

    override def name: String = "gbx_rst_fillnodata"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_FillNodata(c(0), Literal(100.0), Literal(0))
        case 2 => RST_FillNodata(c(0), c(1), Literal(0))
        case 3 => RST_FillNodata(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_fillnodata takes 1 to 3 arguments (tile, [max_search_dist, [smoothing_iter]]); got $n"
        )
    }

}
