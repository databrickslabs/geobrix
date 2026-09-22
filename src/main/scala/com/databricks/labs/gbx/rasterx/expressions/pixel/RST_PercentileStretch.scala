package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.operations.PercentileStretch
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Per-band percentile contrast stretch to 8-bit uint8.
  *
  * For each band:
  *   1. Compute lo_pct and hi_pct percentiles from valid (non-NoData) pixels
  *   2. Clip pixel values to [lo_pct_val, hi_pct_val]
  *   3. Linearly rescale to [0, 255]
  *   4. Output as uint8
  *   5. NoData pixels → 0 in output
  *
  * Returns a single-band uint8 GTiff with the same extent and CRS as the input.
  *   - `lo_pct` (required, 0-100): lower percentile threshold
  *   - `hi_pct` (required, 0-100): upper percentile threshold
  */
case class RST_PercentileStretch(
    tile: Expression,
    loPctExpr: Expression,
    hiPctExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tile, loPctExpr, hiPctExpr, ExpressionConfigExpr()
    )
    // Pin lo_pct and hi_pct as DoubleType so SQL decimal literals (e.g. ``25.0``) coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(
        tile.dataType, DoubleType, DoubleType, StringType
    )
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = RST_PercentileStretch.name
    override def replacement: Expression = invoke(RST_PercentileStretch)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_PercentileStretch extends WithExpressionInfo {

    def eval(row: InternalRow, loPct: Double, hiPct: Double, conf: UTF8String): InternalRow =
        runDispatch(row, loPct, hiPct, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, loPct: Double, hiPct: Double, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, options, loPct, hiPct)
              com.databricks.labs.gbx.rasterx.gdal.RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              com.databricks.labs.gbx.rasterx.gdal.RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Computes percentiles from the input dataset, creates a new uint8 GTiff,
      * and writes rescaled pixel values to each band.
      */
    def execute(
        ds: Dataset, options: Map[String, String], loPct: Double, hiPct: Double
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_PercentileStretch.execute: source Dataset is null")
        require(
            loPct >= 0.0 && loPct <= 100.0,
            s"gbx_rst_percentile_stretch: lo_pct must be in [0, 100]; got $loPct"
        )
        require(
            hiPct >= 0.0 && hiPct <= 100.0,
            s"gbx_rst_percentile_stretch: hi_pct must be in [0, 100]; got $hiPct"
        )
        require(
            loPct < hiPct,
            s"gbx_rst_percentile_stretch: lo_pct ($loPct) must be < hi_pct ($hiPct)"
        )

        PercentileStretch.execute(ds, options, loPct, hiPct)
    }

    override def name: String = "gbx_rst_percentile_stretch"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => RST_PercentileStretch(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_percentile_stretch takes 3 arguments (tile, lo_pct, hi_pct); got $n"
        )
    }

}
