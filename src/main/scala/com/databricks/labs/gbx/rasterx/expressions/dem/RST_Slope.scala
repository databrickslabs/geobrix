package com.databricks.labs.gbx.rasterx.expressions.dem

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Compute slope from a single-band DEM tile via `gdal.DEMProcessing("slope")`.
  *
  *   - `unit` (default "degrees"): "degrees" or "percent".
  *   - `xscale` / `yscale` (default: unset): ratio of vertical units to horizontal
  *     units per axis. Both must be supplied together (both-or-neither). When
  *     omitted, GDAL 3.11+ auto-derives the scale from the CRS (degree->metre for
  *     geographic rasters), matching `gdaldem slope` with no `-xscale`/`-yscale`.
  *     Pass both explicitly (e.g. 1.0 / 1.0 for a projected CRS in metres) to
  *     override per axis.
  *
  * Output is a single-band Float32 GTiff with slope per pixel.
  */
case class RST_Slope(
    tile: Expression,
    unitExpr: Expression,
    xscaleExpr: Expression,
    yscaleExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, unitExpr, xscaleExpr, yscaleExpr, ExpressionConfigExpr())
    // Pin types so SQL decimal literals (e.g. ``1.0``) coerce to Double cleanly.
    override def inputTypes: Seq[DataType] = Seq(tile.dataType, StringType, DoubleType, DoubleType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Slope.name
    override def replacement: Expression = invoke(RST_Slope)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_Slope extends WithExpressionInfo {

    def eval(row: InternalRow, unit: UTF8String, xscale: Double, yscale: Double, conf: UTF8String): InternalRow =
        runDispatch(row, unit, xscale, yscale, conf, BinaryType)

    private def runDispatch(row: InternalRow, unit: UTF8String, xscale: Double, yscale: Double, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
              val unitStr = if (unit == null) "degrees" else unit.toString
              val (resDs, resMtd) = execute(ds, unitStr, xscale, yscale)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark.
      *
      * xscale and yscale are both-or-neither: emit `-xscale`/`-yscale` only
      * when BOTH are non-NaN. When either is NaN (the default), omit both so
      * GDAL 3.11+ auto-derives horizontal scale from the CRS.
      */
    def execute(ds: Dataset, unit: String, xscale: Double, yscale: Double): (Dataset, Map[String, String]) = {
        val opts = scala.collection.mutable.Buffer.empty[String]
        // Emit -xscale / -yscale only when BOTH are non-NaN (anisotropic explicit
        // scale). When either is NaN, omit both so GDAL 3.11+ auto-derives from CRS.
        if (!xscale.isNaN && !yscale.isNaN) opts ++= Seq("-xscale", xscale.toString, "-yscale", yscale.toString)
        if (unit != null && unit.equalsIgnoreCase("percent")) opts += "-p"
        RST_DEMProcessingHelper.process(ds, "slope", opts.toSeq)
    }

    override def name: String = "gbx_rst_slope"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Slope(c(0), Literal("degrees"), Literal(Double.NaN), Literal(Double.NaN))
        case 2 => RST_Slope(c(0), c(1), Literal(Double.NaN), Literal(Double.NaN))
        case 3 => throw new IllegalArgumentException(
            "gbx_rst_slope: xscale and yscale must be supplied together; " +
            "got 3 arguments (tile, unit, xscale) but yscale is missing. " +
            "Use 4 arguments: gbx_rst_slope(tile, unit, xscale, yscale)"
        )
        case 4 => RST_Slope(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_slope takes 1, 2, or 4 arguments (tile, [unit, [xscale, yscale]]); got $n"
        )
    }

}
