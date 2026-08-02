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
  *   - `scale` (default: unset): ratio of vertical units to horizontal units.
  *     When omitted, GDAL 3.11+ auto-derives the scale from the CRS (degree->metre
  *     for geographic rasters), matching `gdaldem slope` with no `-s`. Supply an
  *     explicit value (e.g. 1.0 for a projected CRS already in metres) to override.
  *
  * Output is a single-band Float32 GTiff with slope per pixel.
  */
case class RST_Slope(
    tileExpr: Expression,
    unitExpr: Expression,
    scaleExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, unitExpr, scaleExpr, ExpressionConfigExpr())
    // Pin types so SQL decimal literals (e.g. ``1.0``) coerce to Double cleanly.
    override def inputTypes: Seq[DataType] = Seq(tileExpr.dataType, StringType, DoubleType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Slope.name
    override def replacement: Expression = invoke(RST_Slope)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_Slope extends WithExpressionInfo {

    def eval(row: InternalRow, unit: UTF8String, scale: Double, conf: UTF8String): InternalRow =
        runDispatch(row, unit, scale, conf, BinaryType)

    private def runDispatch(row: InternalRow, unit: UTF8String, scale: Double, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
              val unitStr = if (unit == null) "degrees" else unit.toString
              val (resDs, resMtd) = execute(ds, unitStr, scale)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, unit: String, scale: Double): (Dataset, Map[String, String]) = {
        val opts = scala.collection.mutable.Buffer.empty[String]
        // Double.NaN sentinel = "no explicit scale" (mirrors GDAL's own
        // std::isnan(xscale) check): omit -s so GDAL 3.11+ auto-derives the
        // xscale/yscale from the CRS (degree->metre for geographic rasters).
        // Emit -s only when the caller explicitly supplies a scale.
        if (!scale.isNaN) opts ++= Seq("-s", scale.toString)
        if (unit != null && unit.equalsIgnoreCase("percent")) opts += "-p"
        RST_DEMProcessingHelper.process(ds, "slope", opts.toSeq)
    }

    override def name: String = "gbx_rst_slope"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Slope(c(0), Literal("degrees"), Literal(Double.NaN))
        case 2 => RST_Slope(c(0), c(1), Literal(Double.NaN))
        case 3 => RST_Slope(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_slope takes 1 to 3 arguments (tile, [unit, [scale]]); got $n"
        )
    }

}
