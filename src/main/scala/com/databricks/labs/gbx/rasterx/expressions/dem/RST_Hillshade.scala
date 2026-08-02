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
  * Compute hillshade (shaded relief) from a DEM tile via
  * `gdal.DEMProcessing("hillshade")`.
  *
  *   - `azimuth` (default 315.0): light-source azimuth in degrees (0=N, 90=E).
  *   - `altitude` (default 45.0): light-source altitude above horizon in
  *     degrees.
  *   - `zFactor` (default 1.0): vertical exaggeration.
  *
  * Output is a single-band Byte (uint8) GTiff with values 0..255.
  */
case class RST_Hillshade(
    tileExpr: Expression,
    azimuthExpr: Expression,
    altitudeExpr: Expression,
    zFactorExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tileExpr, azimuthExpr, altitudeExpr, zFactorExpr, ExpressionConfigExpr())
    override def inputTypes: Seq[DataType] =
        Seq(tileExpr.dataType, DoubleType, DoubleType, DoubleType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Hillshade.name
    override def replacement: Expression = invoke(RST_Hillshade)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

object RST_Hillshade extends WithExpressionInfo {

    def eval(row: InternalRow, azimuth: Double, altitude: Double, zFactor: Double, conf: UTF8String): InternalRow =
        runDispatch(row, azimuth, altitude, zFactor, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, azimuth: Double, altitude: Double, zFactor: Double,
        conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, azimuth, altitude, zFactor)
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, azimuth: Double, altitude: Double, zFactor: Double): (Dataset, Map[String, String]) = {
        val opts = Seq("-az", azimuth.toString, "-alt", altitude.toString, "-z", zFactor.toString)
        RST_DEMProcessingHelper.process(ds, "hillshade", opts)
    }

    override def name: String = "gbx_rst_hillshade"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Hillshade(c(0), Literal(315.0), Literal(45.0), Literal(1.0))
        case 2 => RST_Hillshade(c(0), c(1), Literal(45.0), Literal(1.0))
        case 3 => RST_Hillshade(c(0), c(1), c(2), Literal(1.0))
        case 4 => RST_Hillshade(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_hillshade takes 1 to 4 arguments (tile, [azimuth, [altitude, [z_factor]]]); got $n"
        )
    }

}
