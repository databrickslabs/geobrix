package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.RasterProject
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/** Returns the upper left x of the raster. */
case class RST_Transform(
    tile: Expression,
    srid: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, srid, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Transform.name
    override def replacement: Expression = invoke(RST_Transform)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Transform extends WithExpressionInfo {

    def eval(row: InternalRow, srid: Int, conf: UTF8String): InternalRow = eval(row, srid, conf, BinaryType)

    def eval(row: InternalRow, srid: Int, conf: UTF8String, dt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resultDs, metadata) = execute(ds, options, srid)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((cell, resultDs, metadata), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          dt
        )

    def execute(ds: Dataset, options: Map[String, String], srid: Int): (Dataset, Map[String, String]) = {
        // A target SRID must be positive: reprojecting to "no CRS" (0) is meaningless.
        // ImportFromEPSG(0) or a code in neither the EPSG nor ESRI authority returns a
        // non-zero OGRERR but does NOT throw, leaving dstSR empty — warp would then
        // silently no-op and produce an invalid raster. Validate up-front so the caller
        // gets a clear error. (ImportFromEPSG auto-recovers ESRI codes like 54008, so a
        // positive EPSG *or* ESRI code is accepted.)
        require(srid > 0, s"rst_transform requires a positive EPSG or ESRI code; got $srid")
        val dstSR = new SpatialReference()
        val rc = dstSR.ImportFromEPSG(srid)
        if (rc != 0) {
            dstSR.delete()
            throw new IllegalArgumentException(
              s"rst_transform: $srid is not a valid EPSG or ESRI code (OGRERR=$rc)")
        }
        RasterProject.project(ds, options, dstSR)
    }

    override def name: String = "gbx_rst_transform"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Transform(c(0), c(1))

}
