package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/**
  * Stamp a CRS on a raster tile's SpatialReference from a CRS STRING, without
  * reprojecting the pixels. Equivalent to `gdal_edit.py -a_srs <crs> <file>` — used
  * when the source file lost its CRS metadata or arrived with the wrong / missing SR
  * header but you know what the correct CRS should be.
  *
  * The CRS argument follows the shared int-cast rule (see `SpatialRefOps.resolveCrs`):
  * an int-castable string (`"4326"`) → EPSG; any other string → GDAL's universal
  * parser (`EPSG:x` / `ESRI:x` / WKT / PROJ4). This is the string companion to
  * `gbx_rst_setsrid` (which takes an EPSG int); unlike that op it accepts non-EPSG
  * CRS (ESRI/WKT) and does NOT require a positive EPSG code.
  *
  * For actual reprojection (with pixel-grid warp) use `gbx_rst_transformcrs`. This
  * function only rewrites the SR header / WKT; pixel coordinates and GeoTransform are
  * unchanged.
  */
case class RST_SetCrs(
    tileExpr: Expression,
    crsExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, crsExpr, ExpressionConfigExpr())
    // Pin crs as StringType so SQL string literals coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(tileExpr.dataType, StringType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_SetCrs.name
    override def replacement: Expression = invoke(RST_SetCrs)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

object RST_SetCrs extends WithExpressionInfo {

    def eval(row: InternalRow, crs: UTF8String, conf: UTF8String): InternalRow =
        runDispatch(row, crs, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, crs: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, options, crs.toString)
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
      * Materialises a fresh GTiff copy of the input so the caller-owned input
      * Dataset is left untouched; the copy then has `SetProjection` called on it
      * (from the resolved CRS's WKT) before being returned. */
    def execute(ds: Dataset, options: Map[String, String], crs: String): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_SetCrs.execute: source Dataset is null")
        val dstSR = SpatialRefOps.resolveCrs(crs)
        val wkt = dstSR.ExportToWkt()
        dstSR.delete()
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val outPath = s"/vsimem/setcrs_$uuid.$extension"
        val (outDs, mtd) = GDALTranslate.executeTranslate(outPath, ds, "gdal_translate", options)
        outDs.SetProjection(wkt)
        outDs.FlushCache()
        (outDs, mtd)
    }

    override def name: String = "gbx_rst_setcrs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_SetCrs(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_setcrs takes 2 arguments (tile, crs); got $n"
        )
    }

}
