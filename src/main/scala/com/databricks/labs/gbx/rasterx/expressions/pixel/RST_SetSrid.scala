package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/**
  * Stamp an EPSG code on a raster tile's SpatialReference, without reprojecting
  * the pixels. Equivalent to `gdal_edit.py -a_srs EPSG:<srid> <file>` — used when
  * the source file lost its CRS metadata or arrived with the wrong / missing
  * SR header but you know what the correct CRS should be.
  *
  * For actual reprojection (with pixel-grid warp) use `gbx_rst_transform`. This
  * function only rewrites the SR header / WKT; pixel coordinates and GeoTransform
  * are unchanged.
  */
case class RST_SetSrid(
    tileExpr: Expression,
    sridExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, sridExpr, ExpressionConfigExpr())
    // Pin srid as IntegerType so SQL integer literals coerce cleanly.
    override def inputTypes: Seq[DataType] = Seq(tileExpr.dataType, IntegerType, StringType)
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_SetSrid.name
    override def replacement: Expression = invoke(RST_SetSrid)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

object RST_SetSrid extends WithExpressionInfo {

    def eval(row: InternalRow, srid: Int, conf: UTF8String): InternalRow =
        runDispatch(row, srid, conf, BinaryType)
    // PySpark commonly passes integer literals as Long; accept that without an
    // input-type coercion failure.
    def eval (row: InternalRow, srid: Long, conf: UTF8String): InternalRow =
        runDispatch(row, srid.toInt, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, srid: Int, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val (resDs, resMtd) = execute(ds, options, srid)
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
      * Dataset is left untouched; the copy then has `SetProjection` called on
      * it before being returned.
      */
    def execute(ds: Dataset, options: Map[String, String], srid: Int): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_SetSrid.execute: source Dataset is null")
        require(srid > 0, s"gbx_rst_setsrid requires a positive EPSG code; got $srid")
        val dstSR = new SpatialReference()
        val rc = dstSR.ImportFromEPSG(srid)
        if (rc != 0) {
            dstSR.delete()
            throw new IllegalArgumentException(s"gbx_rst_setsrid: unknown EPSG code $srid (OGRERR=$rc)")
        }
        val wkt = dstSR.ExportToWkt()
        dstSR.delete()
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val outPath = s"/vsimem/setsrid_$uuid.$extension"
        val (outDs, mtd) = GDALTranslate.executeTranslate(outPath, ds, "gdal_translate", options)
        outDs.SetProjection(wkt)
        outDs.FlushCache()
        (outDs, mtd)
    }

    override def name: String = "gbx_rst_setsrid"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_SetSrid(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_setsrid takes 2 arguments (tile, srid); got $n"
        )
    }

}
