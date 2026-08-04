package com.databricks.labs.gbx.rasterx.expressions.pixel

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.locationtech.jts.geom.Geometry

/**
  * Sample raster pixel values at a point geometry — returns one Double per
  * band, in band-index order.
  *
  * The point is reprojected from its source CRS to the raster's CRS (Rule 1: an
  * EWKB/EWKT embedded SRID wins; else the optional `crs` arg — int SRID or CRS
  * string; else assumed already in the raster CRS), then the affine GeoTransform
  * maps the world coordinate to a pixel (col, row) read via
  * `band.ReadRaster(col, row, 1, 1)`. Points outside the extent return `null`.
  *
  * Geometries other than POINT are rejected up front — use `gbx_rst_polygonize`
  * or a clip + reduction for polygon sampling.
  */
case class RST_Sample(
    tileExpr: Expression,
    geomExpr: Expression,
    crsExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tileExpr, geomExpr, crsExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(DoubleType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Sample.name
    override def replacement: Expression = invoke(RST_Sample)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

object RST_Sample extends WithExpressionInfo {

    def eval(row: InternalRow, geom: Any, conf: UTF8String): ArrayData =
        doInvoke(row, geom, null, conf, BinaryType)

    def eval(row: InternalRow, geom: Any, crs: UTF8String, conf: UTF8String): ArrayData =
        doInvoke(row, geom, crs, conf, BinaryType)

    private def doInvoke(
        row: InternalRow, geom: Any, crs: UTF8String, conf: UTF8String, dt: DataType
    ): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, dt)
                val parsed: Geometry = geom match {
                    case g: UTF8String  => JTS.fromWKT(g.toString)
                    case g: Array[Byte] => JTS.fromWKB(g)
                    case other          =>
                        throw new IllegalArgumentException(
                            s"gbx_rst_sample: unsupported geom payload type ${if (other == null) "null" else other.getClass.getName}"
                        )
                }
                require(parsed.getGeometryType == "Point",
                    s"gbx_rst_sample requires a POINT geometry; got ${parsed.getGeometryType}")
                val clip = Option(crs).map(_.toString).filter(_.nonEmpty)
                val (x, y) = reprojectToRaster(
                  ds, parsed.getCoordinate.x, parsed.getCoordinate.y, parsed.getSRID, clip)
                val res = execute(ds, x, y)
                RasterDriver.releaseDataset(ds)
                if (res == null) null else ArrayData.toArrayData(res)
            },
            row,
            dt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    /** Reproject a point (x, y) from its source CRS to the raster's CRS. Rule 1:
      * embedded SRID wins; else the explicit `crs`; else assume aligned. A missing
      * source or raster CRS, or any transform failure, returns the point as-is
      * (never errors). */
    private def reprojectToRaster(
        ds: Dataset, x: Double, y: Double, srid: Int, crs: Option[String]
    ): (Double, Double) = {
        val dsSR = ds.GetSpatialRef
        if (dsSR == null) return (x, y)
        val srcSR =
            if (srid > 0) Some(SpatialRefOps.resolveCrs(srid.toString))
            else crs.map(SpatialRefOps.resolveCrs)
        srcSR match {
            case Some(sr) =>
                try {
                    if (sr.IsSame(dsSR) == 1) (x, y)
                    else {
                        val tf = new org.gdal.osr.CoordinateTransformation(sr, dsSR)
                        val pt = tf.TransformPoint(x, y)
                        tf.delete(); sr.delete()
                        (pt(0), pt(1))
                    }
                } catch { case _: Throwable => (x, y) }
            case None => (x, y)
        }
    }

    /** Pure compute path — extracted for direct unit-testing without Spark.
      *
      * Returns ``null`` if the world coordinate falls outside the raster's
      * pixel extent; otherwise returns an array of one Double per band.
      *
      * Note: the caller is expected to pass `(x, y)` already in the raster's
      * CRS. A full geom-with-SRID reprojection is intentionally NOT applied
      * here — match the convention of `RST_WorldToRasterCoord` which assumes
      * the world coordinate is already CRS-aligned. (Callers wanting CRS
      * reprojection can wrap this in `gbx_rst_clip`-style preprocessing.)
      */
    def execute(ds: Dataset, x: Double, y: Double): Array[Double] = {
        require(ds != null, "RST_Sample.execute: source Dataset is null")
        val w = ds.GetRasterXSize
        val h = ds.GetRasterYSize
        val gt = ds.GetGeoTransform()
        require(gt != null && gt.length == 6, "gbx_rst_sample: raster has no GeoTransform")
        // GeoTransform: [originX, pixelWidthX, rotX, originY, rotY, pixelHeightY]
        // Inverse via standard 2x2 determinant — covers rotated rasters too.
        val det = gt(1) * gt(5) - gt(2) * gt(4)
        if (det == 0.0) return null // degenerate transform
        val dx = x - gt(0)
        val dy = y - gt(3)
        val col = ((gt(5) * dx - gt(2) * dy) / det).toInt
        val row = ((-gt(4) * dx + gt(1) * dy) / det).toInt
        if (col < 0 || col >= w || row < 0 || row >= h) return null
        val nBands = ds.GetRasterCount
        val out = new Array[Double](nBands)
        var b = 1
        while (b <= nBands) {
            val band = ds.GetRasterBand(b)
            val buf = new Array[Double](1)
            band.ReadRaster(col, row, 1, 1, buf)
            out(b - 1) = buf(0)
            b += 1
        }
        out
    }

    override def name: String = "gbx_rst_sample"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        // 2-arg (no crs) stays valid; 3-arg adds the optional source-CRS override.
        case 2 => RST_Sample(c(0), c(1), Literal(null, StringType))
        case 3 => RST_Sample(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_sample takes 2 or 3 arguments (tile, point_geom, [crs]); got $n"
        )
    }

}
