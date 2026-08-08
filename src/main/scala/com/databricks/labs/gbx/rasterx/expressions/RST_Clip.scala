package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{ClipToGeom, SpatialRefOps}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.Geometry

/** The expression for clipping a raster by a vector.
  *
  * Optional 4th arg `clipCrsExpr` (String) declares the source CRS of a plain
  * WKB/WKT cutline (an EWKB/EWKT embedded SRID still wins); absent -> assume the
  * cutline is already in the raster CRS. */
case class RST_Clip(
    tile: Expression,
    geomExpr: Expression,
    cutlineAllTouchedExpr: Expression,
    clipCrsExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tile, geomExpr, cutlineAllTouchedExpr, clipCrsExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Clip.name
    // propagateNull=false: builder() injects Literal(null, StringType) as the optional clipCrs
    // default, so a null clipCrs must NOT short-circuit the whole result to null (eval must run).
    // The shared eval below guards a null primary tile row so the prior null-tile→null holds.
    override def replacement: Expression = invoke(RST_Clip, propagateNull = false)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Clip extends WithExpressionInfo {

    def eval(row: InternalRow, geom: Any, cutlineAllTouched: Boolean, conf: UTF8String): InternalRow =
        eval(row, geom, cutlineAllTouched, null, conf, BinaryType)

    def eval(
        row: InternalRow, geom: Any, cutlineAllTouched: Boolean, clipCrs: UTF8String, conf: UTF8String
    ): InternalRow =
        eval(row, geom, cutlineAllTouched, clipCrs, conf, BinaryType)

    def eval(
        row: InternalRow, geom: Any, cutlineAllTouched: Boolean, clipCrs: UTF8String,
        conf: UTF8String, dt: DataType
    ): InternalRow =
        // With propagateNull=false the invoke now runs eval even for a null primary tile OR a null
        // geom; preserve the prior "null in -> null tile out" behavior. Without these guards a null
        // row NPEs in rowToTile, and a null geom hits the exhaustive geom match (no `case other`)
        // -> MatchError -> a non-null error tile from safeEval. Contract: null tile or null geom -> null.
        if (row == null || geom == null) null
        else RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (_, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val geometry = geom match {
                  case g: UTF8String  => JTS.fromWKT(g.toString)
                  case g: Array[Byte] => JTS.fromWKB(g)
              }
              val clipCrsOpt = Option(clipCrs).map(_.toString).filter(_.nonEmpty)
              val (resultDs, metadata) = execute(ds, options, geometry, cutlineAllTouched, clipCrsOpt)
              RasterDriver.releaseDataset(ds)
              val res = RasterSerializationUtil.tileToRow((row.getLong(0), resultDs, metadata), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resultDs)
              res
          },
          row,
          dt
        )

    def execute(
        ds: Dataset, options: Map[String, String], geom: Geometry, cutlineAllTouched: Boolean,
        clipCrs: Option[String] = None
    ): (Dataset, Map[String, String]) = {
        val epsgCode = geom.getSRID
        // Rule 1 source-CRS: an embedded SRID (EWKB/EWKT) wins; else the explicit
        // clipCrs (int SRID or CRS string, incl. ESRI/WKT); else None -> fall back to
        // the raster's CRS (assume the cutline is already in the raster CRS). A bare
        // WKB/WKT cutline leaves SRID=0.
        val srcSR: SpatialReference =
            if (epsgCode > 0) SpatialRefOps.resolveCrs(epsgCode.toString)
            else clipCrs match {
                case Some(c) => SpatialRefOps.resolveCrs(c)
                case None =>
                    val geomSR = new SpatialReference()
                    val dsSR = ds.GetSpatialRef
                    if (dsSR != null) {
                        val dsEpsgCode = SpatialRefOps.getEPSGCode(dsSR)
                        if (dsEpsgCode != 0) geomSR.ImportFromEPSG(dsEpsgCode)
                        else geomSR.ImportFromWkt(dsSR.ExportToWkt())
                    } else {
                        geomSR.SetWellKnownGeogCS("WGS84")
                    }
                    geomSR
            }
        val res = ClipToGeom.clip(ds, options, geom, srcSR, cutlineAllTouched)
        srcSR.delete()
        res
    }

    override def name: String = "gbx_rst_clip"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        // 3-arg (no clip_crs) stays valid; 4-arg adds the source-CRS override.
        case 3 => RST_Clip(c(0), c(1), c(2), Literal(null, StringType))
        case 4 => RST_Clip(c(0), c(1), c(2), c(3))
    }

}
