package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, ViewshedMode, ViewshedOutputType, gdal}

/**
  * Compute a binary viewshed raster from a DEM tile and an observer POINT.
  *
  * Wraps `gdal.ViewshedGenerate`. Output has the same extent / CRS as the
  * source DEM; pixels reachable along an unobstructed line-of-sight from the
  * observer carry the "visible" value (`255`), invisible pixels carry `0`,
  * out-of-range pixels carry `0`, NoData pixels carry NoData.
  *
  *   - `observer_geom`: POINT in the raster's CRS (no implicit reprojection).
  *     Non-POINT geometries are rejected up-front.
  *   - `observer_height`: height of the observer above the DEM at the observer
  *     pixel (e.g. eye height plus mast or tower).
  *   - `target_height` (default `1.6`): height of the target above the DEM at
  *     each tested pixel (~average human eye height).
  *   - `max_distance`: optional clipping distance in CRS ground units; pixels
  *     beyond it are forced to "invisible". `null` = unlimited (only bounded
  *     by the raster extent).
  */
case class RST_Viewshed(
    tileExpr: Expression,
    observerGeomExpr: Expression,
    observerHeightExpr: Expression,
    targetHeightExpr: Expression,
    maxDistanceExpr: Expression,
    crsExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        tileExpr, observerGeomExpr, observerHeightExpr, targetHeightExpr, maxDistanceExpr,
        crsExpr, ExpressionConfigExpr()
    )
    // observer_geom is BinaryType (WKB) or StringType (WKT) — accept the geom
    // expr's type; heights are Double, max_distance Double (nullable), crs String.
    override def inputTypes: Seq[DataType] = Seq(
        tileExpr.dataType, observerGeomExpr.dataType, DoubleType, DoubleType, DoubleType,
        StringType, StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tileExpr)
    override def nullable: Boolean = true
    override def prettyName: String = RST_Viewshed.name
    override def replacement: Expression = invoke(RST_Viewshed)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

}

object RST_Viewshed extends WithExpressionInfo {

    def eval(
        row: InternalRow, geom: Any, observerHeight: Double, targetHeight: Double,
        maxDistance: Any, conf: UTF8String
    ): InternalRow =
        runDispatch(row, geom, observerHeight, targetHeight, maxDistance, null, conf, BinaryType)

    def eval(
        row: InternalRow, geom: Any, observerHeight: Double, targetHeight: Double,
        maxDistance: Any, crs: UTF8String, conf: UTF8String
    ): InternalRow =
        runDispatch(row, geom, observerHeight, targetHeight, maxDistance, crs, conf, BinaryType)

    private def runDispatch(
        row: InternalRow, geomArg: Any, observerHeight: Double, targetHeight: Double,
        maxDistance: Any, crs: UTF8String, conf: UTF8String, dt: DataType
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
              val parsed = geomArg match {
                  case g: UTF8String  => JTS.fromWKT(g.toString)
                  case g: Array[Byte] => JTS.fromWKB(g)
                  case other          =>
                      throw new IllegalArgumentException(
                          s"gbx_rst_viewshed: unsupported observer_geom payload type ${if (other == null) "null" else other.getClass.getName}"
                      )
              }
              require(parsed.getGeometryType == "Point",
                  s"gbx_rst_viewshed requires a POINT observer_geom; got ${parsed.getGeometryType}")
              val coord = parsed.getCoordinate
              // Rule 1 source-CRS reprojection: embedded SRID wins; else the crs arg;
              // else assume the observer is already in the raster CRS (never errors).
              val clip = Option(crs).map(_.toString).filter(_.nonEmpty)
              val (ox, oy) = reprojectObserver(ds, coord.x, coord.y, parsed.getSRID, clip)
              val maxDistOpt = maxDistance match {
                  case null         => None
                  case d: Double    => Some(d)
                  case n: Number    => Some(n.doubleValue())
                  case _            => None
              }
              val (resDs, resMtd) = execute(
                  ds, options, ox, oy, observerHeight, targetHeight, maxDistOpt
              )
              RasterDriver.releaseDataset(ds)
              val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
              RasterDriver.releaseDataset(resDs)
              out
          },
          row,
          dt
        )

    /** Reproject an observer point from its source CRS to the raster's CRS. Rule 1:
      * embedded SRID wins; else the explicit `crs`; else assume aligned. A missing
      * source or raster CRS, or any transform failure, returns the point as-is. */
    private def reprojectObserver(
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
      * Runs `gdal.ViewshedGenerate` with the GVOT_NORMAL output (binary 0/255
      * visibility mask) and GVM_Edge mode (the GDAL CLI default).
      */
    def execute(
        ds: Dataset, options: Map[String, String],
        observerX: Double, observerY: Double, observerHeight: Double, targetHeight: Double,
        maxDistance: Option[Double]
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "RST_Viewshed.execute: source Dataset is null")
        require(observerHeight >= 0.0 && !observerHeight.isNaN && !observerHeight.isInfinity,
            s"gbx_rst_viewshed: observer_height must be >= 0 and finite; got $observerHeight")
        require(targetHeight >= 0.0 && !targetHeight.isNaN && !targetHeight.isInfinity,
            s"gbx_rst_viewshed: target_height must be >= 0 and finite; got $targetHeight")
        maxDistance.foreach { d =>
            require(d > 0.0 && !d.isNaN && !d.isInfinity,
                s"gbx_rst_viewshed: max_distance must be > 0 and finite; got $d")
        }

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val outPath = s"/vsimem/viewshed_$uuid.tif"

        // Visible / invisible / out-of-range / nodata sentinels (Byte-friendly).
        val visibleVal = 255.0
        val invisibleVal = 0.0
        val outOfRangeVal = 0.0
        val noDataVal = 0.0
        val curvCoeff = 0.85714 // GDAL default for earth-curvature correction
        val maxDist = maxDistance.getOrElse(0.0)  // 0 = unlimited per GDAL convention

        val srcBand = ds.GetRasterBand(1)
        val result = gdal.ViewshedGenerate(
            srcBand,
            /* driverName       */ "GTiff",
            /* targetRasterName */ outPath,
            /* creationOptions  */ null,
            /* observerX        */ observerX,
            /* observerY        */ observerY,
            /* observerHeight   */ observerHeight,
            /* targetHeight     */ targetHeight,
            /* visibleVal       */ visibleVal,
            /* invisibleVal     */ invisibleVal,
            /* outOfRangeVal    */ outOfRangeVal,
            /* noDataVal        */ noDataVal,
            /* curvCoeff        */ curvCoeff,
            /* mode             */ ViewshedMode.GVM_Edge,
            /* maxDistance      */ maxDist
        )
        val errMsg = gdal.GetLastErrorMsg()
        if (result == null) {
            throw new RuntimeException(
                s"gbx_rst_viewshed: gdal.ViewshedGenerate failed: " +
                  (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }
        result.FlushCache()

        // Use the symbol to discourage Scala "unused import" pruning if the
        // surrounding GDAL upgrade lands a default output-type variant later.
        val _outputType = ViewshedOutputType.GVOT_NORMAL
        val _ = _outputType

        val metadata = Map(
            "path" -> outPath,
            "driver" -> "GTiff",
            "extension" -> "tif",
            "last_command" -> s"gdal.ViewshedGenerate(observer=($observerX,$observerY),h=$observerHeight)",
            "last_error" -> (if (errMsg == null) "" else errMsg),
            "all_parents" -> Option(ds.GetDescription()).getOrElse(""),
            "size" -> "-1",
            "format" -> "GTiff",
            "compression" -> "DEFLATE",
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (result, metadata)
    }

    override def name: String = "gbx_rst_viewshed"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        val nullCrs = Literal(null, StringType)
        c.length match {
            case 3 => RST_Viewshed(c(0), c(1), c(2), Literal(1.6), Literal(null, DoubleType), nullCrs)
            case 4 => RST_Viewshed(c(0), c(1), c(2), c(3), Literal(null, DoubleType), nullCrs)
            case 5 => RST_Viewshed(c(0), c(1), c(2), c(3), c(4), nullCrs)
            case 6 => RST_Viewshed(c(0), c(1), c(2), c(3), c(4), c(5)) // + crs override
            case n => throw new IllegalArgumentException(
                s"gbx_rst_viewshed takes 3 to 6 arguments (tile, observer_geom, observer_height, [target_height, [max_distance, [crs]]]); got $n"
            )
        }
    }

}
