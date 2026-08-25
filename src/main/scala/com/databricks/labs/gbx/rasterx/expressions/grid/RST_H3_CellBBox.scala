package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.operations.{OSRTransformGeometry, SpatialRefOps}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.Coordinate

import scala.collection.mutable.ArrayBuffer

/** Catalyst expression: bounding box of one H3 cell in the output CRS.
 *
 *  Arguments: `cellid` (LONG or INT), `srid` (INT, default 4326 — an EPSG or ESRI
 *  code), `mode` (STRING: `"centroids"` default or `"spatial_envelope"`),
 *  `kring_pad` (INT, default 0), and an optional `out_crs` (STRING: `EPSG:x` /
 *  `ESRI:x` / WKT) that wins over `srid` when set.
 *
 *  Returns `STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>`. In
 *  `"centroids"` mode the bbox is built from cell centroids (a single cell yields a
 *  degenerate point bbox); in `"spatial_envelope"` mode from the hexagon boundary.
 *  When `kring_pad > 0` the cell is expanded to its k-ring neighbourhood first.
 *  Matches the lightweight tier (`pyrx.functions._h3_cell_bbox_udf`).
 */
case class RST_H3_CellBBox(
    cellidExpr:   Expression,
    sridExpr:     Expression,
    modeExpr:     Expression,
    kringPadExpr: Expression,
    outCrsExpr:   Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] =
        Seq(cellidExpr, sridExpr, modeExpr, kringPadExpr, outCrsExpr)
    override def nullable: Boolean = true
    override def foldable: Boolean = children.forall(_.foldable)

    override def dataType: DataType = RST_H3_CellBBox.BBoxType

    override def eval(input: InternalRow): Any = {
        val raw = cellidExpr.eval(input)
        if (raw == null) return null
        val cellId = raw match {
            case l: Long => l
            case i: Int  => i.toLong
            case o => throw new IllegalArgumentException(
                s"${RST_H3_CellBBox.name}: cellid must be LONG or INT; got ${o.getClass.getName}")
        }
        val srid = sridExpr.eval(input) match {
            case null    => 4326
            case i: Int  => i
            case l: Long => l.toInt
            case o => throw new IllegalArgumentException(
                s"${RST_H3_CellBBox.name}: srid must be INT or LONG; got ${o.getClass.getName}")
        }
        // Output-CRS spec (Rule 2): out_crs string wins over the int srid.
        val outCrs = outCrsExpr.eval(input) match {
            case null => None
            case s: org.apache.spark.unsafe.types.UTF8String =>
                val t = s.toString; if (t.isEmpty) None else Some(t)
            case s: String => if (s.isEmpty) None else Some(s)
            case _ => None
        }
        val mode = modeExpr.eval(input) match {
            case null => "centroids"
            case s: org.apache.spark.unsafe.types.UTF8String => s.toString
            case s: String => s
            case o => throw new IllegalArgumentException(
                s"${RST_H3_CellBBox.name}: mode must be STRING; got ${o.getClass.getName}")
        }
        val kringPad = kringPadExpr.eval(input) match {
            case null    => 0
            case i: Int  => i
            case l: Long => l.toInt
            case o => throw new IllegalArgumentException(
                s"${RST_H3_CellBBox.name}: kring_pad must be INT or LONG; got ${o.getClass.getName}")
        }

        val cells: Iterable[Long] =
            if (kringPad > 0) H3.kRing(cellId, kringPad).toSet else Set(cellId)

        // Collect WGS84 lon/lat sample points per mode.
        val lons = ArrayBuffer.empty[Double]
        val lats = ArrayBuffer.empty[Double]
        mode match {
            case "centroids" =>
                cells.foreach { c =>
                    val ctr = H3.cellIdToCenter(c)  // Coordinate(lat, lng)
                    lons += ctr.y; lats += ctr.x
                }
            case "spatial_envelope" =>
                cells.foreach { c =>
                    H3.cellIdToBoundary(c).foreach { b => lons += b.y; lats += b.x }
                }
            case other =>
                throw new IllegalArgumentException(s"${RST_H3_CellBBox.name}: unknown mode '$other'")
        }

        // Resolve the output CRS: out_crs string wins over the int srid, both via
        // the shared resolver (ESRI codes classify correctly; WKT/PROJ4 supported).
        // Identity short-circuit when the target is grid-native EPSG:4326.
        val dstSR: SpatialReference = outCrs match {
            case Some(c) => SpatialRefOps.resolveCrs(c)
            case None    => SpatialRefOps.resolveCrs(srid.toString)
        }
        val gridSR = new SpatialReference(); gridSR.ImportFromEPSG(H3.crsID)
        val isNative = dstSR.IsSame(gridSR) == 1
        gridSR.delete()
        val (xs, ys) =
            if (isNative) { dstSR.delete(); (lons.toArray, lats.toArray) }
            else {
                val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(H3.crsID)
                try {
                    val xb = ArrayBuffer.empty[Double]
                    val yb = ArrayBuffer.empty[Double]
                    var i = 0
                    while (i < lons.length) {
                        val pt = JTS.point(new Coordinate(lons(i), lats(i)))
                        val tp = OSRTransformGeometry.transform(pt, srcSR, dstSR)
                        val c = tp.getCoordinate
                        xb += c.x; yb += c.y
                        i += 1
                    }
                    (xb.toArray, yb.toArray)
                } finally {
                    srcSR.delete()
                    dstSR.delete()
                }
            }

        InternalRow.fromSeq(Seq(xs.min, ys.min, xs.max, ys.max))
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

/** Companion: SQL name gbx_h3_cell_bbox, 2/3/4-arg builder. */
object RST_H3_CellBBox extends WithExpressionInfo {

    override def name: String = "gbx_h3_cell_bbox"

    val BBoxType: StructType = StructType(Seq(
        StructField("xmin", DoubleType, nullable = false),
        StructField("ymin", DoubleType, nullable = false),
        StructField("xmax", DoubleType, nullable = false),
        StructField("ymax", DoubleType, nullable = false)
    ))

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        val nullCrs = Literal(null, StringType)
        c.length match {
            case 1 => RST_H3_CellBBox(c(0), Literal(4326), Literal("centroids"), Literal(0), nullCrs)
            case 2 => RST_H3_CellBBox(c(0), c(1), Literal("centroids"), Literal(0), nullCrs)
            case 3 => RST_H3_CellBBox(c(0), c(1), c(2), Literal(0), nullCrs)
            case 4 => RST_H3_CellBBox(c(0), c(1), c(2), c(3), nullCrs)
            case 5 => RST_H3_CellBBox(c(0), c(1), c(2), c(3), c(4)) // + out_crs override
            case n => throw new IllegalArgumentException(
                s"$name expects 1-5 arguments (cellid, srid, mode, kring_pad, [out_crs]); got $n")
        }
    }
}
