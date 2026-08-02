package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.operations.OSRTransformGeometry
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.Geometry

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/** Mutable aggregation buffer for [[RST_H3_RasterizeAgg]].
 *
 *  Accumulates `(cellId: Long, value: Double)` pairs streamed one per row.
 *  Serde format: `[count:Int][ cellId:Long, value:Double ]*N`.
 */
final class H3RasterizeAcc(
    val cells: ArrayBuffer[(Long, Double)] = ArrayBuffer.empty
) {

    def add(cellId: Long, v: Double): H3RasterizeAcc = {
        cells += ((cellId, v))
        H3RasterizeAcc.guardSize(cells.length.toLong)
        this
    }

    def merge(other: H3RasterizeAcc): H3RasterizeAcc = {
        cells ++= other.cells
        H3RasterizeAcc.guardSize(cells.length.toLong)
        this
    }

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(cells.length)
        for ((cellId, v) <- cells) {
            out.writeLong(cellId)
            out.writeDouble(v)
        }
        bos.toByteArray
    }
}

object H3RasterizeAcc {

    /** Hard cap on accumulated rows per buffer (16 bytes/row on disk). */
    val MAX_BUFFER_ROWS: Long = 50L * 1000L * 1000L

    def empty: H3RasterizeAcc = new H3RasterizeAcc()

    def deserialize(bytes: Array[Byte]): H3RasterizeAcc = {
        val in  = new DataInputStream(new ByteArrayInputStream(bytes))
        val n   = in.readInt()
        val buf = ArrayBuffer.empty[(Long, Double)]
        var i = 0
        while (i < n) {
            val cellId = in.readLong()
            val v = in.readDouble()
            buf += ((cellId, v))
            i += 1
        }
        new H3RasterizeAcc(buf)
    }

    private[agg] def guardSize(currentRows: Long): Unit = {
        if (currentRows > MAX_BUFFER_ROWS) {
            throw new IllegalStateException(
                s"gbx_rst_h3_rasterize_agg buffer exceeded $MAX_BUFFER_ROWS rows " +
                s"(current = $currentRows). Reduce the group size or tile the workload.")
        }
    }
}

/** UDAF: `gbx_rst_h3_rasterize_agg(cellid, value, srid, pixel_size, xmin, ymin,
 *  xmax, ymax, width, height, mode, kring_pad)`.
 *
 *  Streams `(cellid LONG, value DOUBLE)` per row; the remaining ten arguments are
 *  per-group constants (Literal or constant expressions). On `eval` the cells are
 *  burned into one raster by pixel-centroid mapping -- for each output pixel center
 *  we compute its geographic coordinate (the same affine [[com.databricks.labs.gbx.rasterx.expressions.grid.RST_H3_RasterToGrid]]
 *  uses), index it to an H3 cell via [[H3.pointToCellID]], and write the cell's value
 *  if present. This is the inverse of `RST_H3_RasterToGrid` and matches the lightweight
 *  tier (`pyrx.core.cellraster`).
 *
 *  When an explicit extent (xmin..height) is absent, the snapped, lattice-aligned grid
 *  is derived from the cell set + `kring_pad` (port of `cellraster.compute_gridspec`).
 *  NoData = -9999.0. A null/omitted `value` burns 1.0 (presence mask).
 *
 *  Overlap is last-wins; the accumulated cells are sorted by `(cellId, value)` before
 *  building the value lookup so the winner is deterministic regardless of row-arrival
 *  order (matches the lightweight tier).
 */
case class RST_H3_RasterizeAgg(
    cellIdExpr:    Expression,
    valueExpr:     Expression,
    sridExpr:      Expression,
    pixelSizeExpr: Expression,
    xminExpr:      Expression,
    yminExpr:      Expression,
    xmaxExpr:      Expression,
    ymaxExpr:      Expression,
    widthExpr:     Expression,
    heightExpr:    Expression,
    modeExpr:      Expression,
    kringPadExpr:  Expression,
    exprConfExpr:  Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset:   Int = 0
) extends TypedImperativeAggregate[H3RasterizeAcc] {

    import RST_H3_RasterizeAgg.{evalDoubleOpt, evalInt, evalIntOpt, evalString, NoData}

    override lazy val deterministic: Boolean = true  // canonical fold order (see eval)
    override val nullable: Boolean = true
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def prettyName: String = RST_H3_RasterizeAgg.name

    override def children: Seq[Expression] = Seq(
        cellIdExpr, valueExpr, sridExpr, pixelSizeExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthExpr, heightExpr, modeExpr, kringPadExpr,
        exprConfExpr
    )

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_H3_RasterizeAgg =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11), nc(12))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): H3RasterizeAcc = H3RasterizeAcc.empty

    /** Catalyst-facing update: extract cellid and value from the row, delegate to typed helper. */
    override def update(buffer: H3RasterizeAcc, input: InternalRow): H3RasterizeAcc = {
        val raw = cellIdExpr.eval(input)
        if (raw == null) return buffer
        val cellId = raw match {
            case l: Long => l
            case i: Int  => i.toLong
            case o => throw new IllegalArgumentException(
                s"${RST_H3_RasterizeAgg.name}: cellid must be LONG or INT; got ${o.getClass.getName}")
        }
        val vRaw = valueExpr.eval(input)
        val v = vRaw match {
            case null      => 1.0  // presence mask
            case d: Double => d
            case f: Float  => f.toDouble
            case i: Int    => i.toDouble
            case l: Long   => l.toDouble
            case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
            case o => throw new IllegalArgumentException(
                s"${RST_H3_RasterizeAgg.name}: value must be numeric; got ${o.getClass.getName}")
        }
        update(buffer, cellId, v)
    }

    /** Direct typed update used by unit tests. */
    def update(buffer: H3RasterizeAcc, cellId: Long, v: Double): H3RasterizeAcc =
        buffer.add(cellId, v)

    override def merge(buffer: H3RasterizeAcc, input: H3RasterizeAcc): H3RasterizeAcc =
        buffer.merge(input)

    override def eval(buffer: H3RasterizeAcc): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.cells.isEmpty) return null

        val empty = InternalRow.empty
        val srid     = evalInt(sridExpr,      empty, "srid")
        val pixelOpt = evalDoubleOpt(pixelSizeExpr, empty)
        val xminOpt  = evalDoubleOpt(xminExpr,  empty)
        val yminOpt  = evalDoubleOpt(yminExpr,  empty)
        val xmaxOpt  = evalDoubleOpt(xmaxExpr,  empty)
        val ymaxOpt  = evalDoubleOpt(ymaxExpr,  empty)
        val widthOpt = evalIntOpt(widthExpr,    empty)
        val heightOpt= evalIntOpt(heightExpr,   empty)
        val mode     = evalString(modeExpr,     empty, "mode", "centroids")
        val kringPad = evalIntOpt(kringPadExpr, empty).getOrElse(1)

        // Resolution from the cells; error on mixed.
        val resolution = RST_H3_RasterizeAgg.resolutionOf(buffer.cells.iterator.map(_._1))

        // Canonical fold order: sort by (cellId, value) so the last-wins overlap
        // winner is deterministic regardless of row-arrival order. Build the lookup
        // in that order so later writes win.
        val ordered = buffer.cells.toSeq.sortWith { (a, b) =>
            if (a._1 != b._1) java.lang.Long.compareUnsigned(a._1, b._1) < 0 else a._2 < b._2
        }
        val lut = scala.collection.mutable.LongMap.empty[Double]
        ordered.foreach { case (cellId, v) => lut.update(cellId, v) }

        // Resolve grid spec: explicit extent if fully supplied, else snapped grid.
        val explicit = xminOpt.isDefined && yminOpt.isDefined && xmaxOpt.isDefined &&
            ymaxOpt.isDefined && widthOpt.isDefined && heightOpt.isDefined
        val (xmin, ymin, xmax, ymax, width, height) =
            if (explicit) {
                (xminOpt.get, yminOpt.get, xmaxOpt.get, ymaxOpt.get, widthOpt.get, heightOpt.get)
            } else {
                RST_H3_RasterizeAgg.computeGridspec(
                    buffer.cells.iterator.map(_._1), srid, pixelOpt, mode, kringPad, resolution)
            }

        // Source/dest spatial references for per-pixel reprojection (srid -> WGS84).
        val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(srid)
        val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(H3.crsID)

        val rasterDs = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, width, height, srid)
        try {
            val gt = rasterDs.GetGeoTransform
            val band = rasterDs.GetRasterBand(1)
            val rowBuf = new Array[Double](width)
            var py = 0
            while (py < height) {
                var px = 0
                while (px < width) {
                    // Pixel-centroid geographic coordinate in `srid` (RST_H3_RasterToGrid affine).
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val xGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
                    val yGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
                    // Reproject the pixel center to WGS84 lon/lat, then index to H3.
                    val (lon, lat) =
                        if (srid == H3.crsID) (xGeo, yGeo)
                        else {
                            val pt = JTS.point(new org.locationtech.jts.geom.Coordinate(xGeo, yGeo))
                            val tp = OSRTransformGeometry.transform(pt, srcSR, dstSR)
                            val c = tp.getCoordinate
                            (c.x, c.y)
                        }
                    val cellId = H3.pointToCellID(lon, lat, resolution)
                    rowBuf(px) = lut.getOrElse(cellId, NoData)
                    px += 1
                }
                band.WriteRaster(0, py, width, 1, rowBuf)
                py += 1
            }
            rasterDs.FlushCache()
            val bytes = VectorRasterBridge.toGTiffBytes(rasterDs)
            val mtd = Map(
                "driver"      -> "GTiff",
                "extension"   -> "tif",
                "size"        -> bytes.length.toString,
                "parentPath"  -> "",
                "all_parents" -> ""
            )
            val mapData = SerializationUtil.toMapData[String, String](mtd)
            InternalRow.fromSeq(Seq(0L, bytes, null, null, null, null, null, mapData))
        } finally {
            rasterDs.delete()
            srcSR.delete()
            dstSR.delete()
        }
    }

    override def serialize(obj: H3RasterizeAcc): Array[Byte] = obj.serialize

    override def deserialize(bytes: Array[Byte]): H3RasterizeAcc = H3RasterizeAcc.deserialize(bytes)
}

object RST_H3_RasterizeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_h3_rasterize_agg"

    /** NoData fill value, matching the lightweight tier (`cellraster._NODATA`). */
    val NoData: Double = -9999.0

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 12 => RST_H3_RasterizeAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11))
        case n => throw new IllegalArgumentException(
            s"$name expects 12 arguments " +
            s"(cellid, value, srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad); got $n")
    }

    /** Resolution of a cell set; throws on a mixed-resolution set. */
    private[agg] def resolutionOf(cellIds: Iterator[Long]): Int = {
        val h3 = com.uber.h3core.H3Core.newInstance()
        var res = -1
        cellIds.foreach { c =>
            val r = h3.h3GetResolution(c)
            if (res == -1) res = r
            else if (r != res) throw new IllegalArgumentException(
                s"$name: H3 cell set has mixed resolutions ($res and $r)")
        }
        res
    }

    /** Snapped, lattice-aligned grid spec from a cell set; port of
     *  `cellraster.compute_gridspec` (+ `snap_bounds`).
     *
     *  Returns `(xmin, ymin, xmax, ymax, width, height)`.
     */
    private[agg] def computeGridspec(
        cellIds: Iterator[Long],
        srid: Int,
        pixelSizeOpt: Option[Double],
        mode: String,
        kringPad: Int,
        resolution: Int
    ): (Double, Double, Double, Double, Int, Int) = {
        val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(H3.crsID)
        val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(srid)
        try {
            // Dedup + optional k-ring padding.
            val base = cellIds.toSet
            val cells =
                if (kringPad > 0) base.flatMap(c => H3.kRing(c, kringPad).toSet)
                else base

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
                    throw new IllegalArgumentException(s"$name: unknown mode '$other'")
            }

            // Reproject sample points WGS84 -> srid.
            val (xs, ys) =
                if (srid == H3.crsID) (lons.toArray, lats.toArray)
                else {
                    val xb = ArrayBuffer.empty[Double]
                    val yb = ArrayBuffer.empty[Double]
                    var i = 0
                    while (i < lons.length) {
                        val pt = JTS.point(new org.locationtech.jts.geom.Coordinate(lons(i), lats(i)))
                        val tp = OSRTransformGeometry.transform(pt, srcSR, dstSR)
                        val c = tp.getCoordinate
                        xb += c.x; yb += c.y
                        i += 1
                    }
                    (xb.toArray, yb.toArray)
                }

            val bxmin = xs.min; val bxmax = xs.max
            val bymin = ys.min; val bymax = ys.max

            val pixelSize = pixelSizeOpt.getOrElse {
                val edgeM = H3.edgeLength(resolution) * 1000.0  // edgeLength is km
                if (srid == H3.crsID) {
                    val midlat = (bymin + bymax) / 2.0
                    edgeM / (111320.0 * math.max(math.cos(math.toRadians(midlat)), 1e-6))
                } else edgeM
            }

            // snap_bounds: outward snap to the pixel_size lattice.
            val xmin = math.floor(bxmin / pixelSize) * pixelSize
            val ymax = math.ceil(bymax / pixelSize) * pixelSize
            val width  = math.max(1, math.ceil((bxmax - xmin) / pixelSize).toInt)
            val height = math.max(1, math.ceil((ymax - bymin) / pixelSize).toInt)
            val xmax = xmin + width * pixelSize
            val ymin = ymax - height * pixelSize
            (xmin, ymin, xmax, ymax, width, height)
        } finally {
            srcSR.delete()
            dstSR.delete()
        }
    }

    private[agg] def evalInt(e: Expression, row: InternalRow, label: String): Int =
        e.eval(row) match {
            case null    => throw new IllegalArgumentException(s"$name: $label must not be null")
            case i: Int  => i
            case l: Long => l.toInt
            case o => throw new IllegalArgumentException(
                s"$name: $label must be INT or LONG; got ${o.getClass.getName}")
        }

    private[agg] def evalIntOpt(e: Expression, row: InternalRow): Option[Int] =
        e.eval(row) match {
            case null    => None
            case i: Int  => Some(i)
            case l: Long => Some(l.toInt)
            case o => throw new IllegalArgumentException(
                s"$name: expected INT or LONG; got ${o.getClass.getName}")
        }

    private[agg] def evalDoubleOpt(e: Expression, row: InternalRow): Option[Double] =
        e.eval(row) match {
            case null      => None
            case d: Double => Some(d)
            case f: Float  => Some(f.toDouble)
            case i: Int    => Some(i.toDouble)
            case l: Long   => Some(l.toDouble)
            case dec: org.apache.spark.sql.types.Decimal => Some(dec.toDouble)
            case o => throw new IllegalArgumentException(
                s"$name: expected numeric; got ${o.getClass.getName}")
        }

    private[agg] def evalString(e: Expression, row: InternalRow, label: String, default: String): String =
        e.eval(row) match {
            case null => default
            case s: org.apache.spark.unsafe.types.UTF8String => s.toString
            case s: String => s
            case o => throw new IllegalArgumentException(
                s"$name: $label must be STRING; got ${o.getClass.getName}")
        }
}
