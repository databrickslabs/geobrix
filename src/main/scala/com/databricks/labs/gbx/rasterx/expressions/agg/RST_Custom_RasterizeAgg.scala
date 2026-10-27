package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.custom.Custom_GridSpec
import com.databricks.labs.gbx.gridx.grid.CustomGridSystem
import com.databricks.labs.gbx.rasterx.operations.OSRTransformGeometry
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, V2Tile, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.gdal.osr.SpatialReference

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/** Mutable aggregation buffer for [[RST_Custom_RasterizeAgg]].
 *
 *  Accumulates `(cellId: Long, value: Double)` pairs streamed one per row.
 *  Serde format: `[count:Int][ cellId:Long, value:Double ]*N`.
 */
final class CustomRasterizeAcc(
    val cells: ArrayBuffer[(Long, Double)] = ArrayBuffer.empty
) {

    def add(cellId: Long, v: Double): CustomRasterizeAcc = {
        cells += ((cellId, v))
        CustomRasterizeAcc.guardSize(cells.length.toLong)
        this
    }

    def merge(other: CustomRasterizeAcc): CustomRasterizeAcc = {
        cells ++= other.cells
        CustomRasterizeAcc.guardSize(cells.length.toLong)
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

object CustomRasterizeAcc {

    /** Hard cap on accumulated rows per buffer (16 bytes/row on disk). */
    val MAX_BUFFER_ROWS: Long = 50L * 1000L * 1000L

    def empty: CustomRasterizeAcc = new CustomRasterizeAcc()

    def deserialize(bytes: Array[Byte]): CustomRasterizeAcc = {
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
        new CustomRasterizeAcc(buf)
    }

    private[agg] def guardSize(currentRows: Long): Unit = {
        if (currentRows > MAX_BUFFER_ROWS) {
            throw new IllegalStateException(
                s"gbx_rst_custom_rasterize_agg buffer exceeded $MAX_BUFFER_ROWS rows " +
                s"(current = $currentRows). Reduce the group size or tile the workload.")
        }
    }
}

/** UDAF: `gbx_rst_custom_rasterize_agg(cellid, value, grid, out_srid, pixel_size, xmin, ymin,
 *  xmax, ymax, width, height, mode, kring_pad)`.
 *
 *  Mirrors [[RST_H3_RasterizeAgg]], with the custom `grid` struct (from `gbx_custom_grid(...)`)
 *  inserted after `(cellid, value)` — where the H3 grid identity is implicit. Streams
 *  `(cellid LONG, value DOUBLE)` per row; the remaining eleven arguments are per-group constants.
 *  On `eval` the cells are burned into one raster by pixel-centroid mapping via
 *  [[RasterizeBurn.burn]] with the reconstructed [[CustomGridSystem]] — the inverse of
 *  [[com.databricks.labs.gbx.rasterx.expressions.grid.RST_Custom_RasterToGrid]].
 *
 *  When an explicit extent (xmin..height) is absent, the snapped, lattice-aligned grid is derived
 *  from the cell set + `kring_pad`. NoData = -9999.0. A null/omitted `value` burns 1.0 (presence mask).
 *  A cell id that repeats keeps its LARGEST value, whatever order the rows arrive in: cells are
 *  sorted by `(cellId, value)` before the lookup is built, so the last write for a cell is its
 *  maximum. A null `value` counts as 1.0 in that comparison. The lightweight tier follows the
 *  same rule.
 */
case class RST_Custom_RasterizeAgg(
    cellidExpr:    Expression,
    valueExpr:     Expression,
    gridExpr:      Expression,
    outSridExpr:   Expression,
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
) extends TypedImperativeAggregate[CustomRasterizeAcc] {

    import RST_Custom_RasterizeAgg.{evalDoubleOpt, evalInt, evalIntOpt, evalString}

    override lazy val deterministic: Boolean = true  // canonical fold order (see eval)
    override val nullable: Boolean = true
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def prettyName: String = RST_Custom_RasterizeAgg.name

    override def children: Seq[Expression] = Seq(
        cellidExpr, valueExpr, gridExpr, outSridExpr, pixelSizeExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthExpr, heightExpr, modeExpr, kringPadExpr,
        exprConfExpr
    )

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_Custom_RasterizeAgg =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11), nc(12), nc(13))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): CustomRasterizeAcc = CustomRasterizeAcc.empty

    /** Catalyst-facing update: extract cellid and value from the row, delegate to typed helper. */
    override def update(buffer: CustomRasterizeAcc, input: InternalRow): CustomRasterizeAcc = {
        val raw = cellidExpr.eval(input)
        if (raw == null) return buffer
        val cellId = raw match {
            case l: Long => l
            case i: Int  => i.toLong
            case o => throw new IllegalArgumentException(
                s"${RST_Custom_RasterizeAgg.name}: cellid must be LONG or INT; got ${o.getClass.getName}")
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
                s"${RST_Custom_RasterizeAgg.name}: value must be numeric; got ${o.getClass.getName}")
        }
        update(buffer, cellId, v)
    }

    /** Direct typed update used by unit tests. */
    def update(buffer: CustomRasterizeAcc, cellId: Long, v: Double): CustomRasterizeAcc =
        buffer.add(cellId, v)

    override def merge(buffer: CustomRasterizeAcc, input: CustomRasterizeAcc): CustomRasterizeAcc =
        buffer.merge(input)

    override def eval(buffer: CustomRasterizeAcc): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.cells.isEmpty) return null

        val empty = InternalRow.empty
        val grid     = Custom_GridSpec.systemFromRow(gridExpr.eval(empty).asInstanceOf[InternalRow])
        val srid     = evalInt(outSridExpr,   empty, "out_srid")
        val pixelOpt = evalDoubleOpt(pixelSizeExpr, empty)
        val xminOpt  = evalDoubleOpt(xminExpr,  empty)
        val yminOpt  = evalDoubleOpt(yminExpr,  empty)
        val xmaxOpt  = evalDoubleOpt(xmaxExpr,  empty)
        val ymaxOpt  = evalDoubleOpt(ymaxExpr,  empty)
        val widthOpt = evalIntOpt(widthExpr,    empty)
        val heightOpt= evalIntOpt(heightExpr,   empty)
        val mode     = evalString(modeExpr,     empty, "mode", "centroids")
        val kringPad = evalIntOpt(kringPadExpr, empty).getOrElse(1)

        // Resolution from the cells; error on mixed (decoded from the custom cell-id bit layout).
        val resolution = RST_Custom_RasterizeAgg.resolutionOf(grid, buffer.cells.iterator.map(_._1))

        // Canonical fold order: sort by (cellId, value) and build the lookup in that order, so the
        // last write for a repeated cell is its largest value (a null value was already turned into
        // 1.0), regardless of row-arrival order.
        val ordered = buffer.cells.toSeq.sortWith { (a, b) =>
            if (a._1 != b._1) java.lang.Long.compareUnsigned(a._1, b._1) < 0 else a._2 < b._2
        }
        val lut = scala.collection.mutable.LongMap.empty[Double]
        ordered.foreach { case (cellId, v) => lut.update(cellId, v) }

        // Resolve grid spec: explicit extent if fully supplied, else snapped grid from the cells.
        val explicit = xminOpt.isDefined && yminOpt.isDefined && xmaxOpt.isDefined &&
            ymaxOpt.isDefined && widthOpt.isDefined && heightOpt.isDefined
        val (xmin, ymin, xmax, ymax, width, height) =
            if (explicit) {
                (xminOpt.get, yminOpt.get, xmaxOpt.get, ymaxOpt.get, widthOpt.get, heightOpt.get)
            } else {
                RST_Custom_RasterizeAgg.computeGridspec(
                    grid, buffer.cells.iterator.map(_._1), srid, pixelOpt, mode, kringPad, resolution)
            }

        val rasterDs = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, width, height, srid)
        try {
            RasterizeBurn.burn(grid, lut, srid, resolution, rasterDs, width, height)
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
            V2Tile.row(cellid = 0L, raster = bytes, metadata = mapData)
        } finally {
            rasterDs.delete()
        }
    }

    override def serialize(obj: CustomRasterizeAcc): Array[Byte] = obj.serialize

    override def deserialize(bytes: Array[Byte]): CustomRasterizeAcc = CustomRasterizeAcc.deserialize(bytes)
}

object RST_Custom_RasterizeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_custom_rasterize_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 13 => RST_Custom_RasterizeAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12))
        case n => throw new IllegalArgumentException(
            s"$name expects 13 arguments " +
            s"(cellid, value, grid, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad); got $n")
    }

    /** Resolution of a custom cell set (decoded from the cell-id bit layout); throws on a mixed set. */
    private[agg] def resolutionOf(grid: CustomGridSystem, cellIds: Iterator[Long]): Int = {
        var res = -1
        cellIds.foreach { c =>
            val r = grid.getCellResolution(c)
            if (res == -1) res = r
            else if (r != res) throw new IllegalArgumentException(
                s"$name: custom cell set has mixed resolutions ($res and $r)")
        }
        res
    }

    /** Snapped, lattice-aligned grid spec from a cell set. Custom cell geometries already live in the
     *  grid's native CRS, so sample points are taken directly (no WGS84 hop) and reprojected to `srid`
     *  only when it differs from the grid CRS.
     *
     *  Returns `(xmin, ymin, xmax, ymax, width, height)`.
     */
    private[agg] def computeGridspec(
        grid: CustomGridSystem,
        cellIds: Iterator[Long],
        srid: Int,
        pixelSizeOpt: Option[Double],
        mode: String,
        kringPad: Int,
        resolution: Int
    ): (Double, Double, Double, Double, Int, Int) = {
        // Dedup + optional k-ring padding.
        val base = cellIds.toSet
        val cells =
            if (kringPad > 0) base.flatMap(c => grid.kRing(c, kringPad).toSet)
            else base

        // Collect sample points in the grid's native CRS per mode.
        val xs = ArrayBuffer.empty[Double]
        val ys = ArrayBuffer.empty[Double]
        mode match {
            case "centroids" =>
                cells.foreach { c =>
                    val ctr = grid.cellIdToCenter(c)
                    xs += ctr.x; ys += ctr.y
                }
            case "spatial_envelope" =>
                cells.foreach { c =>
                    grid.cellIdToBoundary(c).foreach { b => xs += b.x; ys += b.y }
                }
            case other =>
                throw new IllegalArgumentException(s"$name: unknown mode '$other'")
        }

        val gridCrs = grid.crsSrid
        // Reproject sample points grid CRS -> srid when they differ.
        val (rxs, rys) =
            if (srid == gridCrs) (xs.toArray, ys.toArray)
            else {
                val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(gridCrs)
                val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(srid)
                try {
                    val xb = ArrayBuffer.empty[Double]
                    val yb = ArrayBuffer.empty[Double]
                    var i = 0
                    while (i < xs.length) {
                        val pt = JTS.point(new org.locationtech.jts.geom.Coordinate(xs(i), ys(i)))
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

        val bxmin = rxs.min; val bxmax = rxs.max
        val bymin = rys.min; val bymax = rys.max

        // Default pixel size: the smaller cell dimension at this resolution (custom cells are planar rects).
        val pixelSize = pixelSizeOpt.getOrElse(
            math.min(grid.getCellWidth(resolution), grid.getCellHeight(resolution)))

        // snap_bounds: outward snap to the pixel_size lattice.
        val xmin = math.floor(bxmin / pixelSize) * pixelSize
        val ymax = math.ceil(bymax / pixelSize) * pixelSize
        val width  = math.max(1, math.ceil((bxmax - xmin) / pixelSize).toInt)
        val height = math.max(1, math.ceil((ymax - bymin) / pixelSize).toInt)
        val xmax = xmin + width * pixelSize
        val ymin = ymax - height * pixelSize
        (xmin, ymin, xmax, ymax, width, height)
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
