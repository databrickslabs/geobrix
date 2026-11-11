package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, GroupArgs, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.custom.Custom_GridSpec
import com.databricks.labs.gbx.gridx.grid.CustomGridSystem
import com.databricks.labs.gbx.rasterx.operations.OSRTransformGeometry
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, VectorRasterBridge}
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
 *  Serde format: `[count:Int][ cellId:Long, value:Double ]*N`, then the captured arguments when there are any.
 */
final class CustomRasterizeAcc(
    val cells: ArrayBuffer[(Long, Double)] = ArrayBuffer.empty
) {

    def add(cellId: Long, v: Double): CustomRasterizeAcc = {
        cells += ((cellId, v))
        CustomRasterizeAcc.guardSize(cells.length.toLong)
        this
    }

    /** The per-group arguments read from a row of the group ([[GroupArgs]]); null when none were captured. */
    var args: Array[Byte] = null

    def merge(other: CustomRasterizeAcc): CustomRasterizeAcc = {
        cells ++= other.cells
        args = GroupArgs.prefer(args, other.args)
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
        GroupArgs.write(out, args)
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
        val acc = new CustomRasterizeAcc(buf)
        acc.args = GroupArgs.read(in)
        acc
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
 *  `(cellid LONG, value DOUBLE)` per row; the grid and the remaining options are read from a row of the group
 *  ([[GroupArgs]]), so they may be columns as well as constants.
 *  On `eval` the cells are burned into one raster by pixel-centroid mapping via
 *  [[RasterizeBurn.burn]] with the reconstructed [[CustomGridSystem]] — the inverse of
 *  [[com.databricks.labs.gbx.rasterx.expressions.grid.RST_Custom_RasterToGrid]].
 *
 *  When an explicit extent (xmin..height) is absent, the snapped, lattice-aligned grid is derived
 *  from the cell set + `kring_pad`. NoData = -9999.0. An omitted `value` (an untyped NULL) is the presence mask and burns 1.0; a NULL in a typed column burns NaN.
 *  A cell id that repeats keeps its LARGEST value, whatever order the rows arrive in: cells are
 *  sorted by `(cellId, value)` before the lookup is built, so the last write for a cell is its
 *  maximum. A NaN never wins against a number (see [[RasterizeBurn.canonicalLut]]); a cell of nothing but NaN burns NaN. The lightweight tier follows the
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

    import RST_Custom_RasterizeAgg.{evalDoubleOpt, evalIntOpt, evalString}

    override lazy val deterministic: Boolean = true  // canonical fold order (see eval)
    override val nullable: Boolean = true
    override lazy val dataType: DataType = BinaryType  // the GeoTIFF bytes: the one contract of this name
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

    // the grid and the eleven options: read from a row of the group, so any of them may be a column
    private val groupArgs = new GroupArgs(Seq(
        gridExpr, outSridExpr, pixelSizeExpr, xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthExpr, heightExpr, modeExpr, kringPadExpr))

    override def createAggregationBuffer(): CustomRasterizeAcc = CustomRasterizeAcc.empty

    /** Catalyst-facing update: extract cellid and value from the row, delegate to typed helper. */
    override def update(buffer: CustomRasterizeAcc, input: InternalRow): CustomRasterizeAcc = {
        if (GroupArgs.lacksFirst(buffer.args)) buffer.args = GroupArgs.prefer(buffer.args, groupArgs.capture(input))
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
            case null      => RasterizeBurn.nullBurn(valueExpr)  // 1.0 for an omitted value, NaN for a missing one
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

        val (args, row) = groupArgs.bind(buffer.args)
        val grid     = Custom_GridSpec.systemFromRow(args(0).eval(row).asInstanceOf[InternalRow])
        // out_srid wins; else the grid's own CRS; a grid with none needs the argument (the lightweight rule)
        val srid     = evalIntOpt(args(1), row).getOrElse(grid.conf.crsID.getOrElse(
            throw new IllegalArgumentException(
                s"${RST_Custom_RasterizeAgg.name}: grid has no CRS (srid=-1); supply out_srid explicitly.")))
        val pixelOpt = evalDoubleOpt(args(2), row)
        val xminOpt  = evalDoubleOpt(args(3), row)
        val yminOpt  = evalDoubleOpt(args(4), row)
        val xmaxOpt  = evalDoubleOpt(args(5), row)
        val ymaxOpt  = evalDoubleOpt(args(6), row)
        val widthOpt = evalIntOpt(args(7),    row)
        val heightOpt= evalIntOpt(args(8),    row)
        val mode     = evalString(args(9),    row, "mode", "centroids")
        val kringPad = evalIntOpt(args(10),   row).getOrElse(1)

        // Resolution from the cells; error on mixed (decoded from the custom cell-id bit layout).
        val resolution = RST_Custom_RasterizeAgg.resolutionOf(grid, buffer.cells.iterator.map(_._1))

        // Canonical fold order: sort by (cellId, value) and build the lookup in that order, so the
        // last write for a repeated cell is its largest value (a NaN sorts first, so any number in its
        // cell wins), regardless of row-arrival order.
        val lut = RasterizeBurn.canonicalLut(buffer.cells.toSeq)

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
            bytes
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
            if (kringPad > 0) base.flatMap(c => grid.kRing(c, kringPad).toSet) // the ring holds only cells that exist
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

        // snap_bounds: outward snap to the pixel_size lattice. The lattice follows the grid's own cell edges, which
        // start at (boundXMin, boundYMin), not at the coordinate origin: snapped from 0, the pixel centres sit
        // half a cell off the cells whenever the bounds are not a multiple of the pixel. That holds in the grid's own
        // CRS only; a canvas in another CRS has no cell edges to follow. `tol` keeps float noise on an aligned edge
        // from adding a pixel. The light level (`_CustomAdapter.snap_lattice`) does the same.
        val (ax, ay, tol) =
            if (srid == gridCrs) (grid.conf.boundXMin.toDouble, grid.conf.boundYMin.toDouble, 1e-9)
            else (0.0, 0.0, 0.0)
        val xmin = ax + math.floor((bxmin - ax) / pixelSize + tol) * pixelSize
        val ymax = ay + math.ceil((bymax - ay) / pixelSize - tol) * pixelSize
        val width  = math.max(1, math.ceil((bxmax - xmin) / pixelSize - tol).toInt)
        val height = math.max(1, math.ceil((ymax - bymin) / pixelSize - tol).toInt)
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
