package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, V2Tile, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/** Mutable aggregation buffer for [[RST_BNG_RasterizeAgg]].
 *
 *  Accumulates `(cellId: Long, value: Double)` pairs streamed one per row. Although the
 *  BNG SQL input `cellid` is a STRING, it is parsed to its internal Long id on `update`
 *  so this buffer stays Long-keyed and the serde is byte-for-byte identical to the H3 and
 *  quadbin rasterize buffers.
 *  Serde format: `[count:Int][ cellId:Long, value:Double ]*N`.
 */
final class BNGRasterizeAcc(
    val cells: ArrayBuffer[(Long, Double)] = ArrayBuffer.empty
) {

    def add(cellId: Long, v: Double): BNGRasterizeAcc = {
        cells += ((cellId, v))
        BNGRasterizeAcc.guardSize(cells.length.toLong)
        this
    }

    def merge(other: BNGRasterizeAcc): BNGRasterizeAcc = {
        cells ++= other.cells
        BNGRasterizeAcc.guardSize(cells.length.toLong)
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

object BNGRasterizeAcc {

    /** Hard cap on accumulated rows per buffer (16 bytes/row on disk). */
    val MAX_BUFFER_ROWS: Long = 50L * 1000L * 1000L

    def empty: BNGRasterizeAcc = new BNGRasterizeAcc()

    def deserialize(bytes: Array[Byte]): BNGRasterizeAcc = {
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
        new BNGRasterizeAcc(buf)
    }

    private[agg] def guardSize(currentRows: Long): Unit = {
        if (currentRows > MAX_BUFFER_ROWS) {
            throw new IllegalStateException(
                s"gbx_rst_bng_rasterize_agg buffer exceeded $MAX_BUFFER_ROWS rows " +
                s"(current = $currentRows). Reduce the group size or tile the workload.")
        }
    }
}

/** UDAF: `gbx_rst_bng_rasterize_agg(cellid, value, srid, pixel_size, xmin, ymin,
 *  xmax, ymax, width, height, mode, kring_pad[, out_crs])`.
 *
 *  Streams `(cellid STRING, value DOUBLE)` per row; the remaining ten arguments are
 *  per-group constants (Literal or constant expressions). The BNG `cellid` is a STRING
 *  (e.g. `"SW123987"`) parsed to its internal Long id via [[BNG.parse]] on `update`. On
 *  `eval` the cells are burned into one raster by pixel-centroid mapping -- for each
 *  output pixel center we compute its coordinate (the same affine
 *  [[com.databricks.labs.gbx.rasterx.expressions.grid.RST_BNG_RasterToGrid]] uses), index
 *  it to a BNG cell via [[BNG.pointToCellID]], and write the cell's value if present. This
 *  is the inverse of `RST_BNG_RasterToGrid` and matches the lightweight tier
 *  (`pyrx.core.cellraster`).
 *
 *  '''27700-native (the BNG divergence).''' BNG cell centroids/boundaries
 *  ([[BNG.cellIdToCenter]] / [[BNG.cellIdToBoundary]]) are already EPSG:27700
 *  eastings/northings, and [[BNG.pointToCellID]] consumes EPSG:27700 directly. Unlike the
 *  H3/quadbin rasterizers there is therefore '''no''' reprojection hop to WGS84: the
 *  gridspec sample points, the pixel-centre burn coordinates, and the output raster CRS
 *  are all EPSG:27700. The raster is built with srid 27700 accordingly.
 *
 *  When an explicit extent (xmin..height) is absent, the snapped, lattice-aligned grid
 *  is derived from the cell set + `kring_pad` (port of `cellraster.compute_gridspec`).
 *  The default `pixel_size` is the cell's metre edge from [[BNG.getEdgeSize]] (1=100km ...
 *  6=1m), which is directly the projected metre resolution. NoData = -9999.0. A
 *  null/omitted `value` burns 1.0 (presence mask).
 *
 *  A cell id that repeats keeps its LARGEST value, whatever order the rows arrive in: the
 *  accumulated cells are sorted by `(cellId, value)` before the value lookup is built, so the
 *  last write for a cell is its maximum. A null `value` counts as 1.0 in that comparison.
 *  The lightweight tier follows the same rule.
 */
case class RST_BNG_RasterizeAgg(
    cellidExpr:    Expression,
    valueExpr:     Expression,
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
    outCrsExpr:    Expression = Literal.create(null, StringType),
    exprConfExpr:  Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset:   Int = 0
) extends TypedImperativeAggregate[BNGRasterizeAcc] {

    import RST_BNG_RasterizeAgg.{evalDoubleOpt, evalIntOpt, evalString, NoData}

    override lazy val deterministic: Boolean = true  // canonical fold order (see eval)
    override val nullable: Boolean = true
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def prettyName: String = RST_BNG_RasterizeAgg.name

    override def children: Seq[Expression] = Seq(
        cellidExpr, valueExpr, outSridExpr, pixelSizeExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthExpr, heightExpr, modeExpr, kringPadExpr,
        outCrsExpr, exprConfExpr
    )

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_BNG_RasterizeAgg =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11), nc(12), nc(13))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): BNGRasterizeAcc = BNGRasterizeAcc.empty

    /** Catalyst-facing update: extract STRING cellid + value from the row, delegate to typed helper.
     *
     *  The BNG `cellid` is a STRING (e.g. `"SW123987"`) parsed to its internal Long id via
     *  [[BNG.parse]]; the accumulator stays Long-keyed so the serde is unchanged.
     */
    override def update(buffer: BNGRasterizeAcc, input: InternalRow): BNGRasterizeAcc = {
        // out_crs is refused, not ignored: a value the engine never reads would be a silent defect.
        if (outCrsExpr.eval(input) != null) throw new IllegalArgumentException(RST_BNG_RasterizeAgg.OutCrsRefusal)
        val raw = cellidExpr.eval(input)
        if (raw == null) return buffer
        val cellId = raw match {
            case s: org.apache.spark.unsafe.types.UTF8String => BNG.parse(s.toString)
            case s: String => BNG.parse(s)
            case o => throw new IllegalArgumentException(
                s"${RST_BNG_RasterizeAgg.name}: cellid must be a BNG STRING id (e.g. 'SW123987'); " +
                s"got ${o.getClass.getName}")
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
                s"${RST_BNG_RasterizeAgg.name}: value must be numeric; got ${o.getClass.getName}")
        }
        update(buffer, cellId, v)
    }

    /** Direct typed update used by unit tests (cellId already parsed to the internal Long id). */
    def update(buffer: BNGRasterizeAcc, cellId: Long, v: Double): BNGRasterizeAcc =
        buffer.add(cellId, v)

    override def merge(buffer: BNGRasterizeAcc, input: BNGRasterizeAcc): BNGRasterizeAcc =
        buffer.merge(input)

    override def eval(buffer: BNGRasterizeAcc): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.cells.isEmpty) return null

        val empty = InternalRow.empty
        val pixelOpt = evalDoubleOpt(pixelSizeExpr, empty)
        val xminOpt  = evalDoubleOpt(xminExpr,  empty)
        val yminOpt  = evalDoubleOpt(yminExpr,  empty)
        val xmaxOpt  = evalDoubleOpt(xmaxExpr,  empty)
        val ymaxOpt  = evalDoubleOpt(ymaxExpr,  empty)
        val widthOpt = evalIntOpt(widthExpr,    empty)
        val heightOpt= evalIntOpt(heightExpr,   empty)
        val mode     = evalString(modeExpr,     empty, "mode", "centroids")
        val kringPad = evalIntOpt(kringPadExpr, empty).getOrElse(1)

        // BNG rasters are always EPSG:27700-native: cell centroids/boundaries and
        // pointToCellID all operate in 27700, so there is no reprojection hop. The
        // outSridExpr argument is retained for signature parity with the H3/quadbin UDAFs.
        val srid = BNG.crsID

        // Resolution from the cells (derived from each cell's Long id); error on mixed.
        val resolution = RST_BNG_RasterizeAgg.resolutionOf(buffer.cells.iterator.map(_._1))

        // Canonical fold order (see RasterizeBurn.canonicalLut): the last write for a repeated cell is its largest
        // value (a null value was already turned into 1.0), regardless of row-arrival order.
        val lut = RasterizeBurn.canonicalLut(buffer.cells.toSeq)

        // Resolve grid spec: explicit extent if fully supplied, else snapped grid.
        val explicit = xminOpt.isDefined && yminOpt.isDefined && xmaxOpt.isDefined &&
            ymaxOpt.isDefined && widthOpt.isDefined && heightOpt.isDefined
        val (xmin, ymin, xmax, ymax, width, height) =
            if (explicit) {
                (xminOpt.get, yminOpt.get, xmaxOpt.get, ymaxOpt.get, widthOpt.get, heightOpt.get)
            } else {
                RST_BNG_RasterizeAgg.computeGridspec(
                    buffer.cells.iterator.map(_._1), pixelOpt, mode, kringPad, resolution)
            }

        // 27700-native output raster; srid == BNG.crsSrid so no per-pixel reprojection.
        val rasterDs = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, width, height, srid)
        try {
            RasterizeBurn.burn(BNG, lut, srid, resolution, rasterDs, width, height)
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

    override def serialize(obj: BNGRasterizeAcc): Array[Byte] = obj.serialize

    override def deserialize(bytes: Array[Byte]): BNGRasterizeAcc = BNGRasterizeAcc.deserialize(bytes)
}

object RST_BNG_RasterizeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_bng_rasterize_agg"

    /** NoData fill value, matching the lightweight tier (`cellraster._NODATA`). */
    val NoData: Double = -9999.0

    /** The refusal of a non-NULL `out_crs`. BNG output is always EPSG:27700; the light tier refuses the argument with
      * the same text (`pyrx.functions.BNG_OUT_CRS_REFUSAL`), so the two tiers agree. */
    val OutCrsRefusal: String = "gbx_rst_bng_rasterize_agg: out_crs is not supported; the output is always " +
        "EPSG:27700. Fix: pass NULL for out_crs, then reproject the tile with gbx_rst_transformcrs."

    /** 12 arguments, or 13 with the trailing `out_crs`, which must be NULL: a non-NULL value is refused. */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 12 => RST_BNG_RasterizeAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11))
        case 13 =>
            c(12) match {
                case Literal(v, _) if v != null => throw new IllegalArgumentException(OutCrsRefusal)
                case _ =>
            }
            RST_BNG_RasterizeAgg(
                c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12))
        case n => throw new IllegalArgumentException(
            s"$name expects 12 or 13 arguments " +
            s"(cellid, value, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad[, out_crs]); got $n")
    }

    /** Resolution of a cell set (derived from each cell's Long id); throws on a mixed-resolution set. */
    private[agg] def resolutionOf(cellIds: Iterator[Long]): Int = RasterizeBurn.bngResolutionOf(cellIds)

    /** Snapped, lattice-aligned grid spec from a cell set; port of
     *  `cellraster.compute_gridspec` (+ `snap_bounds`).
     *
     *  BNG sample points are already EPSG:27700, so there is no reprojection hop.
     *
     *  Returns `(xmin, ymin, xmax, ymax, width, height)`.
     */
    private[agg] def computeGridspec(
        cellIds: Iterator[Long],
        pixelSizeOpt: Option[Double],
        mode: String,
        kringPad: Int,
        resolution: Int
    ): (Double, Double, Double, Double, Int, Int) = {
        // Dedup + optional k-ring padding.
        val base = cellIds.toSet
        val cells =
            if (kringPad > 0) base.flatMap(c => BNG.kRing(c, kringPad).toSet)
            else base

        // Collect EPSG:27700 easting/northing sample points per mode
        // (BNG.cellIdToCenter / cellIdToBoundary return Coordinate(x=easting, y=northing)).
        val xs = ArrayBuffer.empty[Double]
        val ys = ArrayBuffer.empty[Double]
        mode match {
            case "centroids" =>
                cells.foreach { c =>
                    val ctr = BNG.cellIdToCenter(c)  // Coordinate(easting, northing)
                    xs += ctr.x; ys += ctr.y
                }
            case "spatial_envelope" =>
                cells.foreach { c =>
                    BNG.cellIdToBoundary(c).foreach { b => xs += b.x; ys += b.y }
                }
            case other =>
                throw new IllegalArgumentException(s"$name: unknown mode '$other'")
        }

        val bxmin = xs.min; val bxmax = xs.max
        val bymin = ys.min; val bymax = ys.max

        // Default pixel size is the cell's projected metre edge (1=100000m ... 6=1m).
        val pixelSize = pixelSizeOpt.getOrElse(BNG.getEdgeSize(resolution).toDouble)

        // snap_bounds: outward snap to the pixel_size lattice.
        val xmin = math.floor(bxmin / pixelSize) * pixelSize
        val ymax = math.ceil(bymax / pixelSize) * pixelSize
        val width  = math.max(1, math.ceil((bxmax - xmin) / pixelSize).toInt)
        val height = math.max(1, math.ceil((ymax - bymin) / pixelSize).toInt)
        val xmax = xmin + width * pixelSize
        val ymin = ymax - height * pixelSize
        (xmin, ymin, xmax, ymax, width, height)
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
