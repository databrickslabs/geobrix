package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.gdal.gdal.gdal

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Vector => JVector}
import scala.collection.mutable.ArrayBuffer

/** Mutable aggregation buffer for [[RST_RasterizeAgg]].
 *
 *  Accumulates `(geom_wkb, value)` pairs streamed one per row.  Serde format:
 *  `[count:Int][ wkbLen:Int, wkb:Bytes, value:Double ]*N`.
 */
final class RasterizeAcc(
    val features: ArrayBuffer[(Array[Byte], Double)] = ArrayBuffer.empty,
    private var byteSize: Long = 0L
) {

    def add(wkb: Array[Byte], v: Double): RasterizeAcc = {
        if (wkb != null && wkb.length > 0) {
            features += ((wkb, v))
            byteSize += wkb.length.toLong
            RasterizeAcc.guardSize(byteSize)
        }
        this
    }

    def merge(other: RasterizeAcc): RasterizeAcc = {
        features ++= other.features
        byteSize  += other.byteSize
        RasterizeAcc.guardSize(byteSize)
        this
    }

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(features.length)
        for ((wkb, v) <- features) {
            out.writeInt(wkb.length)
            out.write(wkb)
            out.writeDouble(v)
        }
        bos.toByteArray
    }
}

object RasterizeAcc {

    /** Hard cap on accumulated WKB bytes per buffer. */
    val MAX_BUFFER_BYTES: Long = 200L * 1024L * 1024L

    def empty: RasterizeAcc = new RasterizeAcc()

    def deserialize(bytes: Array[Byte]): RasterizeAcc = {
        val in  = new DataInputStream(new ByteArrayInputStream(bytes))
        val n   = in.readInt()
        val buf = ArrayBuffer.empty[(Array[Byte], Double)]
        var total = 0L
        var i = 0
        while (i < n) {
            val len = in.readInt()
            val wkb = new Array[Byte](len)
            if (len > 0) in.readFully(wkb)
            val v = in.readDouble()
            buf += ((wkb, v))
            total += len.toLong
            i += 1
        }
        new RasterizeAcc(buf, total)
    }

    private[agg] def guardSize(currentBytes: Long): Unit = {
        if (currentBytes > MAX_BUFFER_BYTES) {
            throw new IllegalStateException(
                s"gbx_rst_rasterize_agg buffer exceeded ${MAX_BUFFER_BYTES / (1024 * 1024)} MiB " +
                s"(current = ${currentBytes / (1024 * 1024)} MiB). Reduce the group size or tile the workload.")
        }
    }
}

/** UDAF: `gbx_rst_rasterize_agg(geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, srid)`.
 *
 *  Streams `(geom_wkb BINARY, value DOUBLE)` per row; the remaining seven
 *  arguments are per-group constants (Literal or constant expressions).  On
 *  `eval` all accumulated features are burned into one raster via
 *  [[VectorRasterBridge]] -- identical to [[RST_Rasterize.execute]] except
 *  that the OGR layer carries all features rather than just one.
 *
 *  Overlap is last-wins.  A Spark `groupBy().agg()` does not guarantee the order
 *  features reach the aggregator, so before burning we sort the accumulated
 *  features into a canonical order by `(geom_wkb, value)` -- a stable key
 *  intrinsic to each feature.  The overlap winner is therefore deterministic and
 *  identical regardless of row-arrival order (and matches the lightweight tier,
 *  which sorts on the same key).
 */
case class RST_RasterizeAgg(
    geomWkbExpr:  Expression,
    valueExpr:    Expression,
    xminExpr:     Expression,
    yminExpr:     Expression,
    xmaxExpr:     Expression,
    ymaxExpr:     Expression,
    widthPxExpr:  Expression,
    heightPxExpr: Expression,
    sridExpr:     Expression,
    exprConfExpr: Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset:   Int = 0
) extends TypedImperativeAggregate[RasterizeAcc] {

    import RST_RasterizeAgg.{evalDouble, evalInt}

    override lazy val deterministic: Boolean = true  // canonical fold order (see eval)
    override val nullable: Boolean = true
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def prettyName: String = RST_RasterizeAgg.name

    override def children: Seq[Expression] = Seq(
        geomWkbExpr, valueExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, sridExpr,
        exprConfExpr
    )

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_RasterizeAgg =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): RasterizeAcc = RasterizeAcc.empty

    /** Catalyst-facing update: extract geom_wkb and value from the row, delegate to typed helper. */
    override def update(buffer: RasterizeAcc, input: InternalRow): RasterizeAcc = {
        val raw = geomWkbExpr.eval(input)
        if (raw == null) return buffer
        val wkb = raw.asInstanceOf[Array[Byte]]
        val vRaw = valueExpr.eval(input)
        if (vRaw == null) return buffer
        val v = vRaw.asInstanceOf[Double]
        update(buffer, wkb, v)
    }

    /** Direct typed update used by unit tests. */
    def update(buffer: RasterizeAcc, wkb: Array[Byte], v: Double): RasterizeAcc =
        buffer.add(wkb, v)

    override def merge(buffer: RasterizeAcc, input: RasterizeAcc): RasterizeAcc =
        buffer.merge(input)

    override def eval(buffer: RasterizeAcc): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.features.isEmpty) return null

        val empty = InternalRow.empty
        val xmin     = evalDouble(xminExpr,     empty, "xmin")
        val ymin     = evalDouble(yminExpr,     empty, "ymin")
        val xmax     = evalDouble(xmaxExpr,     empty, "xmax")
        val ymax     = evalDouble(ymaxExpr,     empty, "ymax")
        val widthPx  = evalInt(widthPxExpr,     empty, "width_px")
        val heightPx = evalInt(heightPxExpr,    empty, "height_px")
        val srid     = evalInt(sridExpr,        empty, "srid")

        // Canonical fold order: sort by (geom_wkb lexicographic, value) so the
        // last-wins overlap winner is deterministic regardless of the order rows
        // arrived from the groupBy, and matches the lightweight tier.
        val ordered = buffer.features.toSeq.sortWith { (a, b) =>
            val c = RST_RasterizeAgg.compareBytes(a._1, b._1)
            if (c != 0) c < 0 else a._2 < b._2
        }
        val (ogrDs, layer) = VectorRasterBridge.buildOgrLayer(ordered, srid)
        val rasterDs = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, widthPx, heightPx, srid)
        try {
            val bands      = Array(1)
            val burnValues = Array(0.0) // ignored; ATTRIBUTE option overrides
            val options    = new JVector[String]()
            options.add(s"ATTRIBUTE=${VectorRasterBridge.ValueFieldName}")
            gdal.RasterizeLayer(rasterDs, bands, layer, burnValues, options)
            rasterDs.FlushCache()
            val bytes = VectorRasterBridge.toGTiffBytes(rasterDs)
            val mtd = Map(
                "driver"     -> "GTiff",
                "extension"  -> "tif",
                "size"       -> bytes.length.toString,
                "parentPath" -> "",
                "all_parents"-> ""
            )
            val mapData = SerializationUtil.toMapData[String, String](mtd)
            InternalRow.fromSeq(Seq(0L, bytes, null, null, null, null, null, mapData))
        } finally {
            rasterDs.delete()
            ogrDs.delete()
        }
    }

    override def serialize(obj: RasterizeAcc): Array[Byte] = obj.serialize

    override def deserialize(bytes: Array[Byte]): RasterizeAcc = RasterizeAcc.deserialize(bytes)
}

object RST_RasterizeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_rasterize_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 9 => RST_RasterizeAgg(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8))
        case n => throw new IllegalArgumentException(
            s"$name expects 9 arguments " +
            s"(geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, srid); got $n")
    }

    /** Unsigned lexicographic comparison of two byte arrays (stable canonical key). */
    private[agg] def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
        val n = math.min(a.length, b.length)
        var i = 0
        while (i < n) {
            val ai = a(i) & 0xff
            val bi = b(i) & 0xff
            if (ai != bi) return ai - bi
            i += 1
        }
        a.length - b.length
    }

    private[agg] def evalDouble(e: Expression, row: InternalRow, label: String): Double =
        e.eval(row) match {
            case null               => throw new IllegalArgumentException(s"$name: $label must not be null")
            case d: Double          => d
            case f: Float           => f.toDouble
            case i: Int             => i.toDouble
            case l: Long            => l.toDouble
            case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
            case o => throw new IllegalArgumentException(
                s"$name: $label must be numeric; got ${o.getClass.getName}")
        }

    private[agg] def evalInt(e: Expression, row: InternalRow, label: String): Int =
        e.eval(row) match {
            case null   => throw new IllegalArgumentException(s"$name: $label must not be null")
            case i: Int => i
            case l: Long => l.toInt
            case o => throw new IllegalArgumentException(
                s"$name: $label must be INT or LONG; got ${o.getClass.getName}")
        }
}
