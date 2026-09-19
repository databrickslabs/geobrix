package com.databricks.labs.gbx.rasterx.expressions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/**
  * Mutable aggregation buffer for [[RST_BinPointsAgg]].
  *
  * Accumulates scalar `(x, y, z)` triples — one per row in the group.
  * Each triple is three `Double` values = 24 bytes of payload.  The buffer is
  * the working state of the `TypedImperativeAggregate` and is shipped between
  * executors during the merge phase via `serialize` / `deserialize`.
  *
  * A safety cap (default 50 MiB of payload) guards against runaway pipelines
  * that try to reduce millions of points per group; tile the workload by extent
  * when approaching this limit.
  */
final class BinPointsAcc(
    val points: ArrayBuffer[(Double, Double, Double)] = ArrayBuffer.empty,
    private var byteSize: Long = 0L
) extends Serializable {

    /** Payload bytes per `(x, y, z)` triple: 3 doubles × 8 bytes. */
    private val BYTES_PER_POINT = 24L

    def add(x: Double, y: Double, z: Double): BinPointsAcc = {
        points += ((x, y, z))
        byteSize += BYTES_PER_POINT
        BinPointsAcc.guardSize(byteSize)
        this
    }

    def merge(other: BinPointsAcc): BinPointsAcc = {
        points ++= other.points
        byteSize += other.byteSize
        BinPointsAcc.guardSize(byteSize)
        this
    }

    def approxByteSize: Long = byteSize

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(points.length)
        for ((x, y, z) <- points) {
            out.writeDouble(x)
            out.writeDouble(y)
            out.writeDouble(z)
        }
        bos.toByteArray
    }
}

object BinPointsAcc {

    /** Hard cap on the per-buffer payload byte count — guards memory blow-ups. */
    val MAX_BUFFER_BYTES: Long = 50L * 1024L * 1024L

    def empty: BinPointsAcc = new BinPointsAcc()

    def deserialize(bytes: Array[Byte]): BinPointsAcc = {
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val n = in.readInt()
        val buf = ArrayBuffer.empty[(Double, Double, Double)]
        var total: Long = 0L
        var i = 0
        while (i < n) {
            val x = in.readDouble()
            val y = in.readDouble()
            val z = in.readDouble()
            buf += ((x, y, z))
            total += 24L
            i += 1
        }
        new BinPointsAcc(buf, total)
    }

    private[expressions] def guardSize(currentBytes: Long): Unit = {
        if (currentBytes > MAX_BUFFER_BYTES) {
            throw new IllegalStateException(
                s"rst_binpoints_agg buffer exceeded ${MAX_BUFFER_BYTES / (1024 * 1024)} MiB " +
                s"(current = ${currentBytes / (1024 * 1024)} MiB). " +
                s"Tile the workload by extent to process fewer points per group."
            )
        }
    }
}
