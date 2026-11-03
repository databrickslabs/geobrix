package com.databricks.labs.gbx.gridx

import com.databricks.labs.gbx.gridx.grid.GridSystem
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/**
 * Mutable aggregation buffer for the `gbx_<grid>_cellfill` grouped aggregators.
 *
 *  Accumulates `(cellId: Long, value: Option[Double])` pairs streamed one per row; a NULL
 *  input value is stored as `None` (covered-but-missing) and later filled from neighbours.
 *  Serde format: `[count:Int]( cellId:Long, present:Boolean, [value:Double] )*N`.
 */
final class CellFillAcc(
    val cells: ArrayBuffer[(Long, Option[Double])] = ArrayBuffer.empty
) {

    def add(cellId: Long, v: Option[Double]): CellFillAcc = {
        cells += ((cellId, v))
        CellFillAcc.guardSize(cells.length.toLong)
        this
    }

    def merge(other: CellFillAcc): CellFillAcc = {
        cells ++= other.cells
        CellFillAcc.guardSize(cells.length.toLong)
        this
    }

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(cells.length)
        for ((cellId, v) <- cells) {
            out.writeLong(cellId)
            out.writeBoolean(v.isDefined)
            if (v.isDefined) out.writeDouble(v.get)
        }
        bos.toByteArray
    }
}

object CellFillAcc {

    /** Hard cap on accumulated rows per buffer. */
    val MAX_BUFFER_ROWS: Long = 50L * 1000L * 1000L

    def empty: CellFillAcc = new CellFillAcc()

    def deserialize(bytes: Array[Byte]): CellFillAcc = {
        val in  = new DataInputStream(new ByteArrayInputStream(bytes))
        val n   = in.readInt()
        val buf = ArrayBuffer.empty[(Long, Option[Double])]
        var i = 0
        while (i < n) {
            val cellId  = in.readLong()
            val present = in.readBoolean()
            val v = if (present) Some(in.readDouble()) else None
            buf += ((cellId, v))
            i += 1
        }
        new CellFillAcc(buf)
    }

    private[gridx] def guardSize(currentRows: Long): Unit = {
        if (currentRows > MAX_BUFFER_ROWS) {
            throw new IllegalStateException(
                s"gbx_<grid>_cellfill buffer exceeded $MAX_BUFFER_ROWS rows " +
                s"(current = $currentRows). Reduce the group size or tile the workload.")
        }
    }
}

/**
 * Grid-generic cell-space fill core, shared by the four `gbx_<grid>_cellfill` grouped
 * aggregators. Uses only [[GridSystem.kLoop]], so a single implementation serves H3,
 * Quadbin, BNG, and Custom grids.
 *
 *  Semantics: given a group's `(cellid, value)` map, every valid (non-NULL) cell passes
 *  through unchanged; a NULL cell (covered-but-missing) is filled from valid neighbour
 *  cells within `k` rings — gathered ring-by-ring `d = 1..k` via `kLoop(cell, d)`, where
 *  the ring index `d` is the IDW distance. A NULL cell with no valid neighbour within
 *  `k` rings stays NULL.
 *
 *  - `mean`: unweighted mean of all gathered valid neighbour values within `k` rings.
 *  - `idw` : `Σ(v · d^-power) / Σ(d^-power)` over gathered neighbours, `d` = ring index.
 *    At `k = 1` all neighbours have `d = 1`, so `idw` degenerates to `mean`.
 */
object CellFill {

    /**
     * Fill NULL cells in `cells` from valid neighbours within `k` rings.
     *
     *  @param grid   grid system whose `kLoop` defines neighbour rings
     *  @param cells  the group's cell -> optional value map (`None` = covered-but-missing)
     *  @param k      neighbourhood radius in rings (>= 0); `k = 0` performs no fill
     *  @param method `"mean"` or `"idw"`
     *  @param power  IDW distance exponent (used by `idw` only)
     *  @return one `(cellId, value)` per input cell, in ascending unsigned cell-id order
     */
    def fill(
        grid: GridSystem,
        cells: Map[Long, Option[Double]],
        k: Int,
        method: String,
        power: Double
    ): Seq[(Long, Option[Double])] = {
        require(k >= 0, s"cellfill: k must be >= 0; got $k")
        val m = method.toLowerCase
        require(m == "mean" || m == "idw", s"cellfill: method must be 'mean' or 'idw'; got '$method'")

        // Deterministic output order: ascending unsigned cell id.
        val ordered = cells.keys.toSeq.sortWith((a, b) => java.lang.Long.compareUnsigned(a, b) < 0)

        ordered.map { id =>
            cells(id) match {
                case some @ Some(_) => (id, some) // valid cell passes through unchanged
                case None =>
                    // Gather (value, ringDistance) for valid neighbours in rings 1..k.
                    val gathered = ArrayBuffer.empty[(Double, Int)]
                    var d = 1
                    while (d <= k) {
                        grid.kLoop(id, d).foreach { nb =>
                            cells.get(nb).flatten.foreach(v => gathered += ((v, d)))
                        }
                        d += 1
                    }
                    if (gathered.isEmpty) {
                        (id, None) // covered-but-missing with no valid neighbour within k rings
                    } else if (m == "mean") {
                        (id, Some(gathered.iterator.map(_._1).sum / gathered.length))
                    } else {
                        var num = 0.0
                        var den = 0.0
                        gathered.foreach { case (v, dist) =>
                            val w = math.pow(dist.toDouble, -power)
                            num += v * w
                            den += w
                        }
                        (id, Some(num / den))
                    }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Shared plumbing for the four TypedImperativeAggregate wrappers.
    // --------------------------------------------------------------------------------------------

    /** Output element struct type: `struct<cellid, value>` (value nullable). */
    def elementType(cellIdType: DataType): StructType = StructType(Seq(
        StructField("cellid", cellIdType, nullable = false),
        StructField("value",  DoubleType, nullable = true)
    ))

    /** Output array type: `array<struct<cellid, value>>`. */
    def arrayType(cellIdType: DataType): ArrayType = ArrayType(elementType(cellIdType), containsNull = false)

    /** Build the `array<struct<cellid, value>>` output, rendering cell ids via the grid. */
    def toArrayData(grid: GridSystem, filled: Seq[(Long, Option[Double])]): ArrayData = {
        val rows = filled.map { case (id, v) =>
            val cid: Any = grid.renderCellId(id) match {
                case s: String     => UTF8String.fromString(s)
                case u: UTF8String => u
                case other         => other
            }
            val value: Any = v.map(d => d: Any).orNull
            InternalRow(cid, value)
        }.toArray[Any]
        ArrayData.toArrayData(rows)
    }

    /** One value per distinct cell: a cell id that repeats keeps its LARGEST non-null value, whatever order the rows
     *  arrive in (the lightweight rule); `None` (covered but missing) survives only for a cell whose every value is `None`. */
    def largestPerCell(pairs: scala.collection.Seq[(Long, Option[Double])]): Map[Long, Option[Double]] =
        pairs.foldLeft(Map.empty[Long, Option[Double]]) { case (acc, (id, v)) =>
            acc.get(id) match {
                case None                                  => acc.updated(id, v)
                case Some(cur) if v.isDefined && (cur.isEmpty || v.get > cur.get) => acc.updated(id, v)
                case _                                     => acc
            }
        }

    /** The fill as the BINARY payload of the session contract: `[count:Int]( cellId:Long, present:Boolean,
     *  [value:Double] )*N`, big-endian, cell ids as the grid's internal Long (the lightweight tier's `encode`). */
    def toBinary(filled: Seq[(Long, Option[Double])]): Array[Byte] =
        new CellFillAcc(ArrayBuffer(filled: _*)).serialize

    /** Parse a numeric cell id (H3/Quadbin/Custom) to Long; null -> null (row skipped). */
    def parseCellIdLong(raw: Any, fnName: String): java.lang.Long = raw match {
        case null    => null
        case l: Long => l
        case i: Int  => i.toLong
        case o => throw new IllegalArgumentException(
            s"$fnName: cellid must be LONG or INT; got ${o.getClass.getName}")
    }

    /** Parse an input value; null -> None (covered-but-missing), numeric -> Some(double). */
    def parseValue(raw: Any): Option[Double] = raw match {
        case null      => None
        case d: Double => Some(d)
        case f: Float  => Some(f.toDouble)
        case i: Int    => Some(i.toDouble)
        case l: Long   => Some(l.toDouble)
        case dec: org.apache.spark.sql.types.Decimal => Some(dec.toDouble)
        case o => throw new IllegalArgumentException(
            s"cellfill: value must be numeric; got ${o.getClass.getName}")
    }

    /** k parameter; null -> 1 (default). */
    def parseK(e: Expression, row: InternalRow): Int = e.eval(row) match {
        case null    => 1
        case i: Int  => i
        case l: Long => l.toInt
        case o => throw new IllegalArgumentException(
            s"cellfill: k must be INT or LONG; got ${o.getClass.getName}")
    }

    /** method parameter; null -> "mean" (default). */
    def parseMethod(e: Expression, row: InternalRow): String = e.eval(row) match {
        case null          => "mean"
        case s: UTF8String => s.toString
        case s: String     => s
        case o => throw new IllegalArgumentException(
            s"cellfill: method must be STRING; got ${o.getClass.getName}")
    }

    /** power parameter; null -> 2.0 (default). */
    def parsePower(e: Expression, row: InternalRow): Double = e.eval(row) match {
        case null      => 2.0
        case d: Double => d
        case f: Float  => f.toDouble
        case i: Int    => i.toDouble
        case l: Long   => l.toDouble
        case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
        case o => throw new IllegalArgumentException(
            s"cellfill: power must be numeric; got ${o.getClass.getName}")
    }
}
