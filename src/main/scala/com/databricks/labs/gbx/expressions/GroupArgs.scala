package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnsafeProjection, UnsafeRow}

import java.io.{DataInputStream, DataOutputStream}

/** The per-group arguments of an aggregate (a grid, an extent, a mode), read from a row of the group.
  *
  * An aggregate's `eval` sees no input row, so an argument evaluated there must be a constant: a column (a grid
  * struct in a table, say) would be read from an empty row and fail. `capture` keeps the arguments' values of one
  * input row as UnsafeRow bytes, which travel in the aggregation buffer through a shuffle, and `bind` reads each
  * one from them as the argument expression would. The lightweight forms read these arguments from the first row
  * of the group, so a group whose rows carry one value gives the same answer at both levels.
  */
final class GroupArgs(exprs: Seq[Expression]) extends Serializable {

    @transient private lazy val projection: UnsafeProjection = UnsafeProjection.create(exprs)

    private val refs: IndexedSeq[Expression] =
        exprs.zipWithIndex.map { case (e, i) => BoundReference(i, e.dataType, nullable = true) }.toIndexedSeq

    /** The arguments' values in `input`, as bytes the buffer can hold (a copy: the projection reuses its row). */
    def capture(input: InternalRow): Array[Byte] = projection(input).copy().getBytes

    /** The arguments and the row to evaluate them on: each captured value, or, when none was captured (a buffer
      * filled by a typed update in a test), the argument expressions themselves on the empty row (constants only). */
    def bind(bytes: Array[Byte]): (IndexedSeq[Expression], InternalRow) =
        if (bytes == null) (exprs.toIndexedSeq, InternalRow.empty)
        else {
            val r = new UnsafeRow(exprs.length)
            r.pointTo(bytes, bytes.length)
            (refs, r)
        }

}

object GroupArgs {

    /** Whether captured arguments are missing or hold a NULL first argument (the one that decides, a grid). */
    def lacksFirst(bytes: Array[Byte]): Boolean =
        bytes == null || {
            val r = new UnsafeRow(1) // the null bit of the first field is the first bit of the row
            r.pointTo(bytes, bytes.length)
            r.isNullAt(0)
        }

    /** The arguments a buffer keeps: its own, unless they lack the first argument and the other's hold it. */
    def prefer(mine: Array[Byte], other: Array[Byte]): Array[Byte] =
        if (lacksFirst(mine) && !lacksFirst(other)) other else if (mine == null) other else mine

    /** Write captured arguments after a buffer's own fields; nothing when none were captured. */
    def write(out: DataOutputStream, bytes: Array[Byte]): Unit =
        if (bytes != null) {
            out.writeInt(bytes.length)
            out.write(bytes)
        }

    /** Read what [[write]] wrote, or null at the end of the stream. */
    def read(in: DataInputStream): Array[Byte] =
        if (in.available() <= 0) null
        else {
            val b = new Array[Byte](in.readInt())
            in.readFully(b)
            b
        }

}
