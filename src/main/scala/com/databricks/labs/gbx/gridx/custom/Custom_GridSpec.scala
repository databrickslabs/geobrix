package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/** Shared schema and decoder for the grid-spec STRUCT produced by gbx_custom_grid
  * and consumed by all gbx_custom_* operations.
  */
object Custom_GridSpec {

    /** Schema of the grid-spec struct produced by gbx_custom_grid, consumed by all gbx_custom_* ops. */
    val gridStructType: StructType = StructType(Seq(
        StructField("bound_x_min",      LongType,    nullable = false),
        StructField("bound_x_max",      LongType,    nullable = false),
        StructField("bound_y_min",      LongType,    nullable = false),
        StructField("bound_y_max",      LongType,    nullable = false),
        StructField("cell_splits",      IntegerType, nullable = false),
        StructField("root_cell_size_x", IntegerType, nullable = false),
        StructField("root_cell_size_y", IntegerType, nullable = false),
        StructField("srid",             IntegerType, nullable = false)  // -1 == no CRS
    ))

    /** Reconstruct a [[CustomGridSystem]] from a grid-spec InternalRow. */
    def systemFromRow(row: InternalRow): CustomGridSystem = {
        require(row != null, "gbx_custom: grid spec must not be null")
        val srid = row.getInt(7)
        CustomGridSystem(GridConf(
            boundXMin    = row.getLong(0),
            boundXMax    = row.getLong(1),
            boundYMin    = row.getLong(2),
            boundYMax    = row.getLong(3),
            cellSplits   = row.getInt(4),
            rootCellSizeX = row.getInt(5),
            rootCellSizeY = row.getInt(6),
            crsID        = if (srid < 0) None else Some(srid)
        ))
    }

    /** Int-or-Long tolerant (PySpark sends Long for integer literals). */
    def asInt(v: Any, label: String): Int = v match {
        case i: Int  => i
        case l: Long => l.toInt
        case null    => throw new IllegalArgumentException(s"gbx_custom: $label must not be null")
        case o       => throw new IllegalArgumentException(
            s"gbx_custom: $label must be INT or LONG; got ${o.getClass.getName}")
    }

    /** A resolution argument: any whole number, as a number or as its text (`3`, `3.0`, `' 3 '`). A fractional
      * value or text that is not a number raises naming it, so it is never cut to a whole number. */
    def asResolution(v: Any, label: String): Int = {
        def whole(d: Double): Int =
            if (d.isNaN || d.isInfinite || d != Math.rint(d) || d > Int.MaxValue || d < Int.MinValue)
                throw new IllegalArgumentException(s"gbx_custom: $label must be a whole number; got $v")
            else d.toInt
        v match {
            case i: Int                      => i
            case l: Long                     => Math.toIntExact(l)
            case s: Short                    => s.toInt
            case b: Byte                     => b.toInt
            case d: Double                   => whole(d)
            case f: Float                    => whole(f.toDouble)
            case d: org.apache.spark.sql.types.Decimal => whole(d.toDouble)
            case u: org.apache.spark.unsafe.types.UTF8String =>
                whole(scala.util.Try(u.toString.trim.toDouble).getOrElse(Double.NaN))
            case null => throw new IllegalArgumentException(s"gbx_custom: $label must not be null")
            case o    => throw new IllegalArgumentException(
                s"gbx_custom: $label must be a whole number; got ${o.getClass.getName}")
        }
    }

    /** Long-or-Int tolerant (bounds). */
    def asLong(v: Any, label: String): Long = v match {
        case l: Long => l
        case i: Int  => i.toLong
        case null    => throw new IllegalArgumentException(s"gbx_custom: $label must not be null")
        case o       => throw new IllegalArgumentException(
            s"gbx_custom: $label must be INT or LONG; got ${o.getClass.getName}")
    }

}
