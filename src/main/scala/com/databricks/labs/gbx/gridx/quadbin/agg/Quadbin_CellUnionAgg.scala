package com.databricks.labs.gbx.gridx.quadbin.agg

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.quadbin.Quadbin_CellUnion
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

/** Aggregate expression that streams one quadbin cell id (BIGINT) per row,
  * accumulates them, and on finalize calls Quadbin_CellUnion.execute to
  * produce a single MultiPolygon EWKB (SRID=4326).
  *
  * Parity with gbx_bng_cellunion_agg and Mosaic grid_cell_union_agg.
  */
final case class Quadbin_CellUnionAgg(
    cellid: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[QuadbinUnionAcc]
      with UnaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override val child: Expression = cellid
    override val nullable: Boolean = true
    override val dataType: DataType = BinaryType
    override def prettyName: String = Quadbin_CellUnionAgg.name

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)
    override protected def withNewChildInternal(newChild: Expression): Quadbin_CellUnionAgg =
        copy(cellid = newChild)

    override def createAggregationBuffer(): QuadbinUnionAcc = QuadbinUnionAcc.empty

    override def update(b: QuadbinUnionAcc, in: InternalRow): QuadbinUnionAcc = {
        val v = child.eval(in)
        if (v == null) return b
        b.add(v.asInstanceOf[Long])
    }

    override def merge(a: QuadbinUnionAcc, c: QuadbinUnionAcc): QuadbinUnionAcc = a.merge(c)

    override def eval(b: QuadbinUnionAcc): Any =
        Quadbin_CellUnion.execute(b.cells.toArray)

    override def serialize(b: QuadbinUnionAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): QuadbinUnionAcc = QuadbinUnionAcc.deserialize(bytes)

}

/** Companion: SQL name gbx_quadbin_cellunion_agg, builder. */
object Quadbin_CellUnionAgg extends WithExpressionInfo {

    override def name: String = "gbx_quadbin_cellunion_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => Quadbin_CellUnionAgg(c.head)
        case n => throw new IllegalArgumentException(
            s"$name takes exactly 1 argument (cell BIGINT); got $n"
        )
    }

}
