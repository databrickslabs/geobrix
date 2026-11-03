package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.{CellFill, CellFillAcc}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** UDAF: `gbx_bng_cellfill(cellid, value, [k], [method], [power])`.
 *
 *  Grouped aggregator over a BNG `(cellid, value)` grid result: fills each NULL
 *  (covered-but-missing) cell from valid neighbours within `k` rings via [[CellFill.fill]],
 *  returning `array<struct<cellid, value>>`. BNG cell ids surface as formatted STRINGS,
 *  so input cellids are parsed to the internal Long via [[BNG.parseOrNull]] and rendered
 *  back to the BNG string on output (via `renderCellId`).
 */
case class BNG_CellFill(
    cellidExpr: Expression,
    valueExpr:  Expression,
    kExpr:      Expression,
    methodExpr: Expression,
    powerExpr:  Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset:   Int = 0
) extends TypedImperativeAggregate[CellFillAcc] {

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = true
    override lazy val dataType: DataType = BinaryType  // the fill payload: the one contract of this name
    override def prettyName: String = BNG_CellFill.name

    override def children: Seq[Expression] = Seq(cellidExpr, valueExpr, kExpr, methodExpr, powerExpr)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): BNG_CellFill =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): CellFillAcc = CellFillAcc.empty

    override def update(buffer: CellFillAcc, input: InternalRow): CellFillAcc = {
        val id: java.lang.Long = cellidExpr.eval(input) match {
            case null          => null
            case s: UTF8String => BNG.parseOrNull(s.toString)
            case s: String     => BNG.parseOrNull(s)
            case l: Long       => l
            case i: Int        => i.toLong
            case o => throw new IllegalArgumentException(
                s"${BNG_CellFill.name}: cellid must be a BNG STRING or LONG; got ${o.getClass.getName}")
        }
        if (id == null) buffer else buffer.add(id, CellFill.parseValue(valueExpr.eval(input)))
    }

    /** Direct typed update used by unit tests. */
    def update(buffer: CellFillAcc, cellId: Long, v: Option[Double]): CellFillAcc = buffer.add(cellId, v)

    override def merge(buffer: CellFillAcc, input: CellFillAcc): CellFillAcc = buffer.merge(input)

    override def eval(buffer: CellFillAcc): Any = {
        if (buffer.cells.isEmpty) return null
        val row    = InternalRow.empty
        val k      = CellFill.parseK(kExpr, row)
        val method = CellFill.parseMethod(methodExpr, row)
        val power  = CellFill.parsePower(powerExpr, row)
        CellFill.toBinary(CellFill.fill(BNG, CellFill.largestPerCell(buffer.cells), k, method, power))
    }

    override def serialize(obj: CellFillAcc): Array[Byte] = obj.serialize
    override def deserialize(bytes: Array[Byte]): CellFillAcc = CellFillAcc.deserialize(bytes)
}

/** Companion: SQL name gbx_bng_cellfill, builder (arities 2..5). */
object BNG_CellFill extends WithExpressionInfo {

    override def name: String = "gbx_bng_cellfill"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => BNG_CellFill(c(0), c(1), Literal(1), Literal("mean"), Literal(2.0))
        case 3 => BNG_CellFill(c(0), c(1), c(2),       Literal("mean"), Literal(2.0))
        case 4 => BNG_CellFill(c(0), c(1), c(2),       c(3),            Literal(2.0))
        case 5 => BNG_CellFill(c(0), c(1), c(2),       c(3),            c(4))
        case n => throw new IllegalArgumentException(
            s"$name expects 2 to 5 arguments (cellid, value, [k], [method], [power]); got $n")
    }
}
