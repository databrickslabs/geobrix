package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.{CellFill, CellFillAcc}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._

/** UDAF: `gbx_custom_cellfill(cellid, value, grid, [k], [method], [power])`.
 *
 *  Grouped aggregator over a custom-grid `(cellid, value)` grid result: fills each NULL
 *  (covered-but-missing) cell from valid neighbours within `k` rings via [[CellFill.fill]],
 *  returning `array<struct<cellid, value>>`. Mirrors [[com.databricks.labs.gbx.gridx.h3.H3_CellFill]]
 *  with the custom `grid` struct (from `gbx_custom_grid(...)`) inserted after `(cellid, value)` —
 *  where the H3/Quadbin/BNG grid identity is implicit — matching the custom raster functions.
 */
case class Custom_CellFill(
    cellidExpr: Expression,
    valueExpr:  Expression,
    gridExpr:   Expression,
    kExpr:      Expression,
    methodExpr: Expression,
    powerExpr:  Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset:   Int = 0
) extends TypedImperativeAggregate[CellFillAcc] {

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = true
    override lazy val dataType: DataType = BinaryType  // the fill payload: the one contract of this name
    override def prettyName: String = Custom_CellFill.name

    override def children: Seq[Expression] = Seq(cellidExpr, valueExpr, gridExpr, kExpr, methodExpr, powerExpr)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Custom_CellFill =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): CellFillAcc = CellFillAcc.empty

    override def update(buffer: CellFillAcc, input: InternalRow): CellFillAcc = {
        val id = CellFill.parseCellIdLong(cellidExpr.eval(input), Custom_CellFill.name)
        if (id == null) buffer else buffer.add(id, CellFill.parseValue(valueExpr.eval(input)))
    }

    /** Direct typed update used by unit tests. */
    def update(buffer: CellFillAcc, cellId: Long, v: Option[Double]): CellFillAcc = buffer.add(cellId, v)

    override def merge(buffer: CellFillAcc, input: CellFillAcc): CellFillAcc = buffer.merge(input)

    override def eval(buffer: CellFillAcc): Any = {
        if (buffer.cells.isEmpty) return null
        val row    = InternalRow.empty
        val grid   = Custom_GridSpec.systemFromRow(gridExpr.eval(row).asInstanceOf[InternalRow])
        val k      = CellFill.parseK(kExpr, row)
        val method = CellFill.parseMethod(methodExpr, row)
        val power  = CellFill.parsePower(powerExpr, row)
        CellFill.toBinary(CellFill.fill(grid, CellFill.largestPerCell(buffer.cells), k, method, power))
    }

    override def serialize(obj: CellFillAcc): Array[Byte] = obj.serialize
    override def deserialize(bytes: Array[Byte]): CellFillAcc = CellFillAcc.deserialize(bytes)
}

/** Companion: SQL name gbx_custom_cellfill, builder (arities 3..6; grid after cellid/value). */
object Custom_CellFill extends WithExpressionInfo {

    override def name: String = "gbx_custom_cellfill"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => Custom_CellFill(c(0), c(1), c(2), Literal(1), Literal("mean"), Literal(2.0))
        case 4 => Custom_CellFill(c(0), c(1), c(2), c(3),       Literal("mean"), Literal(2.0))
        case 5 => Custom_CellFill(c(0), c(1), c(2), c(3),       c(4),            Literal(2.0))
        case 6 => Custom_CellFill(c(0), c(1), c(2), c(3),       c(4),            c(5))
        case n => throw new IllegalArgumentException(
            s"$name expects 3 to 6 arguments (cellid, value, grid, [k], [method], [power]); got $n")
    }
}
