package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

/** Catalyst expression: returns the k-loop (hollow ring) of custom-grid cell IDs around
  * the given center cell at exactly distance k.
  *
  * The k-loop at distance k includes all cells whose grid position differs from the center
  * cell by exactly k steps in either X or Y (i.e., kRing(k) minus kRing(k-1)), clamped to
  * the grid boundary. k=0 returns an array containing only the center cell.
  *
  * Arguments: cellExpr (BIGINT cell ID), gridExpr (grid-spec STRUCT), kExpr (INT or LONG).
  *
  * Returns: ARRAY<BIGINT> of cell IDs (NOT including the center cell for k>=1).
  */
case class Custom_KLoop(
    cellExpr: Expression,
    gridExpr: Expression,
    kExpr:    Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] = Seq(cellExpr, gridExpr, kExpr)
    override def dataType: DataType        = ArrayType(LongType, containsNull = false)
    override def nullable: Boolean         = true
    override def foldable: Boolean         = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val cellVal = cellExpr.eval(input)
        if (cellVal == null) return null

        val gridVal = gridExpr.eval(input)
        if (gridVal == null) return null

        val sys = Custom_GridSpec.systemFromRow(gridVal.asInstanceOf[InternalRow])  // PARAMETER
        val kVal = kExpr.eval(input)
        if (kVal == null) return null
        val k   = Custom_GridSpec.asInt(kVal, "k")                                  // PARAMETER
        GridErrorHandler.safeEval[ArrayData](null) {
            val cell = cellVal.asInstanceOf[Long]
            val cells: Seq[Long] = sys.kLoop(cell, k)
            ArrayData.toArrayData(cells.toArray)
        }
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_custom_kloop, 3-arg builder. */
object Custom_KLoop extends WithExpressionInfo {

    override def name: String = "gbx_custom_kloop"

    override def builder(): FunctionBuilder = {
        case c if c.length == 3 => Custom_KLoop(c(0), c(1), c(2))
        case c => throw new IllegalArgumentException(
            s"gbx_custom_kloop requires 3 arguments; got ${c.length}")
    }

}
