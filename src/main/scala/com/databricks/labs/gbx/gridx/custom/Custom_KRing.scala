package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

/** Catalyst expression: returns the k-ring of custom-grid cell IDs around the given center cell.
  *
  * The k-ring at distance k includes all cells whose grid position differs from the center
  * cell by at most k steps in both X and Y (Chebyshev / square neighborhood), clamped to
  * the grid boundary.
  *
  * Arguments: cellExpr (BIGINT cell ID), gridExpr (grid-spec STRUCT), kExpr (INT or LONG).
  *
  * Returns: ARRAY<BIGINT> of cell IDs (including the center cell itself).
  */
case class Custom_KRing(
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
        val k   = Custom_GridSpec.asInt(kExpr.eval(input), "k")                    // PARAMETER
        GridErrorHandler.safeEval[ArrayData](null) {
            val cell = cellVal.asInstanceOf[Long]
            val cells: Seq[Long] = sys.kRing(cell, k)
            ArrayData.toArrayData(cells.toArray)
        }
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_custom_kring, 3-arg builder. */
object Custom_KRing extends WithExpressionInfo {

    override def name: String = "gbx_custom_kring"

    override def builder(): FunctionBuilder = {
        case c if c.length == 3 => Custom_KRing(c(0), c(1), c(2))
        case c => throw new IllegalArgumentException(
            s"gbx_custom_kring requires 3 arguments; got ${c.length}")
    }

}
