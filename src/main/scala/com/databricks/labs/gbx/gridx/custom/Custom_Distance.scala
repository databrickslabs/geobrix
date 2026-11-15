package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, LongType}

/** Catalyst expression: returns the Chebyshev grid-ring distance between two custom-grid cells.
  *
  * Distance is defined as max(|dx|, |dy|) in cell-position units — the minimum k such that
  * the second cell appears in kRing(firstCell, k). Consistent with the kRing/kLoop ring semantics.
  * (from [[com.databricks.labs.gbx.gridx.grid.CustomGridSystem#distance]]).
  *
  * Arguments: cellid1 (BIGINT cell ID), gridExpr (grid-spec STRUCT), cellid2 (BIGINT cell ID).
  *
  * Returns: BIGINT Chebyshev grid distance.
  */
case class Custom_Distance(
    cellid1:  Expression,
    gridExpr: Expression,
    cellid2:  Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] = Seq(cellid1, gridExpr, cellid2)
    override def dataType: DataType        = LongType
    override def nullable: Boolean         = true
    override def foldable: Boolean         = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val cell1Val = cellid1.eval(input)
        if (cell1Val == null) return null

        val gridVal = gridExpr.eval(input)
        if (gridVal == null) return null

        val cell2Val = cellid2.eval(input)
        if (cell2Val == null) return null

        val sys = Custom_GridSpec.systemFromRow(gridVal.asInstanceOf[InternalRow])  // PARAMETER
        GridErrorHandler.safeEval[java.lang.Long](null) {
            sys.distance(Custom_GridSpec.asLong(cell1Val, "cellid1"), Custom_GridSpec.asLong(cell2Val, "cellid2"))
        }
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_custom_distance, 3-arg builder. */
object Custom_Distance extends WithExpressionInfo {

    override def name: String = "gbx_custom_distance"

    override def builder(): FunctionBuilder = {
        case c if c.length == 3 => Custom_Distance(c(0), c(1), c(2))
        case c => throw new IllegalArgumentException(
            s"gbx_custom_distance requires 3 arguments; got ${c.length}")
    }

}
