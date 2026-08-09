package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType}

/** Catalyst expression: given a Long cell ID and a grid-spec struct, returns the cell geometry as WKB.
  *
  * Arguments: cellExpr (LONG), gridExpr (STRUCT).
  */
case class Custom_AsWKB(
    cellExpr: Expression,
    gridExpr: Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] = Seq(cellExpr, gridExpr)
    override def dataType: DataType  = BinaryType
    override def nullable: Boolean   = true
    override def foldable: Boolean   = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val cellVal = cellExpr.eval(input)
        if (cellVal == null) return null

        val gridVal = gridExpr.eval(input)
        if (gridVal == null) return null

        val cell = cellVal.asInstanceOf[Long]
        val sys  = Custom_GridSpec.systemFromRow(gridVal.asInstanceOf[InternalRow])

        GridErrorHandler.safeEval[Array[Byte]](null)(JTS.toWKB(sys.cellIdToGeometry(cell)))
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_custom_cellaswkb, 2-arg builder. */
object Custom_AsWKB extends WithExpressionInfo {

    override def name: String = "gbx_custom_cellaswkb"

    override def builder(): FunctionBuilder = {
        case c if c.length == 2 => Custom_AsWKB(c(0), c(1))
        case c => throw new IllegalArgumentException(
            s"gbx_custom_cellaswkb requires 2 arguments; got ${c.length}")
    }

}
