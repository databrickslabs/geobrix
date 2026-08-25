package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.unsafe.types.UTF8String

/** Expression that returns the area of the BNG cell in square kilometres. Argument: cellid. */
case class BNG_CellArea(
    cellid: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellid)
    override def dataType: DataType = DoubleType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_CellArea.name
    override def replacement: Expression = invoke(BNG_CellArea)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name gbx_bng_cellarea, builder, and eval. */
object BNG_CellArea extends WithExpressionInfo {

    def eval(cellID: Long): java.lang.Double = GridErrorHandler.safeEval[java.lang.Double](null)(execute(cellID))
    def eval(cellId: UTF8String): java.lang.Double = {
        val cid = BNG.parseOrNull(cellId.toString)
        if (cid == null) null else GridErrorHandler.safeEval[java.lang.Double](null)(execute(cid))
    }

    def execute(cellID: Long): Double = BNG.area(cellID)

    def execute(cellID: String): Double = {
        val cellIdLong = BNG.parse(cellID)
        BNG.area(cellIdLong)
    }

    override def name: String = "gbx_bng_cellarea"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_CellArea(c(0))


}
