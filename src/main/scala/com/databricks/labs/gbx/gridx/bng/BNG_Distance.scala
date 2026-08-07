package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Expression that returns the grid distance between two BNG cells. Arguments: cellid1, cellid2. */
case class BNG_Distance(
    cellid1: Expression,
    cellid2: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellid1, cellid2)
    override def dataType: DataType = LongType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_Distance.name
    override def replacement: Expression = invoke(BNG_Distance)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_bng_distance, builder, and eval. */
object BNG_Distance extends WithExpressionInfo {

    def eval(cellid1: Long, cellid2: Long): Long = execute(cellid1, cellid2)
    def eval(cellid1: UTF8String, cellid2: UTF8String): Long = execute(cellid1.toString, cellid2.toString)

    def execute(cellid1: Long, cellid2: Long): Long = BNG.distance(cellid1, cellid2)

    def execute(cellid1: String, cellid2: String): Long = {
        val cellid1Long = BNG.parse(cellid1)
        val cellid2Long = BNG.parse(cellid2)
        BNG.distance(cellid1Long, cellid2Long)
    }

    override def name: String = "gbx_bng_distance"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_Distance(c(0), c(1))


}
