package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Expression that returns the Euclidean distance between two BNG cell centres (metres). Arguments: cellid1, cellid2. */
case class BNG_EuclideanDistance(
    cellid1: Expression,
    cellid2: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellid1, cellid2)
    override def dataType: DataType = LongType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_EuclideanDistance.name
    override def replacement: Expression = invoke(BNG_EuclideanDistance)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_bng_euclideandistance, builder, and eval. */
object BNG_EuclideanDistance extends WithExpressionInfo {

    def eval(cellid1: Long, cellid2: Long): java.lang.Long = GridErrorHandler.safeEval[java.lang.Long](null)(execute(cellid1, cellid2))
    def eval(cellid1: UTF8String, cellid2: UTF8String): java.lang.Long = {
        val a = BNG.parseOrNull(cellid1.toString); val b = BNG.parseOrNull(cellid2.toString)
        if (a == null || b == null) null else GridErrorHandler.safeEval[java.lang.Long](null)(execute(a, b))
    }

    def execute(cellid1: Long, cellid2: Long): Long = BNG.euclideanDistance(cellid1, cellid2)

    def execute(cellid1: String, cellid2: String): Long = {
        val cellid1Long = BNG.parse(cellid1)
        val cellid2Long = BNG.parse(cellid2)
        BNG.euclideanDistance(cellid1Long, cellid2Long)
    }

    override def name: String = "gbx_bng_euclideandistance"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_EuclideanDistance(c(0), c(1))


}
