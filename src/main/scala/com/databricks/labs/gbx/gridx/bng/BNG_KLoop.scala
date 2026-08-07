package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Expression that returns the k-loop (hollow ring) of cell IDs at distance k. Arguments: cellid, k. */
case class BNG_KLoop(
    cellid: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellid, k)
    override def dataType: DataType = ArrayType(StringType)
    override def nullable: Boolean = true
    override def prettyName: String = BNG_KLoop.name
    override def replacement: Expression = invoke(BNG_KLoop)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_bng_kloop, builder, and eval. */
object BNG_KLoop extends WithExpressionInfo {

    def eval(cellid: UTF8String, k: Int): ArrayData = {
        val indices = execute(cellid.toString, k).map(UTF8String.fromString).toArray
        ArrayData.toArrayData(indices)
    }

    def eval(cellid: Long, k: Int): ArrayData = {
        val indices = execute(BNG.format(cellid), k).map(UTF8String.fromString).toArray
        ArrayData.toArrayData(indices)
    }

    def execute(cellid: String, k: Int): Iterator[String] = {
        BNG.kLoop(BNG.parse(cellid), k).map(BNG.format)
    }

    def execute(cellid: Long, k: Int): Iterator[String] = {
        BNG.kLoop(cellid, k).map(BNG.format)
    }

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_KLoop(c(0), c(1))

    override def name: String = "gbx_bng_kloop"


}
