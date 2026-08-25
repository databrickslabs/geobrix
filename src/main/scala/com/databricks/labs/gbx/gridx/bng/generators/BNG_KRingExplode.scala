package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.grid.BNG
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Generator expression that explodes the k-ring of a BNG cell into one row per cell. Arguments: cellid, k. */
case class BNG_KRingExplode(
    cellid: Expression,
    k: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(cellid, k)

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val cellidValue = cellid.eval(input)
        val kValue = k.eval(input)

        if (cellidValue == null || kValue == null) return Iterator.empty[InternalRow]

        cellidValue match {
            case s: UTF8String =>
                val cid = BNG.parseOrNull(s.toString)
                if (cid == null) Iterator.empty[InternalRow]
                else BNG.kRing(cid, kValue.asInstanceOf[Int])
                    .map(cid => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(cid)))))
            case l: Long       => BNG
                    .kRing(l, kValue.asInstanceOf[Int])
                    .map(cid => InternalRow.fromSeq(Seq(cid)))
            case _             => throw new IllegalArgumentException(s"Unsupported cellid type: ${cellidValue.getClass.getName}")
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellid", cellid.dataType)))

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_bng_kringexplode, builder. */
object BNG_KRingExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_kringexplode"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_KRingExplode(c(0), c(1))


}
