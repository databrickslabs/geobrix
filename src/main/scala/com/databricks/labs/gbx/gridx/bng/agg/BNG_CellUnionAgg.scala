package com.databricks.labs.gbx.gridx.bng.agg

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Aggregate expression that unions BNG cell chips (binary) into a single chip per group. */
final case class BNG_CellUnionAgg(
    inputChip: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[UnionAcc]
      with UnaryLike[Expression] {

    /** Chip struct type and field indices (robust to field reordering). */
    private def chipStruct = inputChip.dataType.asInstanceOf[StructType]
    private def idFieldIndex: Int = {
      val idx = chipStruct.fields.indexWhere(f => f.dataType == StringType || f.dataType == LongType)
      if (idx >= 0) idx else 0
    }
    private def idType = chipStruct.fields(idFieldIndex).dataType
    private def coreFieldIndex: Int = {
      val idx = chipStruct.fields.indexWhere(f =>
        f.dataType == BooleanType && (f.name.equalsIgnoreCase("core") || f.name.equalsIgnoreCase("isCore")))
      if (idx >= 0) idx else 1
    }
    private def wkbFieldIndex: Int = {
      val idx = chipStruct.fields.indexWhere(f => f.dataType == BinaryType)
      if (idx >= 0) idx else 2
    }
    override lazy val deterministic = true
    override val child: Expression = inputChip
    override val nullable = false
    override val dataType: DataType = BNG.cellType(idType)
    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)
    override def prettyName: String = BNG_CellUnionAgg.name
    override protected def withNewChildInternal(newChild: Expression): BNG_CellUnionAgg = copy(inputChip = newChild)

    override def createAggregationBuffer(): UnionAcc =
        UnionAcc(initialized = false, 0L, hasCore = false, unionWkb = null)
    override def serialize(b: UnionAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): UnionAcc = UnionAcc.deserialize(bytes)

    override def update(b: UnionAcc, in: InternalRow): UnionAcc = {
        val r = child.eval(in).asInstanceOf[InternalRow]
        val cellId = idType match {
            case StringType => BNG.parse(r.getString(idFieldIndex))
            case LongType   => r.getLong(idFieldIndex)
        }
        b.update(cellId, r.getBoolean(coreFieldIndex), r.getBinary(wkbFieldIndex))
    }

    override def merge(a: UnionAcc, c: UnionAcc): UnionAcc = a.merge(c)

    override def eval(b: UnionAcc): Any = {
        require(b.initialized, "empty aggregation buffer")
        val id = idType match {
            case StringType => UTF8String.fromString(BNG.format(b.cellID))
            case LongType   => b.cellID
        }
        if (b.hasCore) InternalRow(id, true, JTS.toWKB(BNG.cellIdToGeometry(b.cellID)))
        else InternalRow(id, false, if (b.unionWkb eq null) JTS.toWKB(JTS.emptyPolygon) else b.unionWkb)
    }

}

/** Companion: SQL name gbx_bng_cellunion_agg, builder. */
object BNG_CellUnionAgg extends WithExpressionInfo {

    override def name: String = "gbx_bng_cellunion_agg"
    override def builder(): FunctionBuilder = c => new BNG_CellUnionAgg(c.head)


}
