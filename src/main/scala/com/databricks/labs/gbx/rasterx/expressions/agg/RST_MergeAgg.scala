package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil, V2Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** Merges rasters into a single raster. */
//noinspection DuplicatedCode
case class RST_MergeAgg(
    tile: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override val child: Expression = tile
    override val nullable: Boolean = true
    lazy val rasterType: DataType = RST_ExpressionUtil.rasterType(tile)
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def prettyName: String = RST_MergeAgg.name

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val value = child.eval(input)
        if (value != null) {
            buffer += InternalRow.copyValue(
                RasterSerializationUtil.normalizeToV2Row(value.asInstanceOf[InternalRow])
            )
        }
        buffer
    }

    def merge(buffer: ArrayBuffer[Any], input: ArrayBuffer[Any]): ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def createAggregationBuffer(): ArrayBuffer[Any] = ArrayBuffer.empty

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(buffer: ArrayBuffer[Any]): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.isEmpty) {
            null
        } else if (buffer.size == 1) {
            buffer.head
        } else {

            // A groupBy().agg() does not guarantee the order tiles reach the aggregator,
            // so a last-wins mosaic would otherwise pick a different overlap winner from
            // run to run. Sort by the tile's raw serialized content -- the GTiff bytes (or
            // the path, for path-backed tiles) each row carries -- a total order intrinsic
            // to the tile with NO ties for distinct content, so the highest-content tile
            // reliably wins the overlap regardless of arrival order, and the lightweight
            // tier (which sorts on the identical raw bytes) picks the same winner.
            //
            // The previous key (geotransform origin, GetDescription tie-break) was
            // nondeterministic: two overlapping tiles sharing an origin tied on the origin
            // and fell back to GetDescription, which for an in-memory BinaryType tile is a
            // per-open /vsimem/<uuid> path -- i.e. random. Raw content has no such hole.
            var dropped = 0
            val tiles = buffer
                .map(_.asInstanceOf[InternalRow])
                .sortBy(row => RST_MergeAgg.contentKey(row))(RST_MergeAgg.unsignedBytesOrdering)
                .flatMap { row =>
                    Try(RasterSerializationUtil.rowToTile(row, rasterType)).toOption match {
                        case Some(t) if t._2 != null => Some(t)
                        case _ => dropped += 1; None
                    }
                }

            if (tiles.isEmpty) {
                null
            } else {
                // If merging multiple index rasters, the index value is dropped
                val idx: Long = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
                val (res, resMtd) = MergeRasters.merge(tiles.map(_._2).toArray, tiles.head._3)

                val finalMtd = if (dropped > 0)
                    resMtd + ("last_error" -> s"RST_MergeAgg: skipped $dropped corrupt input tile(s)")
                else resMtd

                val resRow = RasterSerializationUtil.tileToRow((idx, res, finalMtd), rasterType, exprConf.hConf)

                tiles.foreach(t => RasterDriver.releaseDataset(t._2))
                RasterDriver.releaseDataset(res)

                resRow
            }
        }
    }

    override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = {
        val array = new GenericArrayData(obj.toArray)
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
        val buffer = createAggregationBuffer()
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(dataType, (_, x: Any) => buffer += x)
        buffer
    }

    override protected def withNewChildInternal(newChild: Expression): RST_MergeAgg = copy(tile = newChild)

}

/** Companion: SQL name, builder, and sort helpers for binary tile. */
object RST_MergeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_merge_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => RST_MergeAgg(c(0))

    /** Canonical sort key for a tile row: the raw GTiff bytes (BinaryType, field 1). This is
      * a total order intrinsic to the tile -- bitwise-identical to what the lightweight tier
      * sorts on -- with no random per-open component.
      */
    private[agg] def contentKey(row: InternalRow): Array[Byte] =
        V2Tile.getRaster(row)

    /** Unsigned lexicographic ordering of byte arrays (a stable total order on raw content). */
    private[agg] val unsignedBytesOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
        override def compare(a: Array[Byte], b: Array[Byte]): Int = {
            val n = math.min(a.length, b.length)
            var i = 0
            while (i < n) {
                val ai = a(i) & 0xff
                val bi = b(i) & 0xff
                if (ai != bi) return ai - bi
                i += 1
            }
            a.length - b.length
        }
    }

}
