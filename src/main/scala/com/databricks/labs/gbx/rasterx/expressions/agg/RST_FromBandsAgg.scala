package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.expressions.constructor.RST_FromBands
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** Streaming aggregator: stacks single-band tiles into a multi-band tile.
 *
 *  `gbx_rst_frombands_agg(tile, band_index INT) -> tile`
 *
 *  Unlike the non-agg `gbx_rst_frombands(ARRAY<tile>)` -- where ARRAY position
 *  determines band order -- a UDAF's `merge` concatenates partial buffers in
 *  nondeterministic order across partitions.  Therefore this agg requires an
 *  explicit `band_index INT` streamed per row; `eval` sorts by `band_index`
 *  ascending before stacking via [[RST_FromBands.execute]].  Output band N is
 *  the tile whose band_index is the Nth-smallest.
 *
 *  Serde format (hand-rolled, mirrors [[RST_RasterizeAgg]]'s approach):
 *  `[count:Int][ idx:Int, tileLen:Int, tileBytes:Bytes ]*N`
 */
case class RST_FromBandsAgg(
    tile:             Expression,
    bandIndexExpr:        Expression,
    exprConfExpr:         Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset:   Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]] {

    lazy val rasterType: DataType = RST_ExpressionUtil.rasterType(tile)
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    /** Field count of the input tile struct (3 for v1, 8 for v2), from the declared element schema. */
    private lazy val tileFieldCount: Int = tile.dataType match {
        case st: org.apache.spark.sql.types.StructType => st.fields.length
        case _                                         => 3
    }
    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = true
    override def prettyName: String = RST_FromBandsAgg.name

    override def children: Seq[Expression] = Seq(tile, bandIndexExpr, exprConfExpr)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_FromBandsAgg =
        copy(tile = nc(0), bandIndexExpr = nc(1), exprConfExpr = nc(2))

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): ArrayBuffer[Any] = ArrayBuffer.empty

    /** Copy the BinaryType tile row through as-is.
     *  Raster is always binary; the buffer is uniformly binary so eval/deserialize
     *  can always use BinaryType without branching on rasterType.
     */
    private def toBinaryTileRow(tileRow: InternalRow): InternalRow =
        InternalRow.copyValue(tileRow).asInstanceOf[InternalRow]

    /** Catalyst-facing update: extract tile and band_index from the row. */
    override def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val idxRaw = bandIndexExpr.eval(input)
        if (idxRaw == null) return buffer
        val idx: Int = idxRaw match {
            case i: Int  => i
            case l: Long => l.toInt
            case other   => throw new IllegalArgumentException(
                s"rst_frombands_agg: band_index must be INT or LONG; got ${other.getClass.getName}")
        }
        val tileRaw = tile.eval(input)
        if (tileRaw == null) return buffer
        val binaryTileRow = toBinaryTileRow(tileRaw.asInstanceOf[InternalRow])
        buffer += InternalRow(idx, binaryTileRow)
        buffer
    }

    /** Direct typed update for unit tests (bypasses Literal child eval). */
    def updateWithIndex(buffer: ArrayBuffer[Any], tileRow: InternalRow, idx: Int): ArrayBuffer[Any] = {
        val binaryTileRow = toBinaryTileRow(tileRow)
        buffer += InternalRow(idx, binaryTileRow)
        buffer
    }

    override def merge(buffer: ArrayBuffer[Any], input: ArrayBuffer[Any]): ArrayBuffer[Any] = {
        buffer ++= input
        buffer
    }

    override def eval(buffer: ArrayBuffer[Any]): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.isEmpty) return null

        // Sort by band_index ascending -- this is the critical ordering guarantee.
        val sorted = buffer
            .map(_.asInstanceOf[InternalRow])
            .sortBy(_.getInt(0))

        // Open each buffered tile, skipping corrupt members.
        // Buffer is uniformly BinaryType (normalized in update/updateWithIndex).
        var dropped = 0
        val tiles: Seq[(Long, org.gdal.gdal.Dataset, Map[String, String])] = sorted.flatMap { row =>
            val tileRow = row.getStruct(1, tileFieldCount)
            Try(RasterSerializationUtil.rowToTile(tileRow, org.apache.spark.sql.types.BinaryType)).toOption match {
                case Some(t) if t._2 != null => Some(t)
                case _ => dropped += 1; None
            }
        }.toSeq

        if (tiles.isEmpty) return null

        var resultDs: org.gdal.gdal.Dataset = null
        try {
            val (rds, resMtd) = RST_FromBands.execute(tiles)
            resultDs = rds
            val finalMtd = if (dropped > 0)
                resMtd + ("last_error" -> s"RST_FromBandsAgg: skipped $dropped corrupt input tile(s)")
            else resMtd
            RasterSerializationUtil.tileToRow(
                (tiles.head._1, resultDs, finalMtd),
                rasterType,
                exprConf.hConf
            )
        } finally {
            if (resultDs != null) RasterDriver.releaseDataset(resultDs)
            tiles.foreach(t => RasterDriver.releaseDataset(t._2))
        }
    }

    /** Serde: [count:Int][ idx:Int, tileLen:Int, tileBytes ]*N */
    override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(obj.length)
        for (elem <- obj) {
            val row = elem.asInstanceOf[InternalRow]
            val idx = row.getInt(0)
            val tileRow = row.getStruct(1, tileFieldCount)
            val tileBytes = serializeTileRow(tileRow)
            out.writeInt(idx)
            out.writeInt(tileBytes.length)
            out.write(tileBytes)
        }
        bos.toByteArray
    }

    override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
        val buf = createAggregationBuffer()
        val in  = new DataInputStream(new ByteArrayInputStream(bytes))
        val n   = in.readInt()
        var i   = 0
        while (i < n) {
            val idx     = in.readInt()
            val tileLen = in.readInt()
            val tileBytes = new Array[Byte](tileLen)
            if (tileLen > 0) in.readFully(tileBytes)
            val tileRow = deserializeTileRow(tileBytes)
            buf += InternalRow(idx, tileRow)
            i += 1
        }
        buf
    }

    /** Serialize a tile InternalRow to bytes.
     *  Buffer is uniformly BinaryType (normalized in update/updateWithIndex),
     *  so we can always extract the bytes directly from field 1.
     */
    private def serializeTileRow(tileRow: InternalRow): Array[Byte] = {
        tileRow.getBinary(1)
    }

    /** Deserialize bytes back into a BinaryType tile InternalRow. */
    private def deserializeTileRow(bytes: Array[Byte]): InternalRow = {
        import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
        import org.apache.spark.unsafe.types.UTF8String
        InternalRow.fromSeq(Seq(
            0L,      // cellid placeholder
            bytes,   // raster binary
            ArrayBasedMapData(Array.empty[UTF8String], Array.empty[UTF8String])
        ))
    }

}

/** Companion: SQL name and builder for `gbx_rst_frombands_agg`. */
object RST_FromBandsAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_frombands_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_FromBandsAgg(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"$name expects 2 arguments (tile, band_index INT); got $n")
    }

}
