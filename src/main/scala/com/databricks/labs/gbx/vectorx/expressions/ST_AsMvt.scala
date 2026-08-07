package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.mvt.MvtWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
  * Aggregate expression that encodes a group of `(geom_wkb, attrs_struct)` rows into a single
  * Mapbox Vector Tile (MVT) protobuf blob via `MvtWriter` (GDAL OGR MVT driver).
  *
  * Inputs:
  *   - `geom`      : per-row geometry in WKB, in tile-local coordinates
  *   - `attrs`     : per-row attribute struct (fields carry native int/long/double/bool/string types)
  *   - `layerName` : constant string column holding the MVT layer name
  *
  * Output: `BINARY` — the MVT protobuf bytes for one layer of the tile.
  *
  * Buffer: [[MvtAcc]] — holds the layer name and a list of per-feature
  * `(wkb_bytes, attrs_encoded_bytes)` tuples until the final encode pass.
  *
  * Follows the same `TypedImperativeAggregate` pattern as
  * `com.databricks.labs.gbx.gridx.bng.agg.BNG_CellUnionAgg`. The companion object's
  * `name = "gbx_st_asmvt"` is registered with Spark via
  * `com.databricks.labs.gbx.vectorx.functions.register`.
  */
final case class ST_AsMvt(
    geom: Expression,
    attrs: Expression,
    layerName: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[MvtAcc] {

    override def children: Seq[Expression] = Seq(geom, attrs, layerName)
    override def nullable: Boolean = false
    override def dataType: DataType = BinaryType
    override def prettyName: String = ST_AsMvt.name
    override lazy val deterministic: Boolean = true

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]
    ): ST_AsMvt = copy(
      geom = newChildren(0),
      attrs = newChildren(1),
      layerName = newChildren(2)
    )

    /** Resolve the constant layer-name expression once per group; throws on non-foldable/null. */
    private def evalLayerName(): String = {
        if (!layerName.foldable) {
            throw new IllegalArgumentException(
              "gbx_st_asmvt: layerName must be a constant string expression"
            )
        }
        val v = layerName.eval(InternalRow.empty)
        if (v == null) {
            throw new IllegalArgumentException("gbx_st_asmvt: layerName must not be null")
        }
        v.toString
    }

    override def createAggregationBuffer(): MvtAcc = MvtAcc.empty(evalLayerName())

    override def update(buf: MvtAcc, input: InternalRow): MvtAcc = {
        val wkb = geom.eval(input).asInstanceOf[Array[Byte]]
        if (wkb != null && wkb.length > 0) {
            val attrsRow = attrs.eval(input).asInstanceOf[InternalRow]
            val encoded = encodeAttrs(attrsRow)
            buf.add(wkb, encoded)
        }
        buf
    }

    override def merge(a: MvtAcc, b: MvtAcc): MvtAcc = a.merge(b)

    override def serialize(buf: MvtAcc): Array[Byte] = buf.serialize
    override def deserialize(bytes: Array[Byte]): MvtAcc = MvtAcc.deserialize(bytes)

    // Spark's TypedImperativeAggregate calls this method to emit the final aggregated
    // result from the buffer. Walks features, decodes per-feature attribute payloads,
    // hands them to MvtWriter for protobuf encoding.
    override def eval(buf: MvtAcc): Any = {
        val featuresWithAttrs: Seq[(Array[Byte], Map[String, Any])] =
            buf.features.iterator.map { case (wkb, attrsBytes) =>
                (wkb, decodeAttrs(attrsBytes))
            }.toSeq
        MvtWriter.encode(buf.layerName, MvtWriter.DefaultExtent, featuresWithAttrs)
    }

    /**
      * Encode the attribute struct row to a type-tagged binary payload so the buffer round-trip
      * (`update` → `serialize`/`deserialize` → `eval`) preserves each field's native runtime type
      * — `decodeAttrs` then yields `Map[String, Any]` with Int/Long/Double/Boolean/String values
      * that `MvtWriter` maps to native OGR field types.
      *
      * Format: `num_fields(int)` then per field `(key_len(int), key_utf8_bytes, tag(byte), payload)`
      * where `tag` selects the payload encoding (see the tag constants in [[ST_AsMvt]]).
      */
    private def encodeAttrs(row: InternalRow): Array[Byte] = {
        if (row == null) return null
        val schema = attrs.dataType.asInstanceOf[StructType]
        val baos = new ByteArrayOutputStream()
        val out = new DataOutputStream(baos)
        out.writeInt(schema.fields.length)
        var i = 0
        while (i < schema.fields.length) {
            val name = schema.fields(i).name
            val keyBytes = name.getBytes("UTF-8")
            out.writeInt(keyBytes.length); out.write(keyBytes)
            if (row.isNullAt(i)) {
                out.writeByte(ST_AsMvt.TagNull)
            } else {
                val dt = schema.fields(i).dataType
                dt match {
                    case _: ByteType | _: ShortType | _: IntegerType =>
                        out.writeByte(ST_AsMvt.TagInt)
                        out.writeInt(row.get(i, dt).asInstanceOf[Number].intValue())
                    case _: LongType =>
                        out.writeByte(ST_AsMvt.TagLong); out.writeLong(row.getLong(i))
                    case _: FloatType | _: DoubleType =>
                        out.writeByte(ST_AsMvt.TagDouble)
                        out.writeDouble(row.get(i, dt).asInstanceOf[Number].doubleValue())
                    case _: BooleanType =>
                        out.writeByte(ST_AsMvt.TagBoolean)
                        out.writeBoolean(row.getBoolean(i))
                    case _ =>
                        out.writeByte(ST_AsMvt.TagString)
                        val vBytes = row.get(i, dt).toString.getBytes("UTF-8")
                        out.writeInt(vBytes.length); out.write(vBytes)
                }
            }
            i += 1
        }
        out.flush(); baos.toByteArray
    }

    /** Inverse of [[encodeAttrs]] — reconstructs native-typed values from the type tags. */
    private def decodeAttrs(bytes: Array[Byte]): Map[String, Any] = {
        if (bytes == null) return Map.empty[String, Any]
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val n = in.readInt()
        val builder = Map.newBuilder[String, Any]
        var i = 0
        while (i < n) {
            val keyLen = in.readInt()
            val keyBytes = new Array[Byte](keyLen); in.readFully(keyBytes)
            val key = new String(keyBytes, "UTF-8")
            in.readByte() match {
                case ST_AsMvt.TagNull    => // null → drop the field (writer skips missing keys)
                case ST_AsMvt.TagInt     => builder += key -> in.readInt()
                case ST_AsMvt.TagLong    => builder += key -> in.readLong()
                case ST_AsMvt.TagDouble  => builder += key -> in.readDouble()
                case ST_AsMvt.TagBoolean => builder += key -> in.readBoolean()
                case ST_AsMvt.TagString  =>
                    val vLen = in.readInt()
                    val vBytes = new Array[Byte](vLen); in.readFully(vBytes)
                    builder += key -> new String(vBytes, "UTF-8")
                case other => throw new IllegalStateException(s"gbx_st_asmvt: unknown attr tag $other")
            }
            i += 1
        }
        builder.result()
    }

}

/** Companion: SQL name `gbx_st_asmvt`, builder. */
object ST_AsMvt extends WithExpressionInfo {

    /** Type tags for the per-feature attribute payload (see ST_AsMvt.encodeAttrs / decodeAttrs). */
    private[expressions] final val TagNull: Byte = 0
    private[expressions] final val TagInt: Byte = 1
    private[expressions] final val TagLong: Byte = 2
    private[expressions] final val TagDouble: Byte = 3
    private[expressions] final val TagBoolean: Byte = 4
    private[expressions] final val TagString: Byte = 5

    override def name: String = "gbx_st_asmvt"

    override def builder(): FunctionBuilder = {
        case Seq(g, a, l) => ST_AsMvt(g, a, l)
        case other => throw new IllegalArgumentException(
              s"gbx_st_asmvt: expected (geom, attrs_struct, layer_name) — got ${other.length} args"
            )
    }

    override def usageArgs: String = "geom, attrs_struct, layer_name"

    override def description: String =
        "Aggregator: encodes features into a Mapbox Vector Tile (MVT) protobuf blob."
}
