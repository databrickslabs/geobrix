package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.mvt.{MvtPyramidBuilder, MvtWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/** Generator: explode one `(geom, attrs)` row into one output row per intersecting
 *  `(z, x, y)` tile across a zoom range, encoded as MVT bytes.
 *
 *  Pattern-mirrors [[com.databricks.labs.gbx.rasterx.expressions.web.RST_XYZPyramid]].
 *  Same single-input-row to many-output-rows shape, codegen-fallback. The output element schema
 *  wraps `(z, x, y, mvt_bytes)` in a single `tile` column to satisfy Spark 4.0's multi-output
 *  generator analysis (callers `.alias("t")` and unpack via `t.tile.z`, `t.tile.mvt_bytes`).
 *
 *  Inputs are assumed in EPSG:4326; the helper clips against per-tile lon/lat envelopes and
 *  transforms to MVT tile-local coords before the protobuf encode (single-feature input per
 *  row in 0.4.0; multi-feature aggregation is `groupBy(z, x, y).agg(gbx_st_asmvt(...))`).
 */
case class ST_AsMvtPyramid(
    geomExpr: Expression,
    attrsExpr: Expression,
    minZExpr: Expression,
    maxZExpr: Expression,
    layerNameExpr: Expression,
    extentExpr: Expression = Literal(MvtWriter.DefaultExtent)
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def dataType: DataType = ST_AsMvtPyramid.tileStruct
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = ST_AsMvtPyramid.elementSchemaStatic
    override def children: Seq[Expression] =
        Seq(geomExpr, attrsExpr, minZExpr, maxZExpr, layerNameExpr, extentExpr)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val wkb = geomExpr.eval(input).asInstanceOf[Array[Byte]]
        if (wkb == null || wkb.isEmpty) return Iterator.empty

        val minZ = readInt(minZExpr.eval(input), "min_z")
        val maxZ = readInt(maxZExpr.eval(input), "max_z")
        val extent = readInt(extentExpr.eval(input), "extent")

        val layerNameRaw = layerNameExpr.eval(input)
        if (layerNameRaw == null) {
            throw new IllegalArgumentException("gbx_st_asmvt_pyramid: layer_name must not be null")
        }
        val layerName = layerNameRaw match {
            case s: UTF8String => s.toString
            case other         => other.toString
        }

        val attrsRow = attrsExpr.eval(input).asInstanceOf[InternalRow]
        val attrs = decodeAttrs(attrsRow)

        val tiles = MvtPyramidBuilder.build(Seq((wkb, attrs)), minZ, maxZ, layerName, extent)
        val rows = new ArrayBuffer[InternalRow](tiles.length)
        var i = 0
        while (i < tiles.length) {
            val (z, x, y, bytes) = tiles(i)
            val inner = InternalRow.fromSeq(Seq(z, x, y, bytes))
            rows += InternalRow.fromSeq(Seq(inner))
            i += 1
        }
        rows.iterator
    }

    /** PySpark sends Python ints as LongType; SQL literals come in as IntegerType. Accept both. */
    private def readInt(v: Any, fieldName: String): Int = v match {
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long    => l.toInt
        case i: Int               => i
        case l: Long              => l.toInt
        case null                 => throw new IllegalArgumentException(s"gbx_st_asmvt_pyramid: $fieldName is null")
        case other                => throw new IllegalArgumentException(s"gbx_st_asmvt_pyramid: $fieldName must be Int/Long; got $other")
    }

    /** Decode the per-feature attribute struct into a `Map[String, Any]` consumable by
     *  [[MvtWriter.encode]], preserving each field's native runtime type (Int/Long/Double/
     *  Boolean/String) so the pyramid's tiles carry native MVT value types — matching
     *  `ST_AsMvt`. This runs in-process per row, so values are read straight off the
     *  `InternalRow` (no serialization round-trip). Null fields are dropped — `MvtWriter`
     *  skips missing keys per its schema-derivation rule.
     */
    private def decodeAttrs(row: InternalRow): Map[String, Any] = {
        if (row == null) return Map.empty[String, Any]
        val schema = attrsExpr.dataType.asInstanceOf[StructType]
        val b = Map.newBuilder[String, Any]
        var i = 0
        while (i < schema.fields.length) {
            if (!row.isNullAt(i)) {
                val name = schema.fields(i).name
                val dt = schema.fields(i).dataType
                val value: Any = dt match {
                    case _: ByteType | _: ShortType | _: IntegerType =>
                        row.get(i, dt).asInstanceOf[Number].intValue()
                    case _: LongType   => row.getLong(i)
                    case _: FloatType | _: DoubleType =>
                        row.get(i, dt).asInstanceOf[Number].doubleValue()
                    case _: BooleanType => row.getBoolean(i)
                    case _              => row.get(i, dt).toString
                }
                b += name -> value
            }
            i += 1
        }
        b.result()
    }
}

/** Companion: SQL name, builder, output schema. */
object ST_AsMvtPyramid extends WithExpressionInfo {

    /** Inner `(z, x, y, mvt_bytes)` struct emitted per row. */
    val tileStruct: StructType = StructType(Seq(
        StructField("z", IntegerType, nullable = false),
        StructField("x", IntegerType, nullable = false),
        StructField("y", IntegerType, nullable = false),
        StructField("mvt_bytes", BinaryType, nullable = true)
    ))

    /** Generator element schema: a single `tile` column wrapping the inner struct.
     *  Mirrors `RST_XYZPyramid` so callers alias once and unpack via `t.tile.z` etc. */
    val elementSchemaStatic: StructType = StructType(Seq(
        StructField("tile", tileStruct, nullable = true)
    ))

    override def name: String = "gbx_st_asmvt_pyramid"

    /** Builder: 5 or 6 args. extent defaults to [[MvtWriter.DefaultExtent]] when omitted. */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 5 => ST_AsMvtPyramid(c(0), c(1), c(2), c(3), c(4), Literal(MvtWriter.DefaultExtent))
            case 6 => ST_AsMvtPyramid(c(0), c(1), c(2), c(3), c(4), c(5))
            case n => throw new IllegalArgumentException(
                s"gbx_st_asmvt_pyramid takes 5 or 6 arguments (geom, attrs_struct, min_z, max_z, layer_name, [extent]); got $n"
            )
        }
    }

    override def usageArgs: String = "geom, attrs_struct, min_z, max_z, layer_name, [extent]"

    override def description: String =
        "Generator: emit one row per (z, x, y) tile a feature intersects, encoded as MVT protobuf bytes."
}
