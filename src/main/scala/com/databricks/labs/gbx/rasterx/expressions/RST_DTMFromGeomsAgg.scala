package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry, LineString}

/** UDAF: `gbx_rst_dtmfromgeoms_agg(point, breaklines, merge_tolerance, snap_tolerance,
 *  xmin, ymin, xmax, ymax, width_px, height_px, srid, [no_data])`.
 *
 *  Streams one Z-valued `point` per row into a buffer; every other argument is a
 *  per-group constant (read once in `eval`). Breaklines arrive as a constant ARRAY.
 *  Delegates to [[RST_DTMFromGeoms.execute]] so the result equals the non-agg form.
 */
final case class RST_DTMFromGeomsAgg(
    pointExpr: Expression,
    breaklinesExpr: Expression,
    mergeToleranceExpr: Expression,
    snapToleranceExpr: Expression,
    xminExpr: Expression, yminExpr: Expression, xmaxExpr: Expression, ymaxExpr: Expression,
    widthPxExpr: Expression, heightPxExpr: Expression, outSridExpr: Expression,
    noDataExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[DTMFromGeomsAcc] {

    import RST_DTMFromGeomsAgg.{evalDouble, evalInt, evalExpr, geomsFromArrayData}

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = true
    override val dataType: DataType = StructType(Seq(
        StructField("index_id", LongType, nullable = true),
        StructField("raster", BinaryType, nullable = true),
        StructField("metadata", MapType(StringType, StringType), nullable = true)
    ))
    override def prettyName: String = RST_DTMFromGeomsAgg.name

    override def children: Seq[Expression] = Seq(
        pointExpr, breaklinesExpr, mergeToleranceExpr, snapToleranceExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, outSridExpr, noDataExpr)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_DTMFromGeomsAgg = {
        require(nc.length == 12, s"RST_DTMFromGeomsAgg expects 12 children; got ${nc.length}")
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), outSridExpr = nc(10), nc(11))
    }

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): DTMFromGeomsAcc = DTMFromGeomsAcc.empty

    override def update(buffer: DTMFromGeomsAcc, input: InternalRow): DTMFromGeomsAcc = {
        val pt = evalExpr(pointExpr, input)
        if (pt == null) return buffer
        val geom = pt match {
            case b: Array[Byte] => JTS.fromWKB(b)
            case s: UTF8String  => JTS.fromWKT(s.toString)
            case other          => throw new IllegalArgumentException(
                s"rst_dtmfromgeoms_agg: point column must be BINARY (WKB) or STRING (WKT); got ${other.getClass.getName}")
        }
        if (geom.getCoordinate == null || geom.getCoordinate.getZ.isNaN) {
            throw new IllegalArgumentException(
                "rst_dtmfromgeoms_agg: point has no Z coordinate - supply 3D WKB or WKT with Z values (e.g. 'POINT Z (x y z)')")
        }
        buffer.add(JTS.toWKB3(geom))
    }

    override def merge(a: DTMFromGeomsAcc, b: DTMFromGeomsAcc): DTMFromGeomsAcc = a.merge(b)

    override def eval(buffer: DTMFromGeomsAcc): Any = {
        val empty = InternalRow.empty
        val breaklines: Seq[LineString] = evalExpr(breaklinesExpr, empty) match {
            case null          => Seq.empty
            case ad: ArrayData => geomsFromArrayData(ad).map(_.asInstanceOf[LineString]).toSeq
            case other         => throw new IllegalArgumentException(
                s"rst_dtmfromgeoms_agg: breaklines must be an ARRAY of geometries; got ${other.getClass.getName}")
        }
        val points: Seq[Geometry] = buffer.points.toSeq.map(JTS.fromWKB)
        RST_DTMFromGeoms.execute(
            points, breaklines,
            evalDouble(mergeToleranceExpr, empty, "merge_tolerance"),
            evalDouble(snapToleranceExpr, empty, "snap_tolerance"),
            evalDouble(xminExpr, empty, "xmin"), evalDouble(yminExpr, empty, "ymin"),
            evalDouble(xmaxExpr, empty, "xmax"), evalDouble(ymaxExpr, empty, "ymax"),
            evalInt(widthPxExpr, empty, "width_px"), evalInt(heightPxExpr, empty, "height_px"),
            evalInt(outSridExpr, empty, "out_srid"),
            evalDouble(noDataExpr, empty, "no_data"))
    }

    override def serialize(b: DTMFromGeomsAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): DTMFromGeomsAcc = DTMFromGeomsAcc.deserialize(bytes)
}

object RST_DTMFromGeomsAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_dtmfromgeoms_agg"

    private[expressions] def evalExpr(e: Expression, row: InternalRow): Any = e.eval(row)

    private[expressions] def geomsFromArrayData(data: ArrayData): Array[Geometry] = {
        val n = data.numElements()
        val out = scala.collection.mutable.ArrayBuffer.empty[Geometry]
        var i = 0
        while (i < n) {
            if (!data.isNullAt(i)) {
                out += (data.get(i, null) match {
                    case b: Array[Byte] => JTS.fromWKB(b)
                    case s: UTF8String  => JTS.fromWKT(s.toString)
                    case other          => throw new IllegalArgumentException(
                        s"rst_dtmfromgeoms_agg: breakline element must be BINARY/STRING; got ${other.getClass.getName}")
                })
            }
            i += 1
        }
        out.toArray
    }

    private[expressions] def evalDouble(e: Expression, row: InternalRow, label: String): Double =
        evalExpr(e, row) match {
            case null => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must not be null")
            case d: Double  => d
            case f: Float   => f.toDouble
            case i: Int     => i.toDouble
            case l: Long    => l.toDouble
            case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
            case o => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must be numeric; got ${o.getClass.getName}")
        }

    private[expressions] def evalInt(e: Expression, row: InternalRow, label: String): Int =
        evalExpr(e, row) match {
            case null   => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must not be null")
            case i: Int => i
            case l: Long => l.toInt
            case o => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must be INT or LONG; got ${o.getClass.getName}")
        }

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 11 => RST_DTMFromGeomsAgg(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10),
            Literal(RST_DTMFromGeoms.DefaultNoData))
        case 12 => RST_DTMFromGeomsAgg(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11))
        case n => throw new IllegalArgumentException(
            s"$name takes 11 or 12 arguments (point, breaklines, merge_tolerance, snap_tolerance, " +
            s"xmin, ymin, xmax, ymax, width_px, height_px, out_srid, [no_data]); got $n")
    }
}
