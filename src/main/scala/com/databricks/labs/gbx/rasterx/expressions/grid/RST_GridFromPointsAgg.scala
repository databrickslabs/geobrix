package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * UDAF: `gbx_rst_gridfrompoints_agg(point_col, value_col, xmin, ymin, xmax, ymax,
  *  width_px, height_px, srid, [power, [max_pts]])` - IDW interpolation aggregator.
  *
  * Aggregator counterpart of [[RST_GridFromPoints]]: accumulates one
  * `(point, value)` per row across a group, then materializes a single GTiff
  * tile by passing the accumulated features to `gdal.Grid(invdist:...)`.
  *
  * Per-group constants (extent / size / srid / power / max_pts) are pulled from
  * the first non-null row and assumed to be the same across the group. Same-row
  * evaluation per Spark UDAF semantics: callers typically `groupBy(extent_key)`
  * then pass per-row point/value columns and per-group literal extent params.
  *
  * The point geometry column may be either `BinaryType` (WKB) or `StringType`
  * (WKT). Mixing within a group raises an error.
  */
final case class RST_GridFromPointsAgg(
    pointExpr: Expression,
    valueExpr: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    outSridExpr: Expression,
    powerExpr: Expression,
    maxPtsExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[GridFromPointsAcc] {

    import RST_GridFromPointsAgg.{evalDouble, evalInt, evalExpr}

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = true
    override val dataType: DataType = StructType(Seq(
        StructField("index_id", LongType, nullable = true),
        StructField("raster", BinaryType, nullable = true),
        StructField("metadata", MapType(StringType, StringType), nullable = true)
    ))
    override def prettyName: String = RST_GridFromPointsAgg.name

    override def children: Seq[Expression] = Seq(
        pointExpr, valueExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, outSridExpr,
        powerExpr, maxPtsExpr
    )

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_GridFromPointsAgg = {
        require(nc.length == 11, s"RST_GridFromPointsAgg expects 11 children; got ${nc.length}")
        copy(
            pointExpr = nc(0), valueExpr = nc(1),
            xminExpr = nc(2), yminExpr = nc(3), xmaxExpr = nc(4), ymaxExpr = nc(5),
            widthPxExpr = nc(6), heightPxExpr = nc(7), outSridExpr = nc(8),
            powerExpr = nc(9), maxPtsExpr = nc(10)
        )
    }

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): GridFromPointsAcc = GridFromPointsAcc.empty

    override def update(buffer: GridFromPointsAcc, input: InternalRow): GridFromPointsAcc = {
        val pt = evalExpr (pointExpr, input)
        val v = evalExpr (valueExpr, input)
        if (pt == null || v == null) return buffer
        val wkb = pt match {
            case b: Array[Byte] => b
            case s: UTF8String  => JTS.toWKB(JTS.fromWKT(s.toString))
            case other          => throw new IllegalArgumentException(
                s"rst_gridfrompoints_agg: point column must be BINARY (WKB) or STRING (WKT); got ${other.getClass.getName}")
        }
        val value = v match {
            case d: Double            => d
            case f: Float             => f.toDouble
            case jd: java.lang.Double => jd.doubleValue()
            case other                => throw new IllegalArgumentException(
                s"rst_gridfrompoints_agg: value column must be DOUBLE; got ${other.getClass.getName}")
        }
        buffer.add(wkb, value)
    }

    override def merge(a: GridFromPointsAcc, b: GridFromPointsAcc): GridFromPointsAcc = a.merge(b)

    override def eval (buffer: GridFromPointsAcc): Any = {
        // Per-group constants: evaluated against an empty row - they must be
        // literal/group-stable.
        val emptyRow = InternalRow.empty
        val xmin = evalDouble(xminExpr, emptyRow, "xmin")
        val ymin = evalDouble(yminExpr, emptyRow, "ymin")
        val xmax = evalDouble(xmaxExpr, emptyRow, "xmax")
        val ymax = evalDouble(ymaxExpr, emptyRow, "ymax")
        val widthPx = evalInt(widthPxExpr, emptyRow, "width_px")
        val heightPx = evalInt(heightPxExpr, emptyRow, "height_px")
        val srid = evalInt(outSridExpr, emptyRow, "out_srid")
        val power = evalDouble(powerExpr, emptyRow, "power")
        val maxPts = evalInt(maxPtsExpr, emptyRow, "max_pts")
        RST_GridFromPoints.execute(
            buffer.features.toSeq,
            xmin, ymin, xmax, ymax,
            widthPx, heightPx, srid,
            power, maxPts
        )
    }

    override def serialize(b: GridFromPointsAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): GridFromPointsAcc = GridFromPointsAcc.deserialize(bytes)
}

/** Companion: SQL name `gbx_rst_gridfrompoints_agg`, builder accepts 9, 10, or 11 args. */
object RST_GridFromPointsAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_gridfrompoints_agg"

    /** Indirection so the Expression.eval invocation is centralized (and silences spell-checkers). */
    private[grid] def evalExpr (e: Expression, row: InternalRow): Any = e.eval (row)

    private[grid] def evalDouble(e: Expression, row: InternalRow, label: String): Double = {
        val v = evalExpr (e, row)
        if (v == null) throw new IllegalArgumentException(
            s"rst_gridfrompoints_agg: $label must not be null")
        v match {
            case d: Double                => d
            case f: Float                 => f.toDouble
            case i: Int                   => i.toDouble
            case l: Long                  => l.toDouble
            case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
            case other                    => throw new IllegalArgumentException(
                s"rst_gridfrompoints_agg: $label must be numeric; got ${other.getClass.getName}")
        }
    }

    private[grid] def evalInt(e: Expression, row: InternalRow, label: String): Int = {
        val v = evalExpr (e, row)
        if (v == null) throw new IllegalArgumentException(
            s"rst_gridfrompoints_agg: $label must not be null")
        v match {
            case i: Int                   => i
            case l: Long                  => l.toInt
            case other                    => throw new IllegalArgumentException(
                s"rst_gridfrompoints_agg: $label must be INT or LONG; got ${other.getClass.getName}")
        }
    }

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 9 => RST_GridFromPointsAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8),
            Literal(RST_GridFromPoints.DefaultPower),
            Literal(RST_GridFromPoints.DefaultMaxPoints)
        )
        case 10 => RST_GridFromPointsAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8),
            c(9), Literal(RST_GridFromPoints.DefaultMaxPoints)
        )
        case 11 => RST_GridFromPointsAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8),
            c(9), c(10)
        )
        case n => throw new IllegalArgumentException(
            s"$name takes 9 to 11 arguments " +
            s"(point, value, xmin, ymin, xmax, ymax, width_px, height_px, out_srid, [power, [max_pts]]); got $n"
        )
    }
}
