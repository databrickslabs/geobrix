package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil

/**
  * UDAF: `gbx_rst_binpoints_agg(x, y, z, xmin, ymin, xmax, ymax,
  *  width_px, height_px, srid, [statistic="max"])` — stream one `(x,y,z)` point
  *  per row in a GROUP BY into a single-band Float32 DSM tile.
  *
  * Aggregator counterpart of [[RST_BinPoints]]: accumulates one scalar
  * `(x, y, z)` triple per row across a group, then materialises a single
  * Float32 GTiff tile by delegating to [[RST_BinPoints.execute]].
  *
  * Per-group constants (extent / size / srid / statistic) are evaluated once
  * against `InternalRow.empty` in [[eval]] — they must be literal or group-stable.
  * Per-row inputs (`x`, `y`, `z`) are accumulated in the [[BinPointsAcc]] buffer
  * via [[update]].
  *
  * Supported statistics: `"max"` (default), `"min"`, `"mean"`, `"median"`,
  * `"count"`, `"percentile:<p>"`.  Empty groups return `null`.
  *
  * GDAL is self-initialised in [[eval]] via the [[ExpressionConfigExpr]] child
  * (same pattern as [[com.databricks.labs.gbx.rasterx.expressions.agg.RST_CombineAvgAgg]]):
  * `ExpressionConfig.fromExpr(exprConfExpr)` resolves the serialised config and
  * `RST_ExpressionUtil.init(exprConf)` calls `GDALManager.init → gdal.AllRegister`
  * once per JVM before `RST_BinPoints.execute` calls `GetDriverByName("MEM")`.
  */
final case class RST_BinPointsAgg(
    xExpr: Expression,
    yExpr: Expression,
    zExpr: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    sridExpr: Expression,
    statisticExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[BinPointsAcc] {

    import RST_BinPointsAgg.{evalDouble, evalInt, evalString, evalExpr}

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean           = true
    override lazy val dataType: DataType     = RST_ExpressionUtil.tileDataType(BinaryType)
    override def prettyName: String          = RST_BinPointsAgg.name

    override def children: Seq[Expression] = Seq(
        xExpr, yExpr, zExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, sridExpr,
        statisticExpr
    )

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_BinPointsAgg = {
        require(nc.length == 11, s"RST_BinPointsAgg expects 11 children; got ${nc.length}")
        copy(
            xExpr = nc(0), yExpr = nc(1), zExpr = nc(2),
            xminExpr = nc(3), yminExpr = nc(4), xmaxExpr = nc(5), ymaxExpr = nc(6),
            widthPxExpr = nc(7), heightPxExpr = nc(8), sridExpr = nc(9),
            statisticExpr = nc(10)
        )
    }

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): BinPointsAcc = BinPointsAcc.empty

    override def update(buffer: BinPointsAcc, input: InternalRow): BinPointsAcc = {
        val xv = evalExpr(xExpr, input)
        val yv = evalExpr(yExpr, input)
        val zv = evalExpr(zExpr, input)
        if (xv == null || yv == null || zv == null) return buffer
        val x = coerceToDouble(xv, "x")
        val y = coerceToDouble(yv, "y")
        val z = coerceToDouble(zv, "z")
        buffer.add(x, y, z)
    }

    override def merge(a: BinPointsAcc, b: BinPointsAcc): BinPointsAcc = a.merge(b)

    override def eval(buffer: BinPointsAcc): Any = {
        if (buffer.points.isEmpty) return null
        // Initialise GDAL once per JVM — idempotent when already enabled (e.g. unit tests
        // that call gdal.AllRegister() in beforeAll, or a warm executor that ran a prior
        // GDAL expression).  Cold executor JVMs (no prior GDAL expression) reach here with
        // GDALManager.isEnabled == false and need the full init via ExpressionConfigExpr.
        if (!GDALManager.isEnabled) {
            val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
            RST_ExpressionUtil.init(exprConf)
        }
        val emptyRow = InternalRow.empty
        val xmin = evalDouble(xminExpr, emptyRow, "xmin")
        val ymin = evalDouble(yminExpr, emptyRow, "ymin")
        val xmax = evalDouble(xmaxExpr, emptyRow, "xmax")
        val ymax = evalDouble(ymaxExpr, emptyRow, "ymax")
        val widthPx = evalInt(widthPxExpr, emptyRow, "width_px")
        val heightPx = evalInt(heightPxExpr, emptyRow, "height_px")
        val srid = evalInt(sridExpr, emptyRow, "srid")
        val stat = evalString(statisticExpr, emptyRow, RST_BinPoints.DefaultStatistic)
        val x = buffer.points.map(_._1).toArray
        val y = buffer.points.map(_._2).toArray
        val z = buffer.points.map(_._3).toArray
        val bytes = RST_BinPoints.execute(x, y, z, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, stat)
        if (bytes == null) null else RST_BinPoints.tileRow(bytes)
    }

    override def serialize(b: BinPointsAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): BinPointsAcc = BinPointsAcc.deserialize(bytes)

    /** Coerce an arbitrary Catalyst numeric value to Double (mirrors RST_GridFromPointsAgg). */
    private def coerceToDouble(v: Any, label: String): Double = v match {
        case d: Double                               => d
        case f: Float                                => f.toDouble
        case i: Int                                  => i.toDouble
        case l: Long                                 => l.toDouble
        case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
        case other                                   =>
            throw new IllegalArgumentException(
                s"rst_binpoints_agg: $label column must be numeric; got ${other.getClass.getName}")
    }
}

/** Companion: SQL name `gbx_rst_binpoints_agg`, builder accepts 10 or 11 args. */
object RST_BinPointsAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_binpoints_agg"

    /** Evaluate an expression against the given row (centralized for clarity). */
    private[expressions] def evalExpr(e: Expression, row: InternalRow): Any = e.eval(row)

    private[expressions] def evalDouble(e: Expression, row: InternalRow, label: String): Double = {
        val v = evalExpr(e, row)
        if (v == null) throw new IllegalArgumentException(
            s"rst_binpoints_agg: $label must not be null")
        v match {
            case d: Double                               => d
            case f: Float                                => f.toDouble
            case i: Int                                  => i.toDouble
            case l: Long                                 => l.toDouble
            case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
            case other                                   =>
                throw new IllegalArgumentException(
                    s"rst_binpoints_agg: $label must be numeric; got ${other.getClass.getName}")
        }
    }

    private[expressions] def evalInt(e: Expression, row: InternalRow, label: String): Int = {
        val v = evalExpr(e, row)
        if (v == null) throw new IllegalArgumentException(
            s"rst_binpoints_agg: $label must not be null")
        v match {
            case i: Int  => i
            case l: Long => l.toInt
            case other   =>
                throw new IllegalArgumentException(
                    s"rst_binpoints_agg: $label must be INT or LONG; got ${other.getClass.getName}")
        }
    }

    private[expressions] def evalString(
        e: Expression, row: InternalRow, default: String
    ): String = {
        val v = evalExpr(e, row)
        v match {
            case s: UTF8String => s.toString
            case null          => default
            case other         =>
                throw new IllegalArgumentException(
                    s"rst_binpoints_agg: statistic must be STRING; got ${other.getClass.getName}")
        }
    }

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 10 => RST_BinPointsAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9),
            Literal(RST_BinPoints.DefaultStatistic)
        )
        case 11 => RST_BinPointsAgg(
            c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10)
        )
        case n  => throw new IllegalArgumentException(
            s"$name takes 10 or 11 arguments " +
            "(x, y, z, xmin, ymin, xmax, ymax, width_px, height_px, srid, [statistic]); " +
            s"got $n"
        )
    }
}
