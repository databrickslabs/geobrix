package com.databricks.labs.gbx.vectorx.expressions

/** Generator: explode one (points, breaklines, tolerances, splitFinder, bbox, grid) row into one
 *  output row per Z-valued grid cell center (WKB BINARY) whose center falls inside the TIN hull.
 *
 *  Delegates to:
 *    - [[com.databricks.labs.gbx.vectorx.jts.InterpolateElevation.pointGridBBox]] to build the
 *      regular grid of cell-center points over the bbox.
 *    - [[com.databricks.labs.gbx.vectorx.jts.InterpolateElevation.interpolate]] to run a
 *      constrained Delaunay triangulation and Z-interpolate each grid point.
 *
 *  Points outside the TIN hull are dropped (no_data silently elided).
 *  Each emitted row is a single-column BINARY (WKB, Z-preserving via JTS.toWKB3).
 *
 *  Registered SQL name: `gbx_st_interpolateelevationbbox`.
 *
 *  Signature:
 *    gbx_st_interpolateelevationbbox(
 *        points_array     ARRAY<BINARY|STRING>,
 *        breaklines_array ARRAY<BINARY|STRING>,
 *        merge_tolerance  DOUBLE,
 *        snap_tolerance   DOUBLE,
 *        split_point_finder STRING,
 *        xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
 *        width_px  INT,
 *        height_px INT,
 *        srid      INT)
 *    -> rows of STRUCT<elevation_point BINARY>
 */
import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.jts.{InterpolateElevation, JTS, TriangulationSplitPointTypeEnum}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry, LineString}

case class ST_InterpolateElevationBBox(
    pointsArray: Expression,
    breaklinesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    splitPointFinder: Expression,
    xmin: Expression,
    ymin: Expression,
    xmax: Expression,
    ymax: Expression,
    widthPx: Expression,
    heightPx: Expression,
    srid: Expression,
    modeExpr: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false

    override def elementSchema: StructType = ST_InterpolateElevationBBox.elementSchemaStatic

    override def children: Seq[Expression] =
        Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder,
            xmin, ymin, xmax, ymax, widthPx, heightPx, srid, modeExpr)

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11), nc(12))

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val pointsVal = pointsArray.eval(input)
        if (pointsVal == null) return Iterator.empty

        val ptsElemType = pointsArray.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType
        val pts = geomsFromArrayData(pointsVal.asInstanceOf[ArrayData], ptsElemType)
        if (pts.isEmpty) return Iterator.empty

        val breaklines: Seq[LineString] = {
            val bVal = breaklinesArray.eval(input)
            if (bVal == null) Seq.empty
            else {
                val bElemType = breaklinesArray.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType
                geomsFromArrayData(bVal.asInstanceOf[ArrayData], bElemType).toSeq.map {
                    case l: LineString => l
                    case other => throw new IllegalArgumentException(
                        s"st_interpolateelevationbbox: breaklines must be LineString geometries; got ${other.getClass.getName}")
                }
            }
        }

        val mergeTol = readDouble(mergeTolerance.eval(input), "merge_tolerance")
        val snapTol  = readDouble(snapTolerance.eval(input),  "snap_tolerance")

        val finderStr = splitPointFinder.eval(input) match {
            case s: UTF8String => s.toString
            case s: String     => s
            case null          => throw new IllegalArgumentException(
                "gbx_st_interpolateelevationbbox: split_point_finder must not be null")
            case other         => other.toString
        }
        val finder = TriangulationSplitPointTypeEnum.fromString(finderStr)

        val modeStr = modeExpr.eval(input) match {
            case s: UTF8String => s.toString
            case s: String     => s
            case null          => throw new IllegalArgumentException(
                "gbx_st_interpolateelevationbbox: mode must not be null")
            case other         => other.toString
        }

        val xminVal  = readDouble(xmin.eval(input), "xmin")
        val yminVal  = readDouble(ymin.eval(input), "ymin")
        val xmaxVal  = readDouble(xmax.eval(input), "xmax")
        val ymaxVal  = readDouble(ymax.eval(input), "ymax")
        val widthVal  = readInt(widthPx.eval(input),  "width_px")
        val heightVal = readInt(heightPx.eval(input), "height_px")
        val sridVal   = readInt(srid.eval(input),     "srid")

        val mp   = JTS.multiPoint(pts)
        mp.setSRID(sridVal)
        val grid = InterpolateElevation.pointGridBBox(xminVal, yminVal, xmaxVal, ymaxVal,
                                                      widthVal, heightVal, sridVal)
        val interpolated = InterpolateElevation.interpolate(mp, breaklines, grid,
                                                            mergeTol, snapTol, Some(finder), modeStr)

        interpolated.iterator.map { p =>
            InternalRow(JTS.toWKB3(p))
        }
    }

    /** Decode an ArrayData of BINARY (WKB) or STRING (WKT) geometry elements.
     *
     *  @param data      the array payload from Catalyst eval
     *  @param elemType  the declared element DataType (BinaryType or StringType); used to call
     *                   the typed accessor so that UnsafeArrayData works correctly in Spark 4.0.
     */
    private def geomsFromArrayData(data: ArrayData, elemType: DataType): Array[Geometry] = {
        val n = data.numElements()
        val buf = new Array[Geometry](n)
        var out = 0
        var i = 0
        while (i < n) {
            if (!data.isNullAt(i)) {
                val geom = elemType match {
                    case BinaryType => JTS.fromWKB(data.getBinary(i))
                    case StringType => JTS.fromWKT(data.getUTF8String(i).toString)
                    case _ =>
                        data.get(i, elemType) match {
                            case b: Array[Byte] => JTS.fromWKB(b)
                            case s: UTF8String  => JTS.fromWKT(s.toString)
                            case other          => throw new IllegalArgumentException(
                                "gbx_st_interpolateelevationbbox: geometry array element must be BINARY (WKB) or STRING (WKT); " +
                                s"got ${if (other == null) "null" else other.getClass.getName}")
                        }
                }
                buf(out) = geom
                out += 1
            }
            i += 1
        }
        java.util.Arrays.copyOf(buf, out)
    }

    private def readDouble(v: Any, fieldName: String): Double = v match {
        case d: java.lang.Double => d.doubleValue
        case f: java.lang.Float  => f.toDouble
        case d: Double           => d
        case i: Int              => i.toDouble
        case l: Long             => l.toDouble
        case null                => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationbbox: $fieldName is null")
        case other               => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationbbox: $fieldName must be numeric; got $other")
    }

    private def readInt(v: Any, fieldName: String): Int = v match {
        case i: Int  => i
        case l: Long => l.toInt
        case null    => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationbbox: $fieldName is null")
        case other   => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationbbox: $fieldName must be INT or LONG; got $other")
    }
}

object ST_InterpolateElevationBBox extends WithExpressionInfo {

    /** Single-column element schema: one Z-valued WKB-encoded Point per row. */
    val elementSchemaStatic: StructType = StructType(Seq(
        StructField("elevation_point", BinaryType, nullable = false)
    ))

    override def name: String = "gbx_st_interpolateelevationbbox"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 12 => ST_InterpolateElevationBBox(
            c(0), c(1), c(2), c(3), c(4),
            c(5), c(6), c(7), c(8),
            c(9), c(10), c(11), Literal("constrained"))
        case 13 => ST_InterpolateElevationBBox(
            c(0), c(1), c(2), c(3), c(4),
            c(5), c(6), c(7), c(8),
            c(9), c(10), c(11), c(12))
        case n => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationbbox takes 12 or 13 arguments " +
            s"(points_array, breaklines_array, merge_tolerance, snap_tolerance, split_point_finder, " +
            s"xmin, ymin, xmax, ymax, width_px, height_px, srid, [mode]); got $n"
        )
    }

    override def usageArgs: String =
        "points_array, breaklines_array, merge_tolerance, snap_tolerance, split_point_finder, " +
        "xmin, ymin, xmax, ymax, width_px, height_px, srid, [mode]"

    override def description: String =
        "Generator: emit one row per Z-interpolated grid cell center (WKB BINARY) " +
        "from a constrained Delaunay TIN over the given bbox+pixel grid. " +
        "Cells whose centers fall outside the TIN hull are silently dropped."
}
