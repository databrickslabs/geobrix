package com.databricks.labs.gbx.vectorx.expressions

/** Generator: explode one (points, breaklines, tolerances, splitFinder) row into one output
 *  row per TIN triangle polygon (WKB BINARY).
 *
 *  Delegates to [[com.databricks.labs.gbx.vectorx.jts.InterpolateElevation.triangulate]],
 *  which runs a constrained Delaunay triangulation and returns the triangle Polygons as JTS
 *  geometries. Each polygon is serialised to 2D WKB and emitted as a single-column row.
 *
 *  Registered SQL name: `gbx_st_triangulate`.
 *
 *  Signature:
 *    gbx_st_triangulate(points_geom    ARRAY<BINARY|STRING>,
 *                       breaklines_geom ARRAY<BINARY|STRING>,
 *                       merge_tolerance DOUBLE,
 *                       snap_tolerance  DOUBLE,
 *                       split_point_finder STRING)
 *    -> rows of STRUCT<triangle BINARY>
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
import org.locationtech.jts.geom.Geometry

case class ST_Triangulate(
    pointsArray: Expression,
    breaklinesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    splitPointFinder: Expression,
    modeExpr: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false

    override def elementSchema: StructType = ST_Triangulate.elementSchemaStatic

    override def children: Seq[Expression] =
        Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder, modeExpr)

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5))

    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val pointsVal = pointsArray.eval(input)
        if (pointsVal == null) return Iterator.empty

        val ptsElemType = pointsArray.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType
        val pts = geomsFromArrayData(pointsVal.asInstanceOf[ArrayData], ptsElemType)
        if (pts.isEmpty) return Iterator.empty

        val breaklines: Seq[Geometry] = {
            val bVal = breaklinesArray.eval(input)
            if (bVal == null) Seq.empty
            else {
                val bElemType = breaklinesArray.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType
                geomsFromArrayData(bVal.asInstanceOf[ArrayData], bElemType).toSeq
            }
        }

        val mergeTol = readDouble(mergeTolerance.eval(input), "merge_tolerance")
        val snapTol  = readDouble(snapTolerance.eval(input),  "snap_tolerance")

        val finderStr = splitPointFinder.eval(input) match {
            case s: UTF8String  => s.toString
            case s: String      => s
            case null           => throw new IllegalArgumentException(
                "gbx_st_triangulate: split_point_finder must not be null")
            case other          => other.toString
        }
        val finder = TriangulationSplitPointTypeEnum.fromString(finderStr)

        val modeStr = modeExpr.eval(input) match {
            case s: UTF8String => s.toString
            case s: String     => s
            case null          => throw new IllegalArgumentException(
                "gbx_st_triangulate: mode must not be null")
            case other         => other.toString
        }

        val mp = JTS.multiPoint(pts)
        val triangles = InterpolateElevation.triangulate(mp, breaklines, mergeTol, snapTol, Some(finder), modeStr)

        triangles.iterator.map { t =>
            InternalRow(JTS.toWKB(t))
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
                                "gbx_st_triangulate: geometry array element must be BINARY (WKB) or STRING (WKT); " +
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
        case d: java.lang.Double  => d.doubleValue
        case f: java.lang.Float   => f.toDouble
        case d: Double            => d
        case i: Int               => i.toDouble
        case l: Long              => l.toDouble
        case null                 => throw new IllegalArgumentException(
            s"gbx_st_triangulate: $fieldName is null")
        case other                => throw new IllegalArgumentException(
            s"gbx_st_triangulate: $fieldName must be DOUBLE; got $other")
    }
}

object ST_Triangulate extends WithExpressionInfo {

    /** Single-column element schema: one WKB-encoded triangle polygon per row. */
    val elementSchemaStatic: StructType = StructType(Seq(
        StructField("triangle", BinaryType, nullable = false)
    ))

    override def name: String = "gbx_st_triangulate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 5 => ST_Triangulate(c(0), c(1), c(2), c(3), c(4), Literal("constrained"))
        case 6 => ST_Triangulate(c(0), c(1), c(2), c(3), c(4), c(5))
        case n => throw new IllegalArgumentException(
            s"gbx_st_triangulate takes 5 or 6 arguments " +
            s"(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, [mode]); got $n"
        )
    }

    override def usageArgs: String =
        "points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, [mode]"

    override def description: String =
        "Generator: emit one row per TIN triangle polygon (WKB BINARY) from a constrained Delaunay triangulation."
}
