package com.databricks.labs.gbx.vectorx.expressions

/** Generator: explode one (points, breaklines, tolerances, splitFinder, gridOrigin, grid) row into one
 *  output row per Z-valued grid cell center (WKB BINARY) whose center falls inside the TIN hull.
 *
 *  Delegates to:
 *    - [[com.databricks.labs.gbx.vectorx.jts.InterpolateElevation.pointGridOrigin]] to build the
 *      regular grid of cell-center points from an origin corner + cell counts + per-cell sizes.
 *    - [[com.databricks.labs.gbx.vectorx.jts.InterpolateElevation.interpolate]] to run a
 *      constrained Delaunay triangulation and Z-interpolate each grid point.
 *
 *  Points outside the TIN hull are dropped (no_data silently elided).
 *  Each emitted row is a single-column BINARY (WKB, Z-preserving via JTS.toWKB3).
 *
 *  The grid_origin geometry should carry its SRID; encode it as EWKB (e.g. `JTS.toEWKB`) or use an
 *  EWKT prefix (`SRID=32633;POINT(...)`) so that the SRID propagates to the output points.
 *  Plain WKB and plain WKT carry no SRID; in that case output points will have SRID 0.
 *
 *  Registered SQL name: `gbx_st_interpolateelevationgeom`.
 *
 *  Signature:
 *    gbx_st_interpolateelevationgeom(
 *        points_array      ARRAY<BINARY|STRING>,
 *        breaklines_array  ARRAY<BINARY|STRING>,
 *        merge_tolerance   DOUBLE,
 *        snap_tolerance    DOUBLE,
 *        split_point_finder STRING,
 *        grid_origin       BINARY|STRING,   -- a single POINT geometry (origin corner)
 *        grid_cols         INT,
 *        grid_rows         INT,
 *        cell_size_x       DOUBLE,
 *        cell_size_y       DOUBLE)
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

case class ST_InterpolateElevationGeom(
    pointsArray: Expression,
    breaklinesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    splitPointFinder: Expression,
    gridOrigin: Expression,
    gridCols: Expression,
    gridRows: Expression,
    cellSizeX: Expression,
    cellSizeY: Expression,
    modeExpr: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false

    override def elementSchema: StructType = ST_InterpolateElevationGeom.elementSchemaStatic

    override def children: Seq[Expression] =
        Seq(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder,
            gridOrigin, gridCols, gridRows, cellSizeX, cellSizeY, modeExpr)

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10))

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
                        s"st_interpolateelevationgeom: breaklines must be LineString geometries; got ${other.getClass.getName}")
                }
            }
        }

        val mergeTol = readDouble(mergeTolerance.eval(input), "merge_tolerance")
        val snapTol  = readDouble(snapTolerance.eval(input),  "snap_tolerance")

        val finderStr = splitPointFinder.eval(input) match {
            case s: UTF8String => s.toString
            case s: String     => s
            case null          => throw new IllegalArgumentException(
                "gbx_st_interpolateelevationgeom: split_point_finder must not be null")
            case other         => other.toString
        }
        val finder = TriangulationSplitPointTypeEnum.fromString(finderStr)

        val modeStr = modeExpr.eval(input) match {
            case s: UTF8String => s.toString
            case s: String     => s
            case null          => throw new IllegalArgumentException(
                "gbx_st_interpolateelevationgeom: mode must not be null")
            case other         => other.toString
        }

        val originGeom: Geometry = gridOrigin.eval(input) match {
            case b: Array[Byte] => JTS.fromWKB(b)
            case s: UTF8String  => JTS.fromWKT(s.toString)
            case s: String      => JTS.fromWKT(s)
            case null           => throw new IllegalArgumentException(
                "gbx_st_interpolateelevationgeom: grid_origin must not be null")
            case other          => throw new IllegalArgumentException(
                "gbx_st_interpolateelevationgeom: grid_origin must be BINARY (WKB) or STRING (WKT); " +
                s"got ${other.getClass.getName}")
        }
        val originX  = originGeom.getCoordinate.getX
        val originY  = originGeom.getCoordinate.getY
        val originSrid = originGeom.getSRID

        val cols      = readInt(gridCols.eval(input),    "grid_cols")
        val rows      = readInt(gridRows.eval(input),    "grid_rows")
        val cSizeX    = readDouble(cellSizeX.eval(input), "cell_size_x")
        val cSizeY    = readDouble(cellSizeY.eval(input), "cell_size_y")

        val mp   = JTS.multiPoint(pts)
        mp.setSRID(originSrid)
        val grid = InterpolateElevation.pointGridOrigin(originX, originY, cols, rows, cSizeX, cSizeY, originSrid)
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
                                "gbx_st_interpolateelevationgeom: geometry array element must be BINARY (WKB) or STRING (WKT); " +
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
            s"gbx_st_interpolateelevationgeom: $fieldName is null")
        case other               => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationgeom: $fieldName must be numeric; got $other")
    }

    private def readInt(v: Any, fieldName: String): Int = v match {
        case i: Int  => i
        case l: Long => l.toInt
        case null    => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationgeom: $fieldName is null")
        case other   => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationgeom: $fieldName must be INT or LONG; got $other")
    }
}

object ST_InterpolateElevationGeom extends WithExpressionInfo {

    /** Single-column element schema: one Z-valued WKB-encoded Point per row. */
    val elementSchemaStatic: StructType = StructType(Seq(
        StructField("elevation_point", BinaryType, nullable = false)
    ))

    override def name: String = "gbx_st_interpolateelevationgeom"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 10 => ST_InterpolateElevationGeom(
            c(0), c(1), c(2), c(3), c(4),
            c(5), c(6), c(7), c(8), c(9), Literal("constrained"))
        case 11 => ST_InterpolateElevationGeom(
            c(0), c(1), c(2), c(3), c(4),
            c(5), c(6), c(7), c(8), c(9), c(10))
        case n => throw new IllegalArgumentException(
            s"gbx_st_interpolateelevationgeom takes 10 or 11 arguments " +
            s"(points_array, breaklines_array, merge_tolerance, snap_tolerance, split_point_finder, " +
            s"grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y, [mode]); got $n"
        )
    }

    override def usageArgs: String =
        "points_array, breaklines_array, merge_tolerance, snap_tolerance, split_point_finder, " +
        "grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y, [mode]"

    override def description: String =
        "Generator: emit one row per Z-interpolated grid cell center (WKB BINARY) " +
        "from a constrained Delaunay TIN, using an origin-corner + cell-count + cell-size grid definition. " +
        "Cells whose centers fall outside the TIN hull are silently dropped."
}
