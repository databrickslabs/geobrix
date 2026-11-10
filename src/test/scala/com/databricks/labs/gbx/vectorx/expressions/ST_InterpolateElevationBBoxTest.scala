package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, Point}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for [[ST_InterpolateElevationBBox]] -- expression-level (no Spark session required).
 *
 *  Tilted plane: z = 2x + 3y + 5, sampled at the 4 corners of a 100x100 extent.
 *  A 10x10 grid over (0,0)-(100,100) in srid=32633 should yield exactly 100 Z-valued Points,
 *  each satisfying z == 2*x + 3*y + 5 within 1e-6.
 */
class ST_InterpolateElevationBBoxTest extends AnyFunSuite {

    /** z = 2x + 3y + 5 */
    private def planeZ(x: Double, y: Double): Double = 2.0 * x + 3.0 * y + 5.0

    /** Build a Literal wrapping an ARRAY<BINARY> of WKB-encoded geometries. */
    private def geomArrayLit(wkbs: Array[Byte]*): Literal = {
        val data = new GenericArrayData(wkbs.toArray.asInstanceOf[Array[Any]])
        Literal.create(data, ArrayType(BinaryType, containsNull = false))
    }

    /** Empty ARRAY<BINARY> literal. */
    private def emptyArrayLit: Literal =
        Literal.create(new GenericArrayData(Array.empty[Any]), ArrayType(BinaryType, containsNull = false))

    /** 4 corners of a 100x100 square with Z from the tilted plane. */
    private def cornerPoints: Seq[Array[Byte]] = {
        val corners = Seq((0.0, 0.0), (100.0, 0.0), (0.0, 100.0), (100.0, 100.0))
        corners.map { case (x, y) =>
            JTS.toWKB3(JTS.point(new Coordinate(x, y, planeZ(x, y))))
        }
    }

    /** Invoke the generator and collect all emitted rows. */
    private def evalExpr(expr: ST_InterpolateElevationBBox): Seq[InternalRow] =
        expr.eval(InternalRow.empty).iterator.toSeq

    // -----------------------------------------------------------------------
    // Test 1: 10x10 grid over tilted plane => exactly 100 points with correct Z
    // -----------------------------------------------------------------------
    test("st_interpolateelevationbbox emits 100 points with correct Z for tilted plane (Int args)") {
        val pts = cornerPoints
        val expr = ST_InterpolateElevationBBox(
            geomArrayLit(pts: _*),
            emptyArrayLit,
            Literal(0.0),                                                       // merge_tolerance
            Literal(0.01),                                                      // snap_tolerance
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType), // split_point_finder
            Literal(0.0),   // xmin
            Literal(0.0),   // ymin
            Literal(100.0), // xmax
            Literal(100.0), // ymax
            Literal(10),    // width_px  (Int)
            Literal(10),    // height_px (Int)
            Literal(32633), // srid      (Int)
            Literal("constrained")
        )

        val rows = evalExpr(expr)
        rows.length shouldBe 100

        rows.foreach { row =>
            val wkb = row.getBinary(0)
            wkb should not be null
            val geom = JTS.fromWKB(wkb)
            geom shouldBe a[Point]
            val pt = geom.asInstanceOf[Point]
            val expectedZ = planeZ(pt.getX, pt.getY)
            pt.getCoordinate.getZ should be(expectedZ +- 1e-6)
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: Long args variant (PySpark sends Long for IntegerType columns)
    // -----------------------------------------------------------------------
    test("st_interpolateelevationbbox accepts Long for width_px/height_px/srid and still yields 100 points") {
        val pts = cornerPoints
        val expr = ST_InterpolateElevationBBox(
            geomArrayLit(pts: _*),
            emptyArrayLit,
            Literal(0.0),
            Literal(0.01),
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
            Literal(0.0),
            Literal(0.0),
            Literal(100.0),
            Literal(100.0),
            Literal(10L),    // width_px  as Long
            Literal(10L),    // height_px as Long
            Literal(32633L), // srid      as Long
            Literal("constrained")
        )

        val rows = evalExpr(expr)
        rows.length shouldBe 100

        rows.foreach { row =>
            val wkb = row.getBinary(0)
            val pt = JTS.fromWKB(wkb).asInstanceOf[Point]
            val expectedZ = planeZ(pt.getX, pt.getY)
            pt.getCoordinate.getZ should be(expectedZ +- 1e-6)
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: builder rejects wrong arity
    // -----------------------------------------------------------------------
    test("ST_InterpolateElevationBBox.builder rejects wrong number of arguments") {
        val lit = Literal(0.0)
        an[IllegalArgumentException] should be thrownBy {
            ST_InterpolateElevationBBox.builder()(Seq(lit, lit, lit))
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: 12-arg builder defaults to constrained; conforming also yields 100
    // -----------------------------------------------------------------------
    test("ST_InterpolateElevationBBox.builder defaults the 12-arg call to constrained mode") {
        val pts = cornerPoints
        val built = ST_InterpolateElevationBBox.builder()(Seq(
            geomArrayLit(pts: _*), emptyArrayLit, Literal(0.0), Literal(0.01),
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
            Literal(0.0), Literal(0.0), Literal(100.0), Literal(100.0),
            Literal(10), Literal(10), Literal(32633)
        )).asInstanceOf[ST_InterpolateElevationBBox]

        built.modeExpr shouldBe Literal("constrained")
        evalExpr(built).length shouldBe 100
    }

    test("st_interpolateelevationbbox mode=conforming yields the 100-point grid for a tilted plane") {
        val pts = cornerPoints
        val expr = ST_InterpolateElevationBBox(
            geomArrayLit(pts: _*), emptyArrayLit, Literal(0.0), Literal(0.01),
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
            Literal(0.0), Literal(0.0), Literal(100.0), Literal(100.0),
            Literal(10), Literal(10), Literal(32633), Literal("conforming")
        )
        val rows = evalExpr(expr)
        rows.length shouldBe 100
        rows.foreach { row =>
            val pt = JTS.fromWKB(row.getBinary(0)).asInstanceOf[Point]
            pt.getCoordinate.getZ should be(planeZ(pt.getX, pt.getY) +- 1e-6)
        }
    }

    test("st_interpolateelevationbbox splits a break line into parts, as st_triangulate does") {
        // A multi-line adds each line, a polygon adds its ring and a point element is ignored: the same grid
        // as the parts given one by one, in both modes. Off-plane break line Z makes the answer depend on them.
        val wkt = new org.locationtech.jts.io.WKTReader()
        def wkb(s: String): Array[Byte] = JTS.toWKB3(wkt.read(s))
        val parts = Seq(
          "LINESTRING Z (10 20 500, 90 20 500)",
          "LINESTRING Z (10 80 -50, 90 80 -50)",
          "LINEARRING Z (40 40 300, 60 40 300, 60 60 300, 40 60 300, 40 40 300)"
        ).map(wkb)
        val grouped = Seq(
          "MULTILINESTRING Z ((10 20 500, 90 20 500), (10 80 -50, 90 80 -50))",
          "POLYGON Z ((40 40 300, 60 40 300, 60 60 300, 40 60 300, 40 40 300))",
          "POINT Z (50 50 999)"
        ).map(wkb)
        def run(breaks: Seq[Array[Byte]], mode: String): Seq[(Double, Double, Double)] =
            evalExpr(ST_InterpolateElevationBBox(
              geomArrayLit(cornerPoints: _*), geomArrayLit(breaks: _*), Literal(0.0), Literal(0.01),
              Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
              Literal(0.0), Literal(0.0), Literal(100.0), Literal(100.0),
              Literal(10), Literal(10), Literal(32633), Literal(mode)
            )).map { r =>
                val c = JTS.fromWKB(r.getBinary(0)).getCoordinate
                (c.x, c.y, c.getZ)
            }.sorted
        Seq("constrained", "conforming").foreach { mode =>
            val split = run(parts, mode)
            split.length shouldBe 100
            run(grouped, mode) shouldBe split
            // the break lines change the surface: it is not the corner plane any more
            split.exists { case (x, y, z) => math.abs(z - planeZ(x, y)) > 1.0 } shouldBe true
        }
    }
}
