package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.GeneratorContract
import com.databricks.labs.gbx.vectorx
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

/**
  * Structural contract for the vectorx generators: every row `eval` emits has exactly the columns
  * `elementSchema` declares, with values of the declared types (see [[GeneratorContract]]).
  */
class VectorGeneratorContractTest extends PlanTest with SilentSparkSession {

    private def check(sqlName: String, gen: CollectionGenerator): Unit = {
        assert(GeneratorContract.coverage.get(sqlName).contains(GeneratorContract.VectorSuite), s"$sqlName is not listed for this suite")
        GeneratorContract.assertConforms(sqlName, gen)
    }

    private def geomArrayLit(wkbs: Seq[Array[Byte]]): Literal =
        Literal.create(new GenericArrayData(wkbs.toArray.asInstanceOf[Array[Any]]), ArrayType(BinaryType, containsNull = false))

    private val emptyArrayLit: Literal =
        Literal.create(new GenericArrayData(Array.empty[Any]), ArrayType(BinaryType, containsNull = false))

    private val finder: Expression = Literal.create(UTF8String.fromString("NONENCROACHING"), StringType)

    /** 4 corners of a 100x100 square, Z = 2x + 3y + 5. */
    private val corners: Seq[Array[Byte]] =
        Seq((0.0, 0.0), (100.0, 0.0), (0.0, 100.0), (100.0, 100.0)).map { case (x, y) =>
            JTS.toWKB3(JTS.point(new Coordinate(x, y, 2.0 * x + 3.0 * y + 5.0)))
        }

    /** Generators are built on demand: the MVT writer needs the native readers initialised first. */
    private val cases: Seq[(String, () => CollectionGenerator)] = Seq(
      "gbx_st_triangulate" -> (() =>
          ST_Triangulate(geomArrayLit(corners), emptyArrayLit, Literal(0.01), Literal(0.01), finder, Literal("constrained"))
      ),
      "gbx_st_interpolateelevationbbox" -> (() =>
          ST_InterpolateElevationBBox(
            geomArrayLit(corners), emptyArrayLit, Literal(0.0), Literal(0.01), finder,
            Literal(0.0), Literal(0.0), Literal(100.0), Literal(100.0), Literal(10), Literal(10), Literal(32633), Literal("constrained")
          )
      ),
      "gbx_st_interpolateelevationgeom" -> (() => {
          val origin = JTS.point(new Coordinate(0.0, 0.0))
          origin.setSRID(32633)
          ST_InterpolateElevationGeom(
            geomArrayLit(corners), emptyArrayLit, Literal(0.0), Literal(0.01), finder,
            Literal.create(JTS.toEWKB(origin), BinaryType), Literal(10), Literal(10), Literal(10.0), Literal(10.0), Literal("constrained")
          )
      })
    )

    private def checkCase(sqlName: String): Unit = {
        val make = cases.collectFirst { case (n, f) if n == sqlName => f }.getOrElse(fail(s"no case for $sqlName"))
        check(sqlName, make())
    }

    test("st_triangulate emits exactly the declared triangle column") {
        checkCase("gbx_st_triangulate")
    }

    test("st_interpolateelevationbbox emits exactly the declared elevation_point column") {
        checkCase("gbx_st_interpolateelevationbbox")
    }

    test("st_interpolateelevationgeom emits exactly the declared elevation_point column") {
        checkCase("gbx_st_interpolateelevationgeom")
    }

    // The Column wrappers hand ColumnAdapter a positional Seq: a swapped argument would run, with another grid. Every
    // size and extent below is different, so a swap changes the points that come out.
    test("the TIN Column wrappers pass their arguments in the SQL order, with and without mode") {
        TestL1.installAll(spark)
        import org.apache.spark.sql.functions.{array, lit}
        val pts = array(corners.map(b => lit(b)): _*)
        val none = array().cast("array<binary>")
        val finderCol = lit("NONENCROACHING")
        val mode = lit("constrained")
        val one = spark.range(1)
        def points(df: org.apache.spark.sql.DataFrame): Set[(Double, Double)] =
            df.collect().map { r => val c = JTS.fromWKB(r.getAs[Array[Byte]]("elevation_point")).getCoordinate; (c.x, c.y) }.toSet

        val tri = Seq(
          vectorx.functions.st_triangulate(pts, none, lit(0.01), lit(0.01), finderCol),
          vectorx.functions.st_triangulate(pts, none, lit(0.01), lit(0.01), finderCol, mode)
        )
        tri.foreach(c => assert(one.select(c).count() == 2))

        // a 100 x 50 extent as 4 columns by 2 rows: cell centres x = 12.5, 37.5, 62.5, 87.5 and two distinct y
        val bbox = Seq(
          vectorx.functions.st_interpolateelevationbbox(
            pts, none, lit(0.0), lit(0.01), finderCol, lit(0.0), lit(0.0), lit(100.0), lit(50.0), lit(4), lit(2), lit(32633)
          ),
          vectorx.functions.st_interpolateelevationbbox(
            pts, none, lit(0.0), lit(0.01), finderCol, lit(0.0), lit(0.0), lit(100.0), lit(50.0), lit(4), lit(2), lit(32633), mode
          )
        )
        bbox.foreach { c =>
            val got = points(one.select(c))
            assert(got.size == 8 && got.map(_._1) == Set(12.5, 37.5, 62.5, 87.5) && got.map(_._2).size == 2)
        }

        // an origin at (0, 0) with 4 columns, 3 rows, cells 10 wide and 20 high: x = 5, 15, 25, 35 and y = 10, 30, 50
        val origin = JTS.point(new Coordinate(0.0, 0.0))
        origin.setSRID(32633)
        val geom = Seq(
          vectorx.functions.st_interpolateelevationgeom(
            pts, none, lit(0.0), lit(0.01), finderCol, lit(JTS.toEWKB(origin)), lit(4), lit(3), lit(10.0), lit(20.0)
          ),
          vectorx.functions.st_interpolateelevationgeom(
            pts, none, lit(0.0), lit(0.01), finderCol, lit(JTS.toEWKB(origin)), lit(4), lit(3), lit(10.0), lit(20.0), mode
          )
        )
        geom.foreach { c =>
            val got = points(one.select(c))
            assert(got == (for (x <- Seq(5.0, 15.0, 25.0, 35.0); y <- Seq(10.0, 30.0, 50.0)) yield (x, y)).toSet)
        }
    }

    // Declarative on purpose: the generators this suite covers are the ones in `cases`, so the check does
    // not depend on test order or on a `-z` filter.
    test("this suite covers every generator the contract assigns to it") {
        assert(cases.map(_._1).toSet == GeneratorContract.coveredBy(GeneratorContract.VectorSuite))
    }

    // The test above proves the table lists every generator; the tests before it evaluate the entries they name.
    // This one evaluates EVERY entry, so a case added to the table without its own test still meets the contract.
    test("every entry of the cases table is evaluated against the contract") {
        val evaluated = cases.map { case (sqlName, _) =>
            checkCase(sqlName)
            sqlName
        }
        assert(evaluated == cases.map(_._1) && evaluated.nonEmpty)
    }

}
