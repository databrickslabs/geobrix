package com.databricks.labs.gbx.vectorx.expressions

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
      }),
      "gbx_st_asmvt_pyramid" -> (() => {
          vectorx.functions.register(spark) // initialises the native readers the MVT writer needs
          val poly = new GeometryFactory().createPolygon(
            Array(
              new Coordinate(-30.0, 10.0), new Coordinate(30.0, 10.0), new Coordinate(30.0, 20.0),
              new Coordinate(-30.0, 20.0), new Coordinate(-30.0, 10.0)
            )
          )
          val attrsType = StructType(Seq(StructField("name", StringType), StructField("id", LongType)))
          val attrs = Literal(InternalRow(UTF8String.fromString("region-a"), 1L), attrsType)
          ST_AsMvtPyramid(Literal.create(JTS.toWKB(poly), BinaryType), attrs, Literal(2), Literal(2), Literal("regions"))
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

    test("st_asmvt_pyramid emits exactly the declared tile struct") {
        checkCase("gbx_st_asmvt_pyramid")
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
