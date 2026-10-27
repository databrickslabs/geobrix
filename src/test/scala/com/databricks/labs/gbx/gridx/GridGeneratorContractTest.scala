package com.databricks.labs.gbx.gridx

import com.databricks.labs.gbx.GeneratorContract
import com.databricks.labs.gbx.gridx.bng.generators._
import com.databricks.labs.gbx.gridx.custom.Custom_GridSpec
import com.databricks.labs.gbx.gridx.custom.generators.{Custom_GeometryKLoopExplode, Custom_GeometryKRingExplode}
import com.databricks.labs.gbx.gridx.grid.{BNG, GeomDilation}
import com.databricks.labs.gbx.gridx.quadbin.generators.{Quadbin_GeometryKLoopExplode, Quadbin_GeometryKRingExplode}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/**
  * Structural contract for every gridx generator: each row `eval` emits must have exactly the
  * columns `elementSchema` declares, with values of the declared types. Spark's GenerateExec
  * projects generator rows through `elementSchema`, so a generator that emits more values than it
  * declares has the extras silently dropped (this is how heavy `gbx_bng_tessellateexplode` lost its
  * `core` and `chip` columns).
  */
class GridGeneratorContractTest extends AnyFunSuite {

    private val bngWkt = "POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
    private val bngWkb = JTS.toWKB(JTS.fromWKT(bngWkt))
    private val lonLatWkt = "POLYGON ((-0.2 51.4, 0.0 51.4, 0.0 51.6, -0.2 51.6, -0.2 51.4))"
    private val lonLatWkb = JTS.toWKB(JTS.fromWKT(lonLatWkt))

    private def str(s: String): Expression = Literal.create(s, StringType)
    private def bin(b: Array[Byte]): Expression = Literal.create(b, BinaryType)
    private def int(i: Int): Expression = Literal(i)

    private val customGrid: Expression = {
        val row = InternalRow(0L, 100L, 0L, 100L, 2, 10, 10, 32633)
        Literal(row, Custom_GridSpec.gridStructType)
    }

    private val mode: Expression = str(GeomDilation.DEFAULT_MODE)
    private val coverage: Expression = str(GeomDilation.DEFAULT_COVERAGE)

    private val cell = "TQ3080"
    private val cellLong = Literal(BNG.parse(cell))
    private val customGeom = str("POLYGON ((20 20, 40 20, 40 40, 20 40, 20 20))")

    /** (SQL name, label, generator) for every case this suite evaluates. */
    private val cases: Seq[(String, String, CollectionGenerator)] = Seq(
      ("gbx_bng_kringexplode", "bng_kringexplode(string)", BNG_KRingExplode(str(cell), int(1))),
      ("gbx_bng_kringexplode", "bng_kringexplode(long)", BNG_KRingExplode(cellLong, int(1))),
      ("gbx_bng_kloopexplode", "bng_kloopexplode(string)", BNG_KLoopExplode(str(cell), int(1))),
      ("gbx_bng_kloopexplode", "bng_kloopexplode(long)", BNG_KLoopExplode(cellLong, int(1))),
      ("gbx_bng_geomkringexplode", "bng_geomkringexplode(wkb)", BNG_GeometryKRingExplode(bin(bngWkb), int(3), int(1), mode, coverage)),
      ("gbx_bng_geomkringexplode", "bng_geomkringexplode(wkt)", BNG_GeometryKRingExplode(str(bngWkt), int(3), int(1), mode, coverage)),
      ("gbx_bng_geomkloopexplode", "bng_geomkloopexplode(wkb)", BNG_GeometryKLoopExplode(bin(bngWkb), int(3), int(1), mode, coverage)),
      ("gbx_bng_geomkloopexplode", "bng_geomkloopexplode(wkt)", BNG_GeometryKLoopExplode(str(bngWkt), int(3), int(1), mode, coverage)),
      ("gbx_bng_tessellateexplode", "bng_tessellateexplode(wkb, keep=true)", BNG_TessellateExplode(bin(bngWkb), int(3), Literal(true))),
      ("gbx_bng_tessellateexplode", "bng_tessellateexplode(wkt, keep=true)", BNG_TessellateExplode(str(bngWkt), int(3), Literal(true))),
      ("gbx_bng_tessellateexplode", "bng_tessellateexplode(wkb, keep=false)", BNG_TessellateExplode(bin(bngWkb), int(3), Literal(false))),
      ("gbx_bng_tessellateexplode", "bng_tessellateexplode(wkt, keep=false)", BNG_TessellateExplode(str(bngWkt), int(3), Literal(false))),
      ("gbx_custom_geomkringexplode", "custom_geomkringexplode", Custom_GeometryKRingExplode(customGeom, customGrid, int(2), int(1), mode, coverage)),
      ("gbx_custom_geomkloopexplode", "custom_geomkloopexplode", Custom_GeometryKLoopExplode(customGeom, customGrid, int(2), int(1), mode, coverage)),
      ("gbx_quadbin_geomkringexplode", "quadbin_geomkringexplode(wkb)", Quadbin_GeometryKRingExplode(bin(lonLatWkb), int(8), int(1), mode, coverage)),
      ("gbx_quadbin_geomkringexplode", "quadbin_geomkringexplode(wkt)", Quadbin_GeometryKRingExplode(str(lonLatWkt), int(8), int(1), mode, coverage)),
      ("gbx_quadbin_geomkloopexplode", "quadbin_geomkloopexplode(wkb)", Quadbin_GeometryKLoopExplode(bin(lonLatWkb), int(8), int(1), mode, coverage)),
      ("gbx_quadbin_geomkloopexplode", "quadbin_geomkloopexplode(wkt)", Quadbin_GeometryKLoopExplode(str(lonLatWkt), int(8), int(1), mode, coverage))
    )

    private def checkCase(sqlName: String, label: String, gen: CollectionGenerator): Unit = {
        assert(GeneratorContract.coverage.get(sqlName).contains(GeneratorContract.GridSuite), s"$sqlName is not listed for this suite")
        GeneratorContract.assertConforms(label, gen)
    }

    private def check(prefix: String): Unit = {
        val selected = cases.filter(_._1.startsWith(prefix))
        assert(selected.nonEmpty, s"no cases for $prefix")
        selected.foreach { case (sqlName, label, gen) => checkCase(sqlName, label, gen) }
    }

    test("BNG k-ring / k-loop explode emit exactly the declared cellid column (STRING and LONG cells)") {
        check("gbx_bng_kring")
        check("gbx_bng_kloop")
    }

    test("BNG geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        check("gbx_bng_geomk")
    }

    test("BNG tessellate explode emits exactly the declared cellid, core and chip columns") {
        check("gbx_bng_tessellate")
        // The chip is WKB (BINARY) whatever the input geometry type, like gbx_bng_tessellate and the lightweight tier.
        val expected = Seq(("cellid", StringType), ("core", BooleanType), ("chip", BinaryType))
        for (geom <- Seq(bin(bngWkb), str(bngWkt))) {
            val schema = BNG_TessellateExplode(geom, int(3), Literal(true)).elementSchema
            assert(schema.fields.map(f => (f.name, f.dataType)).toSeq == expected)
        }
    }

    test("Custom geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        check("gbx_custom_")
    }

    test("Quadbin geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        check("gbx_quadbin_")
    }

    // Declarative on purpose: the generators this suite covers are the ones in `cases`, so the check does
    // not depend on test order or on a `-z` filter.
    test("this suite covers every generator the contract assigns to it") {
        assert(cases.map(_._1).toSet == GeneratorContract.coveredBy(GeneratorContract.GridSuite))
    }

    // The test above proves the table lists every generator; the family tests evaluate the entries whose name starts
    // with the prefix they pass. This one evaluates EVERY entry, so a case whose name no family test selects still
    // meets the contract.
    test("every entry of the cases table is evaluated against the contract") {
        val evaluated = cases.map { case (sqlName, label, gen) =>
            checkCase(sqlName, label, gen)
            label
        }
        assert(evaluated == cases.map(_._2) && evaluated.nonEmpty)
    }

}
