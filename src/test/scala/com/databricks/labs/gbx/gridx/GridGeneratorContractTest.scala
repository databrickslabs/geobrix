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

import scala.collection.mutable

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

    private val exercised = mutable.Set.empty[String]

    private def str(s: String): Expression = Literal.create(s, StringType)
    private def bin(b: Array[Byte]): Expression = Literal.create(b, BinaryType)
    private def int(i: Int): Expression = Literal(i)

    private val customGrid: Expression = {
        val row = InternalRow(0L, 100L, 0L, 100L, 2, 10, 10, 32633)
        Literal(row, Custom_GridSpec.gridStructType)
    }

    /** Checks `gen` against its schema and records the SQL name as covered by this suite. */
    private def check(sqlName: String, label: String, gen: CollectionGenerator): Unit = {
        assert(GeneratorContract.coverage.get(sqlName).contains(GeneratorContract.GridSuite), s"$sqlName is not listed for this suite")
        exercised += sqlName
        GeneratorContract.assertConforms(label, gen)
    }

    private val mode: Expression = str(GeomDilation.DEFAULT_MODE)
    private val coverage: Expression = str(GeomDilation.DEFAULT_COVERAGE)

    test("BNG k-ring / k-loop explode emit exactly the declared cellid column (STRING and LONG cells)") {
        val cell = "TQ3080"
        val cellLong = Literal(BNG.parse(cell))
        check("gbx_bng_kringexplode", "bng_kringexplode(string)", BNG_KRingExplode(str(cell), int(1)))
        check("gbx_bng_kringexplode", "bng_kringexplode(long)", BNG_KRingExplode(cellLong, int(1)))
        check("gbx_bng_kloopexplode", "bng_kloopexplode(string)", BNG_KLoopExplode(str(cell), int(1)))
        check("gbx_bng_kloopexplode", "bng_kloopexplode(long)", BNG_KLoopExplode(cellLong, int(1)))
    }

    test("BNG geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        check("gbx_bng_geomkringexplode", "bng_geomkringexplode(wkb)", BNG_GeometryKRingExplode(bin(bngWkb), int(3), int(1), mode, coverage))
        check("gbx_bng_geomkringexplode", "bng_geomkringexplode(wkt)", BNG_GeometryKRingExplode(str(bngWkt), int(3), int(1), mode, coverage))
        check("gbx_bng_geomkloopexplode", "bng_geomkloopexplode(wkb)", BNG_GeometryKLoopExplode(bin(bngWkb), int(3), int(1), mode, coverage))
        check("gbx_bng_geomkloopexplode", "bng_geomkloopexplode(wkt)", BNG_GeometryKLoopExplode(str(bngWkt), int(3), int(1), mode, coverage))
    }

    test("BNG tessellate explode emits exactly the declared cellid, core and chip columns") {
        for (keep <- Seq(true, false)) {
            check("gbx_bng_tessellateexplode", s"bng_tessellateexplode(wkb, keep=$keep)", BNG_TessellateExplode(bin(bngWkb), int(3), Literal(keep)))
            check("gbx_bng_tessellateexplode", s"bng_tessellateexplode(wkt, keep=$keep)", BNG_TessellateExplode(str(bngWkt), int(3), Literal(keep)))
        }
        // The chip is WKB (BINARY) whatever the input geometry type, like gbx_bng_tessellate and the lightweight tier.
        val expected = Seq(("cellid", StringType), ("core", BooleanType), ("chip", BinaryType))
        for (geom <- Seq(bin(bngWkb), str(bngWkt))) {
            val schema = BNG_TessellateExplode(geom, int(3), Literal(true)).elementSchema
            assert(schema.fields.map(f => (f.name, f.dataType)).toSeq == expected)
        }
    }

    test("Custom geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        val geom = str("POLYGON ((20 20, 40 20, 40 40, 20 40, 20 20))")
        check("gbx_custom_geomkringexplode", "custom_geomkringexplode", Custom_GeometryKRingExplode(geom, customGrid, int(2), int(1), mode, coverage))
        check("gbx_custom_geomkloopexplode", "custom_geomkloopexplode", Custom_GeometryKLoopExplode(geom, customGrid, int(2), int(1), mode, coverage))
    }

    test("Quadbin geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        check("gbx_quadbin_geomkringexplode", "quadbin_geomkringexplode(wkb)", Quadbin_GeometryKRingExplode(bin(lonLatWkb), int(8), int(1), mode, coverage))
        check("gbx_quadbin_geomkringexplode", "quadbin_geomkringexplode(wkt)", Quadbin_GeometryKRingExplode(str(lonLatWkt), int(8), int(1), mode, coverage))
        check("gbx_quadbin_geomkloopexplode", "quadbin_geomkloopexplode(wkb)", Quadbin_GeometryKLoopExplode(bin(lonLatWkb), int(8), int(1), mode, coverage))
        check("gbx_quadbin_geomkloopexplode", "quadbin_geomkloopexplode(wkt)", Quadbin_GeometryKLoopExplode(str(lonLatWkt), int(8), int(1), mode, coverage))
    }

    // Keep last: scalatest runs a suite's tests in registration order.
    test("this suite exercised every generator the contract assigns to it") {
        assert(exercised.toSet == GeneratorContract.coveredBy(GeneratorContract.GridSuite))
    }

}
