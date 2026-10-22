package com.databricks.labs.gbx.gridx

import com.databricks.labs.gbx.gridx.bng.generators._
import com.databricks.labs.gbx.gridx.custom.Custom_GridSpec
import com.databricks.labs.gbx.gridx.custom.generators.{Custom_GeometryKLoopExplode, Custom_GeometryKRingExplode}
import com.databricks.labs.gbx.gridx.grid.{BNG, GeomDilation}
import com.databricks.labs.gbx.gridx.quadbin.generators.{Quadbin_GeometryKLoopExplode, Quadbin_GeometryKRingExplode}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
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

    private def valueConforms(v: Any, dt: DataType): Boolean = (v, dt) match {
        case (null, _)                    => true
        case (_: UTF8String, StringType)  => true
        case (_: Long, LongType)          => true
        case (_: Int, IntegerType)        => true
        case (_: Boolean, BooleanType)    => true
        case (_: Array[Byte], BinaryType) => true
        case (r: InternalRow, s: StructType) =>
            r.numFields == s.length && s.fields.zipWithIndex.forall { case (f, i) => valueConforms(r.get(i, f.dataType), f.dataType) }
        case _ => false
    }

    private def assertConforms(name: String, gen: CollectionGenerator): Unit = {
        val rows = gen.eval(InternalRow.empty).iterator.toSeq
        assert(rows.nonEmpty, s"$name: fixture emitted no rows, so the contract was not exercised")
        val schema = gen.elementSchema
        rows.foreach { r =>
            assert(r.numFields == schema.length, s"$name: eval emits ${r.numFields} value(s) but elementSchema declares ${schema.length}: ${schema.simpleString}")
            schema.fields.zipWithIndex.foreach { case (f, i) =>
                val v = r.get(i, f.dataType)
                assert(valueConforms(v, f.dataType), s"$name: column '${f.name}' declared ${f.dataType.simpleString} but eval emits ${if (v == null) "null" else v.getClass.getName}")
            }
        }
        // The projection GenerateExec applies to generator output must accept every emitted row.
        val proj = UnsafeProjection.create(schema.fields.map(_.dataType))
        rows.foreach(r => proj(r))
    }

    private val mode: Expression = str(GeomDilation.DEFAULT_MODE)
    private val coverage: Expression = str(GeomDilation.DEFAULT_COVERAGE)

    test("BNG k-ring / k-loop explode emit exactly the declared cellid column (STRING and LONG cells)") {
        val cell = "TQ3080"
        val cellLong = Literal(BNG.parse(cell))
        assertConforms("bng_kringexplode(string)", BNG_KRingExplode(str(cell), int(1)))
        assertConforms("bng_kringexplode(long)", BNG_KRingExplode(cellLong, int(1)))
        assertConforms("bng_kloopexplode(string)", BNG_KLoopExplode(str(cell), int(1)))
        assertConforms("bng_kloopexplode(long)", BNG_KLoopExplode(cellLong, int(1)))
    }

    test("BNG geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        assertConforms("bng_geomkringexplode(wkb)", BNG_GeometryKRingExplode(bin(bngWkb), int(3), int(1), mode, coverage))
        assertConforms("bng_geomkringexplode(wkt)", BNG_GeometryKRingExplode(str(bngWkt), int(3), int(1), mode, coverage))
        assertConforms("bng_geomkloopexplode(wkb)", BNG_GeometryKLoopExplode(bin(bngWkb), int(3), int(1), mode, coverage))
        assertConforms("bng_geomkloopexplode(wkt)", BNG_GeometryKLoopExplode(str(bngWkt), int(3), int(1), mode, coverage))
    }

    test("BNG tessellate explode emits exactly the declared cellid, core and chip columns") {
        for (keep <- Seq(true, false)) {
            assertConforms(s"bng_tessellateexplode(wkb, keep=$keep)", BNG_TessellateExplode(bin(bngWkb), int(3), Literal(keep)))
            assertConforms(s"bng_tessellateexplode(wkt, keep=$keep)", BNG_TessellateExplode(str(bngWkt), int(3), Literal(keep)))
        }
        // The chip column's declared type follows the input geometry type.
        assert(BNG_TessellateExplode(bin(bngWkb), int(3), Literal(true)).elementSchema.fields.map(f => (f.name, f.dataType)).toSeq ==
            Seq(("cellid", StringType), ("core", BooleanType), ("chip", BinaryType)))
        assert(BNG_TessellateExplode(str(bngWkt), int(3), Literal(true)).elementSchema.fields.map(f => (f.name, f.dataType)).toSeq ==
            Seq(("cellid", StringType), ("core", BooleanType), ("chip", StringType)))
    }

    test("Custom geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        val geom = str("POLYGON ((20 20, 40 20, 40 40, 20 40, 20 20))")
        assertConforms("custom_geomkringexplode", Custom_GeometryKRingExplode(geom, customGrid, int(2), int(1), mode, coverage))
        assertConforms("custom_geomkloopexplode", Custom_GeometryKLoopExplode(geom, customGrid, int(2), int(1), mode, coverage))
    }

    test("Quadbin geometry k-ring / k-loop explode emit exactly the declared cellid column") {
        assertConforms("quadbin_geomkringexplode(wkb)", Quadbin_GeometryKRingExplode(bin(lonLatWkb), int(8), int(1), mode, coverage))
        assertConforms("quadbin_geomkringexplode(wkt)", Quadbin_GeometryKRingExplode(str(lonLatWkt), int(8), int(1), mode, coverage))
        assertConforms("quadbin_geomkloopexplode(wkb)", Quadbin_GeometryKLoopExplode(bin(lonLatWkb), int(8), int(1), mode, coverage))
        assertConforms("quadbin_geomkloopexplode(wkt)", Quadbin_GeometryKLoopExplode(str(lonLatWkt), int(8), int(1), mode, coverage))
    }

}
