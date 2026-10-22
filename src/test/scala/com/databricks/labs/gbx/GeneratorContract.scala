package com.databricks.labs.gbx

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.Assertions._

/**
  * Shared contract for the heavyweight generators (every registered `CollectionGenerator`).
  *
  * Spark projects a generator's rows through its `elementSchema`, so a value emitted beyond the declared
  * columns is silently dropped, and a value of the wrong shape fails with an opaque ClassCastException.
  * Each generator therefore has a contract test that evaluates it and checks the rows against its schema.
  *
  * [[coverage]] names the contract suite for every generator. `GeneratorCoverageGuardTest` compares it with
  * the generators actually registered, so a generator added without a contract test fails the build.
  */
object GeneratorContract {

    val GridSuite = "GridGeneratorContractTest"
    val VectorSuite = "VectorGeneratorContractTest"
    val RasterSuite = "RST_GeneratorRowShapeTest"

    /** SQL function name of every heavyweight generator -> the suite whose tests evaluate it. */
    val coverage: Map[String, String] = Map(
      "gbx_bng_kringexplode"            -> GridSuite,
      "gbx_bng_kloopexplode"            -> GridSuite,
      "gbx_bng_geomkringexplode"        -> GridSuite,
      "gbx_bng_geomkloopexplode"        -> GridSuite,
      "gbx_bng_tessellateexplode"       -> GridSuite,
      "gbx_custom_geomkringexplode"     -> GridSuite,
      "gbx_custom_geomkloopexplode"     -> GridSuite,
      "gbx_quadbin_geomkringexplode"    -> GridSuite,
      "gbx_quadbin_geomkloopexplode"    -> GridSuite,
      "gbx_st_triangulate"              -> VectorSuite,
      "gbx_st_interpolateelevationbbox" -> VectorSuite,
      "gbx_st_interpolateelevationgeom" -> VectorSuite,
      "gbx_st_asmvt_pyramid"            -> VectorSuite,
      "gbx_rst_retile"                  -> RasterSuite,
      "gbx_rst_separatebands"           -> RasterSuite,
      "gbx_rst_tooverlappingtiles"      -> RasterSuite,
      "gbx_rst_maketiles"               -> RasterSuite,
      "gbx_rst_h3_tessellate"           -> RasterSuite,
      "gbx_rst_quadbin_tessellate"      -> RasterSuite,
      "gbx_rst_bng_tessellate"          -> RasterSuite,
      "gbx_rst_custom_tessellate"       -> RasterSuite,
      "gbx_rst_xyzpyramid"              -> RasterSuite
    )

    /** SQL names the given suite is responsible for. */
    def coveredBy(suite: String): Set[String] = coverage.collect { case (name, s) if s == suite => name }.toSet

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

    /** Evaluates `gen` with no input row and checks every emitted row against its `elementSchema`. */
    def assertConforms(label: String, gen: CollectionGenerator): Unit = {
        val rows = gen.eval(InternalRow.empty).iterator.toSeq
        assert(rows.nonEmpty, s"$label: fixture emitted no rows, so the contract was not exercised")
        val schema = gen.elementSchema
        rows.foreach { r =>
            assert(
              r.numFields == schema.length,
              s"$label: eval emits ${r.numFields} value(s) but elementSchema declares ${schema.length}: ${schema.simpleString}"
            )
            schema.fields.zipWithIndex.foreach { case (f, i) =>
                val v = r.get(i, f.dataType)
                assert(
                  valueConforms(v, f.dataType),
                  s"$label: column '${f.name}' declared ${f.dataType.simpleString} but eval emits ${if (v == null) "null" else v.getClass.getName}"
                )
            }
        }
        // The projection GenerateExec applies to generator output must accept every emitted row.
        val proj = UnsafeProjection.create(schema.fields.map(_.dataType))
        rows.foreach(r => proj(r))
    }
}
