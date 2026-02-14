/*
 * Compile-time validation for Scala API Reference examples (docs/docs/api/scala.mdx).
 *
 * Per documentation-test-validation rule: Doc Tests = Compilation Tests.
 * We do NOT execute examples (no SparkSession, file I/O, or SQL execution).
 * If this compiles, the documented API is valid.
 */
package docs.tests.scala.api

import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class ScalaApiExamplesDocTest extends AnyFunSuite {

  // Compile-only: same imports as docs; reference API so compiler checks signatures
  test("RasterX imports and register signature compile") {
    val _: SparkSession => Unit = rx.register _
    succeed
  }

  test("RasterX accessor signatures compile") {
    val _: Column = rx.rst_boundingbox(col("tile"))
    val _: Column = rx.rst_width(col("tile"))
    val _: Column = rx.rst_height(col("tile"))
    val _: Column = rx.rst_numbands(col("tile"))
    val _: Column = rx.rst_metadata(col("tile"))
    succeed
  }

  test("RasterX rst_clip three-arg signature compiles") {
    val _: Column = rx.rst_clip(col("tile"), col("clip"), lit(true))
    succeed
  }

  test("GridX register and cellarea compile") {
    val _: SparkSession => Unit = bx.register _
    succeed
  }

  test("VectorX register and legacy API compile") {
    val _: SparkSession => Unit = vx.register _
    val _: Column = vx.st_legacyaswkb(col("mosaic_geom"))
    succeed
  }

  test("ScalaApiExamples snippet constants exist and are non-empty") {
    assert(ScalaApiExamples.RegisterAllPackages.nonEmpty)
    assert(ScalaApiExamples.RegisterRasterX.nonEmpty)
    assert(ScalaApiExamples.RegisterGridX.nonEmpty)
    assert(ScalaApiExamples.RegisterVectorX.nonEmpty)
    assert(ScalaApiExamples.RasterXAccessorFunctions.nonEmpty)
    assert(ScalaApiExamples.RasterXTransformationFunctions.nonEmpty)
    assert(ScalaApiExamples.RasterXCompleteExample.nonEmpty)
    assert(ScalaApiExamples.GridXBNGFunctions.nonEmpty)
    assert(ScalaApiExamples.VectorXConversionFunctions.nonEmpty)
  }

  test("ScalaApiExamples output constants are non-empty") {
    assert(ScalaApiExamples.RegisterAllPackages_output.nonEmpty)
    assert(ScalaApiExamples.RegisterRasterX_output.nonEmpty)
    assert(ScalaApiExamples.RegisterGridX_output.nonEmpty)
    assert(ScalaApiExamples.RegisterVectorX_output.nonEmpty)
    assert(ScalaApiExamples.RasterXAccessorFunctions_output.nonEmpty)
    assert(ScalaApiExamples.RasterXTransformationFunctions_output.nonEmpty)
    assert(ScalaApiExamples.RasterXCompleteExample_output.nonEmpty)
    assert(ScalaApiExamples.GridXBNGFunctions_output.nonEmpty)
    assert(ScalaApiExamples.VectorXConversionFunctions_output.nonEmpty)
  }
}
