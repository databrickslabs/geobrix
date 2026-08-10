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

  // =========================================================================
  // RasterX per-function Scala example vals
  // =========================================================================

  test("ScalaApiExamples RasterX per-function snippet vals are non-empty") {
    assert(ScalaApiExamples.rst_avg_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_boundingbox_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_numbands_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_width_scala_example.nonEmpty)
  }

  test("ScalaApiExamples RasterX per-function output vals are non-empty") {
    assert(ScalaApiExamples.rst_avg_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_boundingbox_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_numbands_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_width_scala_example_output.nonEmpty)
  }

  test("RasterX rst_avg rst_boundingbox rst_numbands rst_width signatures compile") {
    val _: Column = rx.rst_avg(col("tile"))
    val _: Column = rx.rst_boundingbox(col("tile"))
    val _: Column = rx.rst_numbands(col("tile"))
    val _: Column = rx.rst_width(col("tile"))
    succeed
  }

  test("RasterX accessor signatures compile — full 29-function set") {
    val _: Column = rx.rst_bandmetadata(col("tile"), lit(1))
    val _: Column = rx.rst_format(col("tile"))
    val _: Column = rx.rst_georeference(col("tile"))
    val _: Column = rx.rst_getnodata(col("tile"))
    val _: Column = rx.rst_getsubdataset(col("tile"), lit(""))
    val _: Column = rx.rst_height(col("tile"))
    val _: Column = rx.rst_max(col("tile"))
    val _: Column = rx.rst_median(col("tile"))
    val _: Column = rx.rst_memsize(col("tile"))
    val _: Column = rx.rst_metadata(col("tile"))
    val _: Column = rx.rst_min(col("tile"))
    val _: Column = rx.rst_pixelcount(col("tile"))
    val _: Column = rx.rst_pixelheight(col("tile"))
    val _: Column = rx.rst_pixelwidth(col("tile"))
    val _: Column = rx.rst_rotation(col("tile"))
    val _: Column = rx.rst_scalex(col("tile"))
    val _: Column = rx.rst_scaley(col("tile"))
    val _: Column = rx.rst_skewx(col("tile"))
    val _: Column = rx.rst_skewy(col("tile"))
    val _: Column = rx.rst_srid(col("tile"))
    val _: Column = rx.rst_crs(col("tile"))
    val _: Column = rx.rst_subdatasets(col("tile"))
    val _: Column = rx.rst_summary(col("tile"))
    val _: Column = rx.rst_type(col("tile"))
    val _: Column = rx.rst_upperleftx(col("tile"))
    val _: Column = rx.rst_upperlefty(col("tile"))
    val _: Column = rx.rst_isempty(col("tile"))
    val _: Column = rx.rst_tryopen(col("tile"))
    val _: Column = rx.rst_histogram(col("tile"))
    succeed
  }

  test("ScalaApiExamples accessor snippet vals are non-empty") {
    assert(ScalaApiExamples.rst_bandmetadata_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_format_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_georeference_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_getnodata_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_getsubdataset_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_height_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_max_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_median_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_memsize_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_metadata_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_min_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_pixelcount_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_pixelheight_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_pixelwidth_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_rotation_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_scalex_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_scaley_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_skewx_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_skewy_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_srid_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_crs_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_subdatasets_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_summary_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_type_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_upperleftx_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_upperlefty_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_isempty_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_tryopen_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_histogram_scala_example.nonEmpty)
  }

  test("ScalaApiExamples accessor output vals are non-empty") {
    assert(ScalaApiExamples.rst_bandmetadata_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_format_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_georeference_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_getnodata_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_getsubdataset_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_height_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_max_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_median_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_memsize_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_metadata_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_min_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_pixelcount_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_pixelheight_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_pixelwidth_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_rotation_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_scalex_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_scaley_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_skewx_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_skewy_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_srid_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_crs_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_subdatasets_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_summary_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_type_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_upperleftx_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_upperlefty_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_isempty_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_tryopen_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_histogram_scala_example_output.nonEmpty)
  }
}
