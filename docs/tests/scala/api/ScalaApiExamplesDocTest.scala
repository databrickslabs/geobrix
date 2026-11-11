/*
 * Compile-time validation for the Scala examples in ScalaApiExamples.
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
  test("RasterX imports compile") {
    val _: Column = rx.rst_boundingbox(col("tile"))
    succeed
  }

  test("RasterX accessor signatures compile") {
    val _: Column = rx.rst_boundingbox(col("tile"))
    val _: Column = rx.rst_width(col("tile"))
    val _: Column = rx.rst_height(col("tile"))
    val _: Column = rx.rst_numbands(col("tile"))
    succeed
  }

  test("GridX imports and cellarea compile") {
    val _: Column = bx.bng_cellarea(col("cell"))
    succeed
  }

  test("VectorX legacy API compiles") {
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
    assert(ScalaApiExamples.rst_boundingbox_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_numbands_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_width_scala_example.nonEmpty)
  }

  test("ScalaApiExamples RasterX per-function output vals are non-empty") {
    assert(ScalaApiExamples.rst_boundingbox_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_numbands_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_width_scala_example_output.nonEmpty)
  }

  test("ScalaApiExamples Batch E cellfill + kloop + distance snippet vals are non-empty") {
    assert(ScalaApiExamples.bng_cellfill_scala_example.nonEmpty)
    assert(ScalaApiExamples.h3_cellfill_scala_example.nonEmpty)
    assert(ScalaApiExamples.custom_cellfill_scala_example.nonEmpty)
    assert(ScalaApiExamples.custom_kloop_scala_example.nonEmpty)
    assert(ScalaApiExamples.custom_distance_scala_example.nonEmpty)
  }

  test("ScalaApiExamples Batch E cellfill + kloop + distance output vals are non-empty") {
    assert(ScalaApiExamples.bng_cellfill_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.h3_cellfill_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.custom_cellfill_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.custom_kloop_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.custom_distance_scala_example_output.nonEmpty)
  }

  test("ScalaApiExamples Batch E cellfill + kloop + distance Scala signatures compile") {
    import com.databricks.labs.gbx.gridx.bng.{functions => bx}
    import com.databricks.labs.gbx.gridx.custom.{functions => cx}
    import com.databricks.labs.gbx.gridx.h3.{functions => hx}
    import org.apache.spark.sql.functions._
    val grid = call_function("gbx_custom_grid",
      lit(0), lit(1000000), lit(0), lit(1000000),
      lit(2), lit(1000), lit(1000), lit(27700))
    val _: Column = bx.bng_cellfill(col("cellid"), col("value"))
    val _: Column = bx.bng_cellfill(col("cellid"), col("value"), 1)
    val _: Column = bx.bng_cellfill(col("cellid"), col("value"), 1, "mean")
    val _: Column = bx.bng_cellfill(col("cellid"), col("value"), 1, "mean", 2.0)
    val _: Column = hx.h3_cellfill(col("cellid"), col("value"))
    val _: Column = hx.h3_cellfill(col("cellid"), col("value"), 1)
    val _: Column = hx.h3_cellfill(col("cellid"), col("value"), 1, "mean", 2.0)
    val _: Column = cx.custom_kloop(col("cell"), grid, lit(1))
    val _: Column = cx.custom_kloop(col("cell"), grid, 1)
    val _: Column = cx.custom_distance(col("cell1"), grid, col("cell2"))
    val _: Column = cx.custom_cellfill(col("cellid"), col("value"), grid)
    val _: Column = cx.custom_cellfill(col("cellid"), col("value"), grid, 1)
    val _: Column = cx.custom_cellfill(col("cellid"), col("value"), grid, 1, "mean", 2.0)
    succeed
  }

  test("ScalaApiExamples custom-grid snippet vals are non-empty") {
    assert(ScalaApiExamples.rst_custom_rasterize_agg_scala_example.nonEmpty)
  }

  test("ScalaApiExamples custom-grid output vals are non-empty") {
    assert(ScalaApiExamples.rst_custom_rasterize_agg_scala_example_output.nonEmpty)
  }

  test("RasterX rst_custom_rasterize_agg Scala signatures compile") {
    val grid = call_function("gbx_custom_grid",
      lit(529000), lit(533000), lit(179000), lit(183000),
      lit(2), lit(4000), lit(4000), lit(27700))
    val _: Column = rx.rst_custom_rasterize_agg(col("cellid"), col("value"), grid)
    val _: Column = rx.rst_custom_rasterize_agg(
      col("cellid"), col("value"), grid,
      lit(27700), lit(null).cast("double"),
      lit(null).cast("double"), lit(null).cast("double"),
      lit(null).cast("double"), lit(null).cast("double"),
      lit(null).cast("int"), lit(null).cast("int"),
      lit("centroids"), lit(1))
    succeed
  }

  test("RasterX rst_boundingbox rst_numbands rst_width signatures compile") {
    val _: Column = rx.rst_boundingbox(col("tile"))
    val _: Column = rx.rst_numbands(col("tile"))
    val _: Column = rx.rst_width(col("tile"))
    succeed
  }

  test("RasterX accessor signatures compile — the accessors with a wrapper") {
    val _: Column = rx.rst_format(col("tile"))
    val _: Column = rx.rst_getsubdataset(col("tile"), lit(""))
    val _: Column = rx.rst_height(col("tile"))
    val _: Column = rx.rst_memsize(col("tile"))
    val _: Column = rx.rst_pixelheight(col("tile"))
    val _: Column = rx.rst_pixelwidth(col("tile"))
    val _: Column = rx.rst_rotation(col("tile"))
    val _: Column = rx.rst_scalex(col("tile"))
    val _: Column = rx.rst_scaley(col("tile"))
    val _: Column = rx.rst_skewx(col("tile"))
    val _: Column = rx.rst_skewy(col("tile"))
    val _: Column = rx.rst_srid(col("tile"))
    val _: Column = rx.rst_crs(col("tile"))
    val _: Column = rx.rst_upperleftx(col("tile"))
    val _: Column = rx.rst_upperlefty(col("tile"))
    succeed
  }

  test("ScalaApiExamples accessor snippet vals are non-empty") {
    assert(ScalaApiExamples.rst_format_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_getsubdataset_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_height_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_memsize_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_pixelheight_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_pixelwidth_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_rotation_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_scalex_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_scaley_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_skewx_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_skewy_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_srid_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_crs_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_upperleftx_scala_example.nonEmpty)
    assert(ScalaApiExamples.rst_upperlefty_scala_example.nonEmpty)
  }

  test("ScalaApiExamples accessor output vals are non-empty") {
    assert(ScalaApiExamples.rst_format_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_getsubdataset_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_height_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_memsize_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_pixelheight_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_pixelwidth_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_rotation_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_scalex_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_scaley_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_skewx_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_skewy_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_srid_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_crs_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_upperleftx_scala_example_output.nonEmpty)
    assert(ScalaApiExamples.rst_upperlefty_scala_example_output.nonEmpty)
  }

  test("JVM job examples compile and are non-empty") {
    // Compile-only, like the rest of this suite: the signatures are the ones the docs show.
    def widths(spark: SparkSession) =
      spark.table("main.geobrix.rasters")
        .select(col("path"), rx.rst_width(col("tile")).alias("width"))
    def ring(spark: SparkSession) =
      spark.sql("SELECT main.geobrix.gbx_bng_kring('TQ3080', 1) AS cells")
    val _ = (widths _, ring _)
    assert(ScalaApiExamples.jvm_job_dataframe_example.contains("rx.rst_width"))
    assert(ScalaApiExamples.jvm_job_uc_example.contains("main.geobrix.gbx_bng_kring"))
  }
}
