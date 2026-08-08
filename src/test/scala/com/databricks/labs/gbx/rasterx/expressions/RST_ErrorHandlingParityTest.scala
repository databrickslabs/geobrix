package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Cross-tier parity spine for the RasterX error-handling workstream.
  *
  * Asserts that:
  * 1. Heavy accessors return NULL (not a sentinel) on corrupt input — codifying
  *    the Task 1/2 result as an explicit contract assertion.
  * 2. The ``crashExpressions=true`` dev escape hatch still RAISES instead of
  *    degrading — confirming the safety valve is intact after Tasks 1–7 wired
  *    up the null-return paths.
  *
  * Tests are source-only; no separately-staged JAR is needed — the test command
  * builds what it needs.  These tests MUST pass against the current tree;
  * failure means a parity gap in a prior task, not a test defect.
  */
class RST_ErrorHandlingParityTest extends PlanTest with SilentSparkSession {

    /**
      * Parity: corrupt raster → rst_width is NULL (not 0 or any sentinel).
      *
      * Mirrors the assertion in RST_AccessorsEvalTest but lives here to make the
      * cross-tier parity contract explicit and discoverable.
      */
    test("parity: corrupt raster → rst_width is NULL (heavy, Task 1 contract)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_width(col("raster")).as("w")).collect()
        assert(res.head.get(0) == null, "corrupt raster width must be NULL (not 0 sentinel)")
    }

    /**
      * Parity: corrupt raster → rst_srid is NULL (not 0).
      *
      * Tasks 1/2 contract: SRID 0 was the old sentinel; the degrade path now
      * returns NULL so callers can distinguish "no CRS" from "corrupt tile".
      */
    test("parity: corrupt raster → rst_srid is NULL (heavy, Task 2 contract)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_srid(col("raster")).as("s")).collect()
        assert(res.head.get(0) == null, "corrupt raster SRID must be NULL (not 0 sentinel)")
    }

    /**
      * Parity: corrupt raster → rst_scalex is NULL (not NaN).
      *
      * Mirrors the assertion already present in RST_AccessorsEvalTest; duplicated
      * here to surface the cross-tier scalex-not-NaN contract explicitly.
      */
    test("parity: corrupt raster → rst_scalex is NULL (not NaN) (heavy, Task 1 contract)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_scalex(col("raster")).as("sx")).collect()
        assert(res.head.get(0) == null, "corrupt raster scalex must be NULL (not NaN sentinel)")
    }

    /**
      * Negative guard: crashExpressions=true → corrupt accessor RAISES.
      *
      * The dev escape hatch (``spark.databricks.labs.gbx.expressions.crash.on.error``)
      * must still throw even after Tasks 1–7 wired up the null-return default.
      * Resets the flag in a ``finally`` so it cannot leak into subsequent tests.
      */
    test("crashExpressions=true still raises on corrupt input (escape hatch intact)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        spark.conf.set("spark.databricks.labs.gbx.expressions.crash.on.error", "true")
        try {
            val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("raster")
            intercept[Exception] { df.select(rst_width(col("raster"))).collect() }
        } finally {
            spark.conf.set("spark.databricks.labs.gbx.expressions.crash.on.error", "false")
        }
    }

}
