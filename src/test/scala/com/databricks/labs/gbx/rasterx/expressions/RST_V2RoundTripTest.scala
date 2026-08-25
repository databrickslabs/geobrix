package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, VirtualTileException}
import com.databricks.labs.gbx.udfs
import com.databricks.labs.gbx.udfs.st_buffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

/**
  * End-to-end parity tests for the v2 tile layout (Task 9).
  *
  * Proves:
  *  - heavy rst_* consumes a v1 binary tile (rst_fromcontent) and emits a 9-field v2 struct
  *  - heavy rst_* consumes a v2 materialized tile end-to-end without ClassCastException
  *  - heavy rst_* raises the materialize-first guard through the Spark plan (SparkException wraps
  *    the IllegalArgumentException produced by RasterSerializationUtil.guardMaterialized)
  *  - heavy output schema equals RST_ExpressionUtil.v2TileType field-for-field
  */
class RST_V2RoundTripTest extends PlanTest with SilentSparkSession {

    /** Walk the getCause chain looking for a message that contains all of the given substrings. */
    private def causeChainContains(t: Throwable, substrings: Seq[String]): Boolean = {
        var cur: Throwable = t
        while (cur != null) {
            val msg = Option(cur.getMessage).getOrElse("").toLowerCase
            if (substrings.forall(s => msg.contains(s.toLowerCase))) return true
            cur = cur.getCause
        }
        false
    }

    // ---- 1. v1-in → v2-out --------------------------------------------------

    test("heavy rst_* consumes a v1 binary tile and emits a v2 tile") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        // rst_fromcontent produces a v1 3-field tile (cellid, raster, metadata).
        val df = spark.read
            .format("binaryFile")
            .load(tifPath)
            .limit(1)
            .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))

        // Run a tile-returning op (rst_clip on a buffered bbox).
        val out = df
            .withColumn("bbox", rst_boundingbox(col("tile")))
            .withColumn("clipper", st_buffer(col("bbox"), lit(-520000.0)))
            .select(rst_clip(col("tile"), col("clipper"), lit(true)).as("t"))

        // Output schema must be the v2 9-field struct.
        val tileType = out.schema("t").dataType.asInstanceOf[StructType]
        tileType.fieldNames.toSeq should equal(
            Seq("cellid", "raster", "path", "path_mode", "window", "clip_polygon", "clip_crs", "crs", "metadata"))

        // The raster field must be non-null (materialized bytes, not a virtual tile).
        val row = out.head.getAs[Row]("t")
        assert(row.getAs[Array[Byte]]("raster") != null, "raster field must be non-null (materialized)")
    }

    // ---- 2. v2-materialized-in → v2-out (no ClassCastException) ------------

    test("heavy rst_* consumes a v2 materialized tile end-to-end") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        // Stage 1: build a v2 materialized tile via rst_fromcontent + rst_clip (v1 → v2).
        val stage1 = spark.read
            .format("binaryFile")
            .load(tifPath)
            .limit(1)
            .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("bbox", rst_boundingbox(col("tile")))
            .withColumn("clipper", st_buffer(col("bbox"), lit(-520000.0)))
            .select(rst_clip(col("tile"), col("clipper"), lit(true)).as("t"))
            .cache()

        stage1.count() // materialise

        // Confirm stage1 output is v2 (9 fields).
        val stage1Type = stage1.schema("t").dataType.asInstanceOf[StructType]
        stage1Type.fields.length should be(9)

        // Stage 2: feed the v2 tile into rst_boundingbox — proves layout-aware deserialise
        // handles v2 input through the full Spark execution path (not just the serde unit test).
        noException should be thrownBy {
            val bboxDf = stage1.select(rst_boundingbox(col("t")).as("bbox"))
            val result = bboxDf.head.getAs[Array[Byte]]("bbox")
            // bbox is WKB bytes; assert it is non-null and non-empty.
            assert(result != null, "bbox WKB must be non-null")
            assert(result.length > 0, "bbox WKB must be non-empty")
        }

        stage1.unpersist()
    }

    // ---- 3. Virtual tile raises materialize-first error through Spark plan --

    test("heavy rst_* on a VIRTUAL tile raises the materialize-first error") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        functions.register(spark)

        // Build a v2 virtual tile DataFrame: raster=null, path set, 9-field schema.
        // window is a nested struct; pass null for it and the other pedigree fields.
        val tileSchema = StructType(Seq(
            StructField("tile", RST_ExpressionUtil.v2TileType, nullable = false)))

        val virtualRow = Row(Row(
            0L,                    // cellid
            null,                  // raster (null → virtual)
            "/some/virtual.tif",   // path (set → this is a virtual tile)
            null,                  // path_mode
            null,                  // window
            null,                  // clip_polygon
            null,                  // clip_crs
            null,                  // crs
            Map.empty[String, String] // metadata (last)
        ))

        val dfVirtual = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(virtualRow)),
            tileSchema)

        // rst_boundingbox on a virtual tile MUST throw through the Spark plan.
        val ex = intercept[Exception] {
            dfVirtual.select(rst_boundingbox(col("tile"))).collect()
        }

        // The IllegalArgumentException from guardMaterialized may be wrapped in a SparkException.
        // Walk the cause chain to find the guard message.
        assert(
            causeChainContains(ex, Seq("materialize", "lightweight", "virtual tile")),
            s"Expected 'materialize', 'lightweight', 'virtual tile' somewhere in cause chain.\n" +
            s"Top-level: ${ex.getMessage}\n" +
            Option(ex.getCause).map(c => s"Cause: ${c.getMessage}").getOrElse("(no cause)")
        )
    }

    // ---- 3b. rst_clip on VIRTUAL tile raises error through tile-returning (3-arg) overload ----

    test("heavy rst_clip on a VIRTUAL tile raises the materialize-first error (3-arg overload)") {
        // This test covers the gap fixed by Task 9b: tile-returning ops use the 3-arg
        // safeEval(eval, row, rasterType) overload. Pre-fix it swallowed the VirtualTileException
        // as an error-row and .collect() would NOT throw. Post-fix it re-throws.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        functions.register(spark)

        // Build a v2 virtual tile DataFrame: raster=null, path set, 9-field schema.
        val tileSchema = StructType(Seq(
            StructField("tile", RST_ExpressionUtil.v2TileType, nullable = false)))

        val virtualRow = Row(Row(
            0L,
            null,                  // raster = null → virtual
            "/some/virtual.tif",   // path set
            null,                  // path_mode
            null, null, null, null, // window, clip_polygon, clip_crs, crs
            Map.empty[String, String]))  // metadata (last)

        val dfVirtual = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(virtualRow)),
            tileSchema)

        val ex = intercept[Exception] {
            dfVirtual.select(rst_clip(col("tile"), lit("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"), lit(true))).collect()
        }

        assert(
            causeChainContains(ex, Seq("virtual tile", "materialize", "lightweight")),
            s"Expected 'virtual tile', 'materialize', 'lightweight' in cause chain.\n" +
            s"Top-level: ${ex.getMessage}\n" +
            Option(ex.getCause).map(c => s"Cause: ${c.getMessage}").getOrElse("(no cause)")
        )
    }

    // ---- 3c. F1 regression guard: plain IAE is NOT propagated (null-tolerant) ----

    test("F1 regression: plain IllegalArgumentException from 4-arg safeEval returns null not throw") {
        // Directly exercises the 4-arg safeEval overload with a thunk that throws a plain IAE
        // (not a VirtualTileException) and confirms it returns null (non-crash mode), not a throw.
        // This proves the broad IAE re-throw from Task 9 has been reverted.
        import com.databricks.labs.gbx.expressions.ExpressionConfig
        import org.apache.spark.sql.types.BinaryType
        import org.apache.spark.unsafe.types.UTF8String
        import org.apache.spark.util.SerializableConfiguration
        import com.databricks.labs.gbx.util.SerializationUtil

        val conf = new ExpressionConfig(
            Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "false"),
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        val confB64 = UTF8String.fromString(conf.toB64)

        // Minimal v2 (9-field) row (not a virtual tile — raster field is empty bytes at position 1)
        val metadataMapData = SerializationUtil.toMapData[String, String](Map.empty[String, String])
        val row: InternalRow = new GenericInternalRow(Array[Any](
            1L, Array.emptyByteArray, null, null, null, null, null, null, metadataMapData))

        // Thunk throws a plain IAE (not VirtualTileException) — should be swallowed → null
        val plainIAEResult = RST_ErrorHandler.safeEval(
            () => throw new IllegalArgumentException("not a virtual tile, just a bad arg"),
            row, BinaryType, confB64)
        assert(plainIAEResult == null,
            "plain IAE must be swallowed to null in non-crash mode (F1 regression guard)")

        // Thunk throws VirtualTileException — must propagate
        intercept[VirtualTileException] {
            RST_ErrorHandler.safeEval(
                () => throw new VirtualTileException("virtual tile guard test"),
                row, BinaryType, confB64)
        }
    }

    // ---- 4. Heavy output schema == light V2_TILE_SCHEMA field-for-field -----

    test("heavy v2 output schema equals RST_ExpressionUtil.v2TileType field-for-field") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val heavyOutDF = spark.read
            .format("binaryFile")
            .load(tifPath)
            .limit(1)
            .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("bbox", rst_boundingbox(col("tile")))
            .withColumn("clipper", st_buffer(col("bbox"), lit(-520000.0)))
            .select(rst_clip(col("tile"), col("clipper"), lit(true)).as("t"))

        val heavyType = heavyOutDF.schema("t").dataType.asInstanceOf[StructType]
        val canonical = RST_ExpressionUtil.v2TileType

        // Field names must match exactly.
        heavyType.fieldNames.toSeq should equal(canonical.fieldNames.toSeq)

        // Each field's dataType and nullability must match.
        canonical.fields.foreach { expected =>
            val actual = heavyType(expected.name)
            withClue(s"Field '${expected.name}' dataType mismatch:") {
                actual.dataType should equal(expected.dataType)
            }
            withClue(s"Field '${expected.name}' nullable mismatch:") {
                actual.nullable should equal(expected.nullable)
            }
        }
    }

}
