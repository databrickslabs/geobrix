package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.{ErrorTokenListener, ProjErrorFilter, functions}
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{not => _, _}
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.matchers.should.Matchers._

/**
  * Test suite for RST_Clip, RST_Filter, and RST_Transform expressions
  * through Spark execution.
  */
class RST_TransformationsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_Clip should clip raster using WKT geometry") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn(
              "clipped",
              rst_clip(
                col("raster"),
                lit("POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"),
                lit(true)
              )
            )

        noException should be thrownBy df.collect()
        val result = df.select("clipped").collect()
        result should not be empty
        assert(result.head.get(0) != null)
    }

    test("RST_Clip should clip raster using WKB geometry") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        // First create a WKB from WKT
        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn(
              "wkt",
              lit("POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))")
            )

        // Convert to WKB using JTS (assuming st_aswkb exists)
        import com.databricks.labs.gbx.udfs._
        val dfWithWkb = df.withColumn("wkb", st_aswkb(col("wkt")))
            .withColumn("clipped", rst_clip(col("raster"), col("wkb"), lit(true)))

        noException should be thrownBy dfWithWkb.collect()
    }

    test("RST_Clip should handle cutlineAllTouched parameter") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val geom = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("clip_true", rst_clip(col("raster"), lit(geom), lit(true)))
            .withColumn("clip_false", rst_clip(col("raster"), lit(geom), lit(false)))

        noException should be thrownBy df.collect()
        val result = df.select("clip_true", "clip_false").collect()
        result should not be empty
    }

    test("RST_Clip should preserve raster properties after clipping") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn(
              "clipped",
              rst_clip(
                col("raster"),
                lit("POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"),
                lit(true)
              )
            )
            .withColumn("original_bands", rst_numbands(col("raster")))
            .withColumn("clipped_bands", rst_numbands(col("clipped")))

        val result = df.select("original_bands", "clipped_bands").collect()
        result should not be empty
        val row = result.head
        row.getAs[Int]("original_bands") shouldBe row.getAs[Int]("clipped_bands")
    }

    test("RST_Filter should apply median filter") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("filtered", rst_filter(col("raster"), lit(3), lit("median")))

        noException should be thrownBy df.collect()
        val result = df.select("filtered").collect()
        result should not be empty
        assert(result.head.get(0) != null)
    }

    test("RST_Filter should support multiple filter operations") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val operations = Seq("avg", "median", "mode", "max", "min")
        
        operations.foreach { op =>
            val df: DataFrame = Seq(
              (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            ).toDF("id", "path")
                .withColumn("raster", udfs.rasterFromPath(col("path")))
                .withColumn("filtered", rst_filter(col("raster"), lit(3), lit(op)))

            noException should be thrownBy df.collect()
        }
    }

    test("RST_Filter should work with different kernel sizes") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val kernelSizes = Seq(3, 5, 7)
        
        kernelSizes.foreach { size =>
            val df: DataFrame = Seq(
              (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            ).toDF("id", "path")
                .withColumn("raster", udfs.rasterFromPath(col("path")))
                .withColumn("filtered", rst_filter(col("raster"), lit(size), lit("avg")))

            noException should be thrownBy df.collect()
        }
    }

    test("RST_Filter should preserve dimensions") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("filtered", rst_filter(col("raster"), lit(3), lit("avg")))
            .withColumn("original_width", rst_width(col("raster")))
            .withColumn("filtered_width", rst_width(col("filtered")))
            .withColumn("original_height", rst_height(col("raster")))
            .withColumn("filtered_height", rst_height(col("filtered")))

        val result = df.select("original_width", "filtered_width", "original_height", "filtered_height").collect()
        result should not be empty
        val row = result.head
        row.getAs[Int]("original_width") shouldBe row.getAs[Int]("filtered_width")
        row.getAs[Int]("original_height") shouldBe row.getAs[Int]("filtered_height")
    }

    test("RST_Transform should transform raster to WGS84") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("transformed", rst_transform(col("raster"), lit(4326)))

        noException should be thrownBy df.collect()
        val result = df.select("transformed").collect()
        result should not be empty
        assert(result.head.get(0) != null)
    }

    test("RST_Transform should preserve band count") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("transformed", rst_transform(col("raster"), lit(4326)))
            .withColumn("original_bands", rst_numbands(col("raster")))
            .withColumn("transformed_bands", rst_numbands(col("transformed")))

        val result = df.select("original_bands", "transformed_bands").collect()
        result should not be empty
        val row = result.head
        row.getAs[Int]("original_bands") shouldBe row.getAs[Int]("transformed_bands")
    }

    test("RST_Transform should change SRID") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("transformed", rst_transform(col("raster"), lit(4326)))
            .withColumn("original_srid", rst_srid(col("raster")))
            .withColumn("transformed_srid", rst_srid(col("transformed")))

        val result = df.select("original_srid", "transformed_srid").collect()
        result should not be empty
        val row = result.head
        row.getAs[Int]("transformed_srid") shouldBe 4326
    }

    test("RST_Transform should work with multiple target SRIDs") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val targetSRIDs = Seq(4326, 3857, 32611) // WGS84, Web Mercator, UTM Zone 11N
        
        targetSRIDs.foreach { srid =>
            val df: DataFrame = Seq(
              (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            ).toDF("id", "path")
                .withColumn("raster", udfs.rasterFromPath(col("path")))
                .withColumn("transformed", rst_transform(col("raster"), lit(srid)))

            noException should be thrownBy df.collect()
        }
    }

    test("Combined workflow: Clip, Filter, and Transform") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn(
              "clipped",
              rst_clip(
                col("raster"),
                lit("POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"),
                lit(true)
              )
            )
            .withColumn("filtered", rst_filter(col("clipped"), lit(3), lit("median")))
            .withColumn("transformed", rst_transform(col("filtered"), lit(4326)))

        noException should be thrownBy df.collect()
        val result = df.select("transformed").collect()
        result should not be empty
        assert(result.head.get(0) != null)
    }

    test("Transformations should work with binary content") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("filtered", rst_filter(col("raster"), lit(3), lit("avg")))
            .withColumn("transformed", rst_transform(col("filtered"), lit(4326)))

        noException should be thrownBy df.collect()
    }

    test("RST_Clip should reproject EWKT geometry when SRID differs from raster CRS") {
        // Raster is World Sinusoidal; pass cutline as EWKT in WGS84 (SRID=4326).
        // Without EWKT→SRID propagation + the SRID=0 fallback fix, the raw lon/lat coords
        // would be treated as raster CRS → empty clip / zero-byte output.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val ewkt = "SRID=4326;POLYGON((-80 14, -80 17, -77 17, -77 14, -80 14))"

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("clipped", rst_clip(col("raster"), lit(ewkt), lit(true)))
            .withColumn("clipped_size", rst_memsize(col("clipped")))

        val result = df.select("clipped_size").collect()
        result should not be empty
        val size = result.head.getAs[Long]("clipped_size")
        // Cutline reprojected correctly should produce a real multi-KB TIFF — not the
        // ~422-byte empty-IFD stub produced when reprojection silently no-ops.
        size should be > 5000L
    }

    test("RST_Clip should reproject EWKB geometry when SRID differs from raster CRS") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        import com.databricks.labs.gbx.vectorx.jts.JTS
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val geom = JTS.fromWKT("POLYGON((-80 14, -80 17, -77 17, -77 14, -80 14))")
        geom.setSRID(4326)
        val ewkb = JTS.toEWKB(geom)

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF", ewkb)
        ).toDF("id", "path", "clip_wkb")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("clipped", rst_clip(col("raster"), col("clip_wkb"), lit(true)))
            .withColumn("clipped_size", rst_memsize(col("clipped")))

        val result = df.select("clipped_size").collect()
        result should not be empty
        result.head.getAs[Long]("clipped_size") should be > 5000L
    }

    test("RST_Clip on a NULL primary tile returns null (not an NPE)") {
        // Guards the propagateNull=false path: with the optional clipCrs default injected as
        // Literal(null, StringType), the invoke no longer short-circuits to null on any null arg,
        // so eval now runs even when the primary tile row is null. The null-primary guard in
        // RST_Clip.eval must preserve the prior "null tile in -> null tile out" behavior instead
        // of NPE-ing inside rowToTile/safeEval.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        functions.register(spark)

        // A single row whose v2 tile struct is fully null (nullable outer field).
        val schema = StructType(Seq(
            StructField("tile", RST_ExpressionUtil.v2TileType, nullable = true)))
        val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(null))),
            schema)
            .withColumn(
              "clipped",
              rst_clip(col("tile"), lit("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"), lit(true)))

        noException should be thrownBy df.collect()
        val result = df.select("clipped").collect()
        result should not be empty
        assert(result.head.get(0) == null, "clip of a null tile must be null, not a materialized/error tile")
    }

    test("RST_Clip on a NULL geom returns null (not an error tile)") {
        // Companion to the null-tile guard: with propagateNull=false a null geom now reaches eval;
        // the geom match is exhaustive (no `case other`), so an unguarded null geom would MatchError
        // into safeEval and emit a NON-null error tile. Contract: null geom -> null, like a null tile.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("clipped", rst_clip(col("raster"), lit(null).cast("string"), lit(true)))

        noException should be thrownBy df.collect()
        val result = df.select("clipped").collect()
        result should not be empty
        assert(result.head.get(0) == null, "clip with a null geom must be null, not an error tile")
    }

    test("RST_Proximity short-arity (tile only) returns a non-null tile") {
        // Regression for the propagateNull short-circuit: rst_proximity(tile) hits builder() case 1
        // which injects Literal(null, StringType)/Literal(null, DoubleType) defaults. With the old
        // propagateNull=true, those null defaults short-circuited the whole result to null (eval never
        // ran) — the .execute-only tests never caught it. Now it must produce a real tile.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("prox", rst_proximity(col("raster")))

        noException should be thrownBy df.collect()
        val result = df.select("prox").collect()
        result should not be empty
        assert(result.head.get(0) != null, "rst_proximity(tile) must return a non-null tile")
    }

    test("RST_Proximity on a NULL primary tile returns null (not an NPE)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        functions.register(spark)

        val schema = StructType(Seq(
            StructField("tile", RST_ExpressionUtil.v2TileType, nullable = true)))
        val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(null))),
            schema)
            .withColumn("prox", rst_proximity(col("tile")))

        noException should be thrownBy df.collect()
        val result = df.select("prox").collect()
        result should not be empty
        assert(result.head.get(0) == null, "proximity of a null tile must be null")
    }

    test("RST_Histogram short-arity (tile only) returns a non-null histogram map") {
        // rst_histogram(tile) hits builder() case 1 which injects nullDouble defaults for min/max.
        // Old propagateNull=true nulled the whole result; now it must produce a real MAP.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("hist", rst_histogram(col("raster")))

        noException should be thrownBy df.collect()
        val result = df.select("hist").collect()
        result should not be empty
        assert(result.head.get(0) != null, "rst_histogram(tile) must return a non-null histogram map")
    }

    test("RST_Histogram on a NULL primary tile returns null (not an NPE)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        functions.register(spark)

        val schema = StructType(Seq(
            StructField("tile", RST_ExpressionUtil.v2TileType, nullable = true)))
        val df = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(null))),
            schema)
            .withColumn("hist", rst_histogram(col("tile")))

        noException should be thrownBy df.collect()
        val result = df.select("hist").collect()
        result should not be empty
        assert(result.head.get(0) == null, "histogram of a null tile must be null")
    }

    test("RST_Transform should surface a clear error for invalid SRID") {
        // RST_ErrorHandler.safeEval converts exceptions into error-metadata rows rather than
        // failing the whole job, so we assert the error surfaces in the tile's metadata.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
            .withColumn("transformed", rst_transform(col("raster"), lit(0)))
            .withColumn("err", col("transformed.metadata")("error_message"))

        val result = df.select("err").collect()
        result should not be empty
        val err = result.head.getAs[String]("err")
        err should not be null
        err should include("rst_transform")
    }

}

