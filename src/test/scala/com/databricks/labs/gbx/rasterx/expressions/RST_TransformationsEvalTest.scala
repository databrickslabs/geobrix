package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
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
  * Test suite for the RST_Transform expression
  * through Spark execution.
  */
class RST_TransformationsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_Transform should transform raster to WGS84") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

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
        TestL1.installAll(spark)

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
        TestL1.installAll(spark)

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
        TestL1.installAll(spark)

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

    test("Transformations should work with binary content") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("transformed", rst_transform(col("raster"), lit(4326)))

        noException should be thrownBy df.collect()
    }

    test("RST_Proximity short-arity (tile only) returns a non-null tile") {
        // Regression for the propagateNull short-circuit: rst_proximity(tile) hits builder() case 1
        // which injects Literal(null, StringType)/Literal(null, DoubleType) defaults. With the old
        // propagateNull=true, those null defaults short-circuited the whole result to null (eval never
        // ran) — the .execute-only tests never caught it. Now it must produce a real tile.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

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
        TestL1.installAll(spark)

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

    test("RST_Transform should surface a clear error for invalid SRID") {
        // RST_ErrorHandler.safeEval converts exceptions into error-metadata rows rather than
        // failing the whole job, so we assert the error surfaces in the tile's metadata.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

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

