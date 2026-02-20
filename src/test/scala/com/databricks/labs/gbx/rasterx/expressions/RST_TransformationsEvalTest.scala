package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.{ErrorTokenListener, ProjErrorFilter, functions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{not => _, _}
import org.apache.spark.sql.test.SilentSparkSession
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
                .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
                .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
                .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
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

}

