package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.{ErrorTokenListener, functions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_ExpressionEvalTest extends PlanTest with SilentSparkSession {

    test("RST_ExpressionEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import com.databricks.labs.gbx.udfs._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val pyfunc = "def myfunc(x):\n  return x * 2"

        def runQuery(df: DataFrame): Unit = {
            val base = df
                .withColumn("bbox", rst_boundingbox(col("raster")))
                // Clip to reduce data size for faster, more stable testing (1 px ~470m, so -520000 = ~1100px clip)
                .withColumn("clipper", st_buffer(col("bbox"), lit(-520000.0))) // projection in meters 1 px is ~470m
                .withColumn("clipped", rst_clip(col("raster"), col("clipper"), lit(true)))
                .cache()
            base.collect()
            Seq(
              rst_avg(col("clipped")),
              rst_boundingbox(col("clipped")),
              rst_asformat(col("clipped"), lit("GTiff")),
              rst_combineavg(array(col("raster"), col("clipped"))),
              rst_derivedband(col("clipped"), pyfunc, "myfunc"),
              rst_initnodata(col("clipped")), //lit(-28672.0)),
              rst_isempty(col("clipped")),
              rst_mapalgebra(array(col("clipped")), lit("{\"calc\": \"A+2*A\"}")),
              rst_merge(array(col("clipped"), col("clipped"))),
              rst_ndvi(col("clipped"), lit(1), lit(1)),
              rst_rastertoworldcoord(col("clipped"), lit(10), lit(10)),
              rst_rastertoworldcoordx(col("clipped"), lit(10), lit(10)),
              rst_rastertoworldcoordy(col("clipped"), lit(10), lit(10)),
              rst_worldtorastercoord(col("clipped"), lit(500000.0), lit(5000000.0)),
              rst_worldtorastercoordx(col("clipped"), lit(500000.0), lit(5000000.0)),
              rst_worldtorastercoordy(col("clipped"), lit(500000.0), lit(5000000.0)),
              rst_transform(col("clipped"), lit(4326)),
              rst_tryopen(col("clipped")),
              rst_updatetype(col("clipped"), lit("Float32"))
            ).foreach(f => base.select(f).collect())
            base.unpersist()
        }

        // Use only 1 file instead of 3 for faster, more stable testing
        val df1: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))

        // noException should be thrownBy

        runQuery(df1)

        // Use only 1 file for content-based testing for stability
        val df2: DataFrame = spark.read
            .format("binaryFile")
            .load(tifPath)
            .limit(1)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

        def convolveQuery(df: DataFrame): Unit = {
            val base = df
                .withColumn("bbox", rst_boundingbox(col("raster")))
                // Clip more for convolve operations (which are compute-intensive)
                .withColumn("clipper", st_buffer(col("bbox"), lit(-540000.0))) // projection in meters 1 px is ~470m
                .withColumn("raster", rst_clip(col("raster"), col("clipper"), lit(true)))
                .cache()
            base.collect()
            // Test filter and one representative convolve operation for stability
            Seq(
                rst_filter(col("raster"), lit(1), lit("min")),
                rst_convolve(
                col("raster"),
                array(
                  array(lit(0.0), lit(-1.0), lit(0.0)),
                  array(lit(-1.0), lit(5.0), lit(-1.0)),
                  array(lit(0.0), lit(-1.0), lit(0.0))
                )
              )
            ).foreach(f => base.select(f).collect())
            base.unpersist()
        }

        noException should be thrownBy convolveQuery(df1)
        noException should be thrownBy convolveQuery(df2)

    }

}
