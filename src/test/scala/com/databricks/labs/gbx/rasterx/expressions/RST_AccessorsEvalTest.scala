package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_AccessorsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_AccessorsEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        def runQuery(df: DataFrame): Unit = {
            val base = df.cache()
            base.collect()
            Seq(
              rst_avg(col("raster")),
              rst_bandmetadata(col("raster"), lit(1)),
              rst_boundingbox(col("raster")),
              rst_format(col("raster")),
              rst_georeference(col("raster")),
              rst_getnodata(col("raster")),
              rst_height(col("raster")),
              rst_max(col("raster")),
              rst_median(col("raster")),
              rst_memsize(col("raster")),
              rst_metadata(col("raster")),
              rst_min(col("raster")),
              rst_numbands(col("raster")),
              rst_pixelcount(col("raster")),
              rst_pixelheight(col("raster")),
              rst_pixelwidth(col("raster")),
              rst_rotation(col("raster")),
              rst_scalex(col("raster")),
              rst_scaley(col("raster")),
              rst_skewx(col("raster")),
              rst_skewy(col("raster")),
              rst_srid(col("raster")),
              rst_subdatasets(col("raster")),
              rst_summary(col("raster")),
              rst_type(col("raster")),
              rst_upperleftx(col("raster")),
              rst_upperlefty(col("raster")),
              rst_width(col("raster"))
            ).foreach(f => base.select(f).collect())
            base.unpersist()
        }

        val df1: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"),
          (3, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))

        noException should be thrownBy runQuery(df1)

        val df2: DataFrame = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

        val netCDFPath = this.getClass.getResource("/binary/netcdf-CMIP5/").toString

        val df3: DataFrame = Seq(
          (1, s"$netCDFPath/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path"), "NetCDF"))
            .withColumn("subds", rst_getsubdataset(col("raster"), lit("prAdjust")))

        noException should be thrownBy df3.collect()

    }

    test("RST_Width returns NULL (not 0) on a corrupt raster") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        // Wrap garbage bytes as a tile struct via rst_fromcontent; GDAL open fails -> safeEval swallows to null.
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_width(col("raster")).as("w")).collect()
        assert(res.head.get(0) == null, "corrupt raster width must be NULL, not the 0 sentinel")
    }

    test("RST_Height returns NULL (not -1) on a corrupt raster") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_height(col("raster")).as("h")).collect()
        assert(res.head.get(0) == null, "corrupt raster height must be NULL, not the -1 sentinel")
    }

    test("RST_MemSize returns NULL (not -1L) on a corrupt raster") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_memsize(col("raster")).as("m")).collect()
        assert(res.head.get(0) == null, "corrupt raster memsize must be NULL, not the -1L sentinel")
    }

    test("RST_ScaleX returns NULL (not NaN) on a corrupt raster") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_scalex(col("raster")).as("sx")).collect()
        assert(res.head.get(0) == null, "corrupt raster scalex must be NULL, not NaN sentinel")
    }

}
