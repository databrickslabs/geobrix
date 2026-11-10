package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.GeneratorContract
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._


/**
  * Row-shape contract for the RasterX generator (`gbx_rst_xyzpyramid`, the one with a JVM form): every row it
  * emits, on every path (normal, error), has to fit the `elementSchema` it declares.
  *
  * Spark projects generator rows through `elementSchema`. A generator that emits a row of another shape
  * surfaces as an opaque ClassCastException instead of the documented row.
  */
class RST_GeneratorRowShapeTest extends PlanTest with SilentSparkSession {

    private val xyzPyramid = "gbx_rst_xyzpyramid"

    private lazy val modisPath = this.getClass.getResource("/modis/").toString
    private val b01 = "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"

    /** One row holding a tile that cannot be read (error tile from rst_fromcontent on junk bytes). */
    private def corruptTile(): DataFrame = {
        val sc = spark
        import sc.implicits._
        TestL1.installAll(spark)
        Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", functions.rst_fromcontent(col("content"), lit("GTiff")))
    }

    private def validTile(): DataFrame = {
        val sc = spark
        import sc.implicits._
        TestL1.installAll(spark)
        Seq((1, s"$modisPath/$b01")).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
    }

    test("rst_xyzpyramid: a corrupt tile yields one row with a NULL tile (the pyramid struct cannot carry an error tile)") {
        import com.databricks.labs.gbx.rasterx.functions._
        val rows = corruptTile().select(rst_xyzpyramid(col("raster"), lit(0), lit(1)).as("t")).collect()
        rows.length shouldBe 1
        rows.head.isNullAt(0) shouldBe true
    }

    test("rst_xyzpyramid: a max_z beyond the cap raises the guard message instead of an opaque row-shape error") {
        import com.databricks.labs.gbx.rasterx.functions._
        val ex = intercept[Throwable] {
            validTile().select(rst_xyzpyramid(col("raster"), lit(0), lit(21)).as("t")).collect()
        }
        val chain = LazyList.iterate(Option(ex))(_.flatMap(t => Option(t.getCause))).takeWhile(_.isDefined).flatMap(_.toSeq).toList
        chain.exists(t => t.getMessage != null && t.getMessage.contains("max_z must be <=")) shouldBe true
    }

    test("rst_xyzpyramid: the argument guards run before the tile is read, so a corrupt tile still raises them") {
        import com.databricks.labs.gbx.rasterx.functions._
        val ex = intercept[Throwable] {
            corruptTile().select(rst_xyzpyramid(col("raster"), lit(3), lit(1)).as("t")).collect()
        }
        val chain = LazyList.iterate(Option(ex))(_.flatMap(t => Option(t.getCause))).takeWhile(_.isDefined).flatMap(_.toSeq).toList
        chain.exists(t => t.getMessage != null && t.getMessage.contains("max_z")) shouldBe true
    }

    test("rst_xyzpyramid: the tile-count cap raises its own message instead of a NULL row") {
        import com.databricks.labs.gbx.rasterx.functions._
        // The MODIS tile spans ~10 degrees; a pyramid down to z=20 is far beyond the 10^6 tile cap.
        val ex = intercept[Throwable] {
            validTile().select(rst_xyzpyramid(col("raster"), lit(0), lit(20)).as("t")).collect()
        }
        val chain = LazyList.iterate(Option(ex))(_.flatMap(t => Option(t.getCause))).takeWhile(_.isDefined).flatMap(_.toSeq).toList
        chain.exists(t => t.getMessage != null && t.getMessage.contains("exceeds")) shouldBe true
    }

    test("rst_xyzpyramid: bad render arguments raise instead of NULLing every row") {
        validTile() // registers the functions
        def raised(args: String): String = {
            val ex = intercept[Throwable] {
                validTile().selectExpr(s"gbx_rst_xyzpyramid(raster, 0, 0, $args) AS t").collect()
            }
            LazyList.iterate(Option(ex))(_.flatMap(t => Option(t.getCause))).takeWhile(_.isDefined).flatMap(_.toSeq)
                .flatMap(t => Option(t.getMessage)).mkString(" | ")
        }
        raised("'GIF', 64, 'bilinear'") should include("format must be one of")
        raised("'PNG', 64, 'nearest-ish'") should include("unsupported resampling")
        raised("'PNG', 0, 'bilinear'") should include("size must be in (0, 4096]")
        raised("'PNG', 5000, 'bilinear'") should include("size must be in (0, 4096]")
        raised("'PNG', 64, 'bilinear', 'bogus'") should include("rescale must be")
        raised("'PNG', 64, 'bilinear', '5,1'") should include("min < max")
        raised("'PNG', 64, 'bilinear', 'a,b'") should include("rescale")
    }

    test("rst_xyzpyramid: valid render arguments, in any case, still render tiles") {
        validTile() // registers the functions
        for (rescale <- Seq("'auto'", "'none'", "'0,10000'")) {
            val rows = validTile()
                .selectExpr(s"gbx_rst_xyzpyramid(raster, 0, 0, 'png', 64, 'Bilinear', $rescale) AS t")
                .select(col("t.z").as("z"), col("t.bytes").as("bytes"))
                .collect()
            withClue(s"rescale=$rescale: ") {
                rows.length should be > 0
                rows.foreach { r =>
                    r.isNullAt(0) shouldBe false
                    r.isNullAt(1) shouldBe false
                    r.getAs[Array[Byte]](1).length should be > 0
                }
            }
        }
    }

    // Declarative on purpose: the generators this suite covers are the ones its tests iterate over, so the
    // check does not depend on test order or on a `-z` filter.
    test("this suite covers every generator the contract assigns to it") {
        Set(xyzPyramid) shouldBe GeneratorContract.coveredBy(GeneratorContract.RasterSuite)
    }

}
