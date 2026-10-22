package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.GeneratorContract
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable

/**
  * Row-shape contract for the RasterX generators: every row a generator emits, on every path
  * (normal, error, passthrough), has to fit the single `tile` column its `elementSchema` declares.
  *
  * Spark projects generator rows through `elementSchema`. A generator that emits the tile row
  * itself (9 fields) instead of a one-field row wrapping it hands Spark a Long where it expects
  * a struct, which surfaces as an opaque ClassCastException instead of the documented error row.
  */
class RST_GeneratorRowShapeTest extends PlanTest with SilentSparkSession {

    private val exercised = mutable.Set.empty[String]

    private lazy val modisPath = this.getClass.getResource("/modis/").toString
    private val b01 = "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"

    /** One row holding a tile that cannot be read (error tile from rst_fromcontent on junk bytes). */
    private def corruptTile(): DataFrame = {
        val sc = spark
        import sc.implicits._
        functions.register(spark)
        Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", functions.rst_fromcontent(col("content"), lit("GTiff")))
    }

    private def validTile(): DataFrame = {
        val sc = spark
        import sc.implicits._
        functions.register(spark)
        Seq((1, s"$modisPath/$b01")).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
    }

    private val customGrid: Column =
        struct(lit(0L), lit(100L), lit(0L), lit(100L), lit(2), lit(10), lit(10), lit(32633))

    /** Every tile-emitting generator (SQL name, applied to the `raster` column). */
    private def tileGenerators(): Seq[(String, Column => Column)] = {
        import com.databricks.labs.gbx.rasterx.functions._
        Seq(
          "gbx_rst_retile"             -> ((r: Column) => rst_retile(r, lit(256), lit(256))),
          "gbx_rst_separatebands"      -> ((r: Column) => rst_separatebands(r)),
          "gbx_rst_tooverlappingtiles" -> ((r: Column) => rst_tooverlappingtiles(r, lit(256), lit(256), lit(10))),
          "gbx_rst_maketiles"          -> ((r: Column) => rst_maketiles(r, lit(4))),
          "gbx_rst_h3_tessellate"      -> ((r: Column) => rst_h3_tessellate(r, lit(3))),
          "gbx_rst_quadbin_tessellate" -> ((r: Column) => rst_quadbin_tessellate(r, lit(5))),
          "gbx_rst_bng_tessellate"     -> ((r: Column) => rst_bng_tessellate(r, lit(3))),
          "gbx_rst_custom_tessellate"  -> ((r: Column) => rst_custom_tessellate(r, customGrid, lit(2)))
        )
    }

    test("a failing tile yields one error row in the tile column for every tile generator") {
        val df = corruptTile()
        tileGenerators().foreach { case (name, gen) =>
            exercised += name
            val rows = df.select(gen(col("raster")).as("t")).select(col("t.metadata").getItem("error_message").as("err")).collect()
            withClue(s"$name: ") {
                rows.length shouldBe 1
                rows.head.isNullAt(0) shouldBe false
            }
        }
    }

    test("rst_maketiles with a non-positive size passes the tile through as one row in the tile column") {
        import com.databricks.labs.gbx.rasterx.functions._
        val df = validTile()
        val widths = df
            .select(rst_width(col("raster")).as("w"), rst_maketiles(col("raster"), lit(0)).as("t"))
            .select(col("w"), rst_width(col("t")).as("tw"))
            .collect()
        widths.length shouldBe 1
        widths.head.isNullAt(1) shouldBe false
        widths.head.getInt(1) shouldBe widths.head.getInt(0)
    }

    test("a generator error tile carries the cellid of the tile it failed on") {
        import com.databricks.labs.gbx.rasterx.functions._
        // Tessellate a real raster into tiles that have a cell id, then fail the next generator on them with
        // an invalid `assignment` argument (raised inside the generator's guarded body).
        val tiles = validTile().select(rst_h3_tessellate(col("raster"), lit(1)).as("t")).limit(2)
        val rows = tiles
            .select(col("t.cellid").as("in_cell"), rst_h3_tessellate(col("t"), lit(2), "no-such-assignment").as("e"))
            .select(
              col("in_cell"),
              col("e.cellid").as("out_cell"),
              col("e.metadata").getItem("error_message").as("err")
            )
            .collect()
        rows.length should be > 0
        rows.foreach { r =>
            r.isNullAt(2) shouldBe false // the error row, not a tessellated tile
            r.getLong(1) shouldBe r.getLong(0)
        }
    }

    test("rst_xyzpyramid: a corrupt tile yields one row with a NULL tile (the pyramid struct cannot carry an error tile)") {
        import com.databricks.labs.gbx.rasterx.functions._
        exercised += "gbx_rst_xyzpyramid"
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

    // Keep last: scalatest runs a suite's tests in registration order.
    test("this suite exercised every generator the contract assigns to it") {
        exercised.toSet shouldBe GeneratorContract.coveredBy(GeneratorContract.RasterSuite)
    }

}
