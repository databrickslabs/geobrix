package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/** End-to-end tests for the 9 gbx_quadbin_* functions: register them with Spark, build
  * input DataFrames, evaluate the columnar API, and assert on collected rows. */
class QuadbinFunctionsTest extends PlanTest with SilentSparkSession {

    test("gbx_quadbin_pointascell — non-zero cell at z=10 with resolution 10") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val df = spark.createDataFrame(Seq((-122.4194, 37.7749, 10))).toDF("lon", "lat", "z")
        val row = df.select(quadbin_pointascell(col("lon"), col("lat"), col("z")).alias("cell")).head()
        val cell = row.getLong(0)
        cell should not be 0L
        Quadbin.resolution(cell) shouldBe 10
    }

    test("gbx_quadbin_pointascell — bare decimal literals coerce without CAST (Decimal→Double fix)") {
        // Regression: Spark infers bare decimal literals (10.0) as DecimalType; without an
        // explicit inputTypes pin the reflective dispatch fails with INTERNAL_ERROR.
        // With the inputTypes = Seq(DoubleType, DoubleType, IntegerType) pin, Catalyst inserts
        // the Decimal→Double cast automatically and the query succeeds.
        functions.register(spark)
        val cell = spark.sql("SELECT gbx_quadbin_pointascell(10.0, 89.0, 10)").head().getLong(0)
        cell should not be 0L
        Quadbin.resolution(cell) shouldBe 10
    }

    test("gbx_quadbin_aswkb — returns parseable 5-point polygon EWKB at SRID=4326") {
        functions.register(spark)
        import functions._
        val cell = Quadbin.pointToCell(0.0, 0.0, 8)
        val df = spark.createDataFrame(Seq(Tuple1(cell))).toDF("cell")
        val wkb = df.select(quadbin_aswkb(col("cell")).alias("wkb")).head().getAs[Array[Byte]](0)
        wkb should not be null
        val poly = JTS.fromWKB(wkb)
        poly.getGeometryType shouldBe "Polygon"
        poly.getSRID shouldBe 4326
        poly.getCoordinates.length shouldBe 5
    }

    test("gbx_quadbin_centroid — returns a Point EWKB whose coords lie inside cell bbox") {
        functions.register(spark)
        import functions._
        val cell = Quadbin.pointToCell(151.2093, -33.8688, 12)
        val df = spark.createDataFrame(Seq(Tuple1(cell))).toDF("cell")
        val wkb = df.select(quadbin_centroid(col("cell")).alias("c")).head().getAs[Array[Byte]](0)
        val pt = JTS.fromWKB(wkb)
        pt.getGeometryType shouldBe "Point"
        pt.getSRID shouldBe 4326
        val (xmin, ymin, xmax, ymax) = Quadbin.cellBbox(cell)
        val x = pt.getCoordinate.x
        val y = pt.getCoordinate.y
        assert(x >= xmin - 1e-9 && x <= xmax + 1e-9)
        assert(y >= ymin - 1e-9 && y <= ymax + 1e-9)
    }

    test("gbx_quadbin_resolution — matches the input z for pointascell(_, _, z)") {
        functions.register(spark)
        import functions._
        val df = spark.range(1).select(quadbin_resolution(quadbin_pointascell(lit(0.0), lit(0.0), lit(15))).alias("z"))
        df.head().getInt(0) shouldBe 15
    }

    test("gbx_quadbin_polyfill — at z=5 over a small bbox returns >=1 cells, all at z=5") {
        functions.register(spark)
        import functions._
        val wkb = JTS.toWKB(JTS.fromWKT("POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"))
        val df = spark.createDataFrame(Seq(Tuple1(wkb))).toDF("geom")
        val cells = df.select(quadbin_polyfill(col("geom"), 5).alias("cells")).head().getAs[scala.collection.Seq[Long]](0)
        cells.size should be > 0
        cells.foreach(c => Quadbin.resolution(c) shouldBe 5)
    }

    test("gbx_quadbin_kring — returns 9 cells for an interior cell at k=1") {
        functions.register(spark)
        import functions._
        val cell = Quadbin.pointToCell(0.0, 0.0, 10)
        val df = spark.createDataFrame(Seq(Tuple1(cell))).toDF("cell")
        val ring = df.select(quadbin_kring(col("cell"), 1).alias("ring")).head().getAs[scala.collection.Seq[Long]](0)
        ring should have size 9
    }

    test("gbx_quadbin_tessellate — returns >=1 chip with cell + non-empty geom EWKB") {
        functions.register(spark)
        import functions._
        val wkb = JTS.toWKB(JTS.fromWKT("POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))"))
        val df = spark.createDataFrame(Seq(Tuple1(wkb))).toDF("geom")
        val chips = df.select(quadbin_tessellate(col("geom"), 5).alias("chips")).head().getAs[scala.collection.Seq[Row]](0)
        chips.size should be > 0
        chips.foreach { row =>
            val cell = row.getLong(0)
            val gbytes = row.getAs[Array[Byte]](1)
            Quadbin.resolution(cell) shouldBe 5
            gbytes should not be null
            gbytes.length should be > 0
        }
    }

    test("gbx_quadbin_cellunion — returns non-null geometry EWKB for an array of cells") {
        functions.register(spark)
        import functions._
        val cell = Quadbin.pointToCell(0.0, 0.0, 8)
        val neighbours = Quadbin.kRing(cell, 1).toSeq
        val df = spark.createDataFrame(Seq(Tuple1(neighbours))).toDF("cells")
        val wkb = df.select(quadbin_cellunion(col("cells")).alias("u")).head().getAs[Array[Byte]](0)
        wkb should not be null
        val geom = JTS.fromWKB(wkb)
        geom should not be null
        geom.getSRID shouldBe 4326
        Seq("Polygon", "MultiPolygon") should contain (geom.getGeometryType)
    }

    test("gbx_quadbin_distance — distance(cell, cell) == 0; adjacent neighbour distance == 1") {
        functions.register(spark)
        import functions._
        val cell = Quadbin.pointToCell(0.0, 0.0, 10)
        val neighbour = Quadbin.kRing(cell, 1).find(_ != cell).get
        val df = spark.createDataFrame(Seq((cell, cell, neighbour))).toDF("a", "b", "c")
        val Array(d0, d1) = df.select(
          quadbin_distance(col("a"), col("b")).alias("d0"),
          quadbin_distance(col("a"), col("c")).alias("d1")
        ).head() match { case r => Array(r.getInt(0), r.getInt(1)) }
        d0 shouldBe 0
        d1 shouldBe 1
    }

}
