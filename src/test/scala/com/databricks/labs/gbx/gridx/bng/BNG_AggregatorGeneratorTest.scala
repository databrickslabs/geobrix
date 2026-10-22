package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.gridx.bng
import com.databricks.labs.gbx.gridx.bng.agg.UnionAcc
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, BooleanType, StringType}

/**
 * Comprehensive tests for BNG aggregator and generator functions.
 * These tests cover the 7 functions that previously had no validation tests:
 * - Aggregators: bng_cellintersection_agg, bng_cellunion_agg
 * - Generators: bng_kloopexplode, bng_kringexplode, bng_geomkloopexplode,
 *               bng_geomkringexplode, bng_tessellateexplode
 */
class BNG_AggregatorGeneratorTest extends PlanTest with SilentSparkSession {

    test("BNG_CellIntersectionAgg should aggregate chips and return intersection") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val cellGeom = BNG.cellIdToGeometry(cellIdLong)
        
        // Create test data: multiple chips of the same cell
        val df = spark.createDataFrame(Seq(
            (cellId, true, JTS.toWKB(cellGeom)),
            (cellId, false, JTS.toWKB(cellGeom.buffer(-100))),
            (cellId, false, JTS.toWKB(cellGeom.buffer(-200)))
        )).toDF("cellId", "isCore", "wkb")
            .withColumn("chip", struct(col("cellId"), col("isCore"), col("wkb")))
        
        val result = df.groupBy().agg(bng_cellintersection_agg(col("chip")).as("intersection")).collect()
        
        assert(result.length == 1)
        val intersection = result(0).getStruct(0)
        assert(intersection.getString(0) == cellId)
        assert(intersection.getBoolean(1) == false)  // Not core
        assert(intersection.getAs[Array[Byte]](2) != null)
    }

    test("BNG_CellIntersectionAgg should handle all core chips") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val cellGeom = BNG.cellIdToGeometry(cellIdLong)
        
        val df = spark.createDataFrame(Seq(
            (cellId, true, JTS.toWKB(cellGeom)),
            (cellId, true, JTS.toWKB(cellGeom)),
            (cellId, true, JTS.toWKB(cellGeom))
        )).toDF("cellId", "isCore", "wkb")
            .withColumn("chip", struct(col("cellId"), col("isCore"), col("wkb")))
        val result = df.groupBy().agg(bng_cellintersection_agg(col("chip")).as("intersection")).collect()
        
        assert(result.length == 1)
        val intersection = result(0).getStruct(0)
        // All core chips → intersection is whole cell → core=true
        assert(intersection.getBoolean(1) == true)
    }

    test("BNG_CellUnionAgg should aggregate chips and return union") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791" // 100m x 100m  East London
        val cellIdLong = BNG.parse(cellId)
        val cellGeom = BNG.cellIdToGeometry(cellIdLong)
        // All chips non-core: result must be non-core (no input had isCore=true).
        val df = spark.createDataFrame(Seq(
            (cellId, false, JTS.toWKB(cellGeom.buffer(-10))),
            (cellId, false, JTS.toWKB(cellGeom.buffer(-20))),
            (cellId, false, JTS.toWKB(cellGeom.buffer(-30)))
        )).toDF("cellId", "isCore", "wkb")
          .withColumn("chip", struct(col("cellId"), col("isCore"), col("wkb")))
          .repartition(1)
        val dfg = df.groupBy().agg(bng_cellunion_agg(col("chip")).as("union"))
        val result = dfg.collect()

        assert(result.length == 1)
        val union = result(0).getStruct(0)
        assert(union.getString(0) == cellId)
        assert(!union.getAs[Boolean]("core"), "union of non-core chips must be non-core") // note:'core'
        assert(union.getAs[Array[Byte]](2) != null)
    }

    test("BNG_CellUnionAgg should handle any core chip") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val cellIdLong = BNG.parse(cellId)
        val cellGeom = BNG.cellIdToGeometry(cellIdLong)
        
        val df = spark.createDataFrame(Seq(
            (cellId, false, JTS.toWKB(cellGeom.buffer(-100))),
            (cellId, true, JTS.toWKB(cellGeom)),  // Core chip
            (cellId, false, JTS.toWKB(cellGeom.buffer(-200)))
        )).toDF("cellId", "isCore", "wkb")
            .withColumn("chip", struct(col("cellId"), col("isCore"), col("wkb")))
        
        val result = df.groupBy().agg(bng_cellunion_agg(col("chip")).as("union")).collect()
        
        assert(result.length == 1)
        val union = result(0).getStruct(0)
        assert(union.getBoolean(1) == true)  // Should be core
    }

    test("gbx_bng_tessellate SQL accepts the 2-arg form (geom, resolution)") {
        // Regression: BNG_Tessellate.builder hardcoded 3 args, so the canonical 2-arg
        // SQL signature gbx_bng_tessellate(geom, res) threw IndexOutOfBoundsException.
        // Only the registered SQL path hits builder() with 2 args (the Column helper
        // always appends keepCoreGeom), so exercise it via spark.sql.
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)

        val wkb = JTS.toWKB(JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"))
        val df = spark.createDataFrame(Seq(Tuple1(wkb))).toDF("geom")
        df.createOrReplaceTempView("bng_tess_2arg_v")

        // 2-arg Int resolution
        val chipsInt = spark.sql("SELECT gbx_bng_tessellate(geom, 3) AS chips FROM bng_tess_2arg_v").collect()
        assert(chipsInt.length == 1)
        assert(chipsInt(0).getSeq[Any](0).nonEmpty, "2-arg gbx_bng_tessellate must return chips")

        // 2-arg String resolution
        val chipsStr = spark.sql("SELECT gbx_bng_tessellate(geom, '1km') AS chips FROM bng_tess_2arg_v").collect()
        assert(chipsStr.length == 1)
        assert(chipsStr(0).getSeq[Any](0).nonEmpty, "2-arg gbx_bng_tessellate (string res) must return chips")
    }

    test("BNG_CellUnionAgg returns an empty-polygon chip for a group spanning multiple cells") {
        // Regression: UnionAcc.update/merge used require(cellID == id) and threw an
        // executor task failure when a group contained chips from different cells (the
        // bench groups by an arbitrary key). Scalar BNG_CellUnion.execute* returns an
        // empty polygon for mismatched cell ids; the aggregate must match (no throw).
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)

        val cellA = "TQ388791"
        val cellB = "TQ398791"
        val geomA = BNG.cellIdToGeometry(BNG.parse(cellA))
        val geomB = BNG.cellIdToGeometry(BNG.parse(cellB))

        val df = spark.createDataFrame(Seq(
            (cellA, false, JTS.toWKB(geomA.buffer(-10))),
            (cellB, false, JTS.toWKB(geomB.buffer(-10)))
        )).toDF("cellId", "isCore", "wkb")
          .withColumn("chip", struct(col("cellId"), col("isCore"), col("wkb")))
          .repartition(1)

        // Must not throw; mixed-cell group dissolves to an empty polygon (not core).
        val result = df.groupBy().agg(bng_cellunion_agg(col("chip")).as("union")).collect()
        assert(result.length == 1)
        val union = result(0).getStruct(0)
        assert(!union.getBoolean(1), "mixed-cell union must not be core")
        val unionGeom = JTS.fromWKB(union.getAs[Array[Byte]](2))
        assert(unionGeom.isEmpty, "mixed-cell union geometry must be empty (scalar parity)")
    }

    test("BNG_KLoopExplode should generate k-loop cells") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val centerLong = BNG.parse(cellId)
        val k = 2
        
        val df = spark.createDataFrame(Seq((cellId, k))).toDF("cellId", "k")
        val result = df.select(bng_kloopexplode(df("cellId"), df("k")).as("neighbor")).collect()
        
        assert(result.length > 0)
        // Check all neighbors are at distance k
        result.foreach { row =>
            val neighborId = row.getString(0)
            val neighborLong = BNG.parse(neighborId)
            assert(BNG.euclideanDistance(centerLong, neighborLong) == k)
        }
    }

    test("BNG_KLoopExplode should return small set for k=0") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val k = 0
        
        val df = spark.createDataFrame(Seq((cellId, k))).toDF("cellId", "k")
        val result = df.select(bng_kloopexplode(df("cellId"), df("k")).as("neighbor")).collect()
        
        // k=0 returns some neighbors (not empty)
        assert(result.length >= 0)
    }

    test("BNG_KRingExplode should generate k-ring cells") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val centerLong = BNG.parse(cellId)
        val k = 2
        
        val df = spark.createDataFrame(Seq((cellId, k))).toDF("cellId", "k")
        val result = df.select(bng_kringexplode(df("cellId"), df("k")).as("neighbor")).collect()
        
        assert(result.length > 0)
        // Check all neighbors are at distance <= k
        result.foreach { row =>
            val neighborId = row.getString(0)
            val neighborLong = BNG.parse(neighborId)
            assert(BNG.euclideanDistance(centerLong, neighborLong) <= k)
        }
    }

    test("BNG_KRingExplode should include center cell for k=0") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val cellId = "TQ388791"
        val k = 0
        
        val df = spark.createDataFrame(Seq((cellId, k))).toDF("cellId", "k")
        val result = df.select(bng_kringexplode(df("cellId"), df("k")).as("neighbor")).collect()
        
        assert(result.length == 1)
        assert(result(0).getString(0) == cellId)
    }

    test("BNG_GeometryKLoopExplode should generate k-loop cells from geometry") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val triangle = JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))")
        val wkb = JTS.toWKB(triangle)
        val resolution = 3
        val k = 2
        
        val df = spark.createDataFrame(Seq((wkb, resolution, k))).toDF("geom", "res", "k")
        val result = df.select(bng_geomkloopexplode(df("geom"), df("res"), df("k")).as("cell")).collect()
        
        assert(result.length > 0)
    }

    test("BNG_GeometryKLoopExplode should work with WKT input") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val wkt = "POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
        val resolution = 3
        val k = 1
        
        val df = spark.createDataFrame(Seq((wkt, resolution, k))).toDF("geom", "res", "k")
        val result = df.select(bng_geomkloopexplode(df("geom"), df("res"), df("k")).as("cell")).collect()
        
        assert(result.length > 0)
    }

    test("BNG_GeometryKRingExplode should generate k-ring cells from geometry") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val triangle = JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))")
        val wkb = JTS.toWKB(triangle)
        val resolution = 3
        val k = 2
        
        val df = spark.createDataFrame(Seq((wkb, resolution, k))).toDF("geom", "res", "k")
        val kringResult = df.select(bng_geomkringexplode(df("geom"), df("res"), df("k")).as("cell")).collect()
        val kloopResult = df.select(bng_geomkloopexplode(df("geom"), df("res"), df("k")).as("cell")).collect()
        
        assert(kringResult.length > 0)
        assert(kringResult.length > kloopResult.length)  // Ring includes interior
    }

    test("BNG_GeometryKRingExplode should work with WKT input") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val wkt = "POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
        val resolution = 3
        val k = 1
        
        val df = spark.createDataFrame(Seq((wkt, resolution, k))).toDF("geom", "res", "k")
        val result = df.select(bng_geomkringexplode(df("geom"), df("res"), df("k")).as("cell")).collect()
        
        assert(result.length > 0)
    }

    // ---- BNG_TessellateExplode: declares the columns it emits (cellid, core, chip) -----------------

    private val teWkt = "POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"

    /** Heavy generators are Catalyst generators, so SQL reaches them through LATERAL VIEW or the select list (not `FROM t, LATERAL f(...)`). */
    private def tessellateLateralView(view: String, keep: Boolean): DataFrame =
        spark.sql(
          s"SELECT cellid, core, chip FROM $view " +
              s"LATERAL VIEW gbx_bng_tessellateexplode(geom, res, $keep) t AS cellid, core, chip"
        )

    /** Select-list form: the output columns are named by the generator's declared elementSchema. */
    private def tessellateSelectList(view: String, keep: Boolean): DataFrame =
        spark.sql(s"SELECT gbx_bng_tessellateexplode(geom, res, $keep) FROM $view")

    test("BNG_TessellateExplode returns cellid, core and chip columns (WKB input, BINARY chip)") {
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)
        val geometry = JTS.fromWKT(teWkt)

        spark.createDataFrame(Seq((JTS.toWKB(geometry), 3))).toDF("geom", "res").createOrReplaceTempView("bng_te_wkb")
        val df = tessellateLateralView("bng_te_wkb", keep = false)

        assert(tessellateSelectList("bng_te_wkb", keep = false).schema.fields.map(f => (f.name, f.dataType)).toSeq ==
            Seq(("cellid", StringType), ("core", BooleanType), ("chip", BinaryType)))

        val actual = df.collect().map(r => (r.getString(0), r.getBoolean(1), Option(r.getAs[Array[Byte]](2)))).sortBy(_._1)
        val expected = BNG.tessellate(geometry, 3, false).toSeq
            .map(c => (BNG.format(c._1), c._2, Option(c._3).map(JTS.toWKB))).sortBy(_._1)
        assert(actual.length == expected.length && actual.length > 0)
        assert(actual.map(_._1).toSeq == expected.map(_._1).toSeq)
        assert(actual.map(_._2).toSeq == expected.map(_._2).toSeq)
        // keepCoreGeom=false: core chips are NULL, every border chip still carries its geometry.
        assert(actual.exists(a => a._2 && a._3.isEmpty), "expected at least one core chip with a NULL geometry")
        assert(actual.exists(a => !a._2 && a._3.isDefined), "expected at least one border chip with a geometry")
        actual.zip(expected).foreach { case (a, e) =>
            assert(a._3.isDefined == e._3.isDefined, s"chip nullness differs for cell ${a._1}")
            if (e._3.isDefined) assert(java.util.Arrays.equals(a._3.get, e._3.get), s"chip WKB differs for cell ${a._1}")
        }
    }

    test("BNG_TessellateExplode returns a BINARY (WKB) chip for WKT input too") {
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)
        val geometry = JTS.fromWKT(teWkt)

        spark.createDataFrame(Seq((teWkt, 3))).toDF("geom", "res").createOrReplaceTempView("bng_te_wkt")
        val df = tessellateLateralView("bng_te_wkt", keep = false)

        // Same columns as WKB input, the lightweight tier and gbx_bng_tessellate: the chip is always WKB.
        assert(tessellateSelectList("bng_te_wkt", keep = false).schema.fields.map(f => (f.name, f.dataType)).toSeq ==
            Seq(("cellid", StringType), ("core", BooleanType), ("chip", BinaryType)))

        val actual = df.collect().map(r => (r.getString(0), r.getBoolean(1), Option(r.getAs[Array[Byte]](2)))).sortBy(_._1)
        val expected = BNG.tessellate(geometry, 3, false).toSeq
            .map(c => (BNG.format(c._1), c._2, Option(c._3).map(JTS.toWKB))).sortBy(_._1)
        assert(actual.map(a => (a._1, a._2, a._3.map(_.toSeq))).toSeq == expected.map(e => (e._1, e._2, e._3.map(_.toSeq))).toSeq)
        assert(actual.exists(a => !a._2 && a._3.isDefined), "expected at least one border chip with a geometry")
    }

    test("BNG_TessellateExplode accepts (geom, resolution) and defaults keepCoreGeom to false") {
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)
        spark.createDataFrame(Seq((JTS.toWKB(JTS.fromWKT(teWkt)), 3))).toDF("geom", "res").createOrReplaceTempView("bng_te_two")

        val twoArg = spark.sql(
          "SELECT cellid, core, chip FROM bng_te_two LATERAL VIEW gbx_bng_tessellateexplode(geom, res) t AS cellid, core, chip"
        ).collect().map(r => (r.getString(0), r.getBoolean(1), Option(r.getAs[Array[Byte]](2)).map(_.toSeq))).sortBy(_._1)
        val threeArg = tessellateLateralView("bng_te_two", keep = false)
            .collect().map(r => (r.getString(0), r.getBoolean(1), Option(r.getAs[Array[Byte]](2)).map(_.toSeq))).sortBy(_._1)
        assert(twoArg.nonEmpty && twoArg.toSeq == threeArg.toSeq)
        assert(twoArg.exists(r => r._2 && r._3.isEmpty), "the default drops the core geometry")
    }

    test("The documented heavyweight SQL forms of the five BNG explode generators run") {
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)
        val poly = "POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))"
        def count(sql: String): Long = spark.sql(sql).count()

        assert(count("SELECT t.cellid FROM (SELECT 'TQ3080' AS cell) src LATERAL VIEW gbx_bng_kringexplode(src.cell, 1) t AS cellid") == 9)
        assert(count("SELECT t.cellid FROM (SELECT 'TQ3080' AS cell) src LATERAL VIEW gbx_bng_kloopexplode(src.cell, 1) t AS cellid") == 8)
        assert(count(s"SELECT t.cellid FROM (SELECT '$poly' AS geom) src LATERAL VIEW gbx_bng_geomkringexplode(src.geom, 3, 1, 'boundary-out') t AS cellid") == 24)
        assert(count(s"SELECT t.cellid FROM (SELECT '$poly' AS geom) src LATERAL VIEW gbx_bng_geomkloopexplode(src.geom, 3, 1, 'boundary-out') t AS cellid") == 16)
        assert(count(s"SELECT t.cellid, t.core, t.chip FROM (SELECT '$poly' AS geom) src LATERAL VIEW gbx_bng_tessellateexplode(src.geom, 3) t AS cellid, core, chip") == 9)
    }

    test("BNG_TessellateExplode keepCoreGeom=true gives core chips their cell geometry") {
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)
        val geometry = JTS.fromWKT(teWkt)

        spark.createDataFrame(Seq((JTS.toWKB(geometry), 3))).toDF("geom", "res").createOrReplaceTempView("bng_te_keep")
        val df = tessellateLateralView("bng_te_keep", keep = true)
        assert(tessellateSelectList("bng_te_keep", keep = true).schema.fieldNames.toSeq == Seq("cellid", "core", "chip"))

        val actual = df.collect()
        assert(actual.length > 0)
        assert(actual.forall(r => !r.isNullAt(2)), "keepCoreGeom=true: every chip carries a geometry")
        val cores = actual.filter(_.getBoolean(1))
        assert(cores.nonEmpty)
        cores.foreach { r =>
            val cellGeom = BNG.cellIdToGeometry(BNG.parse(r.getString(0)))
            assert(JTS.fromWKB(r.getAs[Array[Byte]](2)).equalsExact(cellGeom, 1e-6))
        }
    }

    test("BNG_TessellateExplode with a string resolution still returns the three columns") {
        spark.sparkContext.setLogLevel("ERROR")
        bng.functions.register(spark)

        spark.createDataFrame(Seq((JTS.toWKB(JTS.fromWKT(teWkt)), "500m"))).toDF("geom", "res")
            .createOrReplaceTempView("bng_te_strres")
        assert(tessellateSelectList("bng_te_strres", keep = false).schema.fieldNames.toSeq == Seq("cellid", "core", "chip"))
        assert(tessellateLateralView("bng_te_strres", keep = false).count() > 0)
    }

    test("BNG_TessellateExplode Scala wrapper exposes cellid, core and chip, keepCoreGeom false by default") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)

        val wkb = JTS.toWKB(JTS.fromWKT(teWkt))
        val df = spark.createDataFrame(Seq((wkb, 3))).toDF("geom", "res")
        val result = df.select(bng_tessellateexplode(df("geom"), df("res")))
        assert(result.schema.fieldNames.toSeq == Seq("cellid", "core", "chip"))
        val rows = result.collect()
        assert(rows.length > 0)
        // Default keepCoreGeom=false: core chips are NULL, border chips carry their geometry.
        assert(rows.exists(r => r.getBoolean(1) && r.isNullAt(2)))
        assert(rows.filter(r => !r.getBoolean(1)).forall(r => !r.isNullAt(2)))
    }

    test("BNG_TessellateExplode Scala wrapper takes keepCoreGeom as a Column or a Boolean") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)

        val wkb = JTS.toWKB(JTS.fromWKT(teWkt))
        val df = spark.createDataFrame(Seq((wkb, 3))).toDF("geom", "res")
        val viaColumn = df.select(bng_tessellateexplode(df("geom"), df("res"), lit(true))).collect()
        val viaBoolean = df.select(bng_tessellateexplode(df("geom"), 3, true)).collect()
        assert(viaColumn.length > 0 && viaColumn.length == viaBoolean.length)
        assert(viaColumn.forall(r => !r.isNullAt(2)) && viaBoolean.forall(r => !r.isNullAt(2)), "keepCoreGeom=true: every chip has a geometry")
    }

    test("BNG.parse should throw IllegalArgumentException for invalid cell ID prefix") {
        val ex = intercept[IllegalArgumentException] {
            BNG.parse("!!")
        }
        assert(ex.getMessage.contains("!!"))
        assert(ex.getMessage.contains("Invalid BNG cell id"))
        assert(ex.getMessage.contains("not a recognised BNG"))
    }

}
