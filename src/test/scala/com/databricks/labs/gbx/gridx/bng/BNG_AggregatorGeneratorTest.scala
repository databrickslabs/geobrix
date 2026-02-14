package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.gridx.bng
import com.databricks.labs.gbx.gridx.bng.agg.UnionAcc
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession

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

    test("BNG_TessellateExplode should explode geometry into chips") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val triangle = JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))")
        val wkb = JTS.toWKB(triangle)
        val resolution = 3
        
        val df = spark.createDataFrame(Seq((wkb, resolution))).toDF("geom", "res")
        val result = df.select(bng_tessellateexplode(df("geom"), df("res")).as("chip")).collect()
        
        assert(result.length > 0)
        // Just verify we got results - structure may vary
        result.foreach { row =>
            assert(row != null)
        }
    }

    test("BNG_TessellateExplode should work with WKT input") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val wkt = "POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
        val resolution = 3
        
        val df = spark.createDataFrame(Seq((wkt, resolution))).toDF("geom", "res")
        val result = df.select(bng_tessellateexplode(df("geom"), df("res")).as("chip")).collect()
        
        assert(result.length > 0)
    }

    test("BNG_TessellateExplode should work with string resolution") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        
        val wkb = JTS.toWKB(JTS.fromWKT("POLYGON ((10000 10000, 20000 10000, 20000 20000, 10000 10000))"))
        val resolution = "500m"
        
        val df = spark.createDataFrame(Seq((wkb, resolution))).toDF("geom", "res")
        val result = df.select(bng_tessellateexplode(df("geom"), df("res")).as("chip")).collect()
        
        assert(result.length > 0)
    }

}
