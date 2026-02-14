package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.gridx.bng
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession

class BNG_ExpressionEvalTest extends PlanTest with SilentSparkSession {

    test("BNG_ expressions should run in spark sql without errors") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        functions.register(spark)

        val df = spark
            .createDataFrame(
              Seq(
                ("TQ388791", 0.01),
                ("TQ388792", 0.01),
                ("TQ388793", 0.01)
              )
            )
            .toDF("cellId", "value")

        def runQuery(df: DataFrame): Unit = {
            val wkbLit = JTS.toWKB(JTS.fromWKT("POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"))
            val wktLit = "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"
            val coreCell = struct(df("cellId"), lit(true), lit(wkbLit))
            val nonCoreCell = struct(df("cellId"), lit(false), lit(wkbLit))
            df.select(
              bng_aswkb(df("cellId")),
              bng_aswkt(df("cellId")),
              bng_cellarea(df("cellId")),
              bng_cellintersection(coreCell, coreCell),
              bng_cellintersection(coreCell, nonCoreCell),
              bng_cellintersection(nonCoreCell, coreCell),
              bng_cellintersection(nonCoreCell, nonCoreCell),
              bng_cellunion(coreCell, coreCell),
              bng_cellunion(coreCell, nonCoreCell),
              bng_cellunion(nonCoreCell, coreCell),
              bng_cellunion(nonCoreCell, nonCoreCell),
              bng_centroid(df("cellId")),
              bng_distance(df("cellId"), df("cellId")),
              bng_eastnorthasbng(lit(100000), lit(200000), lit(3)),
              bng_eastnorthasbng(lit(100000), lit(200000), lit("50m")),
              bng_euclideandistance(df("cellId"), df("cellId")),
              bng_geomkring(lit(wkbLit), lit(1), lit(1)),
              bng_geomkloop(lit(wkbLit), lit(1), lit(1)),
              bng_kloop(df("cellId"), lit(1)),
              bng_kring(df("cellId"), lit(1)),
              bng_pointascell(lit(wkbLit), lit(1)),
              bng_polyfill(lit(wkbLit), lit(1)),
              bng_polyfill(lit(wkbLit), lit("50m")),
              bng_polyfill(lit(wktLit), lit(1)),
              bng_polyfill(lit(wktLit), lit("50m")),
              bng_tessellate(lit(wkbLit), lit(1)),
              bng_tessellate(lit(wktLit), lit(1)),
              bng_tessellate(lit(wkbLit), lit("50m")),
              bng_tessellate(lit(wktLit), lit("50m"))
            ).write
                .format("noop")
                .mode("overwrite")
                .save()
        }

        runQuery(df)
        runQuery(df.withColumn("cellId", lit(BNG.parse("TQ388791"))))

    }

    test("BNG_ aggregator expressions should run in spark sql without errors") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        functions.register(spark)

        val df = spark
            .createDataFrame(
              Seq(
                ("TQ388791", 0.01)
              )
            )
            .toDF("cellId", "value")

        def runQuery(df: DataFrame): Unit = {
            val wkbLit = JTS.toWKB(JTS.fromWKT("POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"))
            val coreCell = struct(df("cellId"), lit(true), lit(wkbLit))
            df
                .withColumn("gen", explode(array(lit(1), lit(2), lit(3))))
                .withColumn("dummy", lit(1))
                .withColumn("cell", coreCell)
                .repartition(10)
                .groupBy("dummy")
                .agg(
                  bng_cellunion_agg(col("cell")).as("unionCore"),
                  bng_cellintersection_agg(col("cell")).as("intersectCore")
                )
                .write
                .format("noop")
                .mode("overwrite")
                .save()
        }

        runQuery(df)
        runQuery(df.withColumn("cellId", lit(BNG.parse("TQ388791"))))

    }

    test("BNG_ generator expressions should run in spark sql without errors") {
        spark.sparkContext.setLogLevel("ERROR")
        import functions._
        bng.functions.register(spark)
        functions.register(spark)

        val df = spark
            .createDataFrame(
              Seq(
                ("TQ388791", 0.01),
                ("TQ388792", 0.01),
                ("TQ388793", 0.01)
              )
            )
            .toDF("cellId", "value")

        def runQuery(df: DataFrame): Unit = {
            val wkbLit = JTS.toWKB(JTS.fromWKT("POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"))
            val wktLit = "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"
            df.select(
              bng_geomkloopexplode(lit(wktLit), lit(1), lit(1)),
              bng_geomkloopexplode(lit(wkbLit), lit(1), lit(1)),
              bng_geomkringexplode(lit(wktLit), lit(1), lit(1)),
              bng_geomkringexplode(lit(wkbLit), lit(1), lit(1)),
              bng_kloopexplode(col("cellId"), lit(1)),
              bng_kringexplode(col("cellId"), lit(1)),
              bng_tessellateexplode(lit(wkbLit), lit(1)),
              bng_tessellateexplode(lit(wktLit), lit(1)),
              bng_tessellateexplode(lit(wkbLit), lit("500m")),
              bng_tessellateexplode(lit(wktLit), lit("500m"))
            ).write
                .format("noop")
                .mode("overwrite")
                .save()
        }

        runQuery(df)
        runQuery(df.withColumn("cellId", lit(BNG.parse("TQ388791"))))

    }

}
