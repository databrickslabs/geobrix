package com.databricks.labs.gbx.gridx

import com.databricks.labs.gbx.DocExampleSource
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession

/**
  * The SQL tab of each quadbin and custom-grid explode function shows the heavyweight form and runs
  * in a heavyweight-only session. Heavy explode functions are Catalyst generators, reached through
  * `LATERAL VIEW` or the select list; `FROM t, LATERAL f(...)` resolves table functions only, so it
  * reaches nothing here (and the lightweight table function where both tiers are registered).
  */
class GridExplodeDocExamplesTest extends PlanTest with SilentSparkSession {

    private val nycBox = "POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))"
    private val bngBox = "POLYGON((530200 180200, 534800 180200, 534800 184800, 530200 184800, 530200 180200))"
    private val grid = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)"

    test("The documented heavyweight SQL examples of the quadbin and custom explode generators run") {
        spark.sparkContext.setLogLevel("ERROR")
        quadbin.functions.register(spark)
        custom.functions.register(spark)
        // Each explode must return one row per cell of the matching ARRAY function.
        Seq(
          "quadbin_geomkringexplode" -> s"gbx_quadbin_geomkring('$nycBox', 12, 1, 'boundary-out')",
          "quadbin_geomkloopexplode" -> s"gbx_quadbin_geomkloop('$nycBox', 12, 1, 'boundary-out')",
          "custom_geomkringexplode" -> s"gbx_custom_geomkring('$bngBox', $grid, 0, 1, 'boundary-out')",
          "custom_geomkloopexplode" -> s"gbx_custom_geomkloop('$bngBox', $grid, 0, 1, 'boundary-out')"
        ).foreach { case (fn, arrayCall) =>
            val example = DocExampleSource.example("gridx_functions_sql.py", fn)
            val sql = DocExampleSource.sqlOf(example)
            withClue(s"$fn: $sql\n") {
                assert(example.contains("-- heavyweight tier"), "the SQL tab says it is the heavyweight form")
                assert(sql.contains("LATERAL VIEW"), "the heavyweight example uses LATERAL VIEW")
                val cells = spark.sql(sql).collect().map(_.getLong(0)).sorted.toSeq
                val expected = spark.sql(s"SELECT $arrayCall AS a").head().getSeq[Long](0).sorted
                assert(cells.nonEmpty && cells == expected)
            }
        }
    }

}
