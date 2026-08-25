// ExpressionConfigProjGridsSpec.scala
package com.databricks.labs.gbx.expressions

import com.databricks.labs.gbx.operations.ProjGridRegistry
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession

class ExpressionConfigProjGridsSpec extends PlanTest with SilentSparkSession {
  test("registry dirs fold into ExpressionConfig under the synthetic key") {
    ProjGridRegistry.set(Seq("/Volumes/a", "/Volumes/b"), replace = true)
    val ec = ExpressionConfig(spark)
    assert(ec.configs.get("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS")
      .contains("/Volumes/a:/Volumes/b"))
    ProjGridRegistry.set(Seq.empty, replace = true)
  }
}
