/*
 * Doc test for the JVM job body (docs/docs/installation.mdx, "SQL from a JVM job").
 *
 * The Python task that registers the functions cannot run inside this suite. A function with the same
 * SQL name stands in for it: to the JVM, a Python-registered function is a name in the session registry.
 */
package docs.tests.scala.api

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class JvmJobBodyDocTest extends AnyFunSuite {

  test("the job body calls a function that the session registered, and registers nothing itself") {
    val spark = SparkSession.builder().master("local[1]").appName("jvm-job-body").getOrCreate()
    try {
      spark.udf.register("gbx_bng_cellarea", (_: String) => 1.0)
      spark.range(1).selectExpr("'TQ3080' AS cell").createOrReplaceTempView("jvm_job_cells")
      JvmJobBody.run(spark, "jvm_job_cells")
    } finally {
      spark.stop()
    }
  }
}
