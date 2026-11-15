package docs.tests.scala.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{call_function, col}

// The Scala body of a job whose entry point is a Python task. The Python task ran
// gbx.register(spark) and passed spark._jsparkSession, so every gbx_ name resolves here.
object JvmJobBody {

  def run(spark: SparkSession, cellTable: String): Unit = {
    // A scalar, by SQL name
    spark.sql("SELECT gbx_bng_cellarea('TQ3080') AS area_m2").show()

    // The same function in the DataFrame API
    spark.table(cellTable)
      .select(call_function("gbx_bng_cellarea", col("cell")).alias("area_m2"))
      .show()
  }
}
