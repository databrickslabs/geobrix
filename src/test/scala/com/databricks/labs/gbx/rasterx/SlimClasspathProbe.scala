package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.BngEastNorthAsBngUdf

/** Run by [[KernelFacadesSmokeTest]] in a child JVM whose classpath is only the facades, scala-library, JTS and
  * spark-sql-api (what a standard-mode or Unity Catalog sandbox offers). It must not touch Spark itself. */
object SlimClasspathProbe {

    private def attempt(body: => String): String =
        try body catch { case t: Throwable => "RAISES " + t.getClass.getSimpleName + ": " + t.getMessage }

    def main(args: Array[String]): Unit = {
        val udf = new BngEastNorthAsBngUdf()
        val lines = Seq(
          "L2 good: " + attempt(udf.call(530000.0, 180000.0, Integer.valueOf(3))),
          "L2 bad: " + attempt(udf.call(530000.0, 180000.0, Integer.valueOf(1001))),
          "UC good: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "-3")),
          "UC bad: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "1001")))
        // scalastyle:off println
        lines.foreach(println)
        // scalastyle:on println
    }
}
