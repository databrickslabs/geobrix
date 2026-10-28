package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.kernels.uc.BngUc
import com.databricks.labs.gbx.kernels.udf.{BngEastNorthAsBngUdf, BngTessellateUdf}

import scala.jdk.CollectionConverters._

/** Run by [[KernelFacadesSmokeTest]] in a child JVM whose classpath is only the facades, scala-library, JTS and
  * spark-sql-api with what its `Row` needs (what a standard-mode or Unity Catalog sandbox offers). It must not touch
  * Spark itself, and says which of Catalyst, the unsafe types and Kryo could load (none should). */
object SlimClasspathProbe {

    private def attempt(body: => String): String =
        try body catch { case t: Throwable => "RAISES " + t.getClass.getSimpleName + ": " + t.getMessage }

    def main(args: Array[String]): Unit = {
        val udf = new BngEastNorthAsBngUdf()
        val tessellate = new BngTessellateUdf()
        val offset = "POLYGON((530100 180100, 532900 180100, 532900 181900, 530100 181900, 530100 180100))"
        val absent = Seq("org.apache.spark.sql.catalyst.InternalRow", "org.apache.spark.unsafe.types.UTF8String", "com.esotericsoftware.kryo.Kryo")
        // scalastyle:off classforname
        val loadable = absent.filter(n => try { Class.forName(n); true } catch { case _: Throwable => false })
        // scalastyle:on classforname
        val lines = Seq(
          "L2 good: " + attempt(udf.call(530000.0, 180000.0, Integer.valueOf(3))),
          "L2 bad: " + attempt(udf.call(530000.0, 180000.0, Integer.valueOf(1001))),
          "UC good: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "-3")),
          "UC bad: " + attempt(BngUc.eastNorthAsBng(530000.0, 180000.0, "1001")),
          "L2 tessellate good: " + attempt {
              val chips = tessellate.call(offset, Integer.valueOf(3), java.lang.Boolean.FALSE)
              chips.size + " " + chips.asScala.map(_.getString(0)).min // six chips; the lowest cell id
          },
          "L2 tessellate bad: " + attempt(tessellate.call(offset, "3km", java.lang.Boolean.FALSE).size.toString),
          "loadable: " + (if (loadable.isEmpty) "none" else loadable.mkString(",")))
        // scalastyle:off println
        lines.foreach(println)
        // scalastyle:on println
    }
}
