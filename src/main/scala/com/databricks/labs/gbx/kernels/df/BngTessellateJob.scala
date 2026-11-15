package com.databricks.labs.gbx.kernels.df

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.{DataFrame, SparkSession}

case class PolyIn(wkt: String)
case class CellOut(cellid: String, core: Boolean, chip_area: Double)

/** Form (b): BNG tessellate as one Scala mapPartitions. transform() is called over py4j on dedicated
  * (Form level "JVM-DF"); main() is the E-JARtask entry point on Serverless. */
object BngTessellateJob {

    def transform(spark: SparkSession, polys: DataFrame, resolution: Int): DataFrame = {
        import spark.implicits._
        polys
            .select("wkt")
            .as[PolyIn]
            .mapPartitions(_.flatMap { p =>
                BNG.tessellate(JTS.fromWKT(p.wkt), resolution, false).map { case (id, core, chip) =>
                    CellOut(BNG.format(id), core, if (chip == null) 0.0 else chip.getArea)
                }
            })
            .toDF()
    }

    /** args: <source table with a wkt column> <target table> <resolution> */
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val t0 = System.nanoTime()
        transform(spark, spark.table(args(0)), args(2).toInt).write.mode("overwrite").saveAsTable(args(1))
        println(s"E-JARtask wall_s=${(System.nanoTime() - t0) / 1e9}")
    }

}
