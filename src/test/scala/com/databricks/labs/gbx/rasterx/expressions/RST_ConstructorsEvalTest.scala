package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_ConstructorsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_ConstructorsEvalTest should evaluate expressions on raster columns") {
        import com.databricks.labs.gbx.rasterx.functions._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("rst_1", rst_fromcontent(col("content"), lit("GTiff")))
            .withColumn("rst_2", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy {
            df.collect()
        }

    }

}
