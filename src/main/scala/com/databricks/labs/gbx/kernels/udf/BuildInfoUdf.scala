package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.kernels.BuildInfo
import org.apache.spark.sql.api.java.UDF0

/** Java UDF the manager registers on a Spark Connect session to read the JAR identity. */
class BuildInfoUdf extends UDF0[String] {
    override def call(): String = BuildInfo.identity()
}
