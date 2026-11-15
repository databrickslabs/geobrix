package com.databricks.labs.gbx.kernels.udf

import com.databricks.labs.gbx.kernels.KernelGdal
import org.apache.spark.sql.api.java.UDF0

/** Java UDF the manager registers on a Spark Connect session to learn whether GDAL natives load. */
class GdalProbeUdf extends UDF0[String] {
    override def call(): String = KernelGdal.probe()
}
