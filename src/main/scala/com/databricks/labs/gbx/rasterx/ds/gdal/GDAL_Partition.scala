package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition

/**
  * Case class: one partition of a GDAL scan (one source file, tile size hint, expression config).
  * Passed to [[GDAL_Reader]], which opens the file and subdivides it into tiles via BalancedSubdivision.
  */
case class GDAL_Partition(
    filePath: String,
    sizeInMB: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
