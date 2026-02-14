package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

/**
  * Case class: one partition of an OGR scan (file, layer, [start, end) feature range, schema, config).
  * Passed to [[OGR_Reader]], which opens the layer and yields feature rows from start to end.
  */
case class OGR_Partition(
    filePath: String,
    driver: String,
    layer: String,
    asWKB: Boolean,
    schema: StructType,
    start: Int,
    end: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
