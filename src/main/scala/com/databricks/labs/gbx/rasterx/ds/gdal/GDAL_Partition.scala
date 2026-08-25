package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition

/**
  * Case class: one partition of a GDAL scan (one source file, tile size hint, expression config).
  * Passed to [[GDAL_Reader]], which opens the file and subdivides it into tiles via BalancedSubdivision.
  *
  * ``clipCrs`` is the canonical CRS string for the ``clipCrs`` reader option (already
  * resolved), stamped into each tile's v2 ``clip_crs`` field. ``None`` when unset —
  * mirrors the lightweight reader (never errors on absent).
  */
case class GDAL_Partition(
    filePath: String,
    sizeInMB: Int,
    expressionConfig: ExpressionConfig,
    clipCrs: Option[String] = None
) extends InputPartition
      with Serializable
