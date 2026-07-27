package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition

/** One partition of a netcdf_gdal scan: one (file, subdataset variable) pair.
  * Opened by NetCDF_Reader as the GDAL subdataset selector NETCDF:"file":var. */
case class NetCDF_Partition(
    filePath: String,
    subdatasetName: String,
    sizeInMB: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
