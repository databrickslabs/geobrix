package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.rasterx.ds.gdal.GDAL_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** GDAL TableProvider restricted to netCDF (driver = netCDF). Reads CF grid variables as
  * one (source, tile) row per variable; source = the NETCDF:"file":var subdataset selector.
  * Use format "netcdf_gdal". Read-only. */
//noinspection ScalaUnusedSymbol
class NetCDF_DataSource extends GDAL_DataSource with DataSourceExtras {

    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] =
        Map("driver" -> "netCDF")

    override def shortName(): String = "netcdf_gdal"

    override def inferSchema(options: CaseInsensitiveStringMap): StructType =
        super.inferSchema(extraCaseInsensitiveStringMap(options))

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table =
        new NetCDF_Table(schema, extraJavaUtilMap(properties).asScala.toMap)
}
