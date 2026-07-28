package com.databricks.labs.gbx.vectorx.ds.netcdf

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** OGR-based TableProvider for CF Discrete Sampling Geometry features in netCDF (driverName = netCDF).
  * Surfaces native DSG point/profile/trajectory features into the shared vector schema. Read-only.
  * Does NOT flatten swaths to per-cell points — that is the light netcdf_gbx vector mode only. */
//noinspection ScalaUnusedSymbol
class NetCDF_OGR_DataSource extends OGR_DataSource with DataSourceExtras {

    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] =
        Map("driverName" -> "netCDF")

    override def shortName(): String = "netcdf_ogr"

    override protected def writeGuardMessage(path: String): String =
        "'netcdf_ogr' is a read-only reader; write vector data with the light geojson_gbx writer " +
        "(or another _gbx vector writer)."

    override def inferSchema(options: CaseInsensitiveStringMap): StructType =
        super.inferSchema(extraCaseInsensitiveStringMap(options))

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table =
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
}
