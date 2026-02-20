package com.databricks.labs.gbx.rasterx.ds.gtiff

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.rasterx.ds.gdal.GDAL_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** GDAL TableProvider restricted to GeoTIFF (driver = GTiff). Use format "gtiff_gdal" to read/write .tif rasters. */
//noinspection ScalaUnusedSymbol
class GTiff_DataSource extends GDAL_DataSource with DataSourceExtras {

    /** Overrides DataSourceExtras.dsExtraMap: injects driver = GTiff. */
    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] = Map(
        "driver" -> "GTiff"
    )

    /** Overrides parent shortName: returns "gtiff_gdal". */
    override def shortName(): String = "gtiff_gdal"

    /** Overrides parent inferSchema: delegates to super with dsExtraMap options (GTiff). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        super.inferSchema(extraCaseInsensitiveStringMap(options))
    }

    /** Overrides parent getTable: delegates to super with extra options merged. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
    }
}
