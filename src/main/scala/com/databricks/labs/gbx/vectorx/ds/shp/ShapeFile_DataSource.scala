package com.databricks.labs.gbx.vectorx.ds.shp

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** OGR-based TableProvider for ESRI Shapefile (driverName = ESRI Shapefile). */
//noinspection ScalaUnusedSymbol
class ShapeFile_DataSource extends OGR_DataSource with DataSourceExtras {

    /** Overrides DataSourceExtras.dsExtraMap: injects driverName = ESRI Shapefile. */
    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] = Map(
        "driverName" -> "ESRI Shapefile"
    )

    /** Overrides parent shortName: returns "shapefile_ogr". */
    override def shortName(): String = "shapefile_ogr"

    /** Overrides parent inferSchema: delegates to super with dsExtraMap options (shapefile driver). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        super.inferSchema(extraCaseInsensitiveStringMap(options))
    }

    /** Overrides parent getTable: delegates to super with extra options merged. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
    }
}
