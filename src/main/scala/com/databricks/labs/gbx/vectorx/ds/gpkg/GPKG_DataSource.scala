package com.databricks.labs.gbx.vectorx.ds.gpkg

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** OGR-based TableProvider for GeoPackage (driverName = GPKG). */
//noinspection ScalaUnusedSymbol
class GPKG_DataSource extends OGR_DataSource with DataSourceExtras {

    /** Overrides DataSourceExtras.dsExtraMap: injects driverName = GPKG. */
    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] = Map(
        "driverName" -> "GPKG"
    )

    /** Overrides parent shortName: returns "gpkg_ogr". */
    override def shortName(): String = "gpkg_ogr"

    /** Overrides parent writeGuardMessage: names gpkg_ogr and its gpkg_gbx alternative. */
    override protected def writeGuardMessage(path: String): String =
        "'gpkg_ogr' is a read-only reader; write vector data with the light gpkg_gbx writer " +
        "(or geojsonl_gbx for sharded GeoJSONL)."

    /** Overrides parent inferSchema: delegates to super with dsExtraMap options (GPKG). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        super.inferSchema(extraCaseInsensitiveStringMap(options))
    }

    /** Overrides parent getTable: delegates to super with extra options merged. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
    }
}
