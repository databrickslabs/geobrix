package com.databricks.labs.gbx.vectorx.ds.geojson

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** OGR-based TableProvider for GeoJSON (driverName = GeoJSON or GeoJSONSeq when multi=true). */
//noinspection ScalaUnusedSymbol
class GeoJSON_DataSource extends OGR_DataSource with DataSourceExtras {

    /** Overrides DataSourceExtras.dsExtraMap: GeoJSONSeq when multi=true, else GeoJSON. */
    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] = {
        if (checkMap.getOrElse("multi", "true").toBoolean) {
            Map("driverName" -> "GeoJSONSeq")
        } else {
            Map("driverName" -> "GeoJSON")
        }
    }

    /** Overrides parent shortName: returns "geojson_ogr". */
    override def shortName(): String = "geojson_ogr"

    /** Overrides parent inferSchema: delegates to super with dsExtraMap options (GeoJSON/GeoJSONSeq). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        super.inferSchema(extraCaseInsensitiveStringMap(options))
    }

    /** Overrides parent getTable: delegates to super with extra options merged. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
    }
}
