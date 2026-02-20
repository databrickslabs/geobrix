package com.databricks.labs.gbx.vectorx.ds.gdb

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** OGR-based TableProvider for Esri File Geodatabase (driverName = OpenFileGDB). */
//noinspection ScalaUnusedSymbol
class FileGDB_DataSource extends OGR_DataSource with DataSourceExtras {

    /** Overrides DataSourceExtras.dsExtraMap: injects driverName = OpenFileGDB. */
    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] = Map(
        "driverName" -> "OpenFileGDB"
    )

    /** Overrides parent shortName: returns "file_gdb_ogr". */
    override def shortName(): String = "file_gdb_ogr"

    /** Overrides parent inferSchema: delegates to super with dsExtraMap options (File GDB). */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        super.inferSchema(extraCaseInsensitiveStringMap(options))
    }

    /** Overrides parent getTable: delegates to super with extra options merged. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
    }
}
