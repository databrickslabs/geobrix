package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.gdal.ogr.ogr

import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Spark Data Source V2 provider for OGR-backed vector formats (Shapefile, GeoJSON, GeoPackage,
  * FileGDB, etc.). Infers schema from the first file at the given path and delegates to OGR_Table.
  */
//noinspection ScalaUnusedSymbol
class OGR_DataSource extends TableProvider with DataSourceRegister {

    /** Overrides TableProvider.inferSchema: first file at path via OGR_SchemaInference; initializes GDAL and NodeFileManager. */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        val driverName = if (options.containsKey("driverName")) options.get("driverName") else ""

        val sparkSession = SparkSession.builder.getOrCreate
        val config = ExpressionConfig(sparkSession)
        GDALManager.init(config)
        ogr.RegisterAll()

        val hConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf)
        val headPath = HadoopUtils.getFirstFile(options.get("path"), hConf)

        NodeFileManager.init(hConf)
        val localPath = NodeFileManager.readRemote(headPath)

        val schemaOpt = OGR_SchemaInference
            .inferSchemaImpl(
              driverName,
              localPath,
              options.asCaseSensitiveMap().asScala.toMap
            )

        NodeFileManager.releaseRemote(headPath)

        schemaOpt.getOrElse {
            throw new IllegalArgumentException(
              s"Unable to infer schema from file: $headPath. " +
              s"The file may be empty, corrupted, or in an unsupported format. " +
              s"Driver: ${if (driverName.isEmpty) "auto-detect" else driverName}"
            )
        }
    }

    /** Overrides TableProvider.getTable: returns OGR_Table with the given schema and properties. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new OGR_Table(schema, properties.asScala.toMap)
    }

    /** Overrides DataSourceRegister.shortName: returns "ogr". */
    override def shortName(): String = "ogr"

}
