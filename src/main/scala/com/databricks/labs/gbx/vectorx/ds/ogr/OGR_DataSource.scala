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

import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Spark Data Source V2 provider for OGR-backed vector formats (Shapefile, GeoJSON, GeoPackage,
  * FileGDB, etc.). Infers schema from the first file at the given path and delegates to OGR_Table.
  */
//noinspection ScalaUnusedSymbol
class OGR_DataSource extends TableProvider with DataSourceRegister {

    /** Returns an actionable error message for write attempts on this read-only OGR format.
      * Subclasses override to name the specific format and its _gbx alternative.
      */
    protected def writeGuardMessage(path: String): String =
        s"'${shortName()}' is a read-only reader; write vector data with a light _gbx writer " +
        s"(shapefile_gbx, gpkg_gbx, file_gdb_gbx, geojson_gbx) or geojsonl_ogr for sharded GeoJSONL."

    /** Overrides TableProvider.inferSchema: first file at path via OGR_SchemaInference; initializes GDAL and NodeFileManager. */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        val driverName = if (options.containsKey("driverName")) options.get("driverName") else ""

        val sparkSession = SparkSession.builder.getOrCreate
        val config = ExpressionConfig(sparkSession)
        GDALManager.init(config)
        GDALManager.initOgr()

        // Enumerate + stage on the DRIVER through Spark's credential-forwarding listing/read.
        // A raw Hadoop FS getFileStatus on the analyzer thread lacks the UC Volume / WSFS
        // credential and throws FileNotFound on /Volumes (see HadoopUtils.listDataFilesSpark).
        val rawPath = options.get("path")
        val files = HadoopUtils.listDataFilesSpark(sparkSession, rawPath)
        // Use the PRIMARY data file as the schema head, not a sidecar: a shapefile dir lists as
        // [.cpg, .dbf, .prj, .shp, .shx], and files.head (.cpg) is not openable by OGR.
        val headPath = HadoopUtils.primaryDataFile(files).getOrElse(
          throw new IllegalArgumentException(s"No data files found under: $rawPath")
        )
        val lower = rawPath.toLowerCase(java.util.Locale.ROOT).stripSuffix("/")
        val isGdbLike =
          lower.endsWith(".gdb") || lower.endsWith(".gdb.zip") || lower.endsWith(".zip")
        val (localPath, releaseAfter) =
            if (isGdbLike) {
                // FileGDB / zipped dataset: stage the whole dataset. NodeFileManager's Hadoop copy runs
                // on this (analyzer) thread, which DCS dedicated clusters refuse for /Volumes; fall back
                // to credentialed query reads (see HadoopUtils.stageDatasetOrFallBack). The Hadoop config
                // only: the copy retention stays as this JVM's readers set it (the default otherwise).
                NodeFileManager.init(new SerializableConfiguration(sparkSession.sessionState.newHadoopConf))
                HadoopUtils.stageDatasetOrFallBack(
                  headPath,
                  NodeFileManager.readRemote,
                  NodeFileManager.releaseRemote,
                  p => HadoopUtils.stageDatasetForSchemaSpark(sparkSession, p)
                )
            } else {
                (HadoopUtils.stageHeadForSchemaSpark(sparkSession, headPath, files), false)
            }

        val schemaOpt =
            try {
                OGR_SchemaInference.inferSchemaImpl(driverName, localPath, options.asCaseSensitiveMap().asScala.toMap)
            } finally {
                // Let go of the staged copy even when inference throws: the one node-file hold
                // stageDatasetOrFallBack took (released by its local path, the generation it read), or the
                // temp copy of a staged head file or fallback, which nothing else uses.
                if (releaseAfter) NodeFileManager.releaseLocal(localPath)
                else HadoopUtils.discardSchemaCopy(localPath)
            }

        val headSchema = schemaOpt.getOrElse {
            throw new IllegalArgumentException(
              s"Unable to infer schema from file: $headPath. " +
              s"The file may be empty, corrupted, or in an unsupported format. " +
              s"Driver: ${if (driverName.isEmpty) "auto-detect" else driverName}"
            )
        }

        // When multiple .shp stems are present under a directory, verify they all share the
        // same schema. Silently merging divergent schemas produces a union mismatch that
        // surfaces as a confusing read error later; fail early with a clear message instead.
        // This mirrors the light-tier (Python) check in VectorGbxReader.schema() (Task B2).
        val stems = HadoopUtils.shpStems(files)
        if (stems.size > 1) {
            val optMap = options.asCaseSensitiveMap().asScala.toMap
            val headStem = stems.head
            stems.tail.foreach { otherStem =>
                // Look for <stem>.shp first; if absent fall back to <stem>.shp.zip.
                val otherShp = files.find { p =>
                    val n = p.replace("\\", "/").reverse.takeWhile(_ != '/').reverse
                    val lower = n.toLowerCase(java.util.Locale.ROOT)
                    lower == s"${otherStem.toLowerCase(java.util.Locale.ROOT)}.shp" ||
                    lower == s"${otherStem.toLowerCase(java.util.Locale.ROOT)}.shp.zip"
                }.getOrElse(throw new IllegalArgumentException(
                  s"shapefile reader: could not locate '$otherStem.shp' or '$otherStem.shp.zip' under $rawPath"
                ))
                val otherLocal = HadoopUtils.stageHeadForSchemaSpark(sparkSession, otherShp, files)
                val otherSchemaOpt =
                    try OGR_SchemaInference.inferSchemaImpl(driverName, otherLocal, optMap)
                    finally HadoopUtils.discardSchemaCopy(otherLocal)
                otherSchemaOpt.foreach { otherSchema =>
                    if (otherSchema != headSchema) {
                        throw new IllegalArgumentException(
                          HadoopUtils.shapefileDivergenceMsg(rawPath, headStem, otherStem)
                        )
                    }
                }
            }
        }

        headSchema
    }

    /**
      * Overrides TableProvider.supportsExternalMetadata: returns true so that on the write path
      * Spark passes the producer DataFrame's schema directly to getTable without calling inferSchema.
      * inferSchema is still called on the read path (Spark passes None as the user schema there).
      * The returned OGR_Table exposes a newWriteBuilder that immediately rejects the write with an
      * actionable UnsupportedOperationException, so no data is ever written through an OGR reader.
      */
    override def supportsExternalMetadata(): Boolean = true

    /** Overrides TableProvider.getTable: returns OGR_Table with the given schema and properties. */
    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        val path = Option(properties.get("path")).getOrElse("")
        new OGR_Table(schema, properties.asScala.toMap, writeGuardMessage(path))
    }

    /** Overrides DataSourceRegister.shortName: returns "ogr". */
    override def shortName(): String = "ogr"

}
