package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileLease, NodeFileManager}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.StructType
import org.gdal.gdal.gdal
import org.gdal.ogr.DataSource

import scala.util.control.NonFatal

/** What planning found for one file: a feature range `[start, end)` to read, or why the file cannot be
  * planned (`error`, with the GDAL message), or the driver GDAL detected for a directory member of another
  * vector format that is skipped (`skippedDriver`). Exactly one of the three is set; a range has neither. */
case class OgrPlan(path: String, start: Int, end: Int, error: String, skippedDriver: String)

/** Scan/Batch for OGR: plans partitions per (file, chunk) and supplies OGR_Reader as reader factory. */
class OGR_Batch(schema: StructType, options: Map[String, String]) extends Scan with Batch with Logging {

    /** Overrides Scan.readSchema: returns table schema. */
    override def readSchema(): StructType = schema

    /** Overrides Scan.toBatch: returns this batch. */
    override def toBatch: Batch = this

    /** Overrides Batch.planInputPartitions: one partition per (file, start, end) chunk; UDF counts features and splits by chunkSize.
      * A file that cannot be planned fails the read naming it, except in a directory read, where metadata files and
      * files of another vector format are skipped. */
    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val chunkSize = options.getOrElse("chunkSize", "10000").toInt
        require(chunkSize > 0, s"OGR chunkSize must be positive, got $chunkSize")
        val driverName = options.getOrElse("driverName", "")
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val asWKB = options.getOrElse("asWKB", "true").toBoolean
        val sparkSession = SparkSession.builder.getOrCreate
        val exprConfig = ExpressionConfig(sparkSession)
        import sparkSession.implicits._

        // This is needed to make sure correct hconf is set for HadoopUtils
        sparkSession.read.format("com.databricks.labs.gbx.ds.whitelist.WhitelistDataSource").option("path", inPath).load()

        // The planning UDF below runs on this driver and opens files: register GDAL and OGR first, as
        // schema inference does (a read with an explicit schema never runs inference in this JVM).
        GDALManager.init(exprConfig)
        GDALManager.initOgr()
        NodeFileManager.init(exprConfig.hConf)
        // Credential-aware listing (Spark file index) -- a raw driver-thread Hadoop FS listing
        // lacks the UC Volume / WSFS credential and throws FileNotFound on /Volumes.
        // Filter out OGR sidecar extensions (.dbf, .shx, .prj, .cpg, ...) before building
        // partitions: the ESRI Shapefile driver opens a loose .dbf as an attribute-only
        // datasource and counts its records, causing directories of unzipped shapefiles to
        // over-count (e.g. 200k-feature dir -> 600k rows). .shp.zip archives are unaffected
        // (no loose sidecars). Each .shp's stem-siblings are still staged via copyToPath.
        // Metadata-only files (.shp.xml, .aux.xml, .txt, .md) are not datasets and are left out too.
        val listed = HadoopUtils.listDataFilesSpark(sparkSession, inPath)
        val dirListing = OGR_Batch.isDirListing(inPath, listed)
        val primary = HadoopUtils.primaryDataFiles(listed)
        // A file named as the path is always planned, even a sidecar or a metadata-named file.
        val files = if (!dirListing && primary.isEmpty) listed else primary
        if (dirListing) {
            val metadata = listed.filter(HadoopUtils.isMetadataOnly)
            if (metadata.nonEmpty) {
                logWarning(
                  s"OGR read of $inPath skipped ${metadata.size} metadata file(s), which are not datasets: " +
                      metadata.take(5).mkString(", ") + (if (metadata.size > 5) ", ..." else "")
                )
            }
        }

        val filesDf = files.toDF("path")

        val offsetsUDF = udf { (path: String) =>
            // sidecar and metadata files are already filtered out above; this opens only primary data files
            OGR_Batch.planFile(path, driverName, layerN, layerName, chunkSize, dirListing)
        }

        val plans = filesDf
            .select(offsetsUDF(filesDf("path")).as("plans"))
            .select(explode(col("plans")).as("plan"))
            .select("plan.*")
            .as[OgrPlan]
            .collect()

        plans.filter(_.skippedDriver != null).foreach { p =>
            logWarning(
              s"OGR read of $inPath skipped ${p.path}: GDAL opens it with the ${p.skippedDriver} driver, " +
                  s"not the requested $driverName"
            )
        }
        val failed = plans.filter(_.error != null)
        if (failed.nonEmpty) {
            throw new IllegalArgumentException(
              s"OGR read failed to plan ${failed.length} of ${files.size} file(s) " +
                  s"(driver ${if (driverName.isEmpty) "auto-detect" else driverName}): " +
                  failed.take(5).map(p => s"${p.path}: ${p.error}").mkString("; ")
            )
        }

        val partitions = plans.filter(_.skippedDriver == null).map { p =>
            OGR_Partition(
              p.path,
              driverName,
              layerName,
              asWKB,
              schema,
              p.start,
              p.end,
              exprConfig
            )
        }
        partitions.toArray
    }

    /** Overrides Batch.createReaderFactory: builds OGR_Reader for each OGR_Partition. */
    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => {
            new OGR_Reader(partition.asInstanceOf[OGR_Partition])
        }

}

object OGR_Batch {

    // Drivers with no random feature access: OGR's GeoJSON driver re-parses the entire
    // FeatureCollection into memory on every open, so splitting one file into offset
    // chunks means each chunk re-opens + re-parses the whole file (~O(features * chunks)).
    // For these, plan ONE partition per file (parse once, read whole); parallelism comes
    // from reading many files concurrently. Seekable formats (Shapefile/.shx, GPKG, FileGDB)
    // keep chunking -- their opens are cheap and they seek to the chunk start.
    private val nonSeekable = Set("GeoJSON", "GeoJSONSeq")

    /** True when the listing came from a directory (or glob) rather than from a path naming one file or
      * dataset: a single listed entry equal to the path itself is the named file. The two spell the same
      * file differently (`file:///tmp/a` against `file:/tmp/a`, `dbfs:/Volumes/..` against `/Volumes/..`). */
    private[ogr] def isDirListing(inPath: String, listed: Seq[String]): Boolean = {
        def norm(p: String): String = HadoopUtils.cleanPath(p).stripPrefix("file:").dropWhile(_ == '/').stripSuffix("/")
        !(listed.size == 1 && norm(listed.head) == norm(inPath))
    }

    /** Plans one file: counts the features of the layer and splits `[0, n)` into `chunkSize` ranges (one
      * range for a driver that cannot seek, one empty range for an empty layer). It never throws and never
      * returns an empty array: a file that cannot be planned comes back as one plan carrying the reason,
      * or, in a directory read (`dirListing`) with a named driver, as one plan carrying the other driver
      * GDAL opens the file with, which the planner skips. The dataset it opens is deleted and the
      * node-file hold it takes is released on every path. */
    private[ogr] def planFile(
        path: String,
        driverName: String,
        layerN: Int,
        layerName: String,
        chunkSize: Int,
        dirListing: Boolean
    ): Array[OgrPlan] = {
        GDALManager.initOgr()
        var lease: NodeFileLease = null
        var dataset: DataSource = null
        try {
            lease = NodeFileManager.acquire(path)
            dataset = OGR_Driver.open(lease.localPath, driverName)
            if (dataset == null) {
                unplanned(path, lease, driverName, dirListing, s"${driverLabel(driverName)} could not open it: ${gdal.GetLastErrorMsg}")
            } else {
                val layer = if (layerName.isEmpty) dataset.GetLayer(layerN) else dataset.GetLayerByName(layerName)
                if (layer == null) {
                    val layers = (0 until dataset.GetLayerCount()).map(i => dataset.GetLayer(i).GetName())
                    unplanned(
                      path,
                      lease,
                      driverName,
                      dirListing,
                      s"layer '${if (layerName.isEmpty) layerN.toString else layerName}' not found; layers: ${layers.mkString(", ")}"
                    )
                } else {
                    layer.ResetReading()
                    val n = layer.GetFeatureCount().toInt
                    if (n < 0) {
                        unplanned(path, lease, driverName, dirListing, s"could not count its features: ${gdal.GetLastErrorMsg}")
                    } else if (n == 0) {
                        Array(OgrPlan(path, 0, 0, null, null))
                    } else if (nonSeekable.contains(driverName)) {
                        Array(OgrPlan(path, 0, n, null, null)) // one partition: chunking would re-parse the file per chunk
                    } else {
                        (0 until n by chunkSize).map(s => OgrPlan(path, s, Math.min(s + chunkSize, n), null, null)).toArray
                    }
                }
            }
        } catch {
            case NonFatal(e) =>
                unplanned(path, lease, driverName, dirListing, s"${e.getClass.getSimpleName}: ${e.getMessage}")
        } finally {
            try {
                if (dataset != null) dataset.delete()
            } finally {
                if (lease != null) lease.close()
            }
        }
    }

    private def driverLabel(driverName: String): String = if (driverName.isEmpty) "auto-detect" else driverName

    /** The plan for a file that could not be planned: its failure `msg`, or, for a directory member with a named
      * driver that GDAL opens with a different driver, that driver (the file is another vector format). */
    private def unplanned(path: String, lease: NodeFileLease, driverName: String, dirListing: Boolean, msg: String): Array[OgrPlan] = {
        val other = if (dirListing && driverName.nonEmpty && lease != null) otherDriver(lease.localPath, driverName) else null
        if (other == null) Array(OgrPlan(path, 0, 0, msg, null)) else Array(OgrPlan(path, 0, 0, null, other))
    }

    /** The name of the driver GDAL auto-detects for `local` when it is not `driverName`; null when GDAL opens
      * it with no driver or with `driverName`. */
    private def otherDriver(local: String, driverName: String): String =
        try {
            val detected = OGR_Driver.open(local, "")
            if (detected == null) null
            else {
                try {
                    val name = detected.GetDriver().getName
                    if (name == null || name.equalsIgnoreCase(driverName)) null else name
                } finally detected.delete()
            }
        } catch {
            case NonFatal(_) => null
        }

}
