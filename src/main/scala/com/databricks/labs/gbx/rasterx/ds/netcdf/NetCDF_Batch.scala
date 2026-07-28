package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.RasterAccessors
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.StructType
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

/** Scan/Batch for netcdf_gdal: one partition per (file, grid-variable subdataset). */
class NetCDF_Batch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema
    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val sizeInMB = options.getOrElse("sizeInMB", "-1").toInt
        val filterRegex = options.getOrElse("filterRegex", ".*\\.nc$")
        // Optional variable filter (empty => keep all). Names, comma-separated.
        val wanted = options.get("variables").orElse(options.get("variable"))
            .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet).getOrElse(Set.empty[String])

        val spark = SparkSession.builder.getOrCreate
        val exprConfig = ExpressionConfig(spark)
        import spark.implicits._

        val files = HadoopUtils.listDataFilesSpark(spark, inPath)
            .filter(_.matches(filterRegex))
        NodeFileManager.init(exprConfig.hConf)

        // Executor-side enumeration: open each file, list SUBDATASETS, keep grid variables.
        val enumUDF = udf { (path: String) =>
            try {
                // NodeFileManager.hconf is a JVM-static set only by init(); the driver-side
                // init(exprConfig.hConf) above does NOT propagate to executor JVMs, so this
                // UDF must init it here before readRemote (else readRemote hits a null hconf
                // and the whole enumeration is silently swallowed -> zero rows). Mirrors
                // RST_ExpressionUtil.init, which NetCDF_Reader calls on its own executor path.
                NodeFileManager.init(exprConfig.hConf)
                GDALManager.init(exprConfig)
                val localPath = NodeFileManager.readRemote(path)
                val ds = RasterDriver.read(localPath, Map.empty)
                // Shared "is this a true georeferenced raster grid?" probe, applied both to
                // subdataset selectors AND (below) to the file opened directly. A grid must be
                // bigger than 1x1 with >=1 band AND carry real georeferencing: a non-empty CRS
                // OR a non-identity geotransform. Swath products report empty CRS + identity GT
                // (georeferencing lives in a GEOLOCATION array) and are correctly rejected.
                def isGeorefGrid(g: org.gdal.gdal.Dataset): Boolean = {
                    val bigEnough = g.GetRasterXSize > 1 && g.GetRasterYSize > 1 && g.GetRasterCount >= 1
                    val hasCrs = { val p = g.GetProjectionRef; p != null && p.nonEmpty }
                    val gt = new Array[Double](6); g.GetGeoTransform(gt)
                    val identity = gt(0) == 0.0 && gt(1) == 1.0 && gt(2) == 0.0 &&
                                   gt(3) == 0.0 && gt(4) == 0.0 && gt(5) == 1.0
                    bigEnough && (hasCrs || !identity)
                }
                val subs = RasterAccessors.subdatasetsMap(ds)
                // SUBDATASET_i_NAME -> "NETCDF:\"file\":var"; take NAME entries only.
                val vars = subs.toSeq.filter(_._1.endsWith("_NAME")).map { case (_, sel) =>
                    sel.reverse.takeWhile(_ != ':').reverse   // trailing :var
                }.filter(v => !v.endsWith("_bnds") && !v.endsWith("_bounds"))
                // Keep only subdatasets that are true georeferenced raster grids.
                // A subdataset selector (NETCDF:"file":var) is not a filesystem path, so it is opened
                // directly via gdal.Open - RasterDriver.read would try to Hadoop-stage the selector string.
                // Swath subdatasets (e.g. S5P /PRODUCT/methane_mixing_ratio) report an empty projection
                // AND the identity geotransform [0,1,0,0,0,1] - their georeferencing lives in a GEOLOCATION
                // array. They are correctly dropped here, matching the light classify() which returns
                // CURVILINEAR and excludes them from raster mode. Regular grids (coral/CMIP/NASA-NEX) have
                // a real CRS or a non-identity geotransform and are kept.
                val grids = vars.filter { v =>
                    try {
                        val sub = gdal.Open(s"""NETCDF:"$localPath":$v""", GA_ReadOnly)
                        if (sub == null) false
                        else {
                            // Release the probe dataset in finally: if any GDAL JNI accessor throws
                            // between Open and delete, the handle must not leak (executors process
                            // many files/subdatasets per JVM).
                            try {
                                isGeorefGrid(sub)
                            } finally {
                                sub.delete()
                            }
                        }
                    } catch { case _: Throwable => false }
                }
                // Single-variable files have NO SUBDATASETS domain: GDAL opens them as a plain
                // raster (grids is empty). Fall back to emitting ONE partition for the WHOLE FILE
                // if the directly-opened dataset is itself a georeferenced grid. The empty
                // subdatasetName is the sentinel telling NetCDF_Reader to open the file directly
                // rather than build a (malformed) NETCDF:"file": selector with no variable.
                val partitionsForFile: Array[(String, String)] =
                    if (grids.nonEmpty) grids.map(v => (path, v)).toArray
                    else if (isGeorefGrid(ds)) Array((path, ""))
                    else Array.empty[(String, String)]
                RasterDriver.releaseDataset(ds)
                NodeFileManager.releaseRemote(path)
                partitionsForFile
            } catch {
                // A per-file enumeration failure must not silently vanish (a swallowed
                // executor-side error here previously surfaced only as "zero rows"). Print
                // the cause so a misconfigured file/driver is diagnosable, then skip the
                // file (one bad granule should not fail the whole scan).
                case t: Throwable =>
                    println(s"netcdf_gdal: subdataset enumeration failed for $path: " +
                        s"${t.getClass.getName}: ${t.getMessage}")
                    Array.empty[(String, String)]
            }
        }

        val pairs = files.toDF("path")
            .select(explode(enumUDF(col("path"))).as("p"))
            .select("p._1", "p._2").as[(String, String)].collect()

        pairs
            .filter { case (_, v) => wanted.isEmpty || wanted.contains(v) }
            .map { case (file, v) => NetCDF_Partition(file, v, sizeInMB, exprConfig) }
            .toArray[InputPartition]
    }

    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => new NetCDF_Reader(partition.asInstanceOf[NetCDF_Partition])
}
