package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager, NodeFilePathUtil}
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Low-level GDAL raster open/close and read/write.
  *
  * Handles path normalization (vsizip, subdatasets), copying remote paths to local or vsimem
  * for GDAL, and releasing datasets and associated files. Callers must call [[releaseDataset]]
  * when done with a [[Dataset]] to avoid leaks. Prefer [[readFromBytes]] when the raster is
  * already in memory to avoid temp-file lifecycle issues.
  */
object RasterDriver {

    /** True if path is a local filesystem path (not /Volumes/, /dbfs/, or remote). */
    def isLocal(path: String): Boolean = {
        // TODO: fix the file:/ case
        path.startsWith("/") && !path.startsWith("/Volumes/") && !path.startsWith("/dbfs/")
    }

    /** Normalizes path: strips file:, applies zip/subdataset handling when requested. */
    private def cleanPath(path: String, isZip: Boolean, isSubdataset: Boolean): String = {
        if (isZip) {
            if (isSubdataset) handleZipSubdataset(path)
            else handleZip(path)
        } else {
            if (isSubdataset) handleSubdataset(path)
            else path
        }
    }

    /** Converts path to /vsizip/ form when it looks like a zip. */
    private def handleZip(path: String): String = {
        // Ensure the path starts with /vsizip//
        if (path.startsWith("/vsizip//")) path
        else if (path.startsWith("/vsizip/")) path.replace("/vsizip/", "/vsizip//")
        else if (path.startsWith("vsizip/")) path.replace("vsizip/", "vsizip//")
        else if (path.startsWith("/")) s"/vsizip/$path"
        else s"/vsizip//$path"
    }

    /** Ensures subdataset path has DATASET_NAME= form for GDAL. */
    private def handleSubdataset(path: String): String = {
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        // Nothing to do here for subdatasets without zip
        path
    }

    /** vsizip path plus subdataset name for zip+subdataset combo. */
    private def handleZipSubdataset(path: String): String = {
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        val format :: filePath :: subdataset :: Nil = path.split(":").toList
        val cleanZip = handleZip(filePath)
        s"$format:$cleanZip:$subdataset"
    }

    /** If not local, copies path to node cache and returns local path; else returns path. */
    private def copyToLocal(path: String, isLocal: Boolean): String = {
        if (isLocal) path
        else NodeFileManager.readRemote(path)
    }

    /** Open a raster from a path; normalizes vsizip/subdataset and copies remote to local if needed. Caller must release. */
    def read(path: String, options: Map[String, String], shared: Boolean = false): Dataset = {
        val isZip = options.getOrElse("isZip", "false").toBoolean
        val isSubdataset = options.getOrElse("isSubdataset", "false").toBoolean
        val isLocal = this.isLocal(path)
        val readPath = this.copyToLocal(path, isLocal)
        val cleanPath = this.cleanPath(readPath, isZip, isSubdataset)
        val flags = if (shared) GA_ReadOnly | OF_SHARED else GA_ReadOnly
        val dataset = org.gdal.gdal.gdal.Open(cleanPath, flags)
        if (dataset == null) {
            val error = org.gdal.gdal.gdal.GetLastErrorMsg
            throw new RuntimeException(s"Failed to open dataset at path: $cleanPath; Error: $error")
        }
        dataset
    }

    /** Release a Dataset and any associated vsimem or copied files; must be called when done with a Dataset. */
    def releaseDataset(ds: Dataset): Unit = {
        if (ds != null) {
            ds.FlushCache()
            val files = ds.GetFileList().asScala.toSeq.map(_.toString)
            ds.delete()
            files.foreach(f => {
                if (f.contains("/vsimem/")) gdal.Unlink(f)
                else NodeFileManager.releaseRemote(f)
            })
        }
    }

    /** Open a raster from in-memory bytes (vsimem); avoids temp-file lifecycle. Caller must release. */
    def readFromBytes(bytes: Array[Byte], options: Map[String, String]): Dataset = {
        val driverName = options.getOrElse("driver", "GTiff")
        val isZip = options.getOrElse("isZip", "false").toBoolean
        val extension = GDAL.getExtension(driverName)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val tempPath =
            if (isZip) s"/vsizip//vsimem/$uuid.zip/$uuid.$extension"
            else s"/vsimem/temp_raster_$uuid.$extension"
        gdal.FileFromMemBuffer(tempPath, bytes)
        gdal.Open(tempPath)
    }

    /** Write a Dataset to path (local or remote via HadoopUtils); uses GDALTranslate for format/options. */
    def write(ds: Dataset, path: String, options: Map[String, String], hconf: SerializableConfiguration): Unit = {
        val isLocal = this.isLocal(path)
        val driver = ds.GetDriver()
        val isZip = options.getOrElse("isZip", "false").toBoolean
        val isSubdataset = options.getOrElse("isSubdataset", "false").toBoolean
        val writePath =
            if (isLocal) path
            else {
                val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
                val extension = GDAL.getExtension(driver.getShortName)
                s"${NodeFilePathUtil.rootPath}/$uuid.$extension"
            }
        val cleanPath = this.cleanPath(writePath, isZip, isSubdataset)
        ds.FlushCache()
        Files.createDirectories(Paths.get(cleanPath).getParent)
        // Create a copy via gdal_translate to ensure proper format, compression, etc.
        val (res, _) = GDALTranslate.executeTranslate(cleanPath, ds, "gdal_translate", options)
        res.FlushCache()
        res.delete()
        // If not local, copy the file to the remote path
        if (!isLocal) {
            HadoopUtils.copyToPath(cleanPath, path, hconf)
        }
    }

    /** Encode a Dataset to bytes (vsimem then GetMemFileBuffer); uses GDALTranslate when not already in memory. */
    def writeToBytes(ds: Dataset, options: Map[String, String]): Array[Byte] = {
        val isZip = options.getOrElse("isZip", "false").toBoolean
        val driverName = options.getOrElse("driver", "GTiff")
        val extension = GDAL.getExtension(driverName)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val isInMem = ds.GetDescription().contains("/vsimem/")
        ds.FlushCache()
        if (isInMem) {
            // Just return the buffer if the dataset is already in memory
            gdal.GetMemFileBuffer(ds.GetDescription())
        } else {
            val tempPath =
                if (isZip) s"/vsizip//vsimem/$uuid.zip/$uuid.$extension"
                else s"/vsimem/temp_raster_$uuid.$extension"
            // Create a copy via gdal_translate to ensure proper format, compression, etc.
            val (res, _) = GDALTranslate.executeTranslate(tempPath, ds, "gdal_translate", options)
            res.FlushCache()
            res.delete()
            gdal.GetMemFileBuffer(tempPath)
        }
    }

}
