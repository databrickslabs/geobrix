package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.util.NodeFileManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._

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

    /** Encode a Dataset to bytes: always translates to a fresh /vsimem/ path so the output
      * Dataset is closed (via res.delete()) before GetMemFileBuffer reads it. Skipping the
      * close on the caller's writable Dataset leaves GTiff IFD/strip structures unfinalized
      * and GetMemFileBuffer returns a ~422-byte empty-header stub. The temp vsimem file is
      * unlinked after reading to avoid leaking memory across long-running jobs (e.g. many clips).
      *
      * Self-contained-payload invariant: serialized tile.raster bytes must be reachable from
      * any executor in the cluster — so we coerce VRT (which is just XML pointing at other
      * paths) to GTiff before serializing. Any caller upstream that hands us a VRT-driver
      * Dataset (or metadata claiming driver=VRT) gets a real GTiff out. Bytes are sniffed
      * post-write and re-translated if a VRT magic header still appears, as a belt-and-braces
      * check against future regressions or metadata drift. */
    def writeToBytes(ds: Dataset, options: Map[String, String]): Array[Byte] = {
        val isZip = options.getOrElse("isZip", "false").toBoolean
        // VRT bytes are never a valid self-contained tile.raster payload — they reference
        // /vsimem/ tempfiles that only exist on the producing executor. If metadata or the
        // Dataset itself reports VRT, coerce to GTiff for the on-wire serialization. The
        // Dataset's actual on-disk content is already materialized (MergeRasters et al
        // translate VRT → GTiff and return the GTiff Dataset); we just need to stop the
        // metadata from claiming otherwise.
        val rawDriverName = options.getOrElse("driver", Option(ds).map(_.GetDriver().getShortName).getOrElse("GTiff"))
        val driverName = if (rawDriverName == "VRT") "GTiff" else rawDriverName
        val translateOptions =
            if (rawDriverName == "VRT") options ++ Map("driver" -> "GTiff", "format" -> "GTiff")
            else options
        val extension = GDAL.getExtension(driverName)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val tempPath =
            if (isZip) s"/vsizip//vsimem/$uuid.zip/$uuid.$extension"
            else s"/vsimem/temp_raster_$uuid.$extension"
        ds.FlushCache()
        // Create a copy via gdal_translate to ensure proper format/compression AND a clean
        // close of the output handle (required to finalize GTiff headers in vsimem).
        val (res, _) = GDALTranslate.executeTranslate(tempPath, ds, "gdal_translate", translateOptions)
        res.FlushCache()
        res.delete()
        val bytes = gdal.GetMemFileBuffer(tempPath)
        gdal.Unlink(tempPath)
        // Defensive sniff: if somehow VRT bytes are about to be returned, log loud
        // rather than ship a broken-cross-executor payload. Caller code (smoke tests,
        // regression test, prod cluster) will see this in the executor log and we'll
        // know a future regression has slipped past the coercion above.
        if (bytes != null && bytes.length >= 12 && new String(bytes.take(12), "US-ASCII") == "<VRTDataset ") {
            // scalastyle:off println
            System.err.println(
              s"[geobrix] RasterDriver.writeToBytes: refusing to ship VRT bytes (driver=$rawDriverName); " +
                  s"this would produce a tile.raster payload unreachable across executors. " +
                  s"Re-translating to GTiff."
            )
            // scalastyle:on println
            val recoverPath = s"/vsimem/temp_raster_recover_$uuid.tif"
            val (resRecover, _) = GDALTranslate.executeTranslate(
              recoverPath, ds, "gdal_translate",
              options ++ Map("driver" -> "GTiff", "format" -> "GTiff")
            )
            resRecover.FlushCache()
            resRecover.delete()
            val recovered = gdal.GetMemFileBuffer(recoverPath)
            gdal.Unlink(recoverPath)
            recovered
        } else {
            bytes
        }
    }

}
