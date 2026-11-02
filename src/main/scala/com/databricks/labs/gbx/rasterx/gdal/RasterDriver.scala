package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.util.NodeFileManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.control.NonFatal

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

    /** Open a raster from a path; normalizes vsizip/subdataset and copies remote to local if needed. Caller must
      * release the Dataset with [[releaseDataset]].
      *
      * Hold transfer: for a remote path this takes one hold on a node-file copy, and the Dataset opened on that
      * copy owns exactly that one hold. [[releaseDataset]] gives it back, because GDAL lists the copy's local
      * path; if the open fails, it is given back here. `isZip` / `isSubdataset` are refused for a remote path:
      * the Dataset would be opened on a /vsizip/ or subdataset form of the copy, which GDAL lists instead of
      * the copy's path, so its hold could never be given back. Stage such a source first (as NetCDF_Reader
      * does) and pass the local path. */
    def read(path: String, options: Map[String, String], shared: Boolean = false): Dataset = {
        val isZip = options.getOrElse("isZip", "false").toBoolean
        val isSubdataset = options.getOrElse("isSubdataset", "false").toBoolean
        val isLocal = this.isLocal(path)
        require(
          isLocal || (!isZip && !isSubdataset),
          s"RasterDriver.read: isZip / isSubdataset need a local path, got the remote $path; stage it first"
        )
        val readPath = this.copyToLocal(path, isLocal)
        try {
            val cleanPath = this.cleanPath(readPath, isZip, isSubdataset)
            val flags = if (shared) GA_ReadOnly | OF_SHARED else GA_ReadOnly
            val dataset =
                GDALManager.withHdfLockIf(GDALManager.isHdfFamilyPath(cleanPath))(org.gdal.gdal.gdal.Open(cleanPath, flags))
            if (dataset == null) {
                val error = org.gdal.gdal.gdal.GetLastErrorMsg
                throw new RuntimeException(s"Failed to open dataset at path: $cleanPath; Error: $error")
            }
            dataset
        } catch {
            // No Dataset owns the copy, so releaseDataset never will: let it go here.
            case NonFatal(e) =>
                if (!isLocal) NodeFileManager.releaseLocal(readPath)
                throw e
        }
    }

    /** Release a Dataset and any associated vsimem or copied files; must be called once when done with a Dataset.
      * Each distinct listed file is handled once: a /vsimem/ file is unlinked, a node-file copy's path gives
      * back the one hold the Dataset owns on it (see [[read]]), and any other path (a sidecar, a user's local
      * file) is left alone. */
    def releaseDataset(ds: Dataset): Unit = {
        if (ds != null) {
            val files = GDALManager.withHdfLockIf(GDALManager.isHdfFamily(ds)) {
                ds.FlushCache()
                val listed = ds.GetFileList().asScala.toSeq.map(_.toString).distinct
                ds.delete()
                listed
            }
            files.foreach(f => {
                if (f.contains("/vsimem/")) gdal.Unlink(f)
                else NodeFileManager.releaseLocal(f)
            })
        }
    }

    /** Open a raster from in-memory bytes (vsimem); avoids temp-file lifecycle. Caller must release.
      * Null for NULL bytes: they must never reach `gdal.FileFromMemBuffer`, which crashes the JVM on a null array.
      *
      * The one point where the JVM reads a materialized tile's bytes, so the bytes rule of [[TileBytes]] holds here
      * for every raster function: a driver that can name other files, or a tile decoded over the read cap, throws
      * [[TileBytesRefused]] (an error tile, with its `Fix:`); a driver outside the self-contained class is read as
      * its GeoTIFF translation. Null when no allowed driver reads the bytes. */
    def readFromBytes(bytes: Array[Byte], options: Map[String, String]): Dataset = readBytes(bytes, options, gated = true)

    /** As [[readFromBytes]], without the decoded-size check. For the callers that never decode the whole tile: a
      * header answer, and the retile family, which cuts the tile into windows (the function the gate's own Fix names).
      * The bytes rule still holds (a driver that can name other files is refused, any other is read as its GeoTIFF
      * translation, which for a non-GeoTIFF tile over the read cap is a full copy). A pixel reader uses
      * [[readFromBytes]]. */
    def readFromBytesUngated(bytes: Array[Byte], options: Map[String, String]): Dataset =
        readBytes(bytes, options, gated = false)

    /** The driver `bytes` open with, before any GeoTIFF translation: the tile's own format (netCDF, HFA, ...), which
      * [[readFromBytes]] hides for a driver outside the self-contained class. None when the bytes do not open; a
      * driver that can name other files is refused as it is on a read. Opens the header only. */
    def sourceDriverOf(bytes: Array[Byte], options: Map[String, String]): Option[String] =
        if (bytes == null) None
        else {
            val tempPath = bytesPath(options, java.util.UUID.randomUUID().toString.replace("-", "_"))
            gdal.FileFromMemBuffer(tempPath, bytes)
            var ds: Dataset = null
            try GDALManager.withHdfLockIf(GDALManager.isHdfFamilyBytes(bytes)) {
                try {
                    TileBytes.refusedClassOf(tempPath, bytes).foreach(d => throw TileBytes.refusal(d))
                    ds = gdal.OpenEx(tempPath, OF_RASTER | OF_READONLY, TileBytes.openDrivers)
                    Option(ds).map(_.GetDriver().getShortName)
                } finally if (ds != null) ds.delete()
            } finally gdal.Unlink(tempPath)
        }

    /** The in-memory path tile bytes are opened at: the one rule of [[openFromBytes]] and [[sourceDriverOf]], so the
      * driver a tile reports is the one its read uses (a zipped tile at its `/vsizip/` path). */
    private def bytesPath(options: Map[String, String], uuid: String): String = {
        val extension = GDAL.getExtension(options.getOrElse("driver", "GTiff"))
        if (options.getOrElse("isZip", "false").toBoolean) s"/vsizip//vsimem/$uuid.zip/$uuid.$extension"
        else s"/vsimem/temp_raster_$uuid.$extension"
    }

    /** [[readFromBytes]] or [[readFromBytesUngated]] by `gated`. */
    def readBytes(bytes: Array[Byte], options: Map[String, String], gated: Boolean): Dataset =
        if (bytes == null) null
        // netCDF / HDF bytes are opened, translated and closed one thread at a time (GDALManager.withHdfLock)
        else GDALManager.withHdfLockIf(GDALManager.isHdfFamilyBytes(bytes))(openFromBytes(bytes, options, gated, translate = true))

    /** As [[readFromBytesUngated]], but a driver outside the self-contained class is not translated to a GeoTIFF: the
      * tile's own dataset (netCDF, HDF5, ...), subdatasets and all, which the translation does not keep. For
      * `gbx_rst_subdatasets` and `gbx_rst_getsubdataset`, whose answer is the source file's structure. The bytes rule
      * still holds (a driver that can name other files is refused). The caller reads no pixel from it and releases it;
      * a netCDF / HDF dataset is opened here under [[GDALManager.withHdfLock]] and the caller holds that lock for the
      * rest of its use. */
    def readSourceFromBytes(bytes: Array[Byte], options: Map[String, String]): Dataset =
        if (bytes == null) null
        else GDALManager.withHdfLockIf(GDALManager.isHdfFamilyBytes(bytes))(openFromBytes(bytes, options, gated = false, translate = false))

    private def openFromBytes(bytes: Array[Byte], options: Map[String, String], gated: Boolean, translate: Boolean): Dataset = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val tempPath = bytesPath(options, uuid)
        gdal.FileFromMemBuffer(tempPath, bytes)
        // An unreadable or refused payload opens as null (or throws): nothing owns the in-memory file then, so unlink
        // it here.
        var ds: Dataset = null
        try {
            TileBytes.refusedClassOf(tempPath, bytes).foreach(d => throw TileBytes.refusal(d))
            ds = gdal.OpenEx(tempPath, OF_RASTER | OF_READONLY, TileBytes.openDrivers)
            if (ds == null) {
                gdal.Unlink(tempPath)
                return null
            }
            if (gated) TileBytes.checkSize(ds)
            // a file with no band (a netCDF / HDF container of several variables) has no pixel to translate: it is read
            // as its own dataset, which answers its header and subdatasets, as the light tier reads it
            if (!translate || TileBytes.selfContained(ds) || ds.getRasterCount == 0) ds
            else {
                val gtiff = TileBytes.toGTiff(ds, s"/vsimem/temp_raster_${uuid}_gtiff.tif")
                ds.delete()
                ds = null
                gdal.Unlink(tempPath)
                gtiff
            }
        } catch {
            case e: Exception =>
                if (ds != null) ds.delete()
                gdal.Unlink(tempPath)
                throw e
        }
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
