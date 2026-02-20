package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.{Dataset, gdal}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.{Failure, Try}

/** Dataset-level accessors: subdatasets metadata, in-memory size, empty check, and vsimem unlink. */
object RasterAccessors {

    /** Returns the SUBDATASETS metadata as a name->description map, or empty if none. */
    def subdatasetsMap(ds: Dataset): Map[String, String] = {
        Try(ds.GetMetadata_Dict("SUBDATASETS")) match {
            case Failure(_) => Map.empty[String, String]
            case _          =>
                val dict = ds.GetMetadata_Dict("SUBDATASETS").asScala.toMap.asInstanceOf[Map[String, String]]
                dict
        }
    }

    /** Returns the size in bytes (file size or vsimem buffer length). */
    def memSize(ds: Dataset): Long = {
        val srcPath = ds.GetDescription()
        if (srcPath.contains("/vsimem/")) {
            gdal.GetMemFileBuffer(srcPath).length.toLong
        } else {
            Files.size(Paths.get(srcPath))
        }
    }

    /** Returns true if the dataset is null, has no size, or all bands have no valid pixels. */
    def isEmpty(ds: Dataset): Boolean = {
        if (ds == null || ds.GetRasterXSize <= 0 || ds.GetRasterYSize <= 0) return true
        val n = ds.GetRasterCount; if (n <= 0) return true
        (1 to n).forall(i => BandAccessors.isEmpty(ds.GetRasterBand(i)))
    }

    /** Releases the dataset and, if vsimem, unlinks the buffer; otherwise delegates to RasterDriver. */
    def unlink(ds: Dataset): Unit = {
        // TODO: move to RasterDriver
        if (ds == null) return
        val srcPath = ds.GetDescription()
        if (srcPath.contains("/vsimem/")) {
            ds.delete() // release the dataset
            gdal.Unlink(srcPath)
        } else {
            RasterDriver.releaseDataset(ds)
        }
    }

}
