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

    /** Returns the size in bytes (file size or vsimem buffer length), or -1 if not determinable.
      *
      * Uses `startsWith("/vsimem/")` — a loose `contains` would match subdataset selectors like
      * `NetCDF:/vsimem/xxx.nc:prAdjust`, where `GetMemFileBuffer` returns null and `.length` NPEs.
      *
      * For an in-memory (MEM-driver) dataset the description is empty — and for any other
      * non-filesystem description that is not a subdataset selector — we fall back to
      * encoding the dataset to an in-memory GTiff and measuring the buffer length. This is
      * the same authoritative "encoded raster byte size" the pyrx tiling path measures
      * (core/tiling._encoded_size_bytes), so power-of-4 splitting (BalancedSubdivision) agrees
      * across engines instead of collapsing to a single tile on a -1 sentinel.
      *
      * Subdataset selectors (e.g. `NetCDF:/vsimem/xxx.nc:prAdjust`) still return -1: they are
      * not whole-raster datasets and re-encoding them is neither meaningful nor cheap. */
    def memSize(ds: Dataset): Long = {
        val srcPath = ds.GetDescription()
        if (srcPath.startsWith("/vsimem/")) {
            val buf = gdal.GetMemFileBuffer(srcPath)
            if (buf == null) -1L else buf.length.toLong
        } else if (srcPath.startsWith("/")) {
            Try(Files.size(Paths.get(srcPath))).getOrElse(-1L)
        } else if (isSubdatasetSelector(srcPath)) {
            -1L
        } else {
            // MEM dataset (empty description) or other in-memory form: measure the encoded
            // GTiff byte length, matching the pyrx _encoded_size_bytes contract.
            Try(RasterDriver.writeToBytes(ds, Map.empty).length.toLong).getOrElse(-1L)
        }
    }

    /** True for a `DRIVER:/path:subdataset`-style selector (e.g. `NetCDF:/vsimem/x.nc:var`).
      * Such descriptions are not whole-raster datasets, so memSize cannot re-encode them. */
    private def isSubdatasetSelector(desc: String): Boolean =
        desc.contains(":/") && desc.split(":").length >= 3

    /** Returns true if the dataset is null, has no size, or no pixel of it is valid: the rule of both tiers
      * ([[com.databricks.labs.gbx.rasterx.gdal.ValidMask]]), a pixel being valid when some data band is
      * valid there and, for a raster with an alpha band, its alpha is above 0. An entirely transparent
      * raster is empty, whatever its band count. */
    def isEmpty(ds: Dataset): Boolean = {
        if (ds == null || ds.GetRasterXSize <= 0 || ds.GetRasterYSize <= 0) return true
        val n = ds.GetRasterCount; if (n <= 0) return true
        !com.databricks.labs.gbx.rasterx.gdal.ValidMask.anyValid(ds)
    }

    /** Releases the dataset and, if vsimem, unlinks the buffer; otherwise delegates to RasterDriver.
      *
      * Uses `startsWith("/vsimem/")` — a loose `contains` would match subdataset selectors like
      * `NetCDF:/vsimem/xxx.nc:prAdjust`, where `Unlink(selector)` is a silent no-op and the real
      * underlying vsimem file leaks. Falling through to `releaseDataset` for that case walks
      * `GetFileList` and unlinks the actual file(s). */
    def unlink(ds: Dataset): Unit = {
        // TODO: move to RasterDriver
        if (ds == null) return
        val srcPath = ds.GetDescription()
        if (srcPath.startsWith("/vsimem/")) {
            ds.delete() // release the dataset
            gdal.Unlink(srcPath)
        } else {
            RasterDriver.releaseDataset(ds)
        }
    }

}
