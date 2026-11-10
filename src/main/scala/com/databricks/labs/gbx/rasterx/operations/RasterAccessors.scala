package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.{Dataset, gdal}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.{Failure, Try}

/** Dataset-level accessors: subdatasets metadata and in-memory size. */
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
      * (core/tiling._encoded_size_bytes), so both tiers report the same size instead of a -1
      * sentinel.
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

}
