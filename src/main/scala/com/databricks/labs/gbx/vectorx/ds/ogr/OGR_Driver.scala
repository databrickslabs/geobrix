package com.databricks.labs.gbx.vectorx.ds.ogr

import org.gdal.ogr.{DataSource, ogr}

import java.util.Locale

/** OGR path handling (VSI/zip, .gdb.zip) and DataSource open; used by OGR reader and format-specific data sources. */
object OGR_Driver {

    /** Returns VSI path only (no inner .gdb for .gdb.zip). Used as fallback when derived inner name is wrong. */
    def vsiPathOnly(path: String): String = {
        val isZip = path.toLowerCase(Locale.ROOT).endsWith(".zip") ||
            path.toLowerCase(Locale.ROOT).contains(".zip/")
        if (!isZip) return path
        val vsi = if (path.startsWith("/vsizip//")) path
          else if (path.startsWith("/vsizip/")) path.replace("/vsizip/", "/vsizip//")
          else if (path.startsWith("vsizip/")) path.replace("vsizip/", "vsizip//")
          else if (path.startsWith("/")) s"/vsizip/$path"
          else s"/vsizip//$path"
        vsi.replace("file:", "")
    }

    /** For .zip: returns vsi path; for .gdb.zip returns vsi path with inner .gdb. Otherwise returns path. */
    def handleZip(path: String): String = {
        val isZip = path.toLowerCase(Locale.ROOT).endsWith(".zip") ||
            path.toLowerCase(Locale.ROOT).contains(".zip/")
        if (isZip) {
            val vsi = vsiPathOnly(path)
            // For .gdb.zip, try inner path first: /vsizip/path/to.zip/Name.gdb (e.g. NYC_Sample.gdb.zip -> NYC_Sample.gdb)
            if (path.toLowerCase(Locale.ROOT).endsWith(".gdb.zip")) {
              val base = path.substring(0, path.length - 4) // strip .zip
              val innerName = base.substring(Math.max(0, base.lastIndexOf('/') + 1))
              s"$vsi/$innerName"
            } else vsi
        } else {
            path
        }
    }

    /** Strips file: and applies handleZip. */
    def cleanPath(path: String): String = {
        handleZip(path).replace("file:", "")
    }

    /** Opens path with optional driver (empty = auto); for .gdb.zip tries vsi-only then cleanPath. */
    def open(path: String, driverName: String): DataSource = {
        val cp = cleanPath(path)
        if (driverName.nonEmpty) {
            val driver = ogr.GetDriverByName(driverName)
            if (driver == null) ogr.Open(cp, 0)
            else {
                // For .gdb.zip, try vsizip-only first so GDAL finds the .gdb folder inside (e.g. bridges.gdb.zip -> NYSDOTBridges.gdb)
                val vsiOnly = if (path.toLowerCase(Locale.ROOT).endsWith(".gdb.zip")) vsiPathOnly(path) else null
                var ds = if (vsiOnly != null) driver.Open(vsiOnly, 0) else null
                if (ds == null) ds = driver.Open(cp, 0)
                if (ds == null) throw new Exception(s"""
                                                   |Could not open dataset with driver $driverName at path $path
                                                   |GDAL Error: ${org.gdal.gdal.gdal.GetLastErrorMsg}
                                                   |""".stripMargin)
                ds
            }
        } else ogr.Open(cp, 0)
    }

}
