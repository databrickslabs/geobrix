package com.databricks.labs.gbx.kernels.uc

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.KernelGdal

import scala.util.control.NonFatal

/** Unity Catalog Scala handlers. UC declares one type per parameter, so the resolution is a STRING
  * and an INT argument arrives as its digits. */
object BngUc {
    def eastNorthAsBng(easting: java.lang.Double, northing: java.lang.Double, resolution: String): String = {
        if (easting == null || northing == null || resolution == null) return null
        val res = if (resolution.matches("-?[0-9]+")) resolution.toInt else BNG.resolutionMap(resolution)
        try BNG.format(BNG.pointToCellID(easting, northing, res)) catch { case NonFatal(_) => null }
    }

    /** Expected to fail in the UC sandbox (no GDAL natives); the UC pass records the result. */
    def gdalVersion(): String = KernelGdal.probe()
}
