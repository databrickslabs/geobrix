package com.databricks.labs.gbx.rasterx.operations

import org.gdal.osr.SpatialReference

/** Helpers for OSR SpatialReference: EPSG code extraction and construction from EPSG code. */
object SpatialRefOps {

    /** Returns the EPSG authority code as Int, or 0 if not EPSG (e.g. ESRI). */
    def getEPSGCode(spatialRef: SpatialReference): Int = {
        // Try to get the PROJCS/GEOGCS authority code
        // Returns 0 if no EPSG authority is found (e.g., for ESRI projections like ESRI:54008)
        (spatialRef.GetAuthorityName(null), spatialRef.GetAuthorityCode(null)) match {
            case (name: String, code: String) if name == "EPSG" => code.toInt
            case _                                              => 0  // Default to 0 for non-EPSG projections
        }
    }

    /** Builds a SpatialReference from an EPSG code; uses WGS84 if code <= 0. */
    def fromEPSGCode(getSRID: Int): SpatialReference = {
        val sr = new SpatialReference()
        if (getSRID > 0) {
            sr.ImportFromEPSG(getSRID)
        } else {
            sr.SetWellKnownGeogCS("WGS84") // Default to WGS84 if no valid EPSG code
        }
        sr
    }

}
