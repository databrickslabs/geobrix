package com.databricks.labs.gbx.rasterx.operations

import org.gdal.osr.SpatialReference

import scala.util.Try

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

    /** Resolves a CRS string to a SpatialReference — the ONE place the heavy-tier
      * int-cast rule lives (mirrors the light `pyrx.core.crs.resolve_crs`):
      *   - an int-castable string (e.g. `"4326"`, `" 32633 "`) → `ImportFromEPSG(int)`;
      *   - otherwise → `SetFromUserInput(value)`, GDAL's universal parser that accepts
      *     `EPSG:x` / `ESRI:x` / WKT / PROJ4 / auth strings.
      * Throws IllegalArgumentException if the value cannot be resolved to a valid CRS. */
    def resolveCrs(value: String): SpatialReference = {
        require(value != null, "resolveCrs: CRS value is null")
        val trimmed = value.trim
        require(trimmed.nonEmpty, "resolveCrs: CRS value is empty")
        val sr = new SpatialReference()
        Try(trimmed.toInt).toOption match {
            case Some(epsg) =>
                // ImportFromEPSG auto-recovers ESRI codes (e.g. 54008 -> ESRI:54008), so an
                // int-castable string classifies as EPSG or ESRI. A code in neither authority
                // returns a non-zero OGRERR — but the GDAL Java binding may ALSO throw a native
                // RuntimeException before returning; normalise both into IllegalArgumentException.
                val rc = Try(sr.ImportFromEPSG(epsg)).recover {
                    case e: Throwable =>
                        sr.delete()
                        throw new IllegalArgumentException(
                          s"resolveCrs: $epsg is not a valid EPSG or ESRI code", e)
                }.get
                if (rc != 0) {
                    sr.delete()
                    throw new IllegalArgumentException(
                      s"resolveCrs: $epsg is not a valid EPSG or ESRI code (OGRERR=$rc)")
                }
            case None =>
                // SetFromUserInput returns a non-zero OGRERR for unparseable input, but
                // the GDAL Java binding may ALSO throw a native RuntimeException before
                // returning; normalise both into a clean IllegalArgumentException.
                val rc = Try(sr.SetFromUserInput(trimmed)).recover {
                    case e: Throwable =>
                        sr.delete()
                        throw new IllegalArgumentException(
                          s"resolveCrs: could not parse CRS '$value'", e)
                }.get
                if (rc != 0) {
                    sr.delete()
                    throw new IllegalArgumentException(
                      s"resolveCrs: could not parse CRS '$value' (OGRERR=$rc)")
                }
        }
        sr
    }

    /** Canonical CRS string for a SpatialReference (mirrors the light
      * `crs_to_canonical`): the authority string `NAME:CODE` (e.g. `EPSG:4326`,
      * `ESRI:54008`) when the CRS carries one, else the full WKT. Returns null for a
      * null SpatialReference. */
    def crsToCanonical(spatialRef: SpatialReference): String = {
        if (spatialRef == null) return null
        (spatialRef.GetAuthorityName(null), spatialRef.GetAuthorityCode(null)) match {
            case (name: String, code: String) if name != null && name.nonEmpty &&
                code != null && code.nonEmpty => s"$name:$code"
            case _ => spatialRef.ExportToWkt()
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
