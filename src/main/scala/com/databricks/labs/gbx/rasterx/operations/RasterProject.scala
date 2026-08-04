package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

import java.nio.file.Files

/** Projects a raster to a new CRS via GDAL Warp (-t_srs); returns (Dataset, metadata). Caller must release. */
object RasterProject {

    /** Warps ds to dstSR; writes to vsimem.
      *
      * When the target CRS has an authority (`EPSG:4326`, `ESRI:54008`, ...) the warp
      * uses the compact `-t_srs NAME:CODE` form. For an authority-less target (e.g. a
      * bare WKT / PROJ4 CRS passed through `rst_transformcrs`), `GetAuthorityCode` is
      * null and the `NAME:CODE` string would be `null:null` — a broken target. In that
      * case fall back to the target's WKT (mirrors the `RST_Clip` `ImportFromWkt`
      * escape hatch). Because `GDALWarp.executeWarp` splits the command string on
      * spaces (and WKT is full of spaces), the WKT is written to a temp `.wkt` file and
      * passed by path: GDAL's `-t_srs` accepts a filename holding the SRS definition. */
    def project(ds: Dataset, options: Map[String, String], dstSR: SpatialReference): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resultFileName = s"/vsimem/raster_project_$uuid.$extension"

        // Note that Null is the right value here
        val authName = dstSR.GetAuthorityName(null)
        val authCode = dstSR.GetAuthorityCode(null)

        val hasAuthority =
            authName != null && authName.nonEmpty && authCode != null && authCode.nonEmpty

        if (hasAuthority) {
            GDALWarp.executeWarp(
              resultFileName,
              Array(ds),
              options,
              command = s"gdalwarp -t_srs $authName:$authCode"
            )
        } else {
            // Authority-less target (WKT/PROJ4): write the WKT to a temp file (no spaces
            // in the path) so the space-split command survives, and pass it by path.
            val wktFile = Files.createTempFile(s"raster_project_tsrs_$uuid", ".wkt")
            try {
                Files.write(wktFile, dstSR.ExportToWkt().getBytes("UTF-8"))
                GDALWarp.executeWarp(
                  resultFileName,
                  Array(ds),
                  options,
                  command = s"gdalwarp -t_srs ${wktFile.toString}"
                )
            } finally {
                Files.deleteIfExists(wktFile)
            }
        }
    }

}
