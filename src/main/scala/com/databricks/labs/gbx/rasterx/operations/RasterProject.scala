package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/** Projects a raster to a new CRS via GDAL Warp (-t_srs); returns (Dataset, metadata). Caller must release. */
object RasterProject {

    /** Warps ds to dstSR (auth:code); writes to vsimem. */
    def project(ds: Dataset, options: Map[String, String], dstSR: SpatialReference): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver()
        val extension = GDAL.getExtension(driver.getShortName)
        val resultFileName = s"/vsimem/raster_project_$uuid.$extension"

        // Note that Null is the right value here
        val authName = dstSR.GetAuthorityName(null)
        val authCode = dstSR.GetAuthorityCode(null)

        GDALWarp.executeWarp(
          resultFileName,
          Array(ds),
          options,
          command = s"gdalwarp -t_srs $authName:$authCode"
        )
    }

}
