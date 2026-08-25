package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/** Shared raster→grid reprojection guard for the H3/quadbin RasterToGrid families.
  *
  * H3 and quadbin interpret pixel centroids as EPSG:4326 lon/lat, so a raster in a
  * different CRS (e.g. UTM) must be reprojected to 4326 before the pixel→cell math —
  * otherwise easting/northing are silently read as lon/lat (a wrong-answer footgun).
  * Mirrors `RST_BNG_RasterToGrid`'s warp-to-27700, using nearest-neighbour resampling
  * because raster→grid aggregation is a pixel-counting operation and any interpolating
  * kernel would fabricate pixel values and corrupt the statistics.
  *
  * Never errors on an absent CRS: a raster with no projection is assumed already in the
  * grid-native CRS and returned unchanged.
  */
object GridReprojection {

    /** Reproject `ds` to `gridSrid` (EPSG) unless it is already there or CRS-less.
      *
      * @return `(workDs, reprojected)` — when `reprojected` is true the caller MUST
      *         release `workDs` (it is a fresh Dataset); when false, `workDs eq ds`.
      */
    def toGridCrs(ds: Dataset, gridSrid: Int): (Dataset, Boolean) = {
        val srcWkt = ds.GetProjection()
        // CRS-less -> assume already grid-native (no error, matches the light tier).
        if (srcWkt == null || srcWkt.isEmpty) return (ds, false)

        val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(gridSrid)
        val srcSR = new SpatialReference(); srcSR.ImportFromWkt(srcWkt)
        val alreadyNative = srcSR.IsSame(dstSR) == 1
        srcSR.delete(); dstSR.delete()
        if (alreadyNative) return (ds, false)

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver().getShortName)
        val resultPath = s"/vsimem/raster_grid_${gridSrid}_$uuid.$extension"
        val (result, _) = GDALWarp.executeWarp(
          resultPath,
          Array(ds),
          Map.empty[String, String],
          command = s"gdalwarp -t_srs EPSG:$gridSrid -r near"
        )
        (result, true)
    }
}
