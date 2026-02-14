package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import org.gdal.gdal.{Dataset, gdal}

/** Merges multiple single-band rasters into one multi-band raster via VRT (-separate) + gdal_translate. Returns (Dataset, metadata). Caller must release. */
object MergeBands {

    /** VRT from dss with -separate, then translate with given resampling; returns (Dataset, metadata). */
    def merge(dss: Seq[Dataset], options: Map[String, String], resampling: String): (Dataset, Map[String, String]) = {
        val uuid1 = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)

        val vrtPath = s"/vsimem/merge_bands_vrt_$uuid1.vrt"
        val rasterPath = s"/vsimem/merge_bands_$uuid1.$extension"

        val (vrtRaster, vrtOptions) = GDALBuildVRT.executeVRT(
          vrtPath,
          dss.toArray,
          options,
          command = s"gdalbuildvrt -separate -resolution highest"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdal_translate -r $resampling",
          vrtOptions
        )

        vrtRaster.delete()
        gdal.Unlink(vrtPath)

        result
    }

    /** Same as merge(dss, options, resampling) but with custom pixel size (x, y). Returns (Dataset, metadata). Caller must release. */
    def merge(
        dss: Seq[Dataset],
        options: Map[String, String],
        pixel: (Double, Double),
        resampling: String
    ): (Dataset, Map[String, String]) = {
        val uuid1 = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)

        val vrtPath = s"/vsimem/merge_bands_vrt_$uuid1.vrt"
        val rasterPath = s"/vsimem/merge_bands_$uuid1.$extension"

        val (vrtRaster, vrtOptions) = GDALBuildVRT.executeVRT(
          vrtPath,
          dss.toArray,
          options,
          command = s"gdalbuildvrt -separate -resolution user -tr ${pixel._1} ${pixel._2}"
        )

        val result = GDALTranslate.executeTranslate(
          rasterPath,
          vrtRaster,
          command = s"gdalwarp -r $resampling",
          vrtOptions
        )

        vrtRaster.delete()
        gdal.Unlink(vrtPath)

        result
    }

}
