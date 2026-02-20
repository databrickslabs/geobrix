package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.Dataset

/** Re-encodes a raster to a new GDAL format (e.g. COG, Zarr) via gdal_translate. */
object TranslateFormat {

    /** Converts the raster to newFormat; returns (new Dataset, metadata). Caller must release the Dataset. */
    def update(
        raster: Dataset,
        options: Map[String, String],
        newFormat: String
    ): (Dataset, Map[String, String]) = {

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(newFormat)
        val resultFileName = s"/vsimem/translate_format_$uuid.$extension"

        val result = GDALTranslate.executeTranslate(
          resultFileName,
          raster,
          command = s"gdal_translate",
          options ++ Map(
            "format" -> newFormat,
            "extension" -> extension
          )
        )

        result
    }
}
