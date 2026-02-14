package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/** Pixel-wise average of input rasters via VRT Python pixel function; output type double. Returns (Dataset, metadata). Caller must release. */
object CombineAVG {

    /** Average = sum/div per pixel (div = count of non-zero); delegates to PixelCombineRasters. */
    def compute(rasters: Array[Dataset], options: Map[String, String]): (Dataset, Map[String, String]) = {

        val pythonFunc = """
                           |import numpy as np
                           |import sys
                           |
                           |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
                           |    stacked_array = np.array(in_ar)
                           |    pixel_sum = np.sum(stacked_array, axis=0)
                           |    div = np.sum(stacked_array > 0, axis=0)
                           |    div = np.where(div==0, 1, div)
                           |    np.divide(pixel_sum, div, out=out_ar, casting='unsafe')
                           |    np.clip(out_ar, stacked_array.min(), stacked_array.max(), out=out_ar)
                           |""".stripMargin
        PixelCombineRasters.combine(rasters, options, pythonFunc, "average")
    }

}
