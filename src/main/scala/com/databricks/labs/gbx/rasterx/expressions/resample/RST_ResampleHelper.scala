package com.databricks.labs.gbx.rasterx.expressions.resample

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.gdal.gdal.Dataset

/**
  * Shared thin wrapper around `gdal.Warp` for the three resample expressions.
  *
  *  - `gbx_rst_resample(tile, factor, algorithm)` - multiplicative resample
  *  - `gbx_rst_resample_to_size(tile, width_px, height_px, algorithm)` - explicit pixel dims
  *  - `gbx_rst_resample_to_res(tile, x_res, y_res, algorithm)` - explicit ground resolution
  *
  * All three forms pass `-r <algorithm>` plus exactly one of `-ts <w> <h>` (size) or
  * `-tr <xres> <yres>` (resolution) to `gdalwarp`. Multiplicative `factor` is converted
  * to an explicit pixel size up-front so the same `-ts` path can serve both `factor`
  * and `to_size` callers.
  *
  * Caller is responsible for releasing the returned Dataset (via
  * `RasterDriver.releaseDataset` or `Dataset.delete()`).
  */
object RST_ResampleHelper {

    /** Allowed gdalwarp -r resampling algorithms (same set as RST_ToWebMercator). */
    val AllowedAlgorithms: Set[String] = Set(
        "near", "bilinear", "cubic", "cubicspline", "lanczos",
        "average", "mode", "max", "min", "med", "q1", "q3"
    )

    /** The `gdalwarp -r` word of `algorithm` (any case; `nearest` is `near`), or an error that names it and lists
      * the allowed ones. A caller checks it before its error handler, so a wrong word fails the call. */
    def validateAlgorithm(algorithm: String, fnName: String): String = {
        // scalastyle:off caselocale
        val lower = algorithm.toLowerCase
        // scalastyle:on caselocale
        val word = if (lower == "nearest") "near" else lower
        require(
            AllowedAlgorithms.contains(word),
            s"$fnName: unsupported resampling algorithm '$algorithm'; allowed: " +
                AllowedAlgorithms.toSeq.sorted.mkString(", ") + " (and nearest)"
        )
        word
    }

    /** A multiplicative factor must be a positive finite number. */
    def checkFactor(factor: Double): Unit =
        require(factor > 0.0 && !java.lang.Double.isInfinite(factor) && !java.lang.Double.isNaN(factor),
            s"rst_resample: factor must be a positive finite number; got $factor")

    /** An output size must be positive in both directions. */
    def checkSize(widthPx: Int, heightPx: Int): Unit = {
        require(widthPx > 0, s"rst_resample_to_size: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_resample_to_size: height_px must be positive; got $heightPx")
    }

    /** An output resolution must be positive in both directions. */
    def checkRes(xRes: Double, yRes: Double): Unit = {
        require(xRes > 0.0, s"rst_resample_to_res: x_res must be positive; got $xRes")
        require(yRes > 0.0, s"rst_resample_to_res: y_res must be positive; got $yRes")
    }

    /** Resample by an explicit output pixel size (width x height). */
    def warpToSize(
        ds: Dataset,
        options: Map[String, String],
        widthPx: Int,
        heightPx: Int,
        algorithm: String
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "rst_resample: source Dataset is null")
        checkSize(widthPx, heightPx)
        val alg = validateAlgorithm(algorithm, "rst_resample")
        val outPath = newVsimemPath(ds)
        GDALWarp.executeWarp(
            outPath,
            Array(ds),
            options,
            command = s"gdalwarp -ts $widthPx $heightPx -r $alg"
        )
    }

    /** Resample by an explicit ground resolution (xRes, yRes) in source CRS units. */
    def warpToRes(
        ds: Dataset,
        options: Map[String, String],
        xRes: Double,
        yRes: Double,
        algorithm: String
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "rst_resample_to_res: source Dataset is null")
        checkRes(xRes, yRes)
        val alg = validateAlgorithm(algorithm, "rst_resample_to_res")
        val outPath = newVsimemPath(ds)
        GDALWarp.executeWarp(
            outPath,
            Array(ds),
            options,
            command = s"gdalwarp -tr $xRes $yRes -r $alg"
        )
    }

    /** Resample by a multiplicative factor; >1 upsamples, 0<factor<1 downsamples. */
    def warpByFactor(
        ds: Dataset,
        options: Map[String, String],
        factor: Double,
        algorithm: String
    ): (Dataset, Map[String, String]) = {
        require(ds != null, "rst_resample: source Dataset is null")
        checkFactor(factor)
        val srcW = ds.GetRasterXSize
        val srcH = ds.GetRasterYSize
        val newW = math.max(1, math.round(srcW * factor).toInt)
        val newH = math.max(1, math.round(srcH * factor).toInt)
        warpToSize(ds, options, newW, newH, algorithm)
    }

    /** Build a /vsimem path with the driver's natural extension (mirrors RST_ToWebMercator). */
    private def newVsimemPath(ds: Dataset): String = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver()
        val ext = GDAL.getExtension(driver.getShortName)
        s"/vsimem/raster_resample_$uuid.$ext"
    }
}
