package com.databricks.labs.gbx.rasterx.expressions.resample

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.gdal.gdal.Dataset

/**
  * Shared thin wrapper around `gdal.Warp` for the two JVM resample forms that remain.
  *
  *  - `gbx_rst_resample_to_size(tile, width_px, height_px, algorithm)` - explicit pixel dims
  *  - `gbx_rst_to_webmercator(tile, resampling)` - validates its algorithm here
  *
  * `warpToSize` passes `-r <algorithm>` and `-ts <w> <h>` to `gdalwarp`.
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

    /** An output size must be positive in both directions. */
    def checkSize(widthPx: Int, heightPx: Int): Unit = {
        require(widthPx > 0, s"rst_resample_to_size: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_resample_to_size: height_px must be positive; got $heightPx")
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

    /** Build a /vsimem path with the driver's natural extension (mirrors RST_ToWebMercator). */
    private def newVsimemPath(ds: Dataset): String = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val driver = ds.GetDriver()
        val ext = GDAL.getExtension(driver.getShortName)
        s"/vsimem/raster_resample_$uuid.$ext"
    }
}
