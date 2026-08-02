package com.databricks.labs.gbx.rasterx.expressions.dem

import org.gdal.gdal.{Dataset, DEMProcessingOptions, gdal}

import java.util.UUID
import java.util.{Vector => JVector}

/**
  * Shared thin wrapper around `gdal.DEMProcessing` for terrain-analysis
  * expressions (slope, aspect, hillshade, TRI, TPI, roughness, color_relief).
  *
  * All 7 Wave 8a expressions follow the same pattern: take a single source
  * Dataset, run `gdal.DEMProcessing(processing, opts)` against it to materialize
  * a derived raster at a `/vsimem/` GTiff path, and return that result Dataset
  * together with output metadata. The caller is responsible for releasing the
  * returned Dataset (via `RasterDriver.releaseDataset` or `Dataset.delete()`).
  *
  * `processing` is the GDAL processing-mode string: "slope", "aspect",
  * "hillshade", "TRI", "TPI", "Roughness", "color-relief". `options` is the
  * sequence of command-line tokens (e.g. `Seq("-s", "1.0", "-p")`) that gets
  * forwarded into a `DEMProcessingOptions` Vector.
  *
  * For "color-relief" mode, callers must supply a fourth arg `colorFilename`;
  * for the other modes pass `null` (the GDAL Java binding accepts null).
  */
object RST_DEMProcessingHelper {

    /** Default output extension; GTiff is the RasterX binary-tile invariant. */
    private val OutputExtension = "tif"

    /**
      * Run gdal.DEMProcessing(processing, opts) against `srcDs` and return
      * (resultDataset, metadata). Caller must release the returned Dataset.
      *
      * The result lives at a `/vsimem/` GTiff path; downstream serialization
      * (RasterDriver.writeToBytes / tileToRow) handles materialization to bytes.
      */
    def process(
        srcDs: Dataset,
        processing: String,
        options: Seq[String] = Seq.empty,
        colorFilename: String = null
    ): (Dataset, Map[String, String]) = {
        require(srcDs != null, "RST_DEMProcessingHelper.process: source Dataset is null")
        require(processing != null && processing.nonEmpty, "RST_DEMProcessingHelper.process: processing mode required")

        val outPath = s"/vsimem/dem_${UUID.randomUUID().toString.replace("-", "")}.$OutputExtension"

        // Force GTiff output so the binary-tile path can serialize via toGTiffBytes.
        // GDAL's DEMProcessing defaults to GTiff for .tif output paths but we set
        // it explicitly to avoid surprises if the input driver implies something else.
        val opts = new JVector[String]()
        opts.add("-of")
        opts.add("GTiff")
        options.foreach(opts.add)

        val demOpts = new DEMProcessingOptions(opts)
        val result =
            try {
                gdal.DEMProcessing(outPath, srcDs, processing, colorFilename, demOpts)
            } finally {
                demOpts.delete()
            }
        val errMsg = gdal.GetLastErrorMsg()
        if (result == null) {
            throw new RuntimeException(
                s"gdal.DEMProcessing($processing) failed: " + (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg)
            )
        }
        result.FlushCache()

        val metadata = Map(
            "path" -> outPath,
            "driver" -> "GTiff",
            "extension" -> OutputExtension,
            "last_command" -> s"gdal.DEMProcessing($processing)",
            "last_error" -> (if (errMsg == null) "" else errMsg),
            "all_parents" -> Option(srcDs.GetDescription()).getOrElse(""),
            "size" -> "-1",
            "format" -> "GTiff",
            "compression" -> "DEFLATE",
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (result, metadata)
    }

}
