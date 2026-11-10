package com.databricks.labs.gbx.rasterx.expressions.dem

import com.databricks.labs.gbx.rasterx.operator.OperatorOptions
import org.gdal.gdal.{Dataset, DEMProcessingOptions, gdal}

import java.util.UUID
import java.util.{Vector => JVector}

/**
  * Shared thin wrapper around `gdal.DEMProcessing` for the terrain-analysis expression that still
  * has a JVM form (TRI).
  *
  * It takes a single source Dataset, runs `gdal.DEMProcessing(processing, opts)` against it to
  * materialize a derived raster at a `/vsimem/` GTiff path, and returns that result Dataset together
  * with output metadata. The caller is responsible for releasing the returned Dataset (via
  * `RasterDriver.releaseDataset` or `Dataset.delete()`).
  *
  * `processing` is the GDAL processing-mode string (for example "TRI"). `options` is the sequence of
  * command-line tokens (e.g. `Seq("-s", "1.0", "-p")`) that gets forwarded into a
  * `DEMProcessingOptions` Vector.
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
      * `who` is the calling SQL function's name; the argument errors are stated in its name.
      */
    def process(
        srcDs: Dataset,
        processing: String,
        who: String,
        options: Seq[String] = Seq.empty
    ): (Dataset, Map[String, String]) = {
        require(srcDs != null, s"$who: source Dataset is null")
        require(processing != null && processing.nonEmpty, s"$who: processing mode required")

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
                gdal.DEMProcessing(outPath, srcDs, processing, null, demOpts)
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
            "compression" -> OperatorOptions.DefaultCompression,
            "isZipped" -> "false",
            "isSubset" -> "false"
        )
        (result, metadata)
    }

}
