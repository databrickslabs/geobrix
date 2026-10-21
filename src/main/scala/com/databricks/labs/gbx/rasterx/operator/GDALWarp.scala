package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{Dataset, WarpOptions, gdal}

import java.nio.file.{Files, Paths}
import scala.util.Try

/** Runs gdal.Warp to reproject/mosaic rasters to outputPath; returns (Dataset, metadata). Caller must release. */
object GDALWarp {

    /** Adds -s_srs EPSG:4326 when dataset has no SRS. */
    private def addSrcSRS(options: java.util.Vector[String], ds: Dataset): java.util.Vector[String] = {
        val srs = ds.GetSpatialRef()
        if (srs == null) {
            val srsVec = new java.util.Vector[String]()
            srsVec.add("-s_srs")
            srsVec.add("EPSG:4326")
            srsVec.addAll(options)
            srsVec
        } else {
            options
        }
    }

    /** An argument as it would be typed in a shell: quoted when it holds spaces (for `last_command`). */
    private def quoteIfSpaced(arg: String): String = if (arg.contains(" ")) "\"" + arg + "\"" else arg

    /** Warps dss to outputPath; appends options and injects SRS if needed. Returns (Dataset, metadata).
      * `extraArgs` are appended as given, one argument each: unlike `command`, they are not split on
      * spaces, so a value that holds spaces (a per-band `-srcnodata` list) stays one argument. `like`,
      * when given, stands for the input in the write options and the metadata (its bands pick the
      * predictor, its SRS, driver and description are recorded): for a caller that warps an in-memory
      * view of a dataset rather than the dataset itself. */
    def executeWarp(
        outputPath: String,
        dss: Array[Dataset],
        options: Map[String, String],
        command: String,
        extraArgs: Seq[String] = Seq.empty,
        like: Dataset = null
    ): (Dataset, Map[String, String]) = {
        require(command.startsWith("gdalwarp"), "Not a valid GDAL Warp command.")

        val ref = if (like != null) like else dss.head
        val parents = if (like != null) Array(like) else dss
        val effectiveCommand = OperatorOptions.appendOptions(command, options, ref)
        val warpOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        extraArgs.foreach(warpOptionsVec.add)
        val adjustedVec = addSrcSRS(warpOptionsVec, ref)
        val warpOptions = new WarpOptions(adjustedVec)
        val result = gdal.Warp(outputPath, dss, warpOptions)
        // Format will always be the same as the first raster
        val errorMsg = gdal.GetLastErrorMsg
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(-1)
        val newOptions = Map(
          "path" -> outputPath,
          "parentPath" -> options.getOrElse("path", ""),
          "driver" -> options.getOrElse("driver", ref.GetDriver().getShortName),
          "last_command" -> (effectiveCommand +: extraArgs.map(quoteIfSpaced)).mkString(" "),
          "last_error" -> errorMsg,
          "all_parents" -> parents.map(_.GetDescription()).mkString(";"), // TODO: this should be a union of all parents of all dss
          "size" -> size.toString,
          "format" -> ref.GetDriver().getShortName,
          "compression" -> options.getOrElse("compression", OperatorOptions.DefaultCompression),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        Try(result.FlushCache())
        (result, newOptions)
    }

}
