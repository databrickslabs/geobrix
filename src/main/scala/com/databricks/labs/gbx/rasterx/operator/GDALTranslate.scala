package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{Dataset, TranslateOptions, gdal}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

/** Runs gdal.Translate to write a Dataset to outputPath; returns (Dataset, metadata). Caller must release the returned Dataset. */
object GDALTranslate {

    /** Translates raster to outputPath; appends options via OperatorOptions. Returns (Dataset, metadata). */
    def executeTranslate(
        outputPath: String,
        raster: Dataset,
        command: String,
        options: Map[String, String]
    ): (Dataset, Map[String, String]) = {
        require(command.startsWith("gdal_translate"), "Not a valid GDAL Translate command.")
        val effectiveCommand = OperatorOptions.appendOptions(command, options, raster)
        val translateOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        val translateOptions = new TranslateOptions(translateOptionsVec)
        val result = gdal.Translate(outputPath, raster, translateOptions)
        val errorMsg = gdal.GetLastErrorMsg
        val sourcePath = raster.GetFileList().asScala.headOption.map(_.toString).getOrElse("unknown source path")
        val size = Try(Files.size(Paths.get(outputPath))).getOrElse(-1L)
        // TODO: build a JNA bridge for VSI mem estimate
        // Record the OUTPUT driver here, not the input's. Operations like
        // MergeRasters / MergeBands / PixelCombineRasters pass a VRT-driver
        // Dataset as input to executeTranslate; if we stamped the input's
        // driver (VRT) into the returned metadata, downstream code paths
        // (notably RasterDriver.writeToBytes via the extension calc and the
        // -of flag in OperatorOptions.appendOptions) would emit VRT bytes
        // for what is actually a materialized GTiff on disk — yielding a
        // tile.raster payload that references a /vsimem/ tempfile only
        // reachable on the producing executor.
        val outputDriverName = Option(result).map(_.GetDriver().getShortName).getOrElse(raster.GetDriver().getShortName)
        val newOptions = Map(
          "path" -> outputPath,
          "sourcePath" -> sourcePath,
          "driver" -> outputDriverName,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> size.toString, // For in memory we always return -1
          "format" -> outputDriverName,
          "compression" -> options.getOrElse("compression", OperatorOptions.DefaultCompression),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        Try(result.FlushCache())
        (result, newOptions)
    }

}
