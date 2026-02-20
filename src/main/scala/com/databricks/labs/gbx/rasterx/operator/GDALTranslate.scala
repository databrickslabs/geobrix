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
        val newOptions = Map(
          "path" -> outputPath,
          "sourcePath" -> sourcePath,
          "driver" -> raster.GetDriver().getShortName,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> size.toString, // For in memory we always return -1
          "format" -> raster.GetDriver().getShortName,
          "compression" -> options.getOrElse("compression", "ZSTD"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        Try(result.FlushCache())
        (result, newOptions)
    }

}
