package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.util.SysUtils
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

import java.nio.file.{Files, Paths}
import scala.util.Try

/** Runs gdal_calc.py to produce a raster from a calc expression; returns (Dataset, metadata). */
object GDALCalc {

    /**
      * Split a command string into argv tokens on UNQUOTED whitespace, stripping the
      * matched surrounding quotes from each token.
      *
      * The command is built as one space-delimited string, but a value may itself contain
      * spaces — notably `--calc="(B - A) / (B + A)"`. A naive `split(" ")` would shatter that
      * into broken tokens (`--calc="(B`, `-`, `A)`, …) and gdal_calc would receive a malformed
      * `--calc`, producing empty output. This tokenizer keeps a quoted run (single or double
      * quotes) as one token and removes the quote characters, so gdal_calc gets the clean value.
      *
      * For a command with no quotes it yields exactly the same tokens as `split(" ")` did
      * (empty tokens dropped), so unquoted commands are unaffected. `Process(parts)` execs
      * directly (no shell), so quotes are ours to interpret here, not the shell's.
      */
    private[operator] def tokenizeCommand(command: String): Seq[String] = {
        val tokens = scala.collection.mutable.ArrayBuffer.empty[String]
        val cur = new StringBuilder
        var inToken = false
        var quote: Char = 0 // 0 = not in a quoted run; otherwise the active quote char
        command.foreach { c =>
            if (quote != 0) {
                if (c == quote) quote = 0 // closing quote — drop it, stay in the same token
                else { cur.append(c); () }
            } else if (c == '"' || c == '\'') {
                quote = c // opening quote — drop it, mark token started
                inToken = true
            } else if (c == ' ' || c == '\t') {
                if (inToken) { tokens += cur.toString; cur.setLength(0); inToken = false }
            } else {
                cur.append(c); inToken = true
            }
        }
        if (inToken) tokens += cur.toString
        tokens.toSeq
    }

    private val gdal_calc: String = {
        val calcPath = SysUtils.runCommand(Seq("which", "gdal_calc.py"))._1.split("\n").headOption.getOrElse("")
        if (calcPath.isEmpty) {
            throw new RuntimeException("Could not find gdal_calc.py.")
        }
        if (calcPath == "ERROR") {
            "/usr/lib/python3/dist-packages/osgeo_utils/gdal_calc.py"
        } else {
            calcPath
        }
    }

    /** Runs gdal_calc command (via python3), opens resultPath as Dataset; returns (Dataset, metadata). Caller must release. */
    def executeCalc(
        gdalCalcCommand: String,
        resultPath: String,
        options: Map[String, String],
        ds: Dataset
    ): (Dataset, Map[String, String]) = {
        require(gdalCalcCommand.startsWith("gdal_calc"), "Not a valid GDAL Calc command.")
        val effectiveCommand = OperatorOptions.appendOptions(gdalCalcCommand, options, ds)
        val toRun = effectiveCommand.replaceFirst("gdal_calc", gdal_calc)
        // Quote-aware tokenization: keeps a quoted, space-containing --calc value as one argv
        // token (see tokenizeCommand). A naive split(" ") mangled spaced expressions.
        val commandRes = SysUtils.runCommand(Seq("python3", "-u") ++ tokenizeCommand(toRun))
        val errorMsg = gdal.GetLastErrorMsg
        val result = gdal.Open(resultPath, GA_ReadOnly)
        val size = Try {
            if (resultPath.startsWith("/vsimem/")) gdal.GetMemFileBuffer(resultPath).length
            else Files.size(Paths.get(resultPath))
        }.getOrElse(-1L)
        // noinspection DuplicatedCode
        // TODO: make errors better, this is quite aggressive
        val newOptions = Map(
          "path" -> resultPath,
          "parentPath" -> resultPath,
          "driver" -> "GTiff",
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> resultPath,
          "size" -> size.toString,
          "full_error" -> s"""
                             |GDAL Calc command failed:
                             |GDAL err:
                             |$errorMsg
                             |STDOUT:
                             |${commandRes._2}
                             |STDERR:
                             |${commandRes._3}
                             |""".stripMargin
        )
        (result, newOptions)
    }

}
