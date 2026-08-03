package com.databricks.labs.gbx.rasterx.expressions.spectral

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
  * Helpers for building ``RST_MapAlgebra`` JSON specs from a calc string + a
  * map of single-letter band aliases to 1-based band indices.
  *
  * The 5 Wave 8b spectral-index expressions (EVI, SAVI, NDWI, NBR, Index) all
  * use the same single-source pattern: one input raster, multiple per-band
  * reads from that raster, and a per-pixel ``calc`` formula. ``MapAlgebra``'s
  * spec accepts ``A_index``/``A_band``/.../``Z_index``/``Z_band`` keys plus a
  * top-level ``calc`` and optional ``extra_options``; the helper here keeps
  * the JSON construction in one place and pins ``--type=Float32`` so the
  * gdal_calc output preserves fractional index values regardless of the input
  * dtype (Byte/UInt16 EO products would otherwise truncate).
  */
object SpectralIndexSpec {

    /** Cap at the MapAlgebra A..Z alphabet; far more than any built-in index needs. */
    private val MaxAliases = 26

    /**
      * Build a JSON spec where every band alias references the same source
      * dataset (index 0). Returns a string suitable to pass to
      * ``RST_MapAlgebra.execute(Seq(ds), Map.empty, spec)``.
      *
      * The calc must already reference the alias letters (``A``, ``B``, ...).
      * ``--type=Float32`` is appended via ``extra_options`` so the gdal_calc
      * result is a Float32 raster regardless of the input dtype.
      */
    def singleSourceSpec(calc: String, bandAliases: Seq[(String, Int)]): String = {
        require(calc != null && calc.nonEmpty, "calc formula required")
        require(bandAliases != null && bandAliases.nonEmpty, "at least one band alias required")
        require(bandAliases.length <= MaxAliases, s"too many band aliases (max $MaxAliases)")
        bandAliases.foreach { case (alias, idx) =>
            require(alias != null && alias.length == 1 && alias.charAt(0) >= 'A' && alias.charAt(0) <= 'Z',
                s"alias must be a single uppercase letter A..Z; got '$alias'")
            require(idx >= 1, s"band index for '$alias' must be 1-based >= 1; got $idx")
        }
        val parts = scala.collection.mutable.Buffer.empty[String]
        parts += "\"calc\":\"" + escape(calc) + "\""
        bandAliases.foreach { case (alias, idx) =>
            parts += "\"" + alias + "_index\":0"
            parts += "\"" + alias + "_band\":" + idx
        }
        parts += "\"extra_options\":\"--type=Float32\""
        "{" + parts.mkString(",") + "}"
    }

    /** JSON-escape backslashes and double-quotes inside the calc string. */
    private def escape(s: String): String =
        s.replace("\\", "\\\\").replace("\"", "\\\"")

    /**
      * gdal_calc can't read ``/vsimem/`` paths, so when an expression's eval
      * path opens the source dataset from in-memory bytes (binary tile flow)
      * we have to copy it to a local file before delegating to RST_MapAlgebra.
      * Returns ``(localDs, localPath)``; caller is responsible for releasing
      * ``localDs`` AND deleting ``localPath`` once the result has been
      * materialized.
      */
    def materializeToLocal(ds: Dataset): (Dataset, String) = {
        require(ds != null, "materializeToLocal: source Dataset is null")
        // Pre-create the per-JVM staging dir; on a fresh executor JVM this dir
        // does not yet exist and gdal_translate would fail to write into it.
        // (Same defensive create as PixelCombineRasters / ClipToGeom.)
        Files.createDirectories(NodeFilePathUtil.rootPath)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val path = s"${NodeFilePathUtil.rootPath}/spectral_$uuid.$extension"
        val (dsCpy, _) = GDALTranslate.executeTranslate(path, ds, "gdal_translate", Map.empty)
        (dsCpy, path)
    }

    /** Release the local copy from ``materializeToLocal``; tolerates missing files. */
    def releaseLocal(ds: Dataset, path: String): Unit = {
        if (ds != null) RasterDriver.releaseDataset(ds)
        if (path != null) Try(Files.deleteIfExists(Paths.get(path)))
    }

    /**
      * Shared Spark-side dispatch for all 5 spectral-index expressions.
      *
      * Handles the boilerplate that's identical across EVI / SAVI / NDWI /
      * NBR / Index:
      *
      *   1. Parse ``ExpressionConfig`` and initialise GDAL state.
      *   2. Deserialise the input tile row to a Dataset.
      *   3. For BinaryType (in-memory ``/vsimem/``) translate to a local
      *      file because gdal_calc.py can't read ``/vsimem/`` sources
      *      (mirrors the workaround in ``RST_MapAlgebra.eval`` and
      *      ``RST_NDVI.eval``).
      *   4. Invoke the caller-supplied compute function ``f(localDs)`` which
      *      returns the gdal_calc result ``(Dataset, metadata)``.
      *   5. Serialize the result back to an ``InternalRow`` and tidy up the
      *      temp files / Datasets in afterwards.
      *
      * Callers (the 5 case-class companions) only need to supply ``f`` -
      * everything else stays in one place.
      */
    def runRasterCalc(
        row: InternalRow, conf: UTF8String, dt: DataType
    )(f: Dataset => (Dataset, Map[String, String])): InternalRow = RST_ErrorHandler.safeEval(
        () => {
            val exprConf = ExpressionConfig.fromB64(conf.toString)
            RST_ExpressionUtil.init(exprConf)
            val (cell, ds, _) = RasterSerializationUtil.rowToTile(row, dt)
            // gdal_calc cannot read /vsimem/ - for BinaryType, copy to local first.
            val maybeLocal: Option[(Dataset, String)] =
                if (dt == BinaryType) Some(materializeToLocal(ds)) else None
            val calcDs = maybeLocal.map(_._1).getOrElse(ds)
            val (resDs, resMtd) = f(calcDs)
            // Release input handles - both the /vsimem/ original (binary) and
            // the local copy (binary) or the path-opened ds (string).
            maybeLocal.foreach { case (d, p) => releaseLocal(d, p) }
            RasterDriver.releaseDataset(ds)
            val out = RasterSerializationUtil.tileToRow((cell, resDs, resMtd), dt, exprConf.hConf)
            // gdal_calc writes its result to a real /tmp path - delete after
            // we've serialized the bytes.
            val resPath = if (resDs != null) resDs.GetDescription() else null
            RasterDriver.releaseDataset(resDs)
            if (resPath != null && !resPath.startsWith("/vsimem/")) {
                Try(Files.deleteIfExists(Paths.get(resPath)))
            }
            out
        },
        row,
        dt
    )

}
