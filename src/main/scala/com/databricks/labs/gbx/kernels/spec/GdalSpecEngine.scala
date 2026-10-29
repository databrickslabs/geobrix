package com.databricks.labs.gbx.kernels.spec

import com.databricks.labs.gbx.kernels.KernelGdal
import org.gdal.gdal.{Algorithm, AlgorithmRegistry, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.util.UUID
import scala.jdk.CollectionConverters._

/** A call's outcome: the output GeoTIFF, or the reason it failed (an error tile), and the argv it ran. */
final case class SpecResult(raster: Array[Byte], error: String, command: String)

/** One engine call on the JVM's GDAL: the mirror of `engine/run.py`.
  *
  *   1. The input bytes are sniffed (GeoTIFF magic only), written to `/vsimem/gbx-spec/<uuid>/in<i>.tif` and their
  *      headers read (size, bands, type, CRS); the call must fit the decoded budget.
  *   1. The argv is bound ([[GdalSpecArgv.build]]) and checked; the algorithm is instantiated (single use), parsed,
  *      run and finalized; the output is read from `/vsimem`.
  *   1. Every file under the call's prefix is removed, and every algorithm handle deleted, whatever happened.
  *
  * A failure of GDAL's own run is a data error (an error tile with GDAL's message, its `/vsimem` names shown as
  * `<input>`/`<output>`); a value the binders or GDAL's parser refuse is a [[SpecParamException]]. GDAL's last error
  * is per thread, and nothing here sets a process-wide GDAL option, so concurrent tasks do not see each other.
  */
object GdalSpecEngine {

    val Prefix = "/vsimem/gbx-spec"
    /** The decoded bytes one call may hold on a classic JVM (`pyrx.core.budget`'s classic budget). */
    val DecodedBudget: Long = 1536L * 1024 * 1024

    private val TiffMagic: Seq[Seq[Byte]] = Seq(
      Seq('I', 'I', '*', 0).map(_.toByte), Seq('M', 'M', 0, '*').map(_.toByte),
      Seq('I', 'I', '+', 0).map(_.toByte), Seq('M', 'M', 0, '+').map(_.toByte)
    )

    private def shown(text: String, names: Seq[(String, String)]): String =
        if (text == null) null
        else names.foldLeft(text) { case (t, (real, placeholder)) => t.replace(real, placeholder) }.replace(Prefix + "/", "")

    private def lastError(fallback: String): String =
        Option(gdal.GetLastErrorMsg()).filter(_.nonEmpty).getOrElse(fallback)

    private def props(sqlName: String, name: String, needsCrs: Boolean): RasterProps = {
        val drivers = new java.util.Vector[String](Seq("GTiff").asJava)
        val ds =
            try gdal.OpenEx(name, gdalconstConstants.OF_RASTER | gdalconstConstants.OF_READONLY, drivers)
            catch { case e: RuntimeException => throw new SpecDataException(e.getMessage) }
        if (ds == null) throw new SpecDataException(lastError(s"$sqlName: the input is not a readable GeoTIFF"))
        try {
            if (needsCrs && ds.GetSpatialRef() == null) {
                throw new SpecDataException(
                  s"$sqlName: the tile has no CRS, so it cannot be reprojected. Fix: set its CRS first with gbx_rst_setcrs"
                )
            }
            RasterProps(ds.GetRasterXSize, ds.GetRasterYSize, ds.GetRasterCount,
              gdal.GetDataTypeName(ds.GetRasterBand(1).getDataType))
        } finally ds.delete()
    }

    private def remove(prefix: String): Unit = {
        Option(gdal.ReadDirRecursive(prefix)).foreach { files =>
            files.asScala.map(_.toString).filterNot(_.endsWith("/")).foreach(f => gdal.Unlink(s"$prefix/$f"))
        }
        scala.util.Try(gdal.RmdirRecursive(prefix)) // nothing left to remove, or GDAL exceptions on
    }

    /** Parse, run and finalize `tokens` on a new `alg` ('raster slope'); every handle is deleted, sub-algorithm
      * first. */
    private def execute(sqlName: String, alg: String, tokens: Seq[String], names: Seq[(String, String)]): Unit = {
        val Array(top, sub) = alg.split(" ", 2)
        val registry: AlgorithmRegistry = gdal.GetGlobalAlgorithmRegistry()
        var group: Algorithm = null
        var step: Algorithm = null
        try {
            group = registry.InstantiateAlg(top)
            step = group.InstantiateSubAlgorithm(sub)
            gdal.ErrorReset()
            val parsed =
                try step.ParseCommandLineArguments(new java.util.Vector[String](tokens.asJava))
                catch { case e: RuntimeException => throw new SpecParamException(s"$sqlName: ${shown(e.getMessage, names)}") }
            if (!parsed) throw new SpecParamException(s"$sqlName: ${shown(lastError("GDAL refused the arguments"), names)}")
            val ran =
                try step.Run() && step.Finalize()
                catch { case e: RuntimeException => throw new SpecDataException(e.getMessage) }
            if (!ran) throw new SpecDataException(lastError(s"$sqlName: GDAL's $alg failed"))
        } finally {
            if (step != null) step.delete()
            if (group != null) group.delete()
            registry.delete()
        }
    }

    /** One call of `fn`'s spec over `rasters` (GeoTIFF bytes) with `values` (param -> String, Double, Boolean or
      * null): its result, or None for a NULL result. Throws [[SpecParamException]] for a value the function refuses. */
    def run(fn: SpecFunction, rasters: Seq[Array[Byte]], values: Map[String, Any]): Option[SpecResult] = {
        KernelGdal.ensureInit()
        val block = fn.gdal
        val prefix = s"$Prefix/${UUID.randomUUID().toString.replace("-", "")}"
        val inNames = rasters.indices.map(i => s"$prefix/in$i.tif")
        val out = s"$prefix/out.tif"
        val placeholders =
            if (block.inputs.head._2 == "raster") Seq("<input>") else rasters.indices.map(i => s"<input$i>")
        val names = inNames.zip(placeholders) :+ (out -> "<output>")
        var command: String = null
        try {
            rasters.zipWithIndex.foreach { case (r, i) =>
                if (r == null || r.length < 4 || !TiffMagic.contains(r.take(4).toSeq)) {
                    throw new SpecDataException(s"${fn.sqlName}: input $i is not a GeoTIFF")
                }
                gdal.FileFromMemBuffer(inNames(i), r)
            }
            val inputs = inNames.map(n => props(fn.sqlName, n, block.needsCrs))
            val decoded = inputs.map(p => p.width.toLong * p.height * p.bands * GdalSpecArgv.ItemSize.getOrElse(p.dtype, 8)).sum
            if (2 * decoded > DecodedBudget) {
                throw new SpecDataException(
                  s"${fn.sqlName}: the call needs ${2 * decoded >> 20} MiB decoded (input and output), over this " +
                      s"compute's ${DecodedBudget >> 20} MiB. Fix: retile the tile first (gbx_rst_retile)"
                )
            }
            GdalSpecArgv.build(fn, values, inputs, inNames, out) match {
                case None => None
                case Some(tokens) =>
                    command = shown((block.alg +: tokens).mkString(" "), names)
                    val fmt = tokens.find(_.startsWith("--output-format=")).map(_.split("=", 2)(1)).getOrElse("")
                    if (!GdalSpecArgv.OutputFormats(fmt)) throw new SpecParamException(s"${fn.sqlName}: the engine does not write $fmt")
                    execute(fn.sqlName, block.alg, tokens, names)
                    Some(SpecResult(gdal.GetMemFileBuffer(out), null, command))
            }
        } catch {
            case e: SpecDataException => Some(SpecResult(null, shown(e.getMessage, names), command))
        } finally remove(prefix)
    }

}
