package com.databricks.labs.gbx.kernels.spec

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.kernels.KernelGdal
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Algorithm, AlgorithmRegistry, TranslateOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** A call's outcome: the output GeoTIFF, or the reason it failed (an error tile), and the argv it ran. */
final case class SpecResult(raster: Array[Byte], error: String, command: String)

/** One engine call on the JVM's GDAL: the mirror of `engine/run.py`.
  *
  *   1. The input bytes are written to `/vsimem/gbx-spec/<uuid>/in<i>.tif` and their headers read (size, bands, type,
  *      CRS); the call must fit the decoded budget. Bytes without the GeoTIFF magic (a PNG or JPEG tile kept in its
  *      own driver) are written as `src<i>.bin` and opened only if a driver of the manifest's `bytes` class identifies
  *      them: drivers whose dataset is the bytes alone, never a name, href or URL read from the data (a VRT, a label
  *      format, netCDF or HDF5 bytes are refused with a Fix, before anything is opened). They are judged by the budget
  *      from their header, and only then translated to `in<i>.tif` (GTiff, no other option); the source goes before
  *      the run.
  *   1. The argv is bound ([[GdalSpecArgv.build]]) and checked; the algorithm is instantiated (single use), parsed,
  *      run and finalized; the output is read from `/vsimem`.
  *   1. Every file under the call's prefix is removed, and every handle deleted, whatever happened.
  *
  * Each GDAL call is judged on its return value after `ErrorReset`, as the light tier judges its calls with GDAL's
  * exceptions off, so one argv has one outcome on both tiers:
  *   - GDAL's run failing is a data error (an error tile with GDAL's message, its `/vsimem` names shown as
  *     `<input>`/`<output>`);
  *   - GDAL refusing the argv is a data error when the call bound a `bad: data` value (that value is the cause), else
  *     a [[SpecParamException]];
  *   - a map-algebra expression GDAL cannot evaluate is a [[SpecParamException]], found by a dry run once per
  *     expression.
  *
  * The engine's settings ([[EngineSettings]]) are thread-local for the call, and GDAL's last error is per thread, so
  * concurrent tasks do not see each other and the process keeps its own configuration.
  */
object GdalSpecEngine {

    val Prefix = "/vsimem/gbx-spec"
    /** The decoded bytes one call may hold on a classic JVM (`pyrx.core.budget`'s classic budget). */
    val DecodedBudget: Long = 1536L * 1024 * 1024
    /** The engine's GDAL settings, thread-local for each call (the light tier's `run.ENGINE_SETTINGS`): one thread per
      * call whatever `GDAL_NUM_THREADS` the process has, no Python in a VRT pixel function, and no VRT raw band (it
      * reads a file's bytes without a driver). */
    val EngineSettings: Seq[(String, String)] =
        Seq("GDAL_NUM_THREADS" -> "1", "GDAL_VRT_ENABLE_PYTHON" -> "NO", "GDAL_VRT_ENABLE_RAWRASTERBAND" -> "NO")

    private val TiffMagic: Seq[Seq[Byte]] = Seq(
      Seq('I', 'I', '*', 0).map(_.toByte), Seq('M', 'M', 0, '*').map(_.toByte),
      Seq('I', 'I', '+', 0).map(_.toByte), Seq('M', 'M', 0, '+').map(_.toByte)
    )
    /** Dry-run outcomes of map-algebra expressions ("" = GDAL evaluates it), by expression and input props. */
    private val validated = new ConcurrentHashMap[String, String]()

    private def shown(text: String, names: Seq[(String, String)]): String =
        if (text == null) null
        else names.foldLeft(text) { case (t, (real, placeholder)) => t.replace(real, placeholder) }.replace(Prefix + "/", "")

    private def lastError(fallback: String): String =
        Option(gdal.GetLastErrorMsg()).filter(_.nonEmpty).getOrElse(fallback)

    /** GDAL, initialized once per JVM: from the session's configuration when the expression carries it, so the
      * session's GDAL options and registered PROJ grid directories apply when the engine is the JVM's first GDAL
      * user. */
    def init(conf: Option[ExpressionConfig]): Unit = conf match {
        case Some(c) => GDALManager.init(c)
        case None    => KernelGdal.ensureInit()
    }

    private def withEngineSettings[T](block: => T): T = {
        val saved = EngineSettings.map { case (k, _) => k -> gdal.GetThreadLocalConfigOption(k, null) }
        EngineSettings.foreach { case (k, v) => gdal.SetThreadLocalConfigOption(k, v) }
        try block
        finally saved.foreach { case (k, v) => gdal.SetThreadLocalConfigOption(k, v) }
    }

    private def open(sqlName: String, name: String, drivers: Seq[String]): org.gdal.gdal.Dataset = {
        gdal.ErrorReset()
        val ds =
            try gdal.OpenEx(name, gdalconstConstants.OF_RASTER | gdalconstConstants.OF_READONLY,
              new java.util.Vector[String](drivers.asJava))
            catch { case e: RuntimeException => throw new SpecDataException(e.getMessage) }
        if (ds == null) throw new SpecDataException(lastError(s"$sqlName: the input is not a raster GeoBrix reads"))
        ds
    }

    private def props(sqlName: String, name: String, needsCrs: Boolean,
                      drivers: Seq[String] = Seq("GTiff")): RasterProps = {
        val ds = open(sqlName, name, drivers)
        try {
            val sr = ds.GetSpatialRef() // a clone the caller owns: released here, not left to the finalizer
            try {
                if (needsCrs && sr == null) {
                    throw new SpecDataException(
                      s"$sqlName: the tile has no CRS, so it cannot be reprojected. Fix: set its CRS first with gbx_rst_setcrs"
                    )
                }
            } finally if (sr != null) sr.delete()
            RasterProps(ds.GetRasterXSize, ds.GetRasterYSize, ds.GetRasterCount,
              gdal.GetDataTypeName(ds.GetRasterBand(1).getDataType))
        } finally ds.delete()
    }

    /** Translate `src` (raster bytes in another driver) to the GeoTIFF `dst`, then remove `src`: the engine's
      * algorithms read GeoTIFF inputs (the light tier's `_to_gtiff` makes the same calls). */
    private def toGTiff(sqlName: String, src: String, dst: String, drivers: Seq[String]): Unit = {
        val options = new TranslateOptions(new java.util.Vector[String](Seq("-of", "GTiff").asJava))
        val ds =
            try open(sqlName, src, drivers)
            catch { case e: Throwable => options.delete(); throw e }
        try {
            gdal.ErrorReset()
            val out =
                try gdal.Translate(dst, ds, options)
                catch { case e: RuntimeException => throw new SpecDataException(e.getMessage) }
            if (out == null) throw new SpecDataException(lastError(s"$sqlName: the input could not be converted to GeoTIFF"))
            out.delete()
        } finally {
            options.delete()
            ds.delete()
        }
        gdal.Unlink(src)
    }

    val BytesFix =
        "Fix: store the tile as a GeoTIFF, or pass it as a virtual tile (a path), which the virtual-tile policy judges"

    /** Throw a [[SpecDataException]] (with a Fix) unless a driver of the `bytes` class (`drivers`) identifies `src`, a
      * tile's bytes without the TIFF magic. Only the header is read: a driver outside the class (a VRT, whose sources
      * GDAL would open) is named, never opened (the light tier's `_bytes_source`). */
    private def bytesSource(sqlName: String, i: Int, src: String, drivers: Seq[String]): Unit = {
        gdal.ErrorReset()
        val drv =
            try gdal.IdentifyDriverEx(src, gdalconstConstants.OF_RASTER)
            catch { case _: RuntimeException => null }
        if (drv == null) {
            throw new SpecDataException(s"$sqlName: input $i is not a raster GeoBrix reads from tile bytes. $BytesFix")
        }
        val name = drv.getShortName
        if (!drivers.contains(name)) {
            throw new SpecDataException(
              s"$sqlName: input $i is a $name raster, which may name other files, so GeoBrix does not open it from " +
                  s"tile bytes. $BytesFix"
            )
        }
    }

    private def remove(prefix: String): Unit = {
        Option(gdal.ReadDirRecursive(prefix)).foreach { files =>
            files.asScala.map(_.toString).filterNot(_.endsWith("/")).foreach(f => gdal.Unlink(s"$prefix/$f"))
        }
        scala.util.Try(gdal.RmdirRecursive(prefix)) // nothing left to remove, or GDAL exceptions on
    }

    /** Parse, run and finalize `tokens` on a new `alg` ('raster slope'); every handle is deleted, sub-algorithm
      * first. `refused(message)` makes the exception for an argv GDAL refuses. */
    private def execute(alg: String, tokens: Seq[String], names: Seq[(String, String)],
                        refused: String => RuntimeException): Unit = {
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
                catch { case e: RuntimeException => throw refused(shown(e.getMessage, names)) }
            if (!parsed) throw refused(shown(lastError("GDAL refused the arguments"), names))
            gdal.ErrorReset()
            val ran =
                try step.Run() && step.Finalize()
                catch { case e: RuntimeException => throw new SpecDataException(e.getMessage) }
            if (!ran) throw new SpecDataException(lastError(s"GDAL's $alg failed"))
        } finally {
            if (step != null) step.delete()
            if (group != null) group.delete()
            registry.delete()
        }
    }

    /** For each input, a blank GeoTIFF with only its props (size, bands, type; no pixel written, no georeferencing):
      * what the expression check runs on (the light tier's `_blank_inputs`). Returns (real name, blank name). */
    private def blankInputs(inNames: Seq[String], inputs: Seq[RasterProps], prefix: String): Seq[(String, String)] = {
        val driver = gdal.GetDriverByName("GTiff")
        inNames.zip(inputs).zipWithIndex.map { case ((name, p), i) =>
            val blank = s"$prefix/check$i.tif"
            gdal.ErrorReset()
            val ds =
                try driver.Create(blank, p.width, p.height, p.bands, gdal.GetDataTypeByName(p.dtype), Array("SPARSE_OK=TRUE"))
                catch { case e: RuntimeException => throw new SpecDataException(e.getMessage) }
            if (ds == null) throw new SpecDataException(lastError(s"cannot make the check input for $name"))
            ds.delete()
            name -> blank
        }
    }

    /** A map-algebra expression GDAL cannot evaluate is a parameter error: GDAL's own check (a VRT output, so no
      * pixel is computed) runs once per expression and input props, before the first real run. It runs on blank
      * inputs made from the props alone ([[blankInputs]]), so its outcome is a function of its cache key: a fault of
      * the data itself (inputs whose CRS differ) is left to the real run, where it is that row's error tile and is
      * not cached. */
    private def checkExpression(fn: SpecFunction, tokens: Seq[String], inputs: Seq[RasterProps], inNames: Seq[String],
                                prefix: String, names: Seq[(String, String)]): Unit = {
        val key = (tokens.filter(t => t.startsWith("--calc=") || t.startsWith("--dialect=")) ++ inputs.map(_.toString))
            .mkString("\u0000")
        val known = Option(validated.get(key)).getOrElse {
            val blanks = blankInputs(inNames, inputs, prefix)
            val shownBlanks = names ++ blanks.flatMap { case (real, blank) => names.find(_._1 == real).map(blank -> _._2) }
            val dry = tokens.filterNot(t => t.startsWith("--output") || t.startsWith("--creation-option"))
                .map(t => blanks.foldLeft(t) { case (acc, (real, blank)) => acc.replace(real, blank) }) ++
                Seq(s"--output=$prefix/check.vrt", "--output-format=VRT")
            val outcome =
                try { execute(fn.gdal.alg, dry, shownBlanks, m => new SpecParamException(m)); "" }
                catch {
                    case e: SpecDataException  => e.getMessage
                    case e: SpecParamException => e.getMessage
                }
            if (validated.size > 1024) validated.clear()
            validated.put(key, outcome)
            outcome
        }
        if (known.nonEmpty) {
            val expr = tokens.find(_.startsWith("--calc=")).map(_.split("=", 2)(1)).getOrElse("")
            throw new SpecParamException(s"${fn.sqlName}: GDAL cannot evaluate the expression '$expr' ($known)")
        }
    }

    /** One call of `fn`'s spec over `rasters` (raster bytes: GeoTIFF, or another driver GeoBrix reads, translated
      * first) with `values` (param -> String, Double, Boolean or null): its result, or None for a NULL result. Throws [[SpecParamException]] for a value the function refuses.
      * `conf` is the session's configuration (it initializes GDAL when the engine is the JVM's first user of it);
      * `section` is the allowlist. */
    def run(
        fn: SpecFunction,
        rasters: Seq[Array[Byte]],
        values: Map[String, Any],
        conf: Option[ExpressionConfig] = None,
        section: GdalSection = GdalSpecs.section
    ): Option[SpecResult] = {
        init(conf)
        val block = fn.gdal
        val prefix = s"$Prefix/${UUID.randomUUID().toString.replace("-", "")}"
        val inNames = rasters.indices.map(i => s"$prefix/in$i.tif")
        val out = s"$prefix/out.tif"
        val placeholders =
            if (block.inputs.head._2 == "raster") Seq("<input>") else rasters.indices.map(i => s"<input$i>")
        // a raster without the GeoTIFF magic is written here first, read only by a self-contained driver (the
        // manifest's bytes class), and translated to GeoTIFF once the budget holds (its props are its header's)
        val srcNames = rasters.indices.map(i => s"$prefix/src$i.bin")
        val names = inNames.zip(placeholders) ++ srcNames.zip(placeholders) :+ (out -> "<output>")
        var command: String = null
        withEngineSettings {
            try {
                rasters.indexWhere(_ == null) match {
                    case -1 => ()
                    case i  => throw new SpecDataException(s"${fn.sqlName}: input $i has no raster")
                }
                val translated =
                    rasters.indices.filterNot(i => rasters(i).length >= 4 && TiffMagic.contains(rasters(i).take(4).toSeq))
                rasters.zipWithIndex.foreach { case (r, i) =>
                    gdal.FileFromMemBuffer(if (translated.contains(i)) srcNames(i) else inNames(i), r)
                }
                val drivers = section.bytesDrivers // self-contained drivers only, from the allowlist this call runs under
                translated.foreach(i => bytesSource(fn.sqlName, i, srcNames(i), drivers))
                val inputs = inNames.indices.map { i =>
                    if (translated.contains(i)) props(fn.sqlName, srcNames(i), block.needsCrs, drivers)
                    else props(fn.sqlName, inNames(i), block.needsCrs)
                }
                val decoded =
                    inputs.map(p => p.width.toLong * p.height * p.bands * GdalSpecArgv.ItemSize.getOrElse(p.dtype, 8)).sum
                if (2 * decoded > DecodedBudget) {
                    throw new SpecDataException(
                      s"${fn.sqlName}: the call needs ${2 * decoded >> 20} MiB decoded (input and output), over this " +
                          s"compute's ${DecodedBudget >> 20} MiB. Fix: retile the tile first (gbx_rst_retile)"
                    )
                }
                translated.foreach(i => toGTiff(fn.sqlName, srcNames(i), inNames(i), drivers))
                GdalSpecArgv.build(fn, values, inputs, inNames, out, section) match {
                    case None => None
                    case Some(tokens) =>
                        command = shown((block.alg +: tokens).mkString(" "), names)
                        val fmt = tokens.find(_.startsWith("--output-format=")).map(_.split("=", 2)(1)).getOrElse("")
                        if (!GdalSpecArgv.OutputFormats(fmt)) {
                            throw new SpecParamException(s"${fn.sqlName}: the engine does not write $fmt")
                        }
                        val data = GdalSpecArgv.dataBound(fn, values)
                        val refused: String => RuntimeException = m =>
                            if (data) new SpecDataException(s"${fn.sqlName}: $m")
                            else new SpecParamException(s"${fn.sqlName}: $m")
                        if (block.alg == "raster calc") checkExpression(fn, tokens, inputs, inNames, prefix, names)
                        execute(block.alg, tokens, names, refused)
                        Some(SpecResult(gdal.GetMemFileBuffer(out), null, command))
                }
            } catch {
                case e: SpecDataException => Some(SpecResult(null, shown(e.getMessage, names), command))
            } finally remove(prefix)
        }
    }

}
