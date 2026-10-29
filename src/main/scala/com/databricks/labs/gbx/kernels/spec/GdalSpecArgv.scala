package com.databricks.labs.gbx.kernels.spec

import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.operator.OperatorOptions
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants

import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.matching.Regex

/** A parameter value the function refuses: raised, naming the parameter and the value. */
class SpecParamException(msg: String) extends IllegalArgumentException(msg)

/** A value or a tile the function cannot process: the result is an error tile carrying this reason. */
class SpecDataException(msg: String) extends RuntimeException(msg)

/** What the engine reads from an input's header before it binds: size, band count and band-1 GDAL type. */
final case class RasterProps(width: Int, height: Int, bands: Int, dtype: String)

/** Binders and the argv: (a row's spec, the call's values, its input rasters) -> the tokens GDAL parses.
  *
  * The JVM mirror of `engine/argv.py` and `engine/spec.py`; `python/geobrix/test/engine/fixtures/argv_golden.json`
  * holds both tiers to the same tokens. The order is fixed: the inputs, `--input-format`, the spec's arguments in
  * template order, then the output, its format and creation options, `--num-threads` where the algorithm has it,
  * and `--quiet`. Every token is `--name=value` or a bare `--name`.
  */
object GdalSpecArgv {

    /** The argv tokens the engine writes itself; no spec and no caller sets them. */
    val EngineArgs: Set[String] = Set("input", "input-format", "output", "output-format", "creation-option", "num-threads", "quiet")
    /** The algorithms that take --num-threads (the engine sets it to 1: a Spark task is one core). */
    val Threaded: Set[String] = Set("raster reproject")
    /** Output formats the engine writes. Never VRT or GDALG. */
    val OutputFormats: Set[String] = Set("GTiff", "MEM")
    /** The fix for a CRS GDAL cannot build (`engine/argv.CRS_BUILD_FIX`). */
    val CrsBuildFix: String =
        "Fix: pass EPSG:<code>, WKT or PROJJSON that GDAL can build; for a grid, register its directory with " +
            "register_proj_grids, or mark it optional (@name)"

    val ItemSize: Map[String, Int] = Map(
      "Byte" -> 1, "UInt8" -> 1, "Int8" -> 1, "UInt16" -> 2, "Int16" -> 2, "Float16" -> 2, "UInt32" -> 4,
      "Int32" -> 4, "Float32" -> 4, "CInt16" -> 4, "UInt64" -> 8, "Int64" -> 8, "Float64" -> 8, "CInt32" -> 8,
      "CFloat32" -> 8, "CFloat64" -> 16
    )

    private val Vars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    /** A map-algebra variable: one capital letter that is not part of a longer name, a number or an indexed V[n]. */
    private val Var: Regex = """(?<![A-Za-z0-9_\]])([A-Z])(?![A-Za-z0-9_\[])""".r
    private val Dialects = Seq("muparser", "builtin")
    /** The envelope's closed vocabulary: calc, dialect, and per variable V_index / V_band. */
    private val EnvelopeKeys: Set[String] = Set("calc", "dialect") ++ Vars.flatMap(v => Seq(s"${v}_index", s"${v}_band"))
    private val TemplateCacheSize = 512
    private val templates = new java.util.LinkedHashMap[(String, Map[String, Any], Seq[RasterProps], GdalSection), Option[Seq[String]]](
      64, 0.75f, true
    )

    private final case class Calc(expr: String, dialect: String, variables: Seq[(String, Int)])

    /** A double as both tiers print it: the shortest round-trip digits, plain notation, no trailing zeros. */
    def fmtDouble(x: Double): String =
        if (x == 0.0) "0" else new java.math.BigDecimal(java.lang.Double.toString(x)).stripTrailingZeros.toPlainString

    /** Refuse a string value that could name a file or a URL. Binder output is not checked: the engine writes it. */
    def checkValue(sqlName: String, param: String, value: Any): Unit = value match {
        case s: String =>
            val v = s.trim
            if (v.startsWith("/") || v.startsWith("\\") || v.startsWith("@") || v.contains("://") || v.contains("..")) {
                throw new SpecParamException(s"$sqlName: $param must not name a file or a URL (got '$s')")
            }
        case _ => ()
    }

    private def algArgs(section: GdalSection, alg: String, who: String): Seq[String] =
        section.allow.getOrElse(alg, throw new IllegalArgumentException(s"$who: the algorithm '$alg' is not on the allowlist"))

    /** The engine core's check of the argv a spec produced: every token a long argument on the algorithm's
      * allowlist and off the deny list. */
    def guardTokens(section: GdalSection, alg: String, tokens: Seq[String]): Unit = {
        val allowed = algArgs(section, alg, alg).toSet
        tokens.foreach { t =>
            if (!t.startsWith("--")) throw new IllegalArgumentException(s"$alg: the token '$t' is not a long argument")
            val name = t.drop(2).takeWhile(_ != '=')
            if (section.deny(name) || EngineArgs(name)) throw new IllegalArgumentException(s"$alg: the argument '$name' is deny-listed")
            if (!allowed(name)) throw new IllegalArgumentException(s"$alg: the argument '$name' is not on the allowlist")
        }
    }

    private def isNull(v: Any): Boolean = v == null || (v match {
        case d: Double => d.isNaN
        case d: java.lang.Double => d.isNaN
        case _ => false
    })

    private def fail(fn: SpecFunction, a: SpecArg, message: String): Exception =
        if (a.bad == "data") new SpecDataException(s"${fn.sqlName}: $message") else new SpecParamException(s"${fn.sqlName}: $message")

    private def asDouble(v: Any): Double = v match {
        case d: Double => d
        case n: java.lang.Number => n.doubleValue
        case s: String => s.trim.toDouble
    }

    /** One argument's value text, or None to leave a bare flag out (a false `bool`). */
    private def bindValue(fn: SpecFunction, a: SpecArg, value: Any): Option[String] = a.tpe.get match {
        case "double" =>
            val x = asDouble(value)
            if (x.isInfinite) throw fail(fn, a, s"${a.source.get} must be finite (got $value)")
            Some(fmtDouble(x))
        case "int" => Some(asDouble(value).toLong.toString)
        case "bool" => if (value == true) Some("") else None
        case "choice" =>
            val word = value.toString.trim.toLowerCase(Locale.ROOT)
            a.choices.find(_._1.toLowerCase(Locale.ROOT) == word).map(_._2).orElse {
                val allowed = a.choices.map(c => s"'${c._1}'").mkString(", ")
                throw fail(fn, a, s"${a.source.get} must be one of $allowed (got '$value')")
            }
        case "crs" =>
            // The canonical resolver refuses a file name, a URL or a grid named by path, and parses with file and
            // network access off; the light binder (`engine/argv.canonical_crs`) makes the same OSR calls and judges
            // them the same way: a failing return, or a failure GDAL reports during the parse (PROJ cannot build the
            // CRS, e.g. a required grid it cannot find), refuses the value.
            val sr =
                try {
                    checkValue(fn.sqlName, a.source.get, value)
                    gdal.ErrorReset()
                    SpatialRefOps.resolveCrs(value.toString)
                } catch {
                    case NonFatal(e) =>
                        throw fail(fn, a, s"${a.source.get}: ${e.getMessage.stripPrefix(fn.sqlName + ": ").stripPrefix("resolveCrs: ")}")
                }
            try {
                if (gdal.GetLastErrorType() >= gdalconstConstants.CE_Failure) {
                    throw fail(fn, a, s"${a.source.get}: GDAL cannot build the CRS (${gdal.GetLastErrorMsg()}). $CrsBuildFix")
                }
                Some(SpatialRefOps.crsToCanonical(sr))
            } finally sr.delete()
        case other => throw new IllegalArgumentException(s"${fn.sqlName}: binder '$other' is not built in this release")
    }

    private def isInt(n: JsonNode): Boolean = n != null && n.isIntegralNumber

    /** The map-algebra envelope -> GDAL `raster calc`'s expression, dialect and one `--input=V=` per variable
      * (see `engine/argv.py:_calc_binding`, which this mirrors). */
    private def calcBinding(fn: SpecFunction, a: SpecArg, value: Any, inputs: Seq[RasterProps]): Calc = {
        var s = value.toString.trim
        if (inputs.length > Vars.length) {
            throw new SpecParamException(s"${fn.sqlName}: map algebra takes at most ${Vars.length} rasters (A to Z); got ${inputs.length}")
        }
        val bindings = scala.collection.mutable.LinkedHashMap.empty[String, (Int, Int)]
        var dialect = "muparser"
        if (s.startsWith("{")) {
            val env =
                try new ObjectMapper().readTree(s)
                catch { case NonFatal(e) => throw new SpecParamException(s"${fn.sqlName}: the expression is not valid JSON (${e.getMessage})") }
            if (!env.isObject || env.get("calc") == null || !env.get("calc").isTextual) {
                throw new SpecParamException(
                  s"""${fn.sqlName}: the expression envelope needs a "calc" string, e.g. {"calc": "A * 2"}"""
                )
            }
            if (env.has("extra_options")) {
                throw new SpecParamException(
                  s"${fn.sqlName}: extra_options is not supported (gdal_calc flags have no place in GDAL's " +
                      "raster calc). Fix: express the computation in calc"
                )
            }
            val unknown = env.fieldNames.asScala.filterNot(EnvelopeKeys).toSeq.sorted
            if (unknown.nonEmpty) {
                throw new SpecParamException(
                  s"${fn.sqlName}: the expression envelope has unknown key(s) ${unknown.mkString(", ")}; it takes calc, " +
                      "dialect, and V_index / V_band for a variable V from A to Z"
                )
            }
            if (env.has("dialect")) {
                dialect = env.get("dialect").asText
                if (!Dialects.contains(dialect)) {
                    throw new SpecParamException(s"${fn.sqlName}: dialect must be one of ${Dialects.mkString(", ")} (got '$dialect')")
                }
            }
            Vars.zipWithIndex.foreach { case (v, ordinal) =>
                val (ik, bk) = (s"${v}_index", s"${v}_band")
                if (env.has(ik) || env.has(bk)) {
                    val (in, bn) = (env.get(ik), env.get(bk))
                    if ((in != null && !isInt(in)) || (bn != null && !isInt(bn))) {
                        throw new SpecParamException(s"${fn.sqlName}: $ik and $bk must be integers (got $in, $bn)")
                    }
                    val idx = if (in == null) ordinal else in.asInt
                    val band = if (bn == null) 1 else bn.asInt
                    if (idx < 0 || idx >= inputs.length) {
                        throw new SpecParamException(s"${fn.sqlName}: $ik=$idx is out of range for ${inputs.length} raster(s)")
                    }
                    if (band < 1 || band > inputs(idx).bands) {
                        throw new SpecParamException(
                          s"${fn.sqlName}: $bk=$band is out of range: raster $idx has ${inputs(idx).bands} band(s)"
                        )
                    }
                    bindings(v.toString) = (idx, band)
                }
            }
            s = env.get("calc").asText
        }
        checkValue(fn.sqlName, a.source.get, s)
        val positional = inputs.indices.map(i => Vars(i).toString -> (i, 1)).toMap
        if (dialect == "builtin") {
            if (bindings.exists { case (v, b) => !positional.get(v).contains(b) }) {
                throw new SpecParamException(
                  s"${fn.sqlName}: raster and band selection (A_index, A_band, ...) needs the muparser dialect"
                )
            }
            return Calc(s, dialect, positional.toSeq.sortBy(_._1).map { case (v, (i, _)) => v -> i })
        }
        val resolved = positional ++ bindings
        val unbound = Var.findAllMatchIn(s).map(_.group(1)).toSet.filterNot(resolved.contains).toSeq.sorted
        if (unbound.nonEmpty) {
            throw new SpecParamException(s"${fn.sqlName}: the expression uses ${unbound.mkString(", ")}, which no raster is bound to")
        }
        val expr = Var.replaceAllIn(s, m => Regex.quoteReplacement(s"${m.group(1)}[${resolved(m.group(1))._2}]"))
        Calc(expr, dialect, resolved.toSeq.sortBy(_._1).map { case (v, (i, _)) => v -> i })
    }

    /** True when this call binds a value to a `bad: data` argument: GDAL refusing the argv is then that value's fault,
      * a data error (an error tile), not a parameter error. Mirrors `engine/argv.data_bound`. */
    def dataBound(fn: SpecFunction, values: Map[String, Any]): Boolean =
        fn.gdal.args.exists(a => a.bad == "data" && a.source.exists(s => !isNull(values.getOrElse(s, null))))

    private def placeholders(fn: SpecFunction, n: Int): Seq[String] =
        if (fn.gdal.inputs.head._2 == "raster") Seq("<input>") else (0 until n).map(i => s"<input$i>")

    /** The argv with the placeholder input and output names, bound once per distinct (spec, values, input props),
      * so a literal argument is resolved once, not per row. */
    private def template(fn: SpecFunction, values: Map[String, Any], inputs: Seq[RasterProps], section: GdalSection): Option[Seq[String]] = {
        val block = fn.gdal
        val inNames = placeholders(fn, inputs.length)
        val byArg = block.args.map(a => a.arg -> a).toMap
        var calc: Option[Calc] = None
        val tokens = scala.collection.mutable.ArrayBuffer.empty[String]
        for (a <- block.args) {
            if (a.source.isEmpty) {
                if (a.constFlag.contains(true)) tokens += s"--${a.arg}"
                a.constText.foreach(t => tokens += s"--${a.arg}=$t")
            } else {
                val v = values.getOrElse(a.source.get, null)
                val partner = a.withArg.map(byArg)
                if (partner.exists(p => isNull(v) != isNull(values.getOrElse(p.source.get, null)))) {
                    throw new SpecParamException(s"${fn.sqlName}: ${a.source.get} and ${partner.get.source.get} must be given together")
                }
                if (isNull(v)) {
                    a.nullRule match {
                        case "omit"  => ()
                        case "raise" => throw new SpecParamException(s"${fn.sqlName}: ${a.source.get} must not be NULL")
                        case _       => return None
                    }
                } else if (a.tpe.contains("calc_envelope")) {
                    val c = calcBinding(fn, a, v, inputs)
                    calc = Some(c)
                    tokens += s"--${a.arg}=${c.expr}"
                    tokens += s"--dialect=${c.dialect}"
                } else {
                    if (!a.tpe.contains("crs")) checkValue(fn.sqlName, a.source.get, v)
                    bindValue(fn, a, v).foreach { text =>
                        tokens += (if (a.tpe.contains("bool")) s"--${a.arg}" else s"--${a.arg}=$text")
                    }
                }
            }
        }
        guardTokens(section, block.alg, tokens.toSeq)
        val ins = calc match {
            case Some(c) => c.variables.map { case (v, i) => s"--input=$v=${inNames(i)}" }
            case None    => inNames.map(n => s"--input=$n")
        }
        val first = inputs.head
        val dtype = if (block.outDtype == "input") first.dtype else block.outDtype
        val decoded = inputs.map(r => r.width.toLong * r.height * r.bands * ItemSize.getOrElse(r.dtype, 8)).sum
        val creation = OperatorOptions.engineCreationOptions(dtype, decoded, first.width, first.height)
        Some(
          ins ++ Seq("--input-format=GTiff") ++ tokens ++ Seq("--output=<output>", "--output-format=GTiff") ++
              creation.map(o => s"--creation-option=$o") ++
              (if (Threaded(block.alg)) Seq("--num-threads=1") else Nil) ++ Seq("--quiet")
        )
    }

    /** The argv for one call of `fn`, or None when a NULL value makes the result NULL.
      *
      * `values` maps the function's params to this call's values (String, Double, Boolean or null); `inputs` are the
      * input rasters' header props; `names` their `/vsimem` names (default the placeholders `<input>`, or `<input0>`,
      * `<input1>`, ... for an array of rasters). Throws SpecParamException (raise) or SpecDataException (an error tile)
      * when a binder refuses a value. Mirrors `engine/argv.build`, its cache included.
      */
    def build(
        fn: SpecFunction,
        values: Map[String, Any],
        inputs: Seq[RasterProps],
        names: Seq[String] = Nil,
        output: String = "<output>",
        section: GdalSection = GdalSpecs.section
    ): Option[Seq[String]] = {
        val key = (fn.sqlName, values, inputs, section)
        val cached = templates.synchronized(Option(templates.get(key))).getOrElse {
            val t = template(fn, values, inputs, section)
            templates.synchronized {
                templates.put(key, t)
                while (templates.size > TemplateCacheSize) templates.remove(templates.keySet.iterator.next)
            }
            t
        }
        cached.map { tokens =>
            val real = (placeholders(fn, inputs.length).zip(if (names.nonEmpty) names else placeholders(fn, inputs.length)) :+
                ("<output>" -> output)).toMap
            tokens.map { tok =>
                if (tok.startsWith("--input=") || tok.startsWith("--output=")) {
                    val i = tok.lastIndexOf('=')
                    tok.substring(0, i + 1) + real.getOrElse(tok.substring(i + 1), tok.substring(i + 1))
                } else tok
            }
        }
    }

}
