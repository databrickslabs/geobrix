package com.databricks.labs.gbx.kernels.spec

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.jdk.CollectionConverters._

/** One argument of a spec's template: a fixed value (`constFlag` for a bare flag, else `constText`) or a param
  * bound through a binder (`source`, `tpe`). Mirrors `manifest.GdalArg`. */
final case class SpecArg(
    arg: String,
    constFlag: Option[Boolean],
    constText: Option[String],
    source: Option[String],
    tpe: Option[String],
    choices: Seq[(String, String)],
    nullRule: String,
    withArg: Option[String],
    bad: String
)

/** A row's `gdal` block (mirrors `manifest.GdalSpec`). */
final case class GdalSpec(
    alg: String,
    inputs: Seq[(String, String)],
    outKind: String,
    outDtype: String,
    write: String,
    args: Seq[SpecArg],
    needsCrs: Boolean
)

final case class SpecParam(name: String, sessionType: String, default: Option[String])

/** A manifest row that carries a `gdal` block: what the JVM needs to bind and run it. */
final case class SpecFunction(sqlName: String, params: Seq[SpecParam], minArgs: Int, gdal: GdalSpec)

/** The manifest's top-level `gdal` section (mirrors `manifest.GdalSection`). The driver classification: every
  * raster driver the payload's GDAL registers is in `allowedDrivers` (a reader can read it) or `refusedDrivers` (a
  * pipeline, an index, a service or a network API; never registered where the engine runs, see
  * [[com.databricks.labs.gbx.rasterx.gdal.GDALManager.DefaultSkippedDrivers]]). `bytesDrivers` are the allowed
  * drivers whose dataset is the bytes alone (no name, href or URL read from the data is opened): the only drivers the
  * engine opens a materialized tile's non-TIFF bytes with (the manifest's comment, `manifest.GdalSection`, gives
  * the class and why each other driver is out). `bytesRefusedLight` is the class the lightweight (Python) tier
  * refuses for a tile's bytes; the engine here reads it only to hold it valid, as the Python loader does. */
final case class GdalSection(
    pin: String,
    deny: Set[String],
    allow: Map[String, Seq[String]],
    allowedDrivers: Seq[String],
    refusedDrivers: Seq[String],
    bytesDrivers: Seq[String] = Nil,
    bytesRefusedLight: Seq[String] = Nil
)

/** The GDAL spec engine's specs, read from the copy of `functions.json` the build packs into the JAR, never from
  * a caller, so a spec cannot be forged from SQL. The parse refuses any key outside the closed vocabulary, as the
  * Python loader (`manifest.parse_gdal_spec`) does. */
object GdalSpecs {

    val Resource = "/com/databricks/labs/gbx/manifest/functions.json"

    private val SectionKeys = Set("pin", "deny", "allow", "raster_drivers")
    private val RasterDriverKeys = Set("allowed", "refused", "bytes", "bytes_refused_light")
    private val OutKeys = Set("kind", "dtype")
    private val BlockKeys = Set("alg", "in", "out", "write", "args", "needs_crs", "gdal_min")
    private val ArgKeys = Set("arg", "const", "from", "type", "map", "null", "with", "bad")
    private val InKinds = Set("raster", "rasters", "text")
    private val OutKinds = Set("raster", "features", "json")
    private val Binders = Set(
      "double", "int", "bool", "choice", "crs", "geom_wkt", "double_list", "calc_envelope", "text_vsimem",
      "observer_xy", "size_from_factor"
    )
    private val NullRules = Set("omit", "null_result", "raise")
    private val BadRules = Set("raise", "data")
    private val WriteProfiles = Set("tile")

    private lazy val loaded: (GdalSection, Map[String, SpecFunction]) = {
        val in = Option(getClass.getResourceAsStream(Resource)).getOrElse(
          throw new IllegalStateException(s"the geobrix JAR has no $Resource (the function manifest)")
        )
        try parse(new ObjectMapper().readTree(in)) finally in.close()
    }

    def section: GdalSection = loaded._1

    /** The spec of `sqlName`; IllegalArgumentException for a function with no `gdal` block. */
    def function(sqlName: String): SpecFunction =
        loaded._2.getOrElse(sqlName, throw new IllegalArgumentException(s"$sqlName has no GDAL spec in this JAR"))

    def functions: Map[String, SpecFunction] = loaded._2

    private def closed(node: JsonNode, keys: Set[String], what: String): Unit = {
        val extra = node.fieldNames().asScala.filterNot(keys).toSeq.sorted
        if (extra.nonEmpty) throw new IllegalArgumentException(s"gdal spec: unknown $what key(s) ${extra.mkString(", ")}")
    }

    private def oneOf(value: String, allowed: Set[String], what: String): String =
        if (allowed(value)) value
        else throw new IllegalArgumentException(s"gdal spec: $what '$value' is not one of ${allowed.toSeq.sorted.mkString(", ")}")

    private def text(node: JsonNode, key: String, default: String): String =
        Option(node.get(key)).filterNot(_.isNull).map(_.asText).getOrElse(default)

    /** A const value as both tiers print it: an integer as it is, a double by `GdalSpecArgv.fmtDouble`. */
    private def constText(v: JsonNode): String =
        if (v.isIntegralNumber) v.asText else if (v.isNumber) GdalSpecArgv.fmtDouble(v.asDouble) else v.asText

    private def parseArg(a: JsonNode): SpecArg = {
        closed(a, ArgKeys, "argument")
        val name = text(a, "arg", "")
        if (name.isEmpty) throw new IllegalArgumentException(s"gdal spec: an argument needs a name ($a)")
        if (a.has("const") == a.has("from")) {
            throw new IllegalArgumentException(s"gdal spec: argument '$name' needs exactly one of const and from")
        }
        if (a.has("from") && !a.has("type")) {
            throw new IllegalArgumentException(s"gdal spec: argument '$name' binds a param and needs a type")
        }
        val const = Option(a.get("const"))
        val choices = Option(a.get("map")).map(_.fields.asScala.map(e => e.getKey -> e.getValue.asText).toSeq).getOrElse(Nil)
        SpecArg(
          arg = name,
          constFlag = const.filter(_.isBoolean).map(_.asBoolean),
          constText = const.filterNot(_.isBoolean).map(constText),
          source = Option(a.get("from")).map(_.asText),
          tpe = Option(a.get("type")).map(t => oneOf(t.asText, Binders, s"binder of '$name'")),
          choices = choices,
          nullRule = oneOf(text(a, "null", "null_result"), NullRules, s"null rule of '$name'"),
          withArg = Option(a.get("with")).map(_.asText),
          bad = oneOf(text(a, "bad", "raise"), BadRules, s"bad rule of '$name'")
        )
    }

    /** A row's `gdal` block. */
    def parseSpec(g: JsonNode): GdalSpec = {
        closed(g, BlockKeys, "block")
        val out = g.get("out")
        closed(out, OutKeys, "out")
        GdalSpec(
          alg = text(g, "alg", ""),
          inputs = g.get("in").fields.asScala.map(e => e.getKey -> oneOf(e.getValue.asText, InKinds, s"input kind of '${e.getKey}'")).toSeq,
          outKind = oneOf(text(out, "kind", ""), OutKinds, "output kind"),
          outDtype = text(out, "dtype", "input"),
          write = oneOf(text(g, "write", ""), WriteProfiles, "write profile"),
          args = Option(g.get("args")).map(_.elements.asScala.map(parseArg).toSeq).getOrElse(Nil),
          needsCrs = Option(g.get("needs_crs")).exists(_.asBoolean)
        )
    }

    private def names(s: JsonNode, key: String): Seq[String] =
        Option(s.get(key)).map(_.elements.asScala.map(_.asText).toSeq).getOrElse(Nil)

    /** The whole manifest: its `gdal` section and every row that has a `gdal` block. */
    def parse(root: JsonNode): (GdalSection, Map[String, SpecFunction]) = {
        val s = root.get("gdal")
        closed(s, SectionKeys, "section")
        val drivers = Option(s.get("raster_drivers")).getOrElse(throw new IllegalArgumentException(
          "gdal spec: the section needs raster_drivers (allowed and refused)"))
        closed(drivers, RasterDriverKeys, "raster_drivers")
        val allowedDrivers = names(drivers, "allowed")
        val refusedDrivers = names(drivers, "refused")
        val bytesDrivers = names(drivers, "bytes")
        require(
          bytesDrivers.toSet.subsetOf(allowedDrivers.toSet) && bytesDrivers.distinct.size == bytesDrivers.size,
          s"gdal spec: the bytes drivers must be allowed drivers, free of repeats: $bytesDrivers"
        )
        val bytesRefusedLight = names(drivers, "bytes_refused_light")
        require(
          bytesRefusedLight.toSet.subsetOf(allowedDrivers.toSet) && bytesRefusedLight.distinct.size == bytesRefusedLight.size &&
            bytesRefusedLight.toSet.intersect(bytesDrivers.toSet).isEmpty,
          s"gdal spec: the bytes_refused_light drivers must be allowed drivers, free of repeats, and none of the bytes drivers: $bytesRefusedLight"
        )
        // the two classes are disjoint and free of repeats, as the Python parser holds them
        require(
          allowedDrivers.toSet.intersect(refusedDrivers.toSet).isEmpty &&
            allowedDrivers.distinct.size == allowedDrivers.size && refusedDrivers.distinct.size == refusedDrivers.size,
          s"gdal spec: the raster driver classes must be disjoint and free of repeats: $allowedDrivers / $refusedDrivers"
        )
        val section = GdalSection(
          pin = s.get("pin").asText,
          deny = s.get("deny").elements.asScala.map(_.asText).toSet,
          allow = s.get("allow").fields.asScala.map(e => e.getKey -> e.getValue.elements.asScala.map(_.asText).toSeq).toMap,
          allowedDrivers = allowedDrivers,
          refusedDrivers = refusedDrivers,
          bytesDrivers = bytesDrivers,
          bytesRefusedLight = bytesRefusedLight
        )
        val fns = root.get("functions").elements.asScala.filter(_.has("gdal")).map { r =>
            val params = r.get("params").elements.asScala.map { p =>
                SpecParam(p.get("name").asText, p.get("session").get(0).asText, Option(p.get("default")).map(_.asText))
            }.toSeq
            val name = r.get("sql_name").asText
            name -> SpecFunction(name, params, r.get("min_args").asInt, parseSpec(r.get("gdal")))
        }.toMap
        (section, fns)
    }

}
