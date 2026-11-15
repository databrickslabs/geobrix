package com.databricks.labs.gbx.rasterx.gdal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.jdk.CollectionConverters._

/** The manifest's raster driver classification (`gdal.raster_drivers`): every raster driver the payload's GDAL
  * registers is in `allowed` (a reader can read it) or `refused` (a pipeline, an index, a service or a network API;
  * never registered, see [[GDALManager.DefaultSkippedDrivers]]). `bytes` are the allowed drivers whose dataset is the
  * bytes alone (no name, href or URL read from the data is opened). `bytesRefusedLight` is the class of formats that
  * can name other files, whose tile bytes the lightweight (Python) tier refuses and so does [[TileBytes]]. */
final case class RasterDriverClasses(
    allowed: Seq[String],
    refused: Seq[String],
    bytes: Seq[String],
    bytesRefusedLight: Seq[String]
)

/** The driver classes, read from the copy of `functions.json` the build packs into the JAR, never from a caller. The
  * parse holds the same invariants as the Python loader (`manifest.parse_gdal_section`): the classes are disjoint and
  * free of repeats, and `bytes` and `bytesRefusedLight` are allowed drivers. */
object RasterDrivers {

    val Resource = "/com/databricks/labs/gbx/manifest/functions.json"

    private lazy val loaded: RasterDriverClasses = {
        val in = Option(getClass.getResourceAsStream(Resource)).getOrElse(
          throw new IllegalStateException(s"the geobrix JAR has no $Resource (the function manifest)")
        )
        try parse(new ObjectMapper().readTree(in)) finally in.close()
    }

    def classes: RasterDriverClasses = loaded

    private def names(s: JsonNode, key: String): Seq[String] =
        Option(s.get(key)).map(_.elements.asScala.map(_.asText).toSeq).getOrElse(Nil)

    /** The classes of a whole manifest (`root` is the parsed `functions.json`). */
    def parse(root: JsonNode): RasterDriverClasses = {
        val drivers = Option(root.get("gdal")).flatMap(g => Option(g.get("raster_drivers"))).getOrElse(
          throw new IllegalArgumentException("gdal spec: the section needs raster_drivers (allowed and refused)")
        )
        val allowed = names(drivers, "allowed")
        val refused = names(drivers, "refused")
        val bytes = names(drivers, "bytes")
        val bytesRefusedLight = names(drivers, "bytes_refused_light")
        require(
          bytes.toSet.subsetOf(allowed.toSet) && bytes.distinct.size == bytes.size,
          s"gdal spec: the bytes drivers must be allowed drivers, free of repeats: $bytes"
        )
        require(
          bytesRefusedLight.toSet.subsetOf(allowed.toSet) && bytesRefusedLight.distinct.size == bytesRefusedLight.size &&
            bytesRefusedLight.toSet.intersect(bytes.toSet).isEmpty,
          s"gdal spec: the bytes_refused_light drivers must be allowed drivers, free of repeats, and none of the bytes drivers: $bytesRefusedLight"
        )
        require(
          allowed.toSet.intersect(refused.toSet).isEmpty &&
            allowed.distinct.size == allowed.size && refused.distinct.size == refused.size,
          s"gdal spec: the raster driver classes must be disjoint and free of repeats: $allowed / $refused"
        )
        RasterDriverClasses(allowed, refused, bytes, bytesRefusedLight)
    }

}
