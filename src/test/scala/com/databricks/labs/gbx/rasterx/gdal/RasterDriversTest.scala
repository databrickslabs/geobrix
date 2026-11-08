package com.databricks.labs.gbx.rasterx.gdal

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

/** The driver classes of the manifest keep their invariants: disjoint, free of repeats, `bytes` and
  * `bytes_refused_light` allowed drivers, and the two never overlapping. Needs no GDAL. */
class RasterDriversTest extends AnyFunSuite {

    private def classes(allowed: String, refused: String, bytes: String, light: String) =
        RasterDrivers.parse(new ObjectMapper().readTree(
          s"""{"gdal": {"raster_drivers": {"allowed": $allowed, "refused": $refused, "bytes": $bytes,
             |"bytes_refused_light": $light}}}""".stripMargin))

    test("parse keeps a consistent classification and refuses each broken invariant") {
        val ok = classes("""["GTiff","VRT","PNG"]""", """["WMS"]""", """["GTiff"]""", """["VRT"]""")
        assert(ok.allowed == Seq("GTiff", "VRT", "PNG") && ok.refused == Seq("WMS"))
        assert(ok.bytes == Seq("GTiff") && ok.bytesRefusedLight == Seq("VRT"))
        val broken = Seq(
          "a driver both allowed and refused" -> (() => classes("""["GTiff"]""", """["GTiff"]""", "[]", "[]")),
          "a repeated allowed driver" -> (() => classes("""["GTiff","GTiff"]""", "[]", "[]", "[]")),
          "a bytes driver that is not allowed" -> (() => classes("""["GTiff"]""", "[]", """["PNG"]""", "[]")),
          "a bytes_refused_light driver that is also a bytes driver" ->
              (() => classes("""["GTiff"]""", "[]", """["GTiff"]""", """["GTiff"]"""))
        )
        for ((what, build) <- broken) {
            intercept[IllegalArgumentException](build()) // `require` raises IllegalArgumentException
            info(s"refused: $what")
        }
        intercept[IllegalArgumentException](RasterDrivers.parse(new ObjectMapper().readTree("""{"gdal": {}}""")))
    }
}
