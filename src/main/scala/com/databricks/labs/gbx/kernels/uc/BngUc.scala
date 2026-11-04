package com.databricks.labs.gbx.kernels.uc

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.{Coerce, KernelGdal}
import com.databricks.labs.gbx.vectorx.jts.JTS

import org.locationtech.jts.geom.Geometry

import scala.util.control.NonFatal

/** Unity Catalog Scala handlers. UC declares one type per parameter, so the resolution is a STRING
  * and an INT argument arrives as its digits. A UC function has no error tile: a cell id that is not a BNG cell is a
  * data error and gives NULL. The cell-geometry handlers return EWKB (BINARY) carrying SRID 27700, because a UC Scala
  * handler is documented for BINARY and ARRAY results, not for GEOMETRY. */
object BngUc {
    /** Little-endian EWKB, the bytes the UC Python handlers write (shapely's native order), so a Volume with or without
      * the JAR gives the same BINARY. */
    private def toEwkb(geom: Geometry): Array[Byte] = JTS.toEWKB(geom)

    def eastNorthAsBng(easting: java.lang.Double, northing: java.lang.Double, resolution: String): String = {
        if (easting == null || northing == null || resolution == null) return null
        val res = Coerce.bngResolution(resolution, digitStrings = true) // PARAMETER: raises, naming the value
        try BNG.format(BNG.pointToCellID(easting, northing, res)) catch { case NonFatal(_) => null }
    }

    private def parseCell(cellid: String): Option[Long] =
        if (cellid == null) None else try Some(BNG.parse(cellid)) catch { case NonFatal(_) => None }

    def cellArea(cellid: String): java.lang.Double =
        parseCell(cellid).map(c => java.lang.Double.valueOf(BNG.area(c))).orNull

    /** The most a UC function returns per result (the Serverless decoded budget the Python handlers check), and the bytes
      * one cell id of an array is counted at: the same numbers and rule as `handlers._check_array`. */
    private val ResultCapBytes = 64L << 20
    private val ArrayItemBytes = 40L

    /** The refusal of a ring of distance `k` whose `(2k+1)^2` cells are over the cap, checked BEFORE the ring is built
      * (bounded, never truncated); the text and Fix are those of the Python handler. */
    private def checkRing(k: Int): Unit = {
        val side = BigInt(2L * math.max(k, 0) + 1L)
        val size = side * side * ArrayItemBytes
        if (size > BigInt(ResultCapBytes)) {
            def text(n: BigInt) = if (n >= BigInt(1L << 20)) s"${n >> 20} MiB" else s"${n >> 10} KiB"
            throw new IllegalArgumentException(
              s"gbx_bng_kring: the array of cells is ${text(size)}, over the ${text(BigInt(ResultCapBytes))} a Unity Catalog " +
                  "function returns. Fix: lower k, or split the input into smaller groups, or use the session function " +
                  "gbx_bng_kring (after databricks.labs.gbx.register(spark)) or the Python API."
            )
        }
    }

    /** The ring as `scala.collection.Seq`, the type a UC Scala handler documents for an ARRAY result. A ring over the
      * result cap is refused with a Fix before it is built. */
    def kRing(cellid: String, k: java.lang.Integer): scala.collection.Seq[String] =
        if (k == null) null
        else parseCell(cellid).map { c =>
            checkRing(k.intValue)
            BNG.kRing(c, k.intValue).map(BNG.format)
        }.orNull

    /** The cell polygon as little-endian EWKB with SRID 27700; the ring starts at the cell's south-west corner. */
    def asWkb(cellid: String): Array[Byte] = parseCell(cellid).map(c => toEwkb(BNG.cellIdToGeometry(c))).orNull

    /** The cell centre as EWKB with SRID 27700. */
    def centroid(cellid: String): Array[Byte] =
        parseCell(cellid).map { c =>
            val point = BNG.cellIdToGeometry(c).getCentroid
            point.setSRID(BNG.crsID)
            toEwkb(point)
        }.orNull

    /** Expected to fail in the UC sandbox (no GDAL natives); the UC pass records the result. */
    def gdalVersion(): String = KernelGdal.probe()
}
