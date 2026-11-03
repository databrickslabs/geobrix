package com.databricks.labs.gbx.kernels.uc

import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.kernels.{Coerce, KernelGdal}
import com.databricks.labs.gbx.vectorx.jts.JTS

import scala.util.control.NonFatal

/** Unity Catalog Scala handlers. UC declares one type per parameter, so the resolution is a STRING
  * and an INT argument arrives as its digits. A UC function has no error tile: a cell id that is not a BNG cell is a
  * data error and gives NULL. The cell-geometry handlers return EWKB (BINARY) carrying SRID 27700, because a UC Scala
  * handler is documented for BINARY and ARRAY results, not for GEOMETRY. */
object BngUc {
    def eastNorthAsBng(easting: java.lang.Double, northing: java.lang.Double, resolution: String): String = {
        if (easting == null || northing == null || resolution == null) return null
        val res = Coerce.bngResolution(resolution, digitStrings = true) // PARAMETER: raises, naming the value
        try BNG.format(BNG.pointToCellID(easting, northing, res)) catch { case NonFatal(_) => null }
    }

    private def parseCell(cellid: String): Option[Long] =
        if (cellid == null) None else try Some(BNG.parse(cellid)) catch { case NonFatal(_) => None }

    def cellArea(cellid: String): java.lang.Double =
        parseCell(cellid).map(c => java.lang.Double.valueOf(BNG.area(c))).orNull

    /** The ring as `scala.collection.Seq`, the type a UC Scala handler documents for an ARRAY result. */
    def kRing(cellid: String, k: java.lang.Integer): scala.collection.Seq[String] =
        if (k == null) null
        else parseCell(cellid).map(c => BNG.kRing(c, k.intValue).map(BNG.format)).orNull

    /** The cell polygon as EWKB with SRID 27700. */
    def asWkb(cellid: String): Array[Byte] = parseCell(cellid).map(c => JTS.toEWKB(BNG.cellIdToGeometry(c))).orNull

    /** The cell centre as EWKB with SRID 27700. */
    def centroid(cellid: String): Array[Byte] =
        parseCell(cellid).map { c =>
            val point = BNG.cellIdToGeometry(c).getCentroid
            point.setSRID(BNG.crsID)
            JTS.toEWKB(point)
        }.orNull

    /** Expected to fail in the UC sandbox (no GDAL natives); the UC pass records the result. */
    def gdalVersion(): String = KernelGdal.probe()
}
