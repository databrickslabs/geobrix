package com.databricks.labs.gbx.vectorx.jts

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import scala.collection.mutable

/**
  * JTS geometry factory and serialization helpers; thread-safe per-thread caches for readers/writers.
  *
  * Used by GridX (BNG geometry), VectorX, and raster clip/geometry operations. WKB/WKT encode/decode
  * use thread-local-like caches because JTS readers/writers are not thread-safe.
  */
object JTS {

    // Per-thread caches: JTS readers/writers are not thread-safe and this object is shared across tasks.
    private val geometryFactories = mutable.Map[Long, GeometryFactory]()
    private val wkbReaders = mutable.Map[Long, WKBReader]()
    private val wkbWriters = mutable.Map[Long, WKBWriter]()
    private val wkb3Writers = mutable.Map[Long, WKBWriter]()
    private val ewkbWriters = mutable.Map[Long, WKBWriter]()
    private val ewkb3Writers = mutable.Map[Long, WKBWriter]()
    private val wtkWriters = mutable.Map[Long, WKTWriter]()
    private val wtk3Writers = mutable.Map[Long, WKTWriter]()
    private val wtkReaders = mutable.Map[Long, WKTReader]()

    /** Creates a JTS Point at (x, y); uses per-thread GeometryFactory. */
    def point(x: Double, y: Double): Point = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPoint(new Coordinate(x, y))
    }

    /** Creates a JTS Point from a Coordinate; uses per-thread GeometryFactory. */
    def point(coordinate: Coordinate): Point = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPoint(coordinate)
    }

    /** Builds a Polygon from an array of Points (exterior ring); uses per-thread factory. */
    def polygonFromPoints(points: Array[Point]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPolygon(
          points.map(_.getCoordinate)
        )
    }

    /** Builds a Polygon from an array of Coordinates (exterior ring); uses per-thread factory. */
    def polygonFromCoords(coordinates: Array[Coordinate]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPolygon(
          coordinates
        )
    }

    /** Builds a Polygon from (x, y) pairs; uses per-thread factory. */
    def polygonFromXYs(xys: Array[(Double, Double)]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        val coordinates = xys.map { case (x, y) => new Coordinate(x, y) }
        gf.createPolygon(coordinates)
    }

    /** Builds a MultiPolygon from an array of (x, y) rings (one per polygon); uses per-thread factory. */
    def multiPolygonFromXYs(polygons: Array[Array[(Double, Double)]]): MultiPolygon = {
        val polys = polygons.map(polygonFromXYs)
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createMultiPolygon(polys)
    }

    /** Builds a Polygon from an exterior shell and interior rings (holes); preserves Z via Coordinate.
      * Empty `holes` yields a polygon with no interior rings. Uses per-thread factory. */
    def polygonWithHoles(shell: Seq[Coordinate], holes: Seq[Seq[Coordinate]]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        val exterior = gf.createLinearRing(shell.toArray)
        val interiors = holes.iterator.map(ring => gf.createLinearRing(ring.toArray)).toArray
        gf.createPolygon(exterior, interiors)
    }

    /** Builds a MultiPolygon from a sequence of Polygon geometries; uses per-thread factory. */
    def multiPolygon(polygons: Seq[Polygon]): MultiPolygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createMultiPolygon(polygons.toArray)
    }

    /** Creates a single JTS Coordinate(x, y). */
    def coordinatesFromXYs(getX: Double, getY: Double): Coordinate = {
        new Coordinate(getX, getY)
    }

    /** Builds a LineString from (x, y) buffer; uses per-thread factory. */
    def lineStringXYs(xys: mutable.Buffer[(Double, Double)]): LineString = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        val coordinates = xys.map { case (x, y) => new Coordinate(x, y) }.toArray
        gf.createLineString(coordinates)
    }

    /** Builds a LineString from JTS Coordinates (preserves Z); uses per-thread factory.
      *
      * Unlike [[lineStringXYs]] (which takes 2D (x, y) pairs), this keeps the full Coordinate,
      * so a Z value survives the round-trip when the source coordinates carry one. */
    def lineStringCoords(coords: Seq[Coordinate]): LineString = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createLineString(coords.toArray)
    }

    /** Translates geometry by (xd, yd) via AffineTransformation. */
    def translate(xd: Double, yd: Double, geometry: Geometry): Geometry = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        transformation.transform(geometry)
    }

    /** Decodes WKB or EWKB bytes to a JTS Geometry; uses per-thread WKBReader.
      *
      * JTS [[WKBReader]] auto-detects PostGIS "Extended WKB" (EWKB) — when the SRID-present flag
      * is set in the byte-order byte, the reader consumes the trailing SRID int and calls
      * [[Geometry#setSRID]] on the result. Plain WKB returns a Geometry with SRID=0. */
    def fromWKB(bytes: Array[Byte]): Geometry = {
        val tid = Thread.currentThread().getId
        val reader = wkbReaders.getOrElseUpdate(tid, new WKBReader())
        reader.read(bytes)
    }

    /** Builds a MultiLineString from a sequence of LineString geometries; uses per-thread factory. */
    def multiLineString(breaklines: Seq[Geometry]): MultiLineString = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        if (breaklines.isEmpty) {
            gf.createMultiLineString(Array.empty)
        } else {
            val lines = breaklines.map(_.asInstanceOf[LineString]).toArray
            gf.createMultiLineString(lines)
        }
    }

    /** Returns a Point from the geometry's first coordinate (e.g. centroid of one point). */
    def anyPoint(geom: Geometry): Point = {
        val coords = geom.getCoordinate
        point(coords)
    }

    /** Shared empty polygon instance (from WKT). */
    def emptyPolygon: Geometry = JTS.fromWKT("POLYGON EMPTY")

    /** Encodes a JTS Geometry to OGC WKB bytes (no SRID); uses per-thread WKBWriter.
      *
      * Use [[toEWKB]] when you need to preserve SRID across the encoding. */
    def toWKB(intersection: Geometry): Array[Byte] = {
        val tid = Thread.currentThread().getId
        val writer = wkbWriters.getOrElseUpdate(tid, new WKBWriter())
        writer.write(intersection)
    }

    /** Encode a JTS Geometry to OGC WKB preserving Z (3 dimensions); per-thread WKBWriter(3). */
    def toWKB3(geom: org.locationtech.jts.geom.Geometry): Array[Byte] = {
        val tid = Thread.currentThread().getId
        val writer = wkb3Writers.getOrElseUpdate(tid, new WKBWriter(3))
        writer.write(geom)
    }

    /** Encodes a JTS Geometry to PostGIS EWKB bytes; embeds SRID when set. Per-thread writer.
      *
      * EWKB is auto-detected on read by [[fromWKB]], so this is the reciprocal for SRID-preserving
      * round-trips. Note: plain WKB consumers (e.g. GDAL `CreateGeometryFromWkb`, Databricks
      * built-in `st_*` functions) may not accept EWKB — use [[toWKB]] for those. */
    def toEWKB(geom: Geometry): Array[Byte] = {
        val tid = Thread.currentThread().getId
        val writer = ewkbWriters.getOrElseUpdate(tid, new WKBWriter(2, true))
        writer.write(geom)
    }

    /** Encodes a JTS Geometry to PostGIS EWKB preserving Z (3 dimensions); embeds SRID when set.
      *
      * Use this instead of [[toEWKB]] when the input geometry may carry Z coordinates and
      * Z preservation is required (e.g. st_setcrs, which stamps a CRS without touching
      * coordinates). Per-thread writer. */
    def toEWKB3(geom: Geometry): Array[Byte] = {
        val tid = Thread.currentThread().getId
        val writer = ewkb3Writers.getOrElseUpdate(tid, new WKBWriter(3, true))
        writer.write(geom)
    }

    /** Encodes to EWKB using 3D output only when the geometry carries Z.
      *
      * 2D geometry (NaN Z) → standard EWKB (same as [[toEWKB]]);
      * 3D geometry (finite Z) → 3D EWKB with Z ordinate.
      * Prevents NaN-Z injection for 2D geometries that should stay 2D. */
    def toEWKBAdaptive(geom: Geometry): Array[Byte] = {
        val coord = geom.getCoordinate
        val hasZ = coord != null && !coord.z.isNaN
        if (hasZ) toEWKB3(geom) else toEWKB(geom)
    }

    /** Encodes to plain WKB (no SRID) using 3D output only when the geometry carries Z.
      *
      * 2D geometry → standard WKB (same as [[toWKB]]);
      * 3D geometry → 3D WKB with Z ordinate. */
    def toWKBAdaptive(geom: Geometry): Array[Byte] = {
        val coord = geom.getCoordinate
        val hasZ = coord != null && !coord.z.isNaN
        if (hasZ) toWKB3(geom) else toWKB(geom)
    }

    /** Serializes to WKT using 3D writer only when the geometry carries Z.
      *
      * 2D geometry → standard WKT (same as [[toWKT]]);
      * 3D geometry → ISO WKT with Z ordinate (`POINT Z (x y z)`). */
    def toWKTAdaptive(geom: Geometry): String = {
        val coord = geom.getCoordinate
        val hasZ = coord != null && !coord.z.isNaN
        if (hasZ) {
            val tid = Thread.currentThread().getId
            val writer = wtk3Writers.getOrElseUpdate(tid, new WKTWriter(3))
            writer.write(geom)
        } else toWKT(geom)
    }

    /** Serializes to EWKT (`SRID=n;WKT` or plain WKT) using 3D writer only when the geometry
      * carries Z. Reciprocal of [[fromWKT]] with Z preservation. */
    def toEWKTAdaptive(geom: Geometry): String = {
        val srid = geom.getSRID
        val wkt = toWKTAdaptive(geom)
        if (srid > 0) s"SRID=$srid;$wkt" else wkt
    }

    /** Parses OGC WKT or PostGIS EWKT to a JTS Geometry; uses per-thread WKTReader.
      *
      * EWKT has the form `SRID=<int>;<WKT>` (e.g. `SRID=4326;POINT(0 0)`). If present, the prefix
      * is stripped and `setSRID` is called on the parsed geometry. Plain WKT parses as before and
      * returns a Geometry with SRID=0. */
    def fromWKT(wkt: String): Geometry = {
        val tid = Thread.currentThread().getId
        val reader = wtkReaders.getOrElseUpdate(tid, new WKTReader())
        val (srid, body) = splitEWKT(wkt)
        val geom = reader.read(body)
        if (srid > 0) geom.setSRID(srid)
        geom
    }

    /** Splits an optional `SRID=<int>;` prefix off a WKT/EWKT string. Returns (srid, wkt-body).
      * Returns (0, input) when no valid prefix is present. Case-insensitive; tolerates surrounding whitespace. */
    private[jts] def splitEWKT(raw: String): (Int, String) = {
        if (raw == null) return (0, raw)
        val trimmed = raw.stripLeading()
        // Cheapest possible check before allocating a substring
        if (trimmed.length < 6 || !(trimmed.charAt(0) == 'S' || trimmed.charAt(0) == 's')) return (0, raw)
        if (!trimmed.regionMatches(true, 0, "SRID=", 0, 5)) return (0, raw)
        val semi = trimmed.indexOf(';', 5)
        if (semi <= 5) return (0, raw)
        val sridStr = trimmed.substring(5, semi).trim
        try {
            val srid = sridStr.toInt
            if (srid <= 0) (0, trimmed.substring(semi + 1)) else (srid, trimmed.substring(semi + 1))
        } catch { case _: NumberFormatException => (0, raw) }
    }

    /** Serializes a JTS Geometry to OGC WKT (no SRID); uses per-thread WKTWriter.
      *
      * Use [[toEWKT]] when you need to preserve SRID across the encoding. */
    def toWKT(geometry: Geometry): String = {
        val tid = Thread.currentThread().getId
        val writer = wtkWriters.getOrElseUpdate(tid, new WKTWriter())
        writer.write(geometry)
    }

    /** Serializes a JTS Geometry to PostGIS EWKT (`SRID=<int>;<WKT>`); falls back to plain WKT
      * when SRID is 0. Reciprocal of [[fromWKT]]. */
    def toEWKT(geometry: Geometry): String = {
        val srid = geometry.getSRID
        val wkt = toWKT(geometry)
        if (srid > 0) s"SRID=$srid;$wkt" else wkt
    }

    /** Douglas-Peucker simplification with given tolerance; preserves SRID. */
    def simplify(geometry: Geometry, tolerance: Double): Geometry = {
        val simplified = DouglasPeuckerSimplifier.simplify(geometry, tolerance)
        simplified.setSRID(geometry.getSRID)
        simplified
    }

    /** Converts Spark ArrayData (StringType WKT or BinaryType WKB) to an array of JTS Geometries. */
    def fromArrayData(data: ArrayData, dt: DataType): Array[Geometry] = {
        dt match {
            case StringType => data.toArray[UTF8String](dt).map(_.toString).map(fromWKT)
            case BinaryType => data.toArray[Array[Byte]](dt).map(fromWKB)
            case _          => throw new IllegalArgumentException(s"Unsupported data type: $dt")
        }
    }

    /** Builds a MultiPoint from an array of Geometries (one point per getCoordinate); uses per-thread factory. */
    def multiPoint(geomPoints: Array[Geometry]): MultiPoint = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createMultiPointFromCoords(geomPoints.map(_.getCoordinate))
    }

}
