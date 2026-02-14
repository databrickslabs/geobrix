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
    private val wtkWriters = mutable.Map[Long, WKTWriter]()
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

    /** Translates geometry by (xd, yd) via AffineTransformation. */
    def translate(xd: Double, yd: Double, geometry: Geometry): Geometry = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        transformation.transform(geometry)
    }

    /** Decodes WKB bytes to a JTS Geometry; uses per-thread WKBReader. */
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

    /** Encodes a JTS Geometry to WKB bytes; uses per-thread WKBWriter. */
    def toWKB(intersection: Geometry): Array[Byte] = {
        // val wkbWriter = new WKBWriter()
        val tid = Thread.currentThread().getId
        val writer = wkbWriters.getOrElseUpdate(tid, new WKBWriter())
        writer.write(intersection)
    }

    /** Parses WKT string to a JTS Geometry; uses per-thread WKTReader. */
    def fromWKT(wkt: String): Geometry = {
        val tid = Thread.currentThread().getId
        val reader = wtkReaders.getOrElseUpdate(tid, new WKTReader())
        reader.read(wkt)
    }

    /** Serializes a JTS Geometry to WKT string; uses per-thread WKTWriter. */
    def toWKT(geometry: Geometry): String = {
        val tid = Thread.currentThread().getId
        val writer = wtkWriters.getOrElseUpdate(tid, new WKTWriter())
        writer.write(geometry)
    }

    /** Douglas–Peucker simplification with given tolerance; preserves SRID. */
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
