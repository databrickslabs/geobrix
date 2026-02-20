package com.databricks.labs.gbx.vectorx.jts.legacy

import com.databricks.labs.gbx.vectorx.jts.{GeometryTypeEnum, JTS}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

/**
  * Case class modeling Polygons and MultiPolygons in legacy Spark-internal form (typeId, srid, boundaries, holes).
  * Used for serialization/deserialization of geometry in legacy vector path; convert to JTS via toJTS.
  */
case class InternalGeometry(
    typeId: Int,
    srid: Int,
    boundaries: Array[Array[InternalCoord]],
    holes: Array[Array[Array[InternalCoord]]]
) {

    /** Converts this internal representation to a JTS Geometry (Point, LineString, Polygon, etc.). */
    def toJTS: Geometry = {
        GeometryTypeEnum.fromTypeId(typeId) match {
            case GeometryTypeEnum.POINT              => JTS.point(boundaries.head.head.toCoordinate)
            case GeometryTypeEnum.MULTIPOINT         => JTS.multiPoint(boundaries.head.map(p => JTS.point(p.toCoordinate)))
            case GeometryTypeEnum.LINESTRING         => JTS.lineStringXYs(boundaries.head.map(c => (c.coords(0), c.coords(1))).toBuffer)
            case GeometryTypeEnum.MULTILINESTRING    =>
                JTS.multiLineString(boundaries.map(ls => JTS.lineStringXYs(ls.map(c => (c.coords(0), c.coords(1))).toBuffer)))
            case GeometryTypeEnum.POLYGON            =>
                // TODO: handle holes
                JTS.polygonFromCoords(boundaries.head.map(c => c.toCoordinate))
            case GeometryTypeEnum.MULTIPOLYGON       =>
                // TODO: handle holes
                JTS.multiPolygonFromXYs(boundaries.map(_.map(c => (c.coords(0), c.coords(1))).toArray))
            case GeometryTypeEnum.GEOMETRYCOLLECTION =>
                // TODO: implement
                throw new IllegalAccessException("GeometryCollection not implemented")
        }
    }

}

/** Companion: InternalCoordType for schema; apply(InternalRow) builds from Spark internal row. */
object InternalGeometry {

    /** Spark DataType for a single InternalCoord (array of doubles). */
    val InternalCoordType: DataType = ArrayType.apply(DoubleType)

    /** Builds InternalGeometry from a Spark InternalRow (typeId, srid, boundaries array, holes array). */
    def apply(input: InternalRow): InternalGeometry = {
        val typeId = input.getInt(0)
        val srid = input.getInt(1)

        val boundaries = input
            .getArray(2)
            .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
            .map(
              _.asInstanceOf[ArrayData]
                  .toObjectArray(InternalCoordType)
                  .map(c => InternalCoord(c.asInstanceOf[ArrayData]))
            )

        val holeGroups = input
            .getArray(3)
            .toObjectArray(ArrayType(ArrayType(ArrayType(InternalCoordType))))
            .map(
              _.asInstanceOf[ArrayData]
                  .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
                  .map(
                    _.asInstanceOf[ArrayData]
                        .toObjectArray(ArrayType(ArrayType(InternalCoordType)))
                        .map(c => InternalCoord(c.asInstanceOf[ArrayData]))
                  )
            )

        new InternalGeometry(typeId, srid, boundaries, holeGroups)
    }

}
