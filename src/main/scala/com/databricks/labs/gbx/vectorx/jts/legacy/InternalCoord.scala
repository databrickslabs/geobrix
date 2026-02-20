package com.databricks.labs.gbx.vectorx.jts.legacy

import org.apache.spark.sql.catalyst.util.ArrayData
import org.locationtech.jts.geom.Coordinate

/**
  * Case class modeling a 2D or 3D point (coords: x, y or x, y, z). Used by legacy InternalGeometry
  * to represent vertices; serializes to Spark ArrayData and converts to/from JTS Coordinate.
  */
case class InternalCoord(coords: Seq[Double]) {

    /** Serializes coords to Spark ArrayData for storage in InternalRow/arrays. */
    def serialize: ArrayData = ArrayData.toArrayData(coords)

    /** Converts to a JTS Coordinate (2D or 3D depending on coords.length). */
    // noinspection ZeroIndexToHead
    def toCoordinate: Coordinate = {
        if (coords.length == 2) {
            new Coordinate(coords(0), coords(1))
        } else {
            new Coordinate(coords(0), coords(1), coords(2))
        }
    }

}

/** Companion: construct from JTS Coordinate or Spark ArrayData. */
object InternalCoord {

    /** Builds InternalCoord from a JTS Coordinate (2 or 3 doubles). */
    def apply(coordinate: Coordinate): InternalCoord = {
        val z = coordinate.getZ
        if (z.isNaN) {
            new InternalCoord(Seq(coordinate.getX, coordinate.getY))
        } else {
            new InternalCoord(Seq(coordinate.getX, coordinate.getY, z))
        }
    }

    /** Builds InternalCoord from Spark ArrayData (2 or 3 doubles). */
    def apply(input: ArrayData): InternalCoord = {
        if (input.numElements() == 2) {
            new InternalCoord(Seq(input.getDouble(0), input.getDouble(1)))
        } else {
            new InternalCoord(Seq(input.getDouble(0), input.getDouble(1), input.getDouble(2)))
        }
    }

}
