package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.util.Try

/** Shared utilities for the VectorX CRS expression companions (ST_Crs, ST_SetCrs, ST_TransformCrs).
  *
  * Kept in a dedicated private object so that each companion object remains self-contained
  * (no inheritance chain) while sharing the geometry parsing logic in one place.
  */
private[expressions] object CrsExpressionUtil {

    /** Parse a Catalyst-evaluated geometry value (Array[Byte] for BinaryType or UTF8String
      * for StringType) to a JTS Geometry. Returns null on parse failure — callers
      * treat null as a pass-through or no-op, never as an error. */
    def parseGeom(geom: Any): Geometry = geom match {
        case b: Array[Byte] => Try(JTS.fromWKB(b)).getOrElse(null)
        case u: UTF8String  => Try(JTS.fromWKT(u.toString)).getOrElse(null)
        case _              => null
    }

    /** Return true if the input is a text medium (UTF8String). */
    def isText(geom: Any): Boolean = geom match {
        case _: UTF8String => true
        case _             => false
    }
}
