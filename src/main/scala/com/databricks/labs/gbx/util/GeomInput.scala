package com.databricks.labs.gbx.util

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.util.Try

/** One normaliser for a GEOMETRY argument evaluated by a Catalyst expression.
  *
  * A geometry argument is BINARY (WKB or EWKB) or STRING (WKT or EWKT). Casting the evaluated value
  * straight to `Array[Byte]` throws a ClassCastException for STRING, so every expression that takes a
  * geometry routes it through here instead. */
object GeomInput {

    /** Parse an evaluated geometry value (Array[Byte] or UTF8String) to a JTS Geometry; null on null,
      * on an unparseable value, or on any other type. */
    def parse(v: Any): Geometry = v match {
        case b: Array[Byte] => Try(JTS.fromWKB(b)).getOrElse(null)
        case u: UTF8String  => Try(JTS.fromWKT(u.toString)).getOrElse(null)
        case _              => null
    }

    /** Normalise an evaluated geometry value to WKB bytes. BINARY passes through untouched (WKB and EWKB
      * stay byte-identical); STRING is parsed and re-encoded as WKB (Z only when the geometry has one).
      * Null on null, on unparseable text, or on any other type. */
    def toWkb(v: Any): Array[Byte] = v match {
        case b: Array[Byte] => b
        case u: UTF8String =>
            val g = parse(u)
            if (g == null) null else JTS.toWKBAdaptive(g)
        case _ => null
    }

    /** Analysis-time check for a geometry argument: BINARY, STRING or a NULL literal; anything else is
      * refused. `paramIndex` is the Spark ordinal word ("first", "second", ...). */
    def checkType(geom: Expression, paramIndex: String): TypeCheckResult = geom.dataType match {
        case BinaryType | StringType | NullType => TypeCheckSuccess
        case _ =>
            DataTypeMismatch(
              errorSubClass = "UNEXPECTED_INPUT_TYPE",
              messageParameters = Map(
                "paramIndex" -> paramIndex,
                "requiredType" -> """("BINARY" or "STRING")""",
                "inputSql" -> s"\"${geom.sql}\"",
                "inputType" -> s"\"${geom.dataType.sql}\""
              )
            )
    }
}
