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

    /** What a CRS core computed, before it is encoded for any particular output medium.
      *
      * Separating "what the answer is" from "how it is serialized" is what lets one core
      * feed both the medium-preserving Scala API (`eval`) and the always-BINARY SQL surface
      * (`evalSql`) without a text→geometry→binary round trip — a round trip that would lose
      * Z, because a 3D WKT writer emits the literal token `NaN` for a missing Z and
      * `JTS.fromWKT` reads that back as 2D. */
    sealed trait CrsOutcome

    object CrsOutcome {

        /** Null (or unusable) input → null output. */
        case object NullOut extends CrsOutcome

        /** Never-error degrade: emit the caller's input verbatim. Holds the ORIGINAL value,
          * not a re-encoding of it, so binary input comes back byte-identical. */
        final case class Unchanged(input: Any) extends CrsOutcome

        /** A computed geometry plus the SRID to carry (`None` clears any stale SRID). */
        final case class Geom(geom: Geometry, srid: Option[Int]) extends CrsOutcome
    }

    /** Encode an outcome in the input's own medium: text → EWKT/WKT, binary → EWKB/WKB.
      *
      * This is the medium-preserving contract of the Scala-level `eval` entry points.
      * Z-adaptive on both sides (a 2D geometry never gains a NaN Z ordinate). */
    def encodeAdaptive(outcome: CrsOutcome, text: Boolean): Any = outcome match {
        case CrsOutcome.NullOut         => null
        case CrsOutcome.Unchanged(in)   => in
        case CrsOutcome.Geom(g, sridOpt) =>
            sridOpt match {
                case Some(srid) =>
                    g.setSRID(srid)
                    if (text) UTF8String.fromString(JTS.toEWKTAdaptive(g)) else JTS.toEWKBAdaptive(g)
                case None =>
                    g.setSRID(0)
                    if (text) UTF8String.fromString(JTS.toWKTAdaptive(g)) else JTS.toWKBAdaptive(g)
            }
    }

    /** Encode an outcome as BINARY — the SQL-surface contract for `gbx_st_setcrs` and
      * `gbx_st_transformcrs`, which declare one fixed return type regardless of whether the
      * geometry argument arrived as BINARY or STRING.
      *
      * A degrade (`Unchanged`) with binary input returns the original bytes untouched; with
      * text input it re-encodes the parsed geometry, preserving whatever SRID the EWKT
      * carried, because a STRING cannot be handed back through a BINARY column. */
    def encodeBinary(outcome: CrsOutcome): Array[Byte] = outcome match {
        case CrsOutcome.NullOut => null
        case CrsOutcome.Unchanged(in) =>
            in match {
                case b: Array[Byte] => b
                case u: UTF8String  =>
                    val g = parseGeom(u)
                    if (g == null) null
                    else if (g.getSRID > 0) JTS.toEWKBAdaptive(g) else JTS.toWKBAdaptive(g)
                case _ => null
            }
        case CrsOutcome.Geom(g, sridOpt) =>
            sridOpt match {
                case Some(srid) => g.setSRID(srid); JTS.toEWKBAdaptive(g)
                case None       => g.setSRID(0); JTS.toWKBAdaptive(g)
            }
    }
}
