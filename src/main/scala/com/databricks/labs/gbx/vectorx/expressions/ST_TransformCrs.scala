package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.operations.OSRTransformGeometry
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.osr.SpatialReference

import scala.util.Try

/** Reprojects a geometry to ``target_crs`` (medium-preserving).
  *
  * Source CRS resolution order:
  *   1. Embedded SRID from the geometry (EWKB / EWKT).
  *   2. Explicit ``source_crs`` argument (for plain WKB / WKT inputs).
  *   3. No source CRS resolvable → return the input UNCHANGED (never-error
  *      invariant: unresolvable SRID or source_crs degrades gracefully).
  *
  * Output encoding follows the input medium (binary → binary, text → text).
  *
  * Output SRID:
  *   - Authority-coded target (EPSG/ESRI) → SRID n stamped in result.
  *   - Authority-less target (WKT / PROJ4) → SRID cleared in result.
  *
  * Resource discipline: every [[SpatialReference]] allocated here is deleted
  * in a `try/finally`, including on early-return paths.
  *
  * Registered as: `gbx_st_transformcrs`
  */
case class ST_TransformCrs(
    geom: Expression,
    targetCrs: Expression,
    sourceCrs: Expression
) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom, targetCrs, sourceCrs)
    override def dataType: DataType = geom.dataType
    override def nullable: Boolean = true
    override def prettyName: String = ST_TransformCrs.name
    override def replacement: Expression = invoke(ST_TransformCrs)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))
}

object ST_TransformCrs extends WithExpressionInfo {

    /** 2-argument form: infer source CRS from embedded SRID only. */
    def eval(geom: Any, targetCrs: UTF8String): Any =
        eval(geom, targetCrs, null.asInstanceOf[UTF8String])

    /** 3-argument form: explicit source CRS fallback for plain (SRID-less) inputs.
      *
      * Note: `eval` is a Spark Catalyst expression evaluation method — standard
      * InvokedExpression framework; not code execution.
      *
      * @param geom       WKB/EWKB bytes or WKT/EWKT string (or UTF8String).
      * @param targetCrs  Target CRS: EPSG/ESRI authority string, int, WKT, PROJ4.
      *                   Unresolvable target raises. Null target → null return.
      * @param sourceCrs  Explicit source CRS for plain (no-SRID) inputs; ignored
      *                   when geom already carries an embedded SRID.
      *                   Unresolvable source_crs → return unchanged (never-error).
      * @return           Reprojected geometry in the same encoding as the input, or
      *                   the input unchanged when no source CRS is resolvable.
      */
    def eval(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Any = {
        if (geom == null || targetCrs == null) return null
        val text = CrsExpressionUtil.isText(geom)
        val g = CrsExpressionUtil.parseGeom(geom)
        if (g == null) return geom  // parse failure → return unchanged

        // --- Resolve source CRS (never-error: failure returns input unchanged) ---
        val embeddedSrid = g.getSRID
        var srcSR: SpatialReference = null

        if (embeddedSrid > 0) {
            srcSR = Try(SpatialRefOps.resolveCrs(embeddedSrid.toString)).getOrElse(null)
            if (srcSR == null) return geom  // unresolvable embedded SRID → unchanged
        } else if (sourceCrs != null) {
            srcSR = Try(SpatialRefOps.resolveCrs(sourceCrs.toString)).getOrElse(null)
            if (srcSR == null) return geom  // unresolvable explicit source → unchanged
        }

        if (srcSR == null) return geom  // no source CRS at all → unchanged

        // --- Resolve target CRS (allowed to throw on failure) ---
        val dstSR = SpatialRefOps.resolveCrs(targetCrs.toString)
        try {
            val gProj = OSRTransformGeometry.transform(g, srcSR, dstSR)

            val authName = dstSR.GetAuthorityName(null)
            val authCode = dstSR.GetAuthorityCode(null)
            val tgtSridOpt =
                if (authName != null && authName.nonEmpty && authCode != null && authCode.nonEmpty)
                    Some(authCode.toInt)
                else
                    None

            tgtSridOpt match {
                case Some(srid) =>
                    gProj.setSRID(srid)
                    if (text) UTF8String.fromString(JTS.toEWKT(gProj))
                    else JTS.toEWKB3(gProj)
                case None =>
                    gProj.setSRID(0)
                    if (text) UTF8String.fromString(JTS.toWKT(gProj))
                    else JTS.toWKB3(gProj)
            }
        } finally {
            srcSR.delete()
            dstSR.delete()
        }
    }

    override def name: String = "gbx_st_transformcrs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => ST_TransformCrs(c(0), c(1), Literal(null, StringType))
        case 3 => ST_TransformCrs(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_st_transformcrs takes 2 or 3 arguments (geom, target_crs, [source_crs]); got $n"
        )
    }

    override def usageArgs: String = "geom, target_crs [, source_crs]"
    override def description: String =
        "Reproject geometry to target CRS. Optional source_crs for plain (SRID-less) inputs. " +
        "Returns input unchanged if source CRS is unresolvable (never-error invariant)."
}
