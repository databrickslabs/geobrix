package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr.{Geometry => OGRGeometry}
import org.gdal.osr.{SpatialReference, osrConstants}
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

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
  *   - Authority-coded target with numeric code (EPSG/ESRI) → SRID n stamped in result.
  *   - Authority-less target (WKT / PROJ4) or non-numeric auth code → SRID cleared.
  *
  * Resource discipline: every [[SpatialReference]] allocated here is deleted
  * in a `try/finally`, including on early-return paths.
  *
  * Registered as: `gbx_st_transformcrs`
  */

/** 2-arg form: source CRS inferred from the geometry's embedded SRID only.
  * Separate case class (no Literal null third child) prevents Catalyst's
  * propagateNull from short-circuiting to NULL when source_crs is absent. */
case class ST_TransformCrs(geom: Expression, targetCrs: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom, targetCrs)
    override def dataType: DataType = geom.dataType
    override def nullable: Boolean = true
    override def prettyName: String = ST_TransformCrs.name
    override def replacement: Expression = invoke(ST_TransformCrs)
    override def inputTypes: Seq[DataType] = Seq(geom.dataType, StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))
}

/** 3-arg form: explicit ``sourceCrs`` fallback for plain (SRID-less) inputs. */
case class ST_TransformCrs3(
    geom: Expression,
    targetCrs: Expression,
    sourceCrs: Expression
) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom, targetCrs, sourceCrs)
    override def dataType: DataType = geom.dataType
    override def nullable: Boolean = true
    override def prettyName: String = ST_TransformCrs.name
    override def replacement: Expression = invoke(ST_TransformCrs3)
    override def inputTypes: Seq[DataType] = Seq(geom.dataType, StringType, StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))
}

object ST_TransformCrs extends WithExpressionInfo {

    /** 2-argument form: infer source CRS from embedded SRID only. */
    def eval(geom: Any, targetCrs: UTF8String): Any =
        TransformCrsCore(geom, targetCrs, null)

    /** 3-argument form: explicit source CRS fallback for plain (SRID-less) inputs.
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
    def eval(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Any =
        TransformCrsCore(geom, targetCrs, sourceCrs)

    override def name: String = "gbx_st_transformcrs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => ST_TransformCrs(c(0), c(1))
        case 3 => ST_TransformCrs3(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_st_transformcrs takes 2 or 3 arguments (geom, target_crs, [source_crs]); got $n"
        )
    }

    override def usageArgs: String = "geom, target_crs [, source_crs]"
    override def description: String =
        "Reproject geometry to target CRS. Optional source_crs for plain (SRID-less) inputs. " +
        "Returns input unchanged if source CRS is unresolvable (never-error invariant)."
}

object ST_TransformCrs3 extends WithExpressionInfo {

    def eval(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Any =
        TransformCrsCore(geom, targetCrs, sourceCrs)

    override def name: String = "gbx_st_transformcrs"

    override def builder(): FunctionBuilder = _ =>
        throw new UnsupportedOperationException("ST_TransformCrs3 builder not used directly")

    override def usageArgs: String = "geom, target_crs, source_crs"
    override def description: String =
        "Reproject geometry to target CRS with explicit source CRS."
}

/** Shared transform implementation for both 2-arg and 3-arg forms. Package-private. */
private[expressions] object TransformCrsCore {

    def apply(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Any = {
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

        // --- Resolve target CRS (allowed to throw on failure).
        // srcSR must be released if this throws to avoid a resource leak.
        val dstSR = try {
            SpatialRefOps.resolveCrs(targetCrs.toString)
        } catch {
            case e: Throwable =>
                srcSR.delete()
                throw e
        }

        try {
            val gProj = transformGeom(g, srcSR, dstSR)

            val authName = dstSR.GetAuthorityName(null)
            val authCode = dstSR.GetAuthorityCode(null)
            val tgtSridOpt = for {
                name <- Option(authName) if name.nonEmpty
                code <- Option(authCode)
                n    <- Try(code.toInt).toOption
            } yield n

            tgtSridOpt match {
                case Some(srid) =>
                    gProj.setSRID(srid)
                    if (text) UTF8String.fromString(JTS.toEWKTAdaptive(gProj))
                    else JTS.toEWKBAdaptive(gProj)
                case None =>
                    gProj.setSRID(0)
                    if (text) UTF8String.fromString(JTS.toWKTAdaptive(gProj))
                    else JTS.toWKBAdaptive(gProj)
            }
        } finally {
            srcSR.delete()
            dstSR.delete()
        }
    }

    /** Z-aware OGR geometry transform.
      *
      * Uses JTS.toWKBAdaptive so 3D geometries carry their Z ordinate into OGR and
      * back out again. Axis-mapping strategy is forced to OAMS_TRADITIONAL_GIS_ORDER
      * on clones so GDAL 3+ EPSG-imported SRs don't silently flip lon/lat to lat/lon. */
    private def transformGeom(
        g: JTSGeometry,
        srcSR: SpatialReference,
        dstSR: SpatialReference
    ): JTSGeometry = {
        if (srcSR.IsSame(dstSR) == 1) return g
        val srcClone = srcSR.Clone()
        val dstClone = dstSR.Clone()
        srcClone.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        dstClone.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        try {
            val ogrGeom = OGRGeometry.CreateFromWkb(JTS.toWKBAdaptive(g))
            ogrGeom.AssignSpatialReference(srcClone)
            ogrGeom.TransformTo(dstClone)
            val res = JTS.fromWKB(ogrGeom.ExportToWkb())
            ogrGeom.delete()
            res
        } finally {
            srcClone.delete()
            dstClone.delete()
        }
    }
}
