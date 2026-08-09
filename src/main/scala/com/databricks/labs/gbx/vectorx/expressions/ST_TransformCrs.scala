package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.operations.SpatialRefOps.CrsInfo
import com.databricks.labs.gbx.vectorx.expressions.CrsExpressionUtil.CrsOutcome
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.ogr.{Geometry => OGRGeometry}
import org.gdal.osr.CoordinateTransformation
import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import scala.util.Try
import scala.util.control.NonFatal

/** 2-arg form: source CRS inferred from the geometry's embedded SRID only.
  * Separate case class (no Literal null third child) prevents Catalyst's
  * propagateNull from short-circuiting to NULL when source_crs is absent.
  *
  * Returns BINARY regardless of the geometry input encoding — see [[ST_TransformCrs]]
  * for why the SQL surface pins one return type while the Scala core stays
  * medium-preserving. */
case class ST_TransformCrs(geom: Expression, targetCrs: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom, targetCrs)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = ST_TransformCrs.name
    override def replacement: Expression = invoke(ST_TransformCrs, methodName = "evalSql")
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
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = ST_TransformCrs.name
    override def replacement: Expression = invoke(ST_TransformCrs3, methodName = "evalSql")
    override def inputTypes: Seq[DataType] = Seq(geom.dataType, StringType, StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))
}

/** Reprojects a geometry to a target CRS.
  *
  * Encoding contract — two layers:
  * - The SQL surface ([[ST_TransformCrs]] / [[ST_TransformCrs3]]) always returns BINARY
  *   (EWKB when the target carries an integer authority code, plain WKB when it does not),
  *   whichever encoding the geometry argument arrived in. One function has one declared
  *   return type: an input-dependent type cannot be used in a fixed-schema view, BINARY/WKB
  *   is how the rest of `gbx_st_*` and the built-in `st_*` functions exchange geometries,
  *   and it avoids a text-medium Z hazard — a 3D WKT writer emits the literal token `NaN`
  *   for a missing Z, which `JTS.fromWKT` reads back as 2D, so chaining CRS calls through
  *   text silently dropped Z.
  * - The Scala core [[eval]] stays medium-preserving (binary in → binary out, text in →
  *   text out) for callers working in text.
  */
object ST_TransformCrs extends WithExpressionInfo {

    /** 2-argument form, medium-preserving: infer source CRS from embedded SRID only. */
    def eval(geom: Any, targetCrs: UTF8String): Any =
        CrsExpressionUtil.encodeAdaptive(
          TransformCrsCore(geom, targetCrs, null), CrsExpressionUtil.isText(geom))

    /** 3-argument form, medium-preserving: explicit source CRS fallback for plain inputs.
      *
      * @param geom       WKB/EWKB bytes or WKT/EWKT string (or UTF8String).
      * @param targetCrs  Target CRS: EPSG/ESRI authority string, int, WKT, PROJ4.
      *                   Unresolvable target raises. Null target → null return.
      * @param sourceCrs  Explicit source CRS for plain (no-SRID) inputs; ignored
      *                   when geom already carries an embedded SRID.
      *                   Unresolvable explicit source_crs raises (parameter condition).
      * @return           Reprojected geometry in the same encoding as the input, or null
      *                   when the source CRS is missing/unresolvable (data condition).
      */
    def eval(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Any =
        CrsExpressionUtil.encodeAdaptive(
          TransformCrsCore(geom, targetCrs, sourceCrs), CrsExpressionUtil.isText(geom))

    /** SQL surface, 2-argument form: always BINARY. */
    def evalSql(geom: Any, targetCrs: UTF8String): Array[Byte] =
        CrsExpressionUtil.encodeBinary(TransformCrsCore(geom, targetCrs, null))

    /** SQL surface, 3-argument form: always BINARY. */
    def evalSql(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Array[Byte] =
        CrsExpressionUtil.encodeBinary(TransformCrsCore(geom, targetCrs, sourceCrs))

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
        "Bad data (unparseable geom, unresolvable embedded SRID, missing source) returns NULL. " +
        "Bad parameter (unresolvable target or explicit source_crs) raises."
}

object ST_TransformCrs3 extends WithExpressionInfo {

    /** SQL surface (3-arg): always BINARY.
      *
      * This companion exists ONLY as the invoke target for the 3-arg [[ST_TransformCrs3]]
      * case class, so `evalSql` is its whole surface. There is deliberately no `eval` here:
      * the medium-preserving 3-arg core is `ST_TransformCrs.eval(geom, target, source)`, and
      * a second copy on this object would be dead code that could silently drift from it. */
    def evalSql(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): Array[Byte] =
        CrsExpressionUtil.encodeBinary(TransformCrsCore(geom, targetCrs, sourceCrs))

    override def name: String = "gbx_st_transformcrs"

    override def usageArgs: String = "geom, target_crs, source_crs"
    override def description: String =
        "Reproject geometry to target CRS with explicit source CRS."
}

/** Shared transform implementation for both 2-arg and 3-arg forms. Package-private. */
private[expressions] object TransformCrsCore {

    /** Reproject ``geom`` to ``targetCrs``.
      *
      * Hot-path design: both CRS ends go through `SpatialRefOps.crsInfo` (cached canonical
      * string + authority SRID, no live GDAL handle) and the reprojection itself through
      * `SpatialRefOps.transformPlan` (cached identity flag + cache-owned CT), so no
      * `SpatialReference` or `CoordinateTransformation` is allocated per row on the steady
      * state. On the steady state — the same CRS pair repeated, which is what a column of
      * geometries looks like — the GDAL work per row is: (a) the OGR geometry round-trip,
      * and (b) when the target carries an area_of_use, an `areaOfUse` lookup on the target's
      * canonical string (resolves, reads, and deletes a SpatialReference). The area_of_use
      * lookup is a candidate for memoization by canonical target string; that is deferred as
      * a follow-up perf improvement.  Nothing here owns a GDAL object, so there is nothing
      * to release: no `finally`, no double-delete, no use-after-delete.
      *
      * Error contract (mirrors light-tier):
      *   - Bad GEOMETRY DATA (unparseable, unresolvable embedded SRID, no source CRS) → NullOut.
      *   - Bad PARAMETER (explicit source_crs unresolvable, target_crs unresolvable) → raises.
      *   - Non-finite output coordinates → NullOut.
      *   - Out-of-domain output (GDAL area_of_use check) → NullOut; skipped when target has none. */
    def apply(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): CrsOutcome = {
        if (geom == null || targetCrs == null) return CrsOutcome.NullOut
        val g = CrsExpressionUtil.parseGeom(geom)
        if (g == null) return CrsOutcome.NullOut  // parse failure → data condition → NULL

        // --- Resolve source CRS ---
        // Rule: embedded SRID (DATA) → unresolvable → NullOut.
        //       explicit sourceCrs (PARAMETER) → unresolvable → RAISE (let crsInfo throw).
        //       no CRS at all (DATA: plain geometry with no context) → NullOut.
        val embeddedSrid = g.getSRID
        val srcInfo: CrsInfo =
            if (embeddedSrid > 0) Try(SpatialRefOps.crsInfo(embeddedSrid.toString)).getOrElse(null)
            else if (sourceCrs != null) SpatialRefOps.crsInfo(sourceCrs.toString)  // raises on bad input
            else null

        // Unresolvable embedded SRID (null result above) or no CRS at all → data → NULL.
        if (srcInfo == null || srcInfo.canonical == null || srcInfo.canonical.isEmpty) {
            return CrsOutcome.NullOut
        }

        // --- Resolve target CRS (parameter → allowed to throw on failure) ---
        val dstInfo = SpatialRefOps.crsInfo(targetCrs.toString)

        // Plan lookup re-resolves the canonical strings on a cache miss. A canonical
        // round-trip failure (authority-less CRS whose exported WKT does not re-parse) is
        // a degenerate data condition — degrade to NullOut rather than raise.
        //
        // NonFatal, not Throwable: a fatal error (OutOfMemoryError, StackOverflowError,
        // InterruptedException) must propagate and fail the task.
        val plan = try {
            SpatialRefOps.transformPlan(srcInfo.canonical, dstInfo.canonical)
        } catch {
            case NonFatal(_) => return CrsOutcome.NullOut
        }
        val gProj = if (plan.identity) g else transformWithCachedCT(g, plan.transformation)

        // Non-finite guard: mirror light's _has_nonfinite_xy (heavy previously had none).
        val coords = gProj.getCoordinates
        val nonFinite = coords.exists(c => c.x.isNaN || c.x.isInfinite || c.y.isNaN || c.y.isInfinite)
        if (nonFinite) return CrsOutcome.NullOut

        // Domain check: compare input lon/lat against the target CRS's area_of_use.
        // Skip entirely when the target carries no area_of_use (authority-less WKT / PROJ4).
        SpatialRefOps.areaOfUse(dstInfo.canonical) match {
            case Some(bbox) =>
                // Get the input in lon/lat. If the source is already EPSG:4326, use g's coords
                // directly; otherwise transform source → EPSG:4326 to obtain lon/lat.
                val lonLat: Array[(Double, Double)] =
                    if (srcInfo.canonical == "EPSG:4326") g.getCoordinates.map(c => (c.x, c.y))
                    else {
                        val toWgs = SpatialRefOps.transformPlan(srcInfo.canonical, "EPSG:4326")
                        val gWgs = if (toWgs.identity) g else transformWithCachedCT(g, toWgs.transformation)
                        gWgs.getCoordinates.map(c => (c.x, c.y))
                    }
                if (!SpatialRefOps.allInBBox(lonLat, bbox)) return CrsOutcome.NullOut
            case None => // no area_of_use → skip domain check
        }

        // authoritySrid is None for an authority-less target (raw WKT / PROJ4) and for a
        // non-numeric authority code (e.g. OGC:CRS84) — both clear the now-stale SRID.
        CrsOutcome.Geom(gProj, dstInfo.authoritySrid)
    }

    /** Z-aware OGR reprojection through a cache-owned CoordinateTransformation.
      *
      * The CT carries OAMS_TRADITIONAL_GIS_ORDER on both ends (baked in at construction by
      * `SpatialRefOps`), so JTS (x=lon, y=lat) input is not axis-flipped. The CT is owned by
      * the thread-local transformer cache and must NOT be deleted here. The only object this
      * method owns is `ogrGeom`, released in a `finally`.
      *
      * Uses JTS.toWKBForOGR (all-coords-must-have-Z rule) so mixed-Z geometries (some NaN Z,
      * some finite Z) are downcast to 2D for OGR rather than throwing — OGR refuses NaN Z
      * ordinates (General Error). All-Z geometries still carry Z through the transform. */
    private[expressions] def transformWithCachedCT(
        g: JTSGeometry, ct: CoordinateTransformation
    ): JTSGeometry = {
        val ogrGeom = OGRGeometry.CreateFromWkb(JTS.toWKBForOGR(g))  // safe for OGR: all-or-nothing Z
        try {
            ogrGeom.Transform(ct)
            JTS.fromWKB(ogrGeom.ExportToWkb())
        } finally {
            ogrGeom.delete()
        }
    }
}
