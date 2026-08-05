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
      *                   Unresolvable source_crs → return unchanged (never-error).
      * @return           Reprojected geometry in the same encoding as the input, or
      *                   the input unchanged when no source CRS is resolvable.
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
        "Returns input unchanged if source CRS is unresolvable (never-error invariant)."
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
      * Hot-path design: this method allocates NO `SpatialReference` and NO
      * `CoordinateTransformation` per row. Both CRS ends go through
      * `SpatialRefOps.crsInfo` (cached canonical string + authority SRID, no live GDAL
      * handle) and the reprojection itself through `SpatialRefOps.transformPlan` (cached
      * identity flag + cache-owned CT). On the steady state — the same CRS pair repeated,
      * which is what a column of geometries looks like — the only GDAL work per row is the
      * OGR geometry round-trip. Nothing here owns a GDAL object, so there is nothing to
      * release: no `finally`, no double-delete, no use-after-delete.
      *
      * Never-error invariant: an unresolvable SOURCE (embedded SRID or explicit
      * `source_crs`) returns the input unchanged. `crsInfo` caches nothing on failure, so a
      * bad source degrades on every row rather than poisoning the cache. Only an
      * unresolvable TARGET raises. */
    def apply(geom: Any, targetCrs: UTF8String, sourceCrs: UTF8String): CrsOutcome = {
        if (geom == null || targetCrs == null) return CrsOutcome.NullOut
        val g = CrsExpressionUtil.parseGeom(geom)
        if (g == null) return CrsOutcome.Unchanged(geom)  // parse failure → return unchanged

        // --- Resolve source CRS (never-error: failure returns input unchanged) ---
        val embeddedSrid = g.getSRID
        val srcInfo: CrsInfo =
            if (embeddedSrid > 0) Try(SpatialRefOps.crsInfo(embeddedSrid.toString)).getOrElse(null)
            else if (sourceCrs != null) Try(SpatialRefOps.crsInfo(sourceCrs.toString)).getOrElse(null)
            else null

        // Unresolvable / absent source CRS → unchanged. An empty canonical string would make
        // the transform-plan lookup raise, which would violate the never-error invariant.
        if (srcInfo == null || srcInfo.canonical == null || srcInfo.canonical.isEmpty) {
            return CrsOutcome.Unchanged(geom)
        }

        // --- Resolve target CRS (allowed to throw on failure — user asked for a bad CRS) ---
        val dstInfo = SpatialRefOps.crsInfo(targetCrs.toString)

        // The plan lookup re-resolves the two CANONICAL strings on a cache miss. The target has
        // already been proven resolvable by `crsInfo` above, so a failure here can only be a
        // canonical round-trip failure (e.g. an authority-less CRS whose exported WKT does not
        // re-parse) — never a bad target. Degrade to unchanged rather than raise, so the
        // never-error invariant holds for every source-side failure mode. A try/catch that does
        // not throw is zero-cost on the JVM, so this costs nothing on the hot path.
        //
        // NonFatal, not Throwable: a fatal error (OutOfMemoryError, StackOverflowError,
        // InterruptedException) must propagate and fail the task. Swallowing one here would
        // silently emit un-reprojected rows — a data-correctness failure disguised as success —
        // and would make an interrupted task ignore cancellation. This matches the `Try`-based
        // source-CRS degrade above, which is also NonFatal-only.
        val plan = try {
            SpatialRefOps.transformPlan(srcInfo.canonical, dstInfo.canonical)
        } catch {
            case NonFatal(_) => return CrsOutcome.Unchanged(geom)
        }
        val gProj = if (plan.identity) g else transformWithCachedCT(g, plan.transformation)

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
