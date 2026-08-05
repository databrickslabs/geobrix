package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.vectorx.expressions.CrsExpressionUtil.{CrsOutcome, encodeAdaptive, encodeBinary}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/** Stamps a CRS on a geometry without reprojecting.
  *
  * Assigns an EPSG or ESRI integer SRID to the geometry. A CRS with no integer
  * authority code raises, because a geometry can only store an integer SRID —
  * that covers authority-less definitions (raw WKT / PROJ4) and resolvable CRSes
  * whose authority code is non-numeric (`OGC:CRS84`, `IGNF:LAMB93`).
  *
  * Encoding contract — two layers:
  * - The SQL surface (this expression) always returns BINARY (EWKB), whichever
  *   encoding the geometry argument arrived in. One function has one declared
  *   return type: an input-dependent type cannot be used in a fixed-schema view,
  *   and BINARY/WKB is how the rest of `gbx_st_*` and the built-in `st_*`
  *   functions exchange geometries.
  * - The Scala core [[ST_SetCrs.eval]] stays medium-preserving (WKB/EWKB in →
  *   EWKB out; WKT/EWKT in → EWKT out) for callers working in text.
  *
  * Registered as: `gbx_st_setcrs`
  */
case class ST_SetCrs(geom: Expression, crs: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom, crs)
    // Fixed BINARY return type — the SQL surface always hands back EWKB.
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = ST_SetCrs.name
    override def replacement: Expression = invoke(ST_SetCrs, methodName = "evalSql")
    // Pin the crs argument to StringType so integer SRIDs (e.g. 32633) are coerced by Catalyst.
    override def inputTypes: Seq[DataType] = Seq(geom.dataType, StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))
}

object ST_SetCrs extends WithExpressionInfo {

    /** Compute the SRID stamp without committing to an output encoding.
      *
      * @param geom  WKB/EWKB bytes or WKT/EWKT string (or UTF8String).
      * @param crs   CRS specification (EPSG/ESRI authority string, integer code, WKT, PROJ4).
      *              A CRS with no integer authority code raises IllegalArgumentException.
      */
    private def core(geom: Any, crs: UTF8String): CrsOutcome = {
        if (geom == null || crs == null) return CrsOutcome.NullOut
        val g = CrsExpressionUtil.parseGeom(geom)
        if (g == null) return CrsOutcome.NullOut

        val sr = SpatialRefOps.resolveCrs(crs.toString)
        try {
            // Same authority rule as SpatialRefOps.crsInfo / ST_TransformCrs — one implementation,
            // so st_setcrs and st_transformcrs can never disagree about which CRSes are stampable.
            SpatialRefOps.authoritySridOf(sr) match {
                case None =>
                    throw new IllegalArgumentException(
                        "st_setcrs: cannot stamp an authority-less CRS onto a geometry — " +
                        "a geometry SRID must be an EPSG or ESRI integer code. " +
                        s"Resolved CRS: ${sr.ExportToWkt().take(120)}"
                    )
                case some => CrsOutcome.Geom(g, some)
            }
        } finally {
            sr.delete()
        }
    }

    /** Medium-preserving Scala core: Array[Byte] (EWKB) in → EWKB out, UTF8String (EWKT) → EWKT. */
    def eval(geom: Any, crs: UTF8String): Any =
        encodeAdaptive(core(geom, crs), CrsExpressionUtil.isText(geom))

    /** SQL surface: always BINARY (EWKB), regardless of the input encoding. */
    def evalSql(geom: Any, crs: UTF8String): Array[Byte] = encodeBinary(core(geom, crs))

    override def name: String = "gbx_st_setcrs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new ST_SetCrs(c(0), c(1))

    override def usageArgs: String = "geom, crs"
    override def description: String =
        "Stamps a CRS on a geometry without reprojecting; returns EWKB. Authority-less CRS raises."
}
