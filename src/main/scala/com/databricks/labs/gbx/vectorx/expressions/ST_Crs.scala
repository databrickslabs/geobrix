package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/** Returns the canonical CRS string (e.g. ``EPSG:4326``, ``ESRI:54008``) for a
  * geometry's embedded SRID, or null.
  *
  * Reads the integer SRID from EWKB / EWKT; classifies it via the authoritative
  * PROJ code sets (EPSG or ESRI) using `SpatialRefOps.resolveCrs` and
  * `SpatialRefOps.crsToCanonical`. Returns null for plain WKB/WKT (SRID=0),
  * null inputs, and unresolvable SRIDs (never-error: an unrecognized code
  * returns null, not an exception).
  *
  * Registered as: `gbx_st_crs`
  */
case class ST_Crs(geom: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = ST_Crs.name
    override def replacement: Expression = invoke(ST_Crs)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))
}

object ST_Crs extends WithExpressionInfo {

    def eval(geom: Any): UTF8String = {
        if (geom == null) return null
        val g = CrsExpressionUtil.parseGeom(geom)
        if (g == null) return null
        val srid = g.getSRID
        if (srid <= 0) return null
        val sr = Try(SpatialRefOps.resolveCrs(srid.toString)).getOrElse(null)
        if (sr == null) return null
        try {
            val canonical = SpatialRefOps.crsToCanonical(sr)
            if (canonical == null) null else UTF8String.fromString(canonical)
        } finally {
            sr.delete()
        }
    }

    override def name: String = "gbx_st_crs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new ST_Crs(c(0))

    override def usageArgs: String = "geom"
    override def description: String =
        "Returns the canonical CRS string (e.g. EPSG:4326, ESRI:54008) for a geometry's embedded SRID, or null."
}
