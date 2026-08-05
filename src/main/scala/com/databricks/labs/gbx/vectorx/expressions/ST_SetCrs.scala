package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/** Stamps a CRS on a geometry without reprojecting (medium-preserving).
  *
  * Assigns an EPSG or ESRI integer SRID to the geometry. Authority-less CRS
  * (WKT / PROJ4 strings that resolve to no EPSG or ESRI authority) raise
  * because a geometry can only store an integer SRID.
  *
  * Encoding contract:
  * - Binary (WKB / EWKB) in → EWKB out (SRID embedded).
  * - Text (WKT / EWKT) in → EWKT out (`SRID=n;WKT`).
  *
  * The SQL surface returns the type that matches the geom input. Spark sees
  * `dataType = geom.dataType` so plan resolution is consistent.
  *
  * Registered as: `gbx_st_setcrs`
  */
case class ST_SetCrs(geom: Expression, crs: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(geom, crs)
    // Return type follows the geometry input medium (binary stays binary, text stays text).
    override def dataType: DataType = geom.dataType
    override def nullable: Boolean = true
    override def prettyName: String = ST_SetCrs.name
    override def replacement: Expression = invoke(ST_SetCrs)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))
}

object ST_SetCrs extends WithExpressionInfo {

    /** Stamp ``crs`` on ``geom`` without reprojecting.
      *
      * @param geom  WKB/EWKB bytes or WKT/EWKT string (or UTF8String).
      * @param crs   CRS specification (EPSG/ESRI authority string, integer code, WKT, PROJ4).
      *              Authority-less CRS raises IllegalArgumentException.
      * @return      Same encoding as input: Array[Byte] (EWKB) or UTF8String (EWKT).
      */
    def eval(geom: Any, crs: UTF8String): Any = {
        if (geom == null || crs == null) return null
        val text = CrsExpressionUtil.isText(geom)
        val g = CrsExpressionUtil.parseGeom(geom)
        if (g == null) return null

        val sr = SpatialRefOps.resolveCrs(crs.toString)
        try {
            val authName = sr.GetAuthorityName(null)
            val authCode = sr.GetAuthorityCode(null)
            val hasAuthority = authName != null && authName.nonEmpty &&
                authCode != null && authCode.nonEmpty
            if (!hasAuthority) {
                throw new IllegalArgumentException(
                    "st_setcrs: cannot stamp an authority-less CRS onto a geometry — " +
                    "a geometry SRID must be an EPSG or ESRI integer code. " +
                    s"Resolved CRS: ${sr.ExportToWkt().take(120)}"
                )
            }
            val srid = authCode.toInt
            g.setSRID(srid)
            if (text) UTF8String.fromString(JTS.toEWKT(g))
            else JTS.toEWKB3(g)
        } finally {
            sr.delete()
        }
    }

    override def name: String = "gbx_st_setcrs"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new ST_SetCrs(c(0), c(1))

    override def usageArgs: String = "geom, crs"
    override def description: String =
        "Stamps a CRS on a geometry without reprojecting. Authority-less CRS raises."
}
