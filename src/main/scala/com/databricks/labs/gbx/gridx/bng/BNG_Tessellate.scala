package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that tessellates a geometry into BNG cells (array of chip structs). Arguments: geom, resolution, keepCoreGeom. */
case class BNG_Tessellate(
    geom: Expression,
    resolution: Expression,
    keepCoreGeom: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution, keepCoreGeom)
    override def dataType: DataType = ArrayType(BNG.cellType(StringType))
    override def nullable: Boolean = true
    override def prettyName: String = BNG_Tessellate.name
    override def replacement: Expression = invoke(BNG_Tessellate)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_bng_tessellate, builder, and eval. */
object BNG_Tessellate extends WithExpressionInfo {

    /** Build the chip rows from the tessellation iterator.
      *
      * The chip geometry is null for core cells when keepCoreGeom is false; JTS.toWKB
      * crashes on a null geometry ("Unknown Geometry type"), so emit a null WKB for
      * those (the struct's geom field is nullable). The catalyst dataType is an
      * ArrayType, so eval MUST return ArrayData — returning a raw Array[InternalRow]
      * ClassCasts in the explode / projection path on collect (cf. Quadbin_Tessellate).
      */
    private def toRows(chips: Iterator[(String, Boolean, Geometry)]): ArrayData = {
        val rows = chips
            .map(c =>
                InternalRow.fromSeq(
                  Seq(
                    UTF8String.fromString(c._1),
                    c._2,
                    if (c._3 == null) null else JTS.toWKB(c._3)
                  )
                )
            )
            .toArray
        ArrayData.toArrayData(rows)
    }

    def eval(wkt: UTF8String, resolution: Int, keepCoreGeom: Boolean): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null)(toRows(executeWKT(wkt.toString, resolution, keepCoreGeom)))

    def eval(wkb: Array[Byte], resolution: Int, keepCoreGeom: Boolean): ArrayData =
        GridErrorHandler.safeEval[ArrayData](null)(toRows(executeWKB(wkb, resolution, keepCoreGeom)))

    def eval(wkt: UTF8String, resolution: UTF8String, keepCoreGeom: Boolean): ArrayData = {
        val res = BNG.resolutionMap(resolution.toString) // PARAMETER: raises on bad resolution
        GridErrorHandler.safeEval[ArrayData](null)(toRows(executeWKT(wkt.toString, res, keepCoreGeom)))
    }

    def eval(wkb: Array[Byte], resolution: UTF8String, keepCoreGeom: Boolean): ArrayData = {
        val res = BNG.resolutionMap(resolution.toString) // PARAMETER: raises on bad resolution
        GridErrorHandler.safeEval[ArrayData](null)(toRows(executeWKB(wkb, res, keepCoreGeom)))
    }

    def executeWKT(wkt: String, resolution: Int, keepCoreGeom: Boolean): Iterator[(String, Boolean, Geometry)] = {
        val geometry: Geometry = JTS.fromWKT(wkt)
        BNG.tessellate(geometry, resolution, keepCoreGeom).map(c => c.copy(_1 = BNG.format(c._1)))
    }

    def executeWKB(bytes: Array[Byte], i: Int, bool: Boolean): Iterator[(String, Boolean, Geometry)] = {
        val geometry: Geometry = JTS.fromWKB(bytes)
        BNG.tessellate(geometry, i, bool).map(c => c.copy(_1 = BNG.format(c._1)))
    }

    override def name: String = "gbx_bng_tessellate"

    /** Accept the canonical 2-arg form gbx_bng_tessellate(geom, resolution) — defaulting
      * keepCoreGeom to false (core chips carry a null geom, materialized downstream) — as
      * well as the explicit 3-arg form. The 2-arg form is the registered signature and the
      * one the light tier exposes; only requiring 3 args threw IndexOutOfBoundsException. */
    override def builder(): FunctionBuilder = {
        case Seq(g, r)    => new BNG_Tessellate(g, r, Literal(false))
        case Seq(g, r, k) => new BNG_Tessellate(g, r, k)
        case other        =>
            throw new IllegalArgumentException(
              s"$name expects 2 or 3 arguments (geom, resolution[, keepCoreGeom]); got ${other.length}"
            )
    }


}
