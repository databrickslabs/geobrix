package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Catalyst expression: given a geometry (WKB or WKT), a grid-spec struct, and a resolution,
  * returns the Long cell ID in the custom grid that contains the point.
  *
  * Arguments: pointExpr (BINARY or STRING), gridExpr (STRUCT), resolutionExpr (INT or LONG).
  */
case class Custom_PointAsCell(
    pointExpr:      Expression,
    gridExpr:       Expression,
    resolutionExpr: Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] = Seq(pointExpr, gridExpr, resolutionExpr)
    override def dataType: DataType  = LongType
    override def nullable: Boolean   = true
    override def foldable: Boolean   = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val pointVal = pointExpr.eval(input)
        if (pointVal == null) return null

        val gridVal = gridExpr.eval(input)
        if (gridVal == null) return null

        val geom: Geometry = Custom_PointAsCell.decodeGeom(pointVal)
        val sys  = Custom_GridSpec.systemFromRow(gridVal.asInstanceOf[InternalRow])
        val res  = Custom_GridSpec.asInt(resolutionExpr.eval(input), "resolution")
        val c    = geom.getCoordinate

        sys.pointToCellID(c.x, c.y, res)
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_custom_pointascell, 3-arg builder. */
object Custom_PointAsCell extends WithExpressionInfo {

    override def name: String = "gbx_custom_pointascell"

    override def builder(): FunctionBuilder = {
        case c if c.length == 3 => Custom_PointAsCell(c(0), c(1), c(2))
        case c => throw new IllegalArgumentException(
            s"gbx_custom_pointascell requires 3 arguments; got ${c.length}")
    }

    private[custom] def decodeGeom(v: Any) = v match {
        case b: Array[Byte] => JTS.fromWKB(b)
        case s: UTF8String  => JTS.fromWKT(s.toString)
        case s: String      => JTS.fromWKT(s)
        case o              => throw new IllegalArgumentException(
            s"gbx_custom: expected BINARY or STRING geometry; got ${o.getClass.getName}")
    }

}
