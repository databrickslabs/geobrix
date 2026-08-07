package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

/** Catalyst expression: fills a geometry with custom-grid cell IDs at the given resolution.
  *
  * Cell inclusion uses **centroid-containment** semantics -- a cell is included if and only if
  * its center point falls strictly inside (or on the boundary of) the input geometry,
  * as determined by JTS `Geometry.contains(centroid)`.
  *
  * Arguments: geomExpr (BINARY WKB or STRING WKT), gridExpr (grid-spec STRUCT), resolutionExpr (INT or LONG).
  *
  * Returns: ARRAY<BIGINT> of cell IDs.
  */
case class Custom_Polyfill(
    geomExpr:       Expression,
    gridExpr:       Expression,
    resolutionExpr: Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] = Seq(geomExpr, gridExpr, resolutionExpr)
    override def dataType: DataType        = ArrayType(LongType, containsNull = false)
    override def nullable: Boolean         = true
    override def foldable: Boolean         = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val geomVal = geomExpr.eval(input)
        if (geomVal == null) return null

        val gridVal = gridExpr.eval(input)
        if (gridVal == null) return null

        val geom = Custom_PointAsCell.decodeGeom(geomVal)
        val sys  = Custom_GridSpec.systemFromRow(gridVal.asInstanceOf[InternalRow])
        val res  = Custom_GridSpec.asInt(resolutionExpr.eval(input), "resolution")

        val cells: Seq[Long] = sys.polyfill(geom, res)
        ArrayData.toArrayData(cells.toArray)
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name gbx_custom_polyfill, 3-arg builder. */
object Custom_Polyfill extends WithExpressionInfo {

    override def name: String = "gbx_custom_polyfill"

    override def builder(): FunctionBuilder = {
        case c if c.length == 3 => Custom_Polyfill(c(0), c(1), c(2))
        case c => throw new IllegalArgumentException(
            s"gbx_custom_polyfill requires 3 arguments; got ${c.length}")
    }

}
