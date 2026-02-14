package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Expression that converts a geometry (WKT/WKB) point to a BNG cell ID string. Arguments: geom, resolution.
  * Resolution must be a BNG resolution: Int index (1–6 or negative for quadrant) or String from resolutionMap (e.g. "1km", "100m"). */
case class BNG_PointAsCell(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = StringType
    override def nullable: Boolean = true
    override def prettyName: String = BNG_PointAsCell.name
    override def replacement: Expression = invoke(BNG_PointAsCell)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_bng_pointascell, builder, and eval. */
object BNG_PointAsCell extends WithExpressionInfo {

    def eval(wkt: UTF8String, resolution: Int): UTF8String = {
        val cellID = executeWKT(wkt.toString, resolution)
        UTF8String.fromString(cellID)
    }

    def eval(wkt: UTF8String, resolution: UTF8String): UTF8String = {
        val res = BNG.resolutionMap(resolution.toString)
        val cellID = executeWKT(wkt.toString, res)
        UTF8String.fromString(cellID)
    }

    def eval(wkb: Array[Byte], resolution: Int): UTF8String = {
        val cellID = executeWKB(wkb, resolution)
        UTF8String.fromString(cellID)
    }

    def eval(wkb: Array[Byte], resolution: UTF8String): UTF8String = {
        val res = BNG.resolutionMap(resolution.toString)
        val cellID = executeWKB(wkb, res)
        UTF8String.fromString(cellID)
    }

    def executeWKT(wkt: String, resolution: Int): String = {
        val geometry = JTS.fromWKT(wkt)
        val cellID = BNG.pointToCellID(geometry.getCentroid.getX, geometry.getCentroid.getY, resolution)
        BNG.format(cellID)
    }

    def executeWKB(bytes: Array[Byte], resolution: Int): String = {
        val geometry = JTS.fromWKB(bytes)
        val cellID = BNG.pointToCellID(geometry.getCentroid.getX, geometry.getCentroid.getY, resolution)
        BNG.format(cellID)
    }

    override def name: String = "gbx_bng_pointascell"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_PointAsCell(c(0), c(1))
}
