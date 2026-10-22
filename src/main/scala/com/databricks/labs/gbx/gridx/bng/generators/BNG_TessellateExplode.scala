package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Generator that tessellates a geometry into BNG cells and explodes one row per chip. Arguments: geom, resolution, keepCoreGeom.
  *
  * Output columns: `cellid` STRING, `core` BOOLEAN, `chip`. The chip type follows the input geometry type:
  * BINARY (WKB) for a BINARY geometry, STRING (WKT) for a STRING geometry. With `keepCoreGeom` false a core
  * cell's chip is NULL; border chips always carry their geometry. */
case class BNG_TessellateExplode(
    geom: Expression,
    resolution: Expression,
    keepCoreGeom: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(geom, resolution, keepCoreGeom)

    /** The chip is encoded like the input geometry: WKT text for a STRING input, WKB bytes otherwise. */
    private def chipType: DataType = geom.dataType match {
        case StringType => StringType
        case _          => BinaryType
    }

    override def elementSchema: StructType = StructType(
      Seq(
        StructField("cellid", StringType),
        StructField("core", BooleanType),
        StructField("chip", chipType)
      )
    )
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2))

    /**
      * Type-wise differences in evaluation are only present on the input data
      * conversion to a [[Geometry]]. The rest of the evaluation is agnostic to
      * the input data type. The evaluation generates a set of core indices that
      * are fully contained by the input [[Geometry]] and a set of border
      * indices that are partially contained by the input [[Geometry]].
      *
      * @param input
      *   Struct containing a geometry and a resolution.
      * @return
      *   A set of serialized chips. This set will be used to generate new rows
      *   of data.
      */
    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val geomRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val keepGeomRaw = keepCoreGeom.eval(input)
        if (geomRaw == null || resolutionRaw == null || keepGeomRaw == null) {
            return Seq.empty
        }
        val resolutionVal = resolution.dataType match {
            case StringType  => BNG.resolutionMap(resolutionRaw.asInstanceOf[UTF8String].toString)
            case IntegerType => resolutionRaw.asInstanceOf[Int]
        }
        val keepCoreGeomVal = keepGeomRaw.asInstanceOf[Boolean]

        GridErrorHandler.safeEval[IterableOnce[InternalRow]](Iterator.empty) {
            val geometryVal = geom.dataType match {
                case StringType => JTS.fromWKT(geomRaw.asInstanceOf[UTF8String].toString)
                case BinaryType => JTS.fromWKB(geomRaw.asInstanceOf[Array[Byte]])
            }
            val wktChips = geom.dataType == StringType
            BNG
                .tessellate(geometryVal, resolutionVal, keepCoreGeomVal)
                .map(c => {
                    val cellId = UTF8String.fromString(BNG.format(c._1))
                    // tessellate already leaves core chips without a geometry when keepCoreGeom is false;
                    // border chips always carry theirs.
                    val chip =
                        if (c._3 == null) null
                        else if (wktChips) UTF8String.fromString(JTS.toWKT(c._3))
                        else JTS.toWKB(c._3)
                    InternalRow.fromSeq(Seq(cellId, c._2, chip))
                })
        }

    }

}

/** Companion: SQL name gbx_bng_tessellateexplode, builder. */
object BNG_TessellateExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_tessellateexplode"
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new BNG_TessellateExplode(c(0), c(1), c(2))


}
