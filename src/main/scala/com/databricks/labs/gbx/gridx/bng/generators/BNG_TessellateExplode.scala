package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Generator that tessellates a geometry into BNG cells and explodes one row per chip. Arguments: geom, resolution[, keepCoreGeom].
  * `keepCoreGeom` is optional and defaults to false (as in `gbx_bng_tessellate` and the lightweight tier); a NULL flag is false too.
  *
  * Output columns: `cellid` STRING, `core` BOOLEAN, `chip` BINARY. The chip is WKB whether the geometry is given as WKB
  * or WKT, like the chips of `gbx_bng_tessellate`. With `keepCoreGeom` false a core cell's chip is NULL; border
  * chips always carry their geometry. */
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

    override def elementSchema: StructType = StructType(
      Seq(
        StructField("cellid", StringType),
        StructField("core", BooleanType),
        StructField("chip", BinaryType)
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
        if (geomRaw == null || resolutionRaw == null) {
            return Seq.empty
        }
        val resolutionVal = resolution.dataType match {
            case StringType  => BNG.resolutionMap(resolutionRaw.asInstanceOf[UTF8String].toString)
            case IntegerType => resolutionRaw.asInstanceOf[Int]
        }
        val keepCoreGeomVal = keepGeomRaw != null && keepGeomRaw.asInstanceOf[Boolean] // NULL is the default, false

        GridErrorHandler.safeEval[IterableOnce[InternalRow]](Iterator.empty) {
            val geometryVal = geom.dataType match {
                case StringType => JTS.fromWKT(geomRaw.asInstanceOf[UTF8String].toString)
                case BinaryType => JTS.fromWKB(geomRaw.asInstanceOf[Array[Byte]])
            }
            BNG
                .tessellate(geometryVal, resolutionVal, keepCoreGeomVal)
                .map(c => {
                    val cellId = UTF8String.fromString(BNG.format(c._1))
                    // tessellate already leaves core chips without a geometry when keepCoreGeom is false;
                    // border chips always carry theirs.
                    val chip = if (c._3 == null) null else JTS.toWKB(c._3)
                    InternalRow.fromSeq(Seq(cellId, c._2, chip))
                })
        }

    }

}

/** Companion: SQL name gbx_bng_tessellateexplode, builder. */
object BNG_TessellateExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_tessellateexplode"
    /** Accepts the two-argument form gbx_bng_tessellateexplode(geom, resolution) -- keepCoreGeom defaults to
      * false, the signature the lightweight tier exposes and `BNG_Tessellate` already accepts -- as well as the
      * explicit three-argument form. */
    override def builder(): FunctionBuilder = {
        case Seq(g, r)    => new BNG_TessellateExplode(g, r, Literal(false))
        case Seq(g, r, k) => new BNG_TessellateExplode(g, r, k)
        case other        =>
            throw new IllegalArgumentException(
              s"$name expects 2 or 3 arguments (geom, resolution[, keepCoreGeom]); got ${other.length}"
            )
    }


}
