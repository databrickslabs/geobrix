package com.databricks.labs.gbx.rasterx.expressions.agg

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite

/** `gbx_rst_bng_rasterize_agg`'s trailing `out_crs` on the heavy tier: NULL is accepted, a non-NULL value is refused
  * with a `Fix:`, at analysis for a literal and at update for a column. The light tier refuses it with the same text
  * (`test/pyrx/test_bng_agg_arity.py` holds the two texts equal). */
class RST_BNG_OutCrsTest extends AnyFunSuite {

    private val twelve: Seq[Expression] = Seq.fill(12)(Literal.create(null, StringType))
    private val refusal = RST_BNG_RasterizeAgg.OutCrsRefusal

    test("the builder takes 12 arguments, or 13 with a NULL out_crs") {
        val build = RST_BNG_RasterizeAgg.builder()
        assert(build(twelve).isInstanceOf[RST_BNG_RasterizeAgg])
        assert(build(twelve :+ Literal.create(null, StringType)).isInstanceOf[RST_BNG_RasterizeAgg])
    }

    test("a literal non-NULL out_crs is refused at analysis, with the reason and a Fix") {
        val e = intercept[IllegalArgumentException](RST_BNG_RasterizeAgg.builder()(twelve :+ Literal("EPSG:4326")))
        assert(e.getMessage == refusal, e.getMessage)
        assert(e.getMessage.contains("out_crs is not supported") && e.getMessage.contains("Fix:"))
    }

    test("a non-NULL out_crs on a row is refused at update, before any cell is read") {
        val agg = RST_BNG_RasterizeAgg(
            cellidExpr = Literal.create(null, StringType),
            valueExpr = Literal(0.0),
            outSridExpr = Literal(27700),
            pixelSizeExpr = Literal(1.0),
            xminExpr = Literal(0.0),
            yminExpr = Literal(0.0),
            xmaxExpr = Literal(10.0),
            ymaxExpr = Literal(10.0),
            widthExpr = Literal(10),
            heightExpr = Literal(10),
            modeExpr = Literal("centroids"),
            kringPadExpr = Literal(0),
            outCrsExpr = Literal("EPSG:4326")
        )
        val input: InternalRow = new GenericInternalRow(0)
        val e = intercept[IllegalArgumentException](agg.update(agg.createAggregationBuffer(), input))
        assert(e.getMessage == refusal, e.getMessage)
    }
}
