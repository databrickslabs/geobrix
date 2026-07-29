package com.databricks.labs.gbx.rasterx.expressions.agg

import org.scalatest.funsuite.AnyFunSuite

class RST_GridRasterizeAggTest extends AnyFunSuite {
    test("quadbin rasterize_agg canonical name + 12-arg builder") {
        assert(RST_Quadbin_RasterizeAgg.name == "gbx_rst_quadbin_rasterize_agg")
        import org.apache.spark.sql.catalyst.expressions.Literal
        val args = (0 until 12).map(i => Literal(i)).toSeq
        assert(RST_Quadbin_RasterizeAgg.builder()(args).isInstanceOf[RST_Quadbin_RasterizeAgg])
    }

    test("bng rasterize_agg canonical name + string cellid parse") {
        assert(RST_BNG_RasterizeAgg.name == "gbx_rst_bng_rasterize_agg")
        import org.apache.spark.sql.catalyst.expressions.Literal
        val args = (0 until 12).map(i => Literal(i)).toSeq
        assert(RST_BNG_RasterizeAgg.builder()(args).isInstanceOf[RST_BNG_RasterizeAgg])
    }
}
