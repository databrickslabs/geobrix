package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.vectorx
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.ogr.ogrConstants
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

/** Spark-session test for [[ST_AsMvtPyramid]] — confirms the generator integrates with
 *  catalyst (function-registry lookup, multi-output schema, single input row → many output
 *  rows) and that the per-tile MVT bytes carry the configured layer name.
 *
 *  Pure-helper coverage (zoom guards, clip math) lives in `MvtPyramidBuilderTest`; this
 *  suite only exercises the Spark integration boundary.
 */
class ST_AsMvtPyramidTest extends PlanTest with SilentSparkSession {

    test("st_asmvt_pyramid emits one row per intersecting tile for a single polygon feature") {
        spark.sparkContext.setLogLevel("ERROR")
        TestL1.installAll(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val coords = Array(
            new Coordinate(-30.0, 10.0),
            new Coordinate(30.0, 10.0),
            new Coordinate(30.0, 20.0),
            new Coordinate(-30.0, 20.0),
            new Coordinate(-30.0, 10.0)
        )
        val poly = gf.createPolygon(coords)
        val df = spark.createDataFrame(Seq(
            (JTS.toWKB(poly), "region-a", 1L)
        )).toDF("geom_wkb", "name", "id")

        // Generator returns a single struct column "tile" wrapping (z, x, y, mvt_bytes).
        val out = df.select(
            call_function("gbx_st_asmvt_pyramid", col("geom_wkb"), struct(col("name"), col("id")), lit(2), lit(2), lit("regions")).alias("t")
        ).collect()

        assert(out.length == 2, s"expected 2 rows (z=2 spans 2 longitudinal tiles), got ${out.length}")
        out.foreach { row =>
            val tile = row.getStruct(0)
            assert(tile.getAs[Int]("z") == 2)
            assert(tile.getAs[Int]("x") >= 0)
            assert(tile.getAs[Int]("y") >= 0)
            val bytes = tile.getAs[Array[Byte]]("mvt_bytes")
            assert(bytes != null && bytes.nonEmpty)
            assert(new String(bytes, "UTF-8").contains("regions"))
        }
    }

    test("st_asmvt_pyramid encodes numeric attributes with native MVT value types") {
        spark.sparkContext.setLogLevel("ERROR")
        TestL1.installAll(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val coords = Array(
            new Coordinate(-30.0, 10.0),
            new Coordinate(30.0, 10.0),
            new Coordinate(30.0, 20.0),
            new Coordinate(-30.0, 20.0),
            new Coordinate(-30.0, 10.0)
        )
        val poly = gf.createPolygon(coords)
        val df = spark.createDataFrame(Seq(
            (JTS.toWKB(poly), 42, 3.5)
        )).toDF("geom_wkb", "pop", "h")

        val out = df.select(
            call_function("gbx_st_asmvt_pyramid", col("geom_wkb"), struct(col("pop"), col("h")), lit(2), lit(2), lit("regions")).alias("t")
        ).collect()
        assert(out.nonEmpty)

        val bytes = out.head.getStruct(0).getAs[Array[Byte]]("mvt_bytes")
        assert(bytes != null && bytes.nonEmpty)

        val types = MvtTestUtil.readFieldTypes(bytes, "regions", Seq("pop", "h"))
        assert(
          types("pop") == ogrConstants.OFTInteger || types("pop") == ogrConstants.OFTInteger64,
          s"expected pop to be integer, got OGR type ${types("pop")}"
        )
        assert(
          types("h") == ogrConstants.OFTReal,
          s"expected h to be real, got OGR type ${types("h")}"
        )
    }
}
