package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.functions._
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SilentSparkSession

/**
  * Expression-level regression tests: every tessellate tile must carry
  * tile.metadata["gridSystem"] naming the DGGS.
  *
  * Tests use the Spark DataFrame API (PlanTest + SilentSparkSession) to exercise
  * the full expression eval path, including the .map { case (newCell, resDs, resMtd) =>
  * val augMtd = resMtd + ("gridSystem" -> ...) } block. A unit test of
  * RasterTessellate.*Iter would not catch this layer.
  *
  * RED-before: before the .map augmentation is added, rows.head["metadata"] has no
  * "gridSystem" key and the assertion fails. GREEN-after: the augmented resMtd is
  * passed to tileToRow, survives serialization, and is present on collect().
  *
  */
class RST_Tessellate_GridSystemMetadataTest extends PlanTest with SilentSparkSession {

    override def beforeAll(): Unit = {
        super.beforeAll()
        TestL1.installAll(spark)
        // Initialize GDAL via the guarded GDALManager. Idempotent: if already initialized from a prior suite, this is a no-op.
        GDALManager.init(ExpressionConfig(spark))
    }

    // ---------------------------------------------------------------------------
    // Shared fixture path for H3 / Quadbin tests (MODIS, Africa/Asia extent).
    // ---------------------------------------------------------------------------
    private lazy val modisPath: String =
        this.getClass.getResource("/modis/").toString

    // ---------------------------------------------------------------------------
    // H3 tessellate — covering + centroid both carry gridSystem="h3"
    // ---------------------------------------------------------------------------

    test("rst_h3_tessellate covering: every output tile has metadata[gridSystem]='h3'") {
        val df = spark.read.format("binaryFile").load(modisPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        // resolution 1 (coarse) keeps the test fast; all cells still carry the key.
        val rows = df
            .withColumn("tile", rst_h3_tessellate(col("raster"), lit(1)))
            .select("tile.metadata")
            .collect()
        assert(rows.nonEmpty, "rst_h3_tessellate covering must yield >=1 tile for MODIS input")
        rows.foreach { row =>
            val md = row.getAs[Map[String, String]](0)
            assert(md != null && md.get("gridSystem") == Some("h3"),
                s"h3 tessellate covering tile must have gridSystem='h3'; got $md")
        }
    }

    test("rst_h3_tessellate centroid: every output tile has metadata[gridSystem]='h3'") {
        val df = spark.read.format("binaryFile").load(modisPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val rows = df
            .withColumn("tile", rst_h3_tessellate(col("raster"), lit(1), "centroid"))
            .select("tile.metadata")
            .collect()
        assert(rows.nonEmpty, "rst_h3_tessellate centroid must yield >=1 tile for MODIS input")
        rows.foreach { row =>
            val md = row.getAs[Map[String, String]](0)
            assert(md != null && md.get("gridSystem") == Some("h3"),
                s"h3 tessellate centroid tile must have gridSystem='h3'; got $md")
        }
    }

    // ---------------------------------------------------------------------------
    // Quadbin tessellate — covering + centroid both carry gridSystem="quadbin"
    // ---------------------------------------------------------------------------

    test("rst_quadbin_tessellate covering: every output tile has metadata[gridSystem]='quadbin'") {
        val df = spark.read.format("binaryFile").load(modisPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        // zoom 3 is very coarse but produces rows; keeps test fast.
        val rows = df
            .withColumn("tile", rst_quadbin_tessellate(col("raster"), lit(3)))
            .select("tile.metadata")
            .collect()
        assert(rows.nonEmpty, "rst_quadbin_tessellate covering must yield >=1 tile for MODIS input")
        rows.foreach { row =>
            val md = row.getAs[Map[String, String]](0)
            assert(md != null && md.get("gridSystem") == Some("quadbin"),
                s"quadbin tessellate covering tile must have gridSystem='quadbin'; got $md")
        }
    }

    test("rst_quadbin_tessellate centroid: every output tile has metadata[gridSystem]='quadbin'") {
        val df = spark.read.format("binaryFile").load(modisPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val rows = df
            .withColumn("tile", rst_quadbin_tessellate(col("raster"), lit(3), "centroid"))
            .select("tile.metadata")
            .collect()
        assert(rows.nonEmpty, "rst_quadbin_tessellate centroid must yield >=1 tile for MODIS input")
        rows.foreach { row =>
            val md = row.getAs[Map[String, String]](0)
            assert(md != null && md.get("gridSystem") == Some("quadbin"),
                s"quadbin tessellate centroid tile must have gridSystem='quadbin'; got $md")
        }
    }

}
