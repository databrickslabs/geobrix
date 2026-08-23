package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.functions._
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants

import java.nio.file.{Files, Path}
import scala.util.Try

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
  * BNG fixture (Ruling I): a small EPSG:27700 GTiff over London (easting 530000,
  * northing 182000, 8x8 pixels at 250m) is written to a temp directory in beforeAll()
  * so the BNG test yields >=1 tile and the gridSystem assertion is non-vacuous.
  */
class RST_Tessellate_GridSystemMetadataTest extends PlanTest with SilentSparkSession {

    // Temp dir for the London EPSG:27700 GTiff fixture (BNG tests only).
    private var bngTifDir: Path = _

    override def beforeAll(): Unit = {
        super.beforeAll()
        functions.register(spark)
        // Initialize GDAL via the guarded GDALManager so we can write the fixture GTiff.
        // Idempotent: if already initialized from a prior suite, this is a no-op.
        GDALManager.init(ExpressionConfig(spark))
        bngTifDir = Files.createTempDirectory("gbx-test-bng27700")
        writeLondon27700Tif(bngTifDir.resolve("london27700.tif").toString)
    }

    override def afterAll(): Unit = {
        Try {
            if (bngTifDir != null) {
                val dir = bngTifDir.toFile
                dir.listFiles().foreach(_.delete())
                dir.delete()
            }
        }
        super.afterAll()
    }

    /**
      * Write a small EPSG:27700 GTiff over central London to `path`.
      * Extent: easting 530000..532000, northing 180000..182000 (2 km x 2 km, London TQ area).
      * 8x8 pixels at 250 m; band 1 filled with 1..64 (all valid, no NoData).
      * At BNG resolution 3 (1 km cells) this raster overlaps ~4 BNG cells => >=1 tile.
      */
    private def writeLondon27700Tif(path: String): Unit = {
        val memDrv  = gdal.GetDriverByName("MEM")
        val tifDrv  = gdal.GetDriverByName("GTiff")
        val memDs   = memDrv.Create("", 8, 8, 1, gdalconstConstants.GDT_Float64)
        // top-left: easting 530000, northing 182000; 250 m pixels, 8x8 -> 2 km x 2 km
        memDs.SetGeoTransform(Array(530000.0, 250.0, 0.0, 182000.0, 0.0, -250.0))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(27700)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        memDs.SetProjection(sr.ExportToWkt())
        memDs.GetRasterBand(1).WriteRaster(0, 0, 8, 8, (1 to 64).map(_.toDouble).toArray)
        memDs.FlushCache()
        tifDrv.CreateCopy(path, memDs)
        memDs.delete()
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

    // ---------------------------------------------------------------------------
    // BNG tessellate — covering + centroid both carry gridSystem="bng"
    // Uses a synthesized EPSG:27700 London GTiff (see writeLondon27700Tif) to
    // guarantee >=1 output tile and a non-vacuous assertion (Ruling I).
    // ---------------------------------------------------------------------------

    test("rst_bng_tessellate covering: every output tile has metadata[gridSystem]='bng'") {
        // Load the London EPSG:27700 fixture — NOT the MODIS Africa/Asia raster.
        val df = spark.read.format("binaryFile").load(bngTifDir.toString)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        // resolution 3 = 1 km BNG cells; the 2 km x 2 km London fixture overlaps ~4 cells.
        val rows = df
            .withColumn("tile", rst_bng_tessellate(col("raster"), lit(3)))
            .select("tile.metadata")
            .collect()
        // Non-vacuous assertion: the London 27700 fixture MUST yield tiles (Ruling I).
        assert(rows.nonEmpty,
            "rst_bng_tessellate covering with a London EPSG:27700 fixture must yield >=1 tile; got 0 rows")
        rows.foreach { row =>
            val md = row.getAs[Map[String, String]](0)
            assert(md != null && md.get("gridSystem") == Some("bng"),
                s"bng tessellate covering tile must have gridSystem='bng'; got $md")
        }
    }

    test("rst_bng_tessellate centroid: every output tile has metadata[gridSystem]='bng'") {
        val df = spark.read.format("binaryFile").load(bngTifDir.toString)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val rows = df
            .withColumn("tile", rst_bng_tessellate(col("raster"), lit(3), "centroid"))
            .select("tile.metadata")
            .collect()
        assert(rows.nonEmpty,
            "rst_bng_tessellate centroid with a London EPSG:27700 fixture must yield >=1 tile; got 0 rows")
        rows.foreach { row =>
            val md = row.getAs[Map[String, String]](0)
            assert(md != null && md.get("gridSystem") == Some("bng"),
                s"bng tessellate centroid tile must have gridSystem='bng'; got $md")
        }
    }

}
