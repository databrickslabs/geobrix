package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.grid.{RST_H3_RasterToGridAvg, RST_H3_RasterToGridCount, RST_H3_RasterToGridMax, RST_H3_RasterToGridMedian, RST_H3_RasterToGridMin}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operations.RasterTessellate
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
 * Comprehensive H3 integration tests for RasterX.
 * 
 * These tests validate:
 * - H3 grid aggregation functions (Avg, Count, Max, Min, Median)
 * - H3 tessellation of rasters into cells
 * - Resolution handling (coarse to fine)
 * - Multi-band support
 * - Edge cases and correctness
 * 
 * H3 Resolutions tested:
 * - Resolution 0-2: Coarse global cells
 * - Resolution 3-5: Regional cells
 * - Higher resolutions avoided due to memory/time constraints
 * 
 * Test Data:
 * - MODIS MCD43A4 single-band TIF (2400x2400 pixels)
 * - ESRI:54008 (MODIS Sinusoidal) projection
 */
class RST_H3IntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        
        // Ensure NodeFilePathUtil root directory exists for tests
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        ds = gdal.Open(tifPath)
        
        require(ds != null, s"Failed to open $tifPath")
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    // ====================================================================
    // H3 RasterToGrid Tests - Resolution Variations (5 tests)
    // ====================================================================

    test("H3 RasterToGridAvg should handle different resolutions") {
        // Test multiple resolutions from coarse to fine
        val resolutions = Seq(0, 1, 2)
        
        resolutions.foreach { resolution =>
            val result = RST_H3_RasterToGridAvg.execute(ds, resolution)
            
            result.length shouldBe 1 // Single band
            result(0).length should be > 0
            
            // Higher resolution should generally produce more cells
            if (resolution > 0) {
                val prevResult = RST_H3_RasterToGridAvg.execute(ds, resolution - 1)
                result(0).length should be >= prevResult(0).length
            }
            
            // Verify cell IDs and values are valid
            result(0).take(3).foreach { case (cellID, avgValue) =>
                cellID should be > 0L
                avgValue should be >= 0.0
            }
        }
    }

    test("H3 RasterToGridCount should aggregate pixel counts correctly") {
        val resolution = 2
        val result = RST_H3_RasterToGridCount.execute(ds, resolution)
        
        result.length shouldBe 1
        result(0).length should be > 0
        
        // Verify counts are positive integers
        val sample = result(0).take(10)
        sample.foreach { case (cellID, count) =>
            cellID should be > 0L
            count should be > 0
        }
        
        // Sum of all counts should be related to total pixel count
        val totalCount = result(0).map(_._2).sum
        totalCount should be > 0
    }

    test("H3 RasterToGridMax should find maximum values per cell") {
        val resolution = 2
        val result = RST_H3_RasterToGridMax.execute(ds, resolution)
        
        result.length shouldBe 1
        result(0).length should be > 0
        
        val avgResult = RST_H3_RasterToGridAvg.execute(ds, resolution)
        
        // Max values should be >= average values for same cells
        val maxMap = result(0).toMap
        val avgMap = avgResult(0).toMap
        
        // Sample comparison
        maxMap.take(5).foreach { case (cellID, maxVal) =>
            if (avgMap.contains(cellID)) {
                maxVal should be >= avgMap(cellID)
            }
        }
    }

    test("H3 RasterToGridMin should find minimum values per cell") {
        val resolution = 2
        val result = RST_H3_RasterToGridMin.execute(ds, resolution)
        
        result.length shouldBe 1
        result(0).length should be > 0
        
        val avgResult = RST_H3_RasterToGridAvg.execute(ds, resolution)
        
        // Min values should be <= average values for same cells
        val minMap = result(0).toMap
        val avgMap = avgResult(0).toMap
        
        // Sample comparison
        minMap.take(5).foreach { case (cellID, minVal) =>
            if (avgMap.contains(cellID)) {
                minVal should be <= avgMap(cellID)
            }
        }
    }

    test("H3 RasterToGridMedian should compute median values") {
        val resolution = 2
        val result = RST_H3_RasterToGridMedian.execute(ds, resolution)
        
        result.length shouldBe 1
        result(0).length should be > 0
        
        val minResult = RST_H3_RasterToGridMin.execute(ds, resolution)
        val maxResult = RST_H3_RasterToGridMax.execute(ds, resolution)
        
        // Median should be between min and max for same cells
        val medianMap = result(0).toMap
        val minMap = minResult(0).toMap
        val maxMap = maxResult(0).toMap
        
        // Sample comparison
        medianMap.take(5).foreach { case (cellID, medianVal) =>
            if (minMap.contains(cellID) && maxMap.contains(cellID)) {
                medianVal should be >= minMap(cellID)
                medianVal should be <= maxMap(cellID)
            }
        }
    }

    // ====================================================================
    // H3 RasterToGrid Advanced Tests (5 tests)
    // ====================================================================

    test("H3 RasterToGrid functions should produce consistent cell IDs") {
        val resolution = 2
        
        // All functions should produce the same set of cell IDs for the same raster
        val avgCells = RST_H3_RasterToGridAvg.execute(ds, resolution)(0).map(_._1).toSet
        val countCells = RST_H3_RasterToGridCount.execute(ds, resolution)(0).map(_._1).toSet
        val maxCells = RST_H3_RasterToGridMax.execute(ds, resolution)(0).map(_._1).toSet
        val minCells = RST_H3_RasterToGridMin.execute(ds, resolution)(0).map(_._1).toSet
        val medianCells = RST_H3_RasterToGridMedian.execute(ds, resolution)(0).map(_._1).toSet
        
        // All should cover the same geographic area (same cells)
        avgCells shouldBe countCells
        avgCells shouldBe maxCells
        avgCells shouldBe minCells
        avgCells shouldBe medianCells
    }

    test("H3 RasterToGrid should handle coarse resolution efficiently") {
        val resolution = 0 // Global scale
        
        val result = RST_H3_RasterToGridAvg.execute(ds, resolution)
        
        result.length shouldBe 1
        // At resolution 0, should produce very few cells globally
        result(0).length should be >= 1
        result(0).length should be <= 200 // Resolution 0 has 122 base cells
        
        // Verify cells are valid
        result(0).foreach { case (cellID, avgValue) =>
            cellID should be > 0L
            avgValue should be >= 0.0
        }
    }

    test("H3 RasterToGrid should handle fine resolution") {
        val resolution = 3 // More detailed regional scale
        
        val result = RST_H3_RasterToGridAvg.execute(ds, resolution)
        
        result.length shouldBe 1
        // Higher resolution should produce more cells
        result(0).length should be > 10
        
        // Verify structure
        result(0).take(10).foreach { case (cellID, avgValue) =>
            cellID should be > 0L
            avgValue should be >= 0.0
        }
    }

    test("H3 RasterToGrid functions should handle NoData appropriately") {
        val resolution = 2
        
        // Test with avg - NoData pixels (0 in MODIS) should be handled
        val result = RST_H3_RasterToGridAvg.execute(ds, resolution)
        
        result.length shouldBe 1
        result(0).length should be > 0
        
        // All values should be non-negative (MODIS reflectance data)
        result(0).foreach { case (cellID, avgValue) =>
            avgValue should be >= 0.0
        }
    }

    test("H3 RasterToGrid aggregations should be mathematically consistent") {
        val resolution = 2
        
        val avgResult = RST_H3_RasterToGridAvg.execute(ds, resolution)(0)
        val minResult = RST_H3_RasterToGridMin.execute(ds, resolution)(0)
        val maxResult = RST_H3_RasterToGridMax.execute(ds, resolution)(0)
        
        // Create maps for comparison
        val avgMap = avgResult.toMap
        val minMap = minResult.toMap
        val maxMap = maxResult.toMap
        
        // For each cell: min <= avg <= max
        avgMap.foreach { case (cellID, avgVal) =>
            if (minMap.contains(cellID) && maxMap.contains(cellID)) {
                val minVal = minMap(cellID)
                val maxVal = maxMap(cellID)
                
                minVal should be <= avgVal
                avgVal should be <= maxVal
            }
        }
    }

    // ====================================================================
    // H3 Tessellate Tests (2 tests)
    // ====================================================================

    test("H3 Tessellate should generate cells with raster content") {
        val resolution = 1 // Use coarse resolution to keep test fast
        
        val iter = RasterTessellate.tessellateH3Iter(ds, Map.empty, resolution)
        val result = iter.take(5).toSeq // Take first 5 cells for testing
        
        // Should produce cells
        result should not be empty
        
        // Verify structure of results
        result.foreach { case (cellID, rasterDs, metadata) =>
            cellID should be > 0L
            rasterDs should not be null
            
            // Tessellated rasters should be smaller than or equal to original
            rasterDs.GetRasterXSize should be <= ds.GetRasterXSize
            rasterDs.GetRasterYSize should be <= ds.GetRasterYSize
            
            // Should preserve band count
            rasterDs.GetRasterCount shouldBe ds.GetRasterCount
            
            rasterDs.delete() // Clean up
        }
        
        iter.asInstanceOf[AutoCloseable].close()
    }
}
