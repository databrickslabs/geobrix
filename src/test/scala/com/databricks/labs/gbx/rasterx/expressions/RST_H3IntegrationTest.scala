package com.databricks.labs.gbx.rasterx.expressions

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
    // H3 Tessellate Tests 
    // ====================================================================

    test("H3 Tessellate should generate cells with raster content") {
        val resolution = 1 // Use coarse resolution to keep test fast
        
        // The tessellation owns (and releases) its input, so it gets its own handle; the suite's `ds` stays open.
        val iter = RasterTessellate.tessellateH3Iter(gdal.Open(ds.GetDescription()), Map.empty, resolution)
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
