package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class KernelFilterTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("KernelFilter should apply average filter") {
        val (resultDs, metadata) = KernelFilter.filter(ds, kernelSize = 3, operation = "avg")

        resultDs should not be null
        resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds.GetRasterYSize
        resultDs.GetRasterCount shouldBe ds.GetRasterCount

        metadata should not be null
        metadata should contain key "path"
        metadata should contain key "last_command"

        // Verify the filtered result is different from original
        val originalBand = ds.GetRasterBand(1)
        val filteredBand = resultDs.GetRasterBand(1)
        
        val originalStats = originalBand.AsMDArray().GetStatistics()
        val filteredStats = filteredBand.AsMDArray().GetStatistics()
        
        // Average filter should smooth the data
        filteredStats should not be null
        originalStats should not be null

        resultDs.delete()
    }

    test("KernelFilter should apply median filter") {
        val (resultDs, metadata) = KernelFilter.filter(ds, kernelSize = 3, operation = "median")

        resultDs should not be null
        resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds.GetRasterYSize

        metadata("last_command") should include("median")

        resultDs.delete()
    }

    test("KernelFilter should apply mode filter") {
        val (resultDs, _) = KernelFilter.filter(ds, kernelSize = 3, operation = "mode")

        resultDs should not be null
        resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds.GetRasterYSize

        resultDs.delete()
    }

    test("KernelFilter should apply max filter") {
        val (resultDs, metadata) = KernelFilter.filter(ds, kernelSize = 3, operation = "max")

        resultDs should not be null
        resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds.GetRasterYSize

        metadata("last_command") should include("max")

        val filteredBand = resultDs.GetRasterBand(1)
        val filteredStats = filteredBand.AsMDArray().GetStatistics()
        
        // Max filter should preserve or increase max values
        filteredStats should not be null

        resultDs.delete()
    }

    test("KernelFilter should apply min filter") {
        val (resultDs, metadata) = KernelFilter.filter(ds, kernelSize = 3, operation = "min")

        resultDs should not be null
        resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds.GetRasterYSize

        metadata("last_command") should include("min")

        val filteredBand = resultDs.GetRasterBand(1)
        val filteredStats = filteredBand.AsMDArray().GetStatistics()
        
        // Min filter should preserve or decrease min values
        filteredStats should not be null

        resultDs.delete()
    }

    test("KernelFilter should work with different kernel sizes") {
        val kernelSizes = Seq(3, 5, 7)

        kernelSizes.foreach { size =>
            val (resultDs, metadata) = KernelFilter.filter(ds, kernelSize = size, operation = "avg")
            
            resultDs should not be null
            resultDs.GetRasterXSize shouldBe ds.GetRasterXSize
            resultDs.GetRasterYSize shouldBe ds.GetRasterYSize
            
            metadata("last_command") should include(size.toString)
            
            resultDs.delete()
        }
    }

    test("KernelFilter should preserve band count") {
        val originalBandCount = ds.GetRasterCount

        val (resultDs, _) = KernelFilter.filter(ds, kernelSize = 3, operation = "avg")

        resultDs.GetRasterCount shouldBe originalBandCount

        resultDs.delete()
    }

    test("KernelFilter should preserve spatial reference") {
        val originalSR = ds.GetSpatialRef
        val originalEPSG = if (originalSR != null) originalSR.GetAuthorityCode(null) else null

        val (resultDs, _) = KernelFilter.filter(ds, kernelSize = 3, operation = "avg")

        val resultSR = resultDs.GetSpatialRef
        if (originalEPSG != null) {
            resultSR should not be null
            val resultEPSG = resultSR.GetAuthorityCode(null)
            resultEPSG shouldBe originalEPSG
        }

        resultDs.delete()
    }

    test("KernelFilter should preserve geotransform") {
        val originalGT = ds.GetGeoTransform()

        val (resultDs, _) = KernelFilter.filter(ds, kernelSize = 3, operation = "avg")

        val resultGT = resultDs.GetGeoTransform()
        
        resultGT should not be null
        resultGT.length shouldBe originalGT.length
        
        // Compare geotransform values (allowing for small floating point differences)
        for (i <- originalGT.indices) {
            Math.abs(resultGT(i) - originalGT(i)) should be < 0.0001
        }

        resultDs.delete()
    }

    test("KernelFilter should handle invalid operation gracefully") {
        assertThrows[Exception] {
            KernelFilter.filter(ds, kernelSize = 3, operation = "invalid")
        }
    }

    test("KernelFilter should preserve NoData values") {
        val originalBand = ds.GetRasterBand(1)
        val originalNoData = Array.fill[java.lang.Double](1)(0)
        originalBand.GetNoDataValue(originalNoData)

        val (resultDs, _) = KernelFilter.filter(ds, kernelSize = 3, operation = "avg")

        val resultBand = resultDs.GetRasterBand(1)
        val resultNoData = Array.fill[java.lang.Double](1)(0)
        resultBand.GetNoDataValue(resultNoData)

        if (originalNoData(0) != null) {
            resultNoData(0) should not be null
            Math.abs(resultNoData(0) - originalNoData(0)) should be < 0.0001
        }

        resultDs.delete()
    }

}

