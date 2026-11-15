package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Covers RasterAccessors.memSize, with emphasis on the in-memory (MEM-driver)
  * dataset case whose GetDescription() is empty -- the form produced by
  * gdal.GetDriverByName("MEM").CreateCopy("", ds). Before the robustness fix this
  * returned -1 instead of the encoded size both tiers report. */
class RasterAccessorsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    /** A real-pixel MEM dataset whose ENCODED (DEFLATE GTiff) size is large: memSize keys on
      * the encoded byte length, so the pixels are incompressible (a 1024x1024 Float64 band of
      * uniform random doubles encodes to ~8 MiB). */
    private def bigMemDataset(): Dataset = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 1024, 1024, 1, gdalconstConstants.GDT_Float64)
        val band = ds.GetRasterBand(1)
        val rng = new java.util.Random(0)
        val buf = Array.fill[Double](1024 * 1024)(rng.nextDouble())
        band.WriteRaster(0, 0, 1024, 1024, buf)
        band.FlushCache()
        ds.FlushCache()
        ds
    }

    test("memSize returns a real encoded size for a MEM dataset with empty description") {
        val ds = bigMemDataset()
        try {
            ds.GetDescription() shouldBe "" // MEM CreateCopy("", ...) leaves the description empty
            val sz = RasterAccessors.memSize(ds)
            sz should be > 0L // NOT the -1 failure sentinel
            // The encoded GTiff buffer is the authoritative size; memSize must equal it.
            sz shouldBe RasterDriver.writeToBytes(ds, Map.empty).length.toLong
        } finally ds.delete()
    }

    test("memSize uses Files.size for a real file path") {
        val tmp = java.nio.file.Files.createTempFile("memsize_", ".tif")
        try {
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(tmp.toString, 64, 64, 1, gdalconstConstants.GDT_Byte)
            ds.FlushCache()
            ds.delete()
            val reopened = gdal.Open(tmp.toString)
            try RasterAccessors.memSize(reopened) shouldBe java.nio.file.Files.size(tmp)
            finally reopened.delete()
        } finally java.nio.file.Files.deleteIfExists(tmp)
    }
}
