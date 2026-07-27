package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Heavy-tier RGBA output shape for rst_tilexyz. Byte-parity with the light tier
 *  is impossible (different encoders) and is NOT tested here; the cross-tier
 *  decode+tolerance parity test lives in the Python suite. These tests assert the
 *  HEAVY output STRUCTURE: band count per format, and that an internal-NoData hole
 *  yields 0-alpha (the display bug being fixed). */
class RST_TileXYZRgbaTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  override def beforeAll(): Unit = {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
    import com.databricks.labs.gbx.util.NodeFilePathUtil
    java.nio.file.Files.createDirectories(NodeFilePathUtil.rootPath)
  }

  // Decode a PNG/WEBP/JPEG byte array via GDAL (/vsimem) into a Dataset for band inspection.
  private def openBytes(bytes: Array[Byte], ext: String) = {
    val p = s"/vsimem/rgbatest_${java.util.UUID.randomUUID().toString.replace("-", "")}.$ext"
    gdal.FileFromMemBuffer(p, bytes)
    val ds = gdal.Open(p)
    (ds, p)
  }

  test("PNG output from a 1-band source is 4-band RGBA") {
    val src = TileXYZTestFixtures.singleBandOverTile()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try ds.GetRasterCount shouldBe 4
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  test("PNG output from a 3-band source is 4-band RGBA") {
    val src = TileXYZTestFixtures.threeBandOverTile()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try ds.GetRasterCount shouldBe 4
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  test("JPEG output is 3-band RGB (no alpha)") {
    val src = TileXYZTestFixtures.threeBandOverTile()
    try {
      val jpg = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "JPEG", 256, "near", "auto")
      val (ds, p) = openBytes(jpg, "jpg")
      try ds.GetRasterCount shouldBe 3
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  test("internal-NoData hole yields a fully-transparent (alpha=0) region") {
    val src = TileXYZTestFixtures.singleBandWithNoDataHole()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try {
        ds.GetRasterCount shouldBe 4
        // Read the alpha band (band 4); some pixels must be 0 (the hole) and some 255.
        val alpha = ds.GetRasterBand(4)
        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val buf = Array.ofDim[Byte](w * h)
        alpha.ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
        val ints = buf.map(_ & 0xff)
        ints.exists(_ == 0) shouldBe true    // the NoData hole is transparent
        ints.exists(_ == 255) shouldBe true  // valid data is opaque
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }
}
