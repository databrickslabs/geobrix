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

  /** N==2 test: two-band uint16 source, "auto" rescale.  The rio-tiler mapping sends
   *  band1 to R/G/B (grey) and band2 to alpha.  With FINDING 1 present, G and B get
   *  the WRONG scale pair (band2's huge range) so R!=G!=B; after the fix all three
   *  grey channels must be equal (±1 rounding). */
  test("N==2 uint16 source under auto rescale produces grey output with R==G==B") {
    val src = TileXYZTestFixtures.twoBandUint16OverTile()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try {
        ds.GetRasterCount shouldBe 4
        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        def readBand(b: Int): Array[Int] = {
          val buf = Array.ofDim[Byte](w * h)
          ds.GetRasterBand(b).ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
          buf.map(_ & 0xff)
        }
        val r = readBand(1); val g = readBand(2); val bArr = readBand(3); val a = readBand(4)
        // Find opaque pixels (alpha==255) -- the source has no NoData so the tile
        // interior should be fully opaque.
        val opaquePixels = a.indices.filter(i => a(i) == 255)
        opaquePixels should not be empty
        // In the opaque region R==G==B (grey channels from the same source band).
        // Allow ±1 for rounding.
        opaquePixels.foreach { i =>
          withClue(s"at pixel $i: R=${r(i)} G=${g(i)} B=${bArr(i)}") {
            math.abs(r(i) - g(i)) shouldBe <=(1)
            math.abs(r(i) - bArr(i)) shouldBe <=(1)
          }
        }
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  /** N==4 test: four-band uint8 source.  Band 4 is fully opaque in the source fixture.
   *  The output must be 4-band RGBA and the interior of the tile must be fully opaque. */
  test("N==4 source produces 4-band RGBA with source alpha preserved") {
    val src = TileXYZTestFixtures.fourBandOverTile()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try {
        ds.GetRasterCount shouldBe 4
        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val buf = Array.ofDim[Byte](w * h)
        ds.GetRasterBand(4).ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
        val alphaBuf = buf.map(_ & 0xff)
        // Source band 4 is 255 everywhere; opaque pixels must exist.
        alphaBuf.exists(_ == 255) shouldBe true
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  /** N>=5 test: five-band source exercises the `case _ => (Seq(1,2,3), total)` branch.
   *  The first three bands become R, G, B and the trailing -dstalpha band becomes alpha;
   *  the extra bands (4 and 5) are ignored. Output must be 4-band RGBA. */
  test("PNG output from a 5-band source is 4-band RGBA (N>=5 branch)") {
    val src = TileXYZTestFixtures.fiveBandOverTile()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try ds.GetRasterCount shouldBe 4
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  /** WEBP uses an outcome-driven alpha retry: RGBA is attempted first; if the encoder
   *  rejects alpha (empty/null bytes returned) the encode is retried as 3-band RGB.
   *  Both paths are valid; neither should produce the source's raw N bands. */
  test("WEBP output is RGBA when the driver supports alpha, else RGB") {
    val src = TileXYZTestFixtures.threeBandOverTile()
    try {
      val webp = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "WEBP", 256, "near", "auto")
      val (ds, p) = openBytes(webp, "webp")
      try {
        val nb = ds.GetRasterCount
        // 4 (RGBA -- alpha-capable GDAL/libwebp build) or 3 (RGB fallback) -- both
        // acceptable; never the source's raw N bands unchanged.
        (nb == 4 || nb == 3) shouldBe true
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  /** Strengthened NoData test: tile is FULLY inside the wider fixture extent (lon [9,13]
   *  lat [47,51]) so outside-footprint alpha=0 cannot pass this test.  The only alpha=0
   *  pixels come from the internal NoData hole.  Corners (top-left / top-right /
   *  bottom-left / bottom-right 16x16 quadrants) must be fully opaque; the center
   *  must contain alpha=0. */
  test("internal-NoData hole yields a fully-transparent (alpha=0) region") {
    val src = TileXYZTestFixtures.singleBandWithNoDataHole()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try {
        ds.GetRasterCount shouldBe 4
        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val buf = Array.ofDim[Byte](w * h)
        ds.GetRasterBand(4).ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
        val ints = buf.map(_ & 0xff)

        // Center 64x64 window (approx middle quarter) must contain at least one alpha==0
        // from the internal hole.
        val cx0 = w / 4; val cx1 = 3 * w / 4
        val cy0 = h / 4; val cy1 = 3 * h / 4
        val centerAlpha = (cy0 until cy1).flatMap { py =>
          (cx0 until cx1).map { px => ints(py * w + px) }
        }
        centerAlpha.exists(_ == 0) shouldBe true

        // Corner pixels (16x16 from each corner) must be fully opaque -- the fixture
        // wide extent guarantees no outside-footprint alpha=0 here.
        val cornerSize = 16
        val corners: Seq[Int] =
          (0 until cornerSize).flatMap { cy =>
            (0 until cornerSize).flatMap { cx => Seq(
              ints(cy * w + cx),                          // top-left
              ints(cy * w + (w - 1 - cx)),                // top-right
              ints((h - 1 - cy) * w + cx),               // bottom-left
              ints((h - 1 - cy) * w + (w - 1 - cx))     // bottom-right
            )}
          }
        corners.exists(_ == 255) shouldBe true
        corners.forall(_ == 255) shouldBe true
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }
}
