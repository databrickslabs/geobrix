package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.V2Tile
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.should.Matchers._

/**
  * The scalar `gbx_rst_tilexyz` splits a bad parameter from bad data: a parameter that is wrong (an unknown
  * `format`, `resampling` or `rescale`, a `size` outside (0, 4096]) raises, as `rst_xyzpyramid` does, while a tile
  * that cannot be read or rendered still yields a transparent PNG, never NULL and never a failed query.
  */
class RST_TileXYZScalarTest extends PlanTest with SilentSparkSession {

    private val pngMagic = Array(0x89, 'P', 'N', 'G').map(_.toByte)

    private def utf8(s: String): UTF8String = UTF8String.fromString(s)

    private def exprConf: UTF8String = utf8(ExpressionConfig(spark).toB64)

    private def tileRow(raster: Array[Byte]): InternalRow =
        V2Tile.row(cellid = 0L, raster = raster, metadata = SerializationUtil.toMapData(Map("driver" -> "GTiff")))

    /** A GeoTIFF tile over the z/x/y of [[TileXYZTestFixtures]]. */
    private def goodTile(): InternalRow = {
        GDALManager.init(ExpressionConfig(spark))
        val src: Dataset = TileXYZTestFixtures.singleBandOverTile()
        try tileRow(RasterDriver.writeToBytes(src, Map("driver" -> "GTiff")))
        finally RasterDriver.releaseDataset(src)
    }

    /** A tile whose raster bytes are not a raster: bad data, not a bad parameter. */
    private def unreadableTile(): InternalRow = tileRow(("not a raster\n" * 32).getBytes("UTF-8"))

    private def render(
        row: InternalRow,
        format: String = "PNG",
        size: Int = 64,
        resampling: String = "near",
        rescale: String = "auto",
        z: Int = TileXYZTestFixtures.z,
        x: Int = TileXYZTestFixtures.x,
        y: Int = TileXYZTestFixtures.y
    ): Array[Byte] =
        RST_TileXYZ.eval(row, z, x, y, utf8(format), size, utf8(resampling), utf8(rescale), exprConf)

    /** The PNG's (width, height, alpha values). */
    private def decodeAlpha(png: Array[Byte]): (Int, Int, Array[Byte]) = {
        val path = s"/vsimem/tilexyz_scalar_${java.util.UUID.randomUUID().toString.replace("-", "")}.png"
        gdal.FileFromMemBuffer(path, png)
        val ds = gdal.Open(path)
        try {
            ds.getRasterCount shouldBe 4
            val (w, h) = (ds.getRasterXSize, ds.getRasterYSize)
            val alpha = new Array[Byte](w * h)
            ds.GetRasterBand(4).ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, alpha)
            (w, h, alpha)
        } finally { ds.delete(); gdal.Unlink(path) }
    }

    private def messageChain(e: Throwable): List[String] =
        Iterator.iterate(e)(_.getCause).takeWhile(_ != null).map(_.getMessage).toList

    // (what is wrong, the render arguments, a fragment of the message)
    private val badParameters: Seq[(String, String, Int, String, String, String)] = Seq(
      ("an unknown format", "GIF", 64, "near", "auto", "rst_tilexyz: format must be one of"),
      ("a size of 0", "PNG", 0, "near", "auto", "size must be in (0, 4096]"),
      ("a size above the maximum", "PNG", 5000, "near", "auto", "size must be in (0, 4096]"),
      ("an unknown resampling", "PNG", 64, "sharpest", "auto", "unsupported resampling 'sharpest'"),
      ("an unknown rescale", "PNG", 64, "near", "bogus", "rst_tilexyz: rescale must be 'auto', 'none', or 'min,max'"),
      ("a rescale pair with min above max", "PNG", 64, "near", "5,1", "rescale (min,max) must have min < max")
    )

    test("a bad parameter raises its message instead of rendering a transparent PNG") {
        val row = goodTile()
        for ((what, format, size, resampling, rescale, fragment) <- badParameters) {
            val e = intercept[IllegalArgumentException](render(row, format, size, resampling, rescale))
            withClue(s"$what: ") { e.getMessage should include(fragment) }
        }
    }

    test("a bad parameter raises through the Long overload SQL literals from PySpark reach") {
        val row = goodTile()
        val e = intercept[IllegalArgumentException](
          RST_TileXYZ.eval(row, 8L, 135L, 87L, utf8("GIF"), 64L, utf8("near"), utf8("auto"), exprConf)
        )
        e.getMessage should include("format must be one of")
    }

    test("a bad parameter raises even when the tile is unreadable: parameters are checked before the data") {
        val e = intercept[IllegalArgumentException](render(unreadableTile(), format = "GIF"))
        e.getMessage should include("format must be one of")
    }

    test("a tile that cannot be read still renders a transparent PNG of the requested size") {
        GDALManager.init(ExpressionConfig(spark))
        val png = render(unreadableTile(), size = 32)
        png.take(4) shouldBe pngMagic
        val (w, h, alpha) = decodeAlpha(png)
        (w, h) shouldBe ((32, 32))
        alpha.forall(_ == 0) shouldBe true
    }

    test("a valid call renders the tile, with data under it") {
        val png = render(goodTile())
        png.take(4) shouldBe pngMagic
        val (w, h, alpha) = decodeAlpha(png)
        (w, h) shouldBe ((64, 64))
        alpha.exists(_ != 0) shouldBe true
    }

    test("gbx_rst_tilexyz in SQL raises for a bad parameter and renders for a valid call") {
        rasterx.functions.register(spark)
        val modis = getClass.getResource("/modis/").toString
        spark.read.format("gdal").option("sizeInMB", "1").load(modis).limit(1).createOrReplaceTempView("tilexyz_scalar_src")
        try {
            val e = intercept[Exception](spark.sql("SELECT gbx_rst_tilexyz(tile, 2, 2, 1, 'GIF') FROM tilexyz_scalar_src").collect())
            withClue(s"the error chain ${messageChain(e)}: ") {
                messageChain(e).exists(m => m != null && m.contains("rst_tilexyz: format must be one of")) shouldBe true
            }
            val png = spark.sql("SELECT gbx_rst_tilexyz(tile, 2, 2, 1, 'PNG', 64, 'near', 'auto') FROM tilexyz_scalar_src")
                .head().getAs[Array[Byte]](0)
            png.take(4) shouldBe pngMagic
        } finally spark.catalog.dropTempView("tilexyz_scalar_src")
    }

}
