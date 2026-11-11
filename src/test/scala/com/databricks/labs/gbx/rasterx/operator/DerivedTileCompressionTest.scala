package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.dem.RST_TRI
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Band
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * Derived heavy tiles (warp / translate / band / DEM / grid rasterize) must default to the documented DEFLATE codec (baseline TIFF, decodable by Java ImageIO and
  * the Databricks image preview) when the caller passes no `compression` option. The default is decided in two places that have to agree: the
  * metadata each operation stamps on its tile (which `RasterDriver.writeToBytes` feeds straight back
  * into `OperatorOptions.appendOptions`) and the `appendOptions` fallback itself. Each test therefore
  * asserts BOTH the stamped `compression` metadata and the codec GDAL actually recorded in the
  * serialized bytes.
  */
class DerivedTileCompressionTest extends AnyFunSuite with BeforeAndAfterAll {

    /** The documented heavyweight default, pinned as a literal so a change to the constant is caught here. */
    private val DerivedDefault = "DEFLATE"

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private def mem(path: String): String = s"/vsimem/dtctest_${UUID.randomUUID().toString.replace("-", "")}_$path"

    /** Builds a single-/multi-band GTiff with a known per-pixel ramp; band i pixel = base*i + idx. */
    private def buildRamp(
        path: String,
        w: Int,
        h: Int,
        bands: Int,
        dtype: Int,
        gt: Array[Double],
        nodata: Option[Double] = None
    ): Dataset = {
        val drv = GDALManager.gtiffDriver()
        val ds = drv.Create(path, w, h, bands, dtype)
        ds.SetGeoTransform(gt)
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        var b = 1
        while (b <= bands) {
            val band = ds.GetRasterBand(b)
            val arr = new Array[Double](w * h)
            var i = 0
            while (i < w * h) { arr(i) = (b * 1000 + i).toDouble % 250.0; i += 1 }
            band.WriteRaster(0, 0, w, h, arr)
            nodata.foreach(band.SetNoDataValue)
            b += 1
        }
        ds.FlushCache()
        ds
    }

    /** A 32x32 float32 EPSG:4326 ramp near London; small enough that H3 res 7 emits a handful of cells. */
    private def withRamp(name: String)(f: Dataset => Unit): Unit = {
        val src = ramp(name)
        try f(src)
        finally RasterDriver.releaseDataset(src)
    }

    /** The 32x32 ramp of [[withRamp]], unreleased (for a consumer that owns its input). */
    private def ramp(name: String): Dataset =
        buildRamp(mem(name), 32, 32, 1, gdalconstConstants.GDT_Float32, Array(-0.0160, 0.001, 0.0, 51.5160, 0.0, -0.001))

    /** The codec GDAL recorded for the serialized tile (IMAGE_STRUCTURE / COMPRESSION), read back from the bytes. */
    private def storedCompression(bytes: Array[Byte]): String = {
        val p = mem("readback.tif")
        gdal.FileFromMemBuffer(p, bytes)
        val back = gdal.Open(p)
        try back.GetMetadataItem("COMPRESSION", "IMAGE_STRUCTURE")
        finally RasterDriver.releaseDataset(back)
    }

    /** Asserts the stamped metadata AND the serialized codec both say `expected`. */
    private def assertCodec(ds: Dataset, meta: Map[String, String], expected: String): Unit = {
        assert(meta("compression") == expected)
        val bytes = RasterDriver.writeToBytes(ds, meta)
        assert(bytes != null && bytes.nonEmpty)
        assert(storedCompression(bytes) == expected)
    }

    test("GDALWarp.executeWarp defaults to DEFLATE") {
        withRamp("warp.tif") { src =>
            // GDALWarp.executeWarp(outputPath: String, dss: Array[Dataset], options: Map[String, String],
            //                      command: String): (Dataset, Map[String, String])
            val (out, meta) = GDALWarp.executeWarp(mem("warped.tif"), Array(src), Map.empty, command = "gdalwarp")
            try assertCodec(out, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("GDALTranslate.executeTranslate defaults to DEFLATE") {
        withRamp("trans.tif") { src =>
            // GDALTranslate.executeTranslate(outputPath: String, raster: Dataset, command: String,
            //                                options: Map[String, String]): (Dataset, Map[String, String])
            val (out, meta) = GDALTranslate.executeTranslate(mem("translated.tif"), src, "gdal_translate", Map.empty)
            try assertCodec(out, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("RST_TRI.execute (DEM helper) defaults to DEFLATE") {
        withRamp("tri.tif") { src =>
            // RST_TRI.execute(ds: Dataset): (Dataset, Map[String, String])
            //     -- delegates to RST_DEMProcessingHelper.process
            val (out, meta) = RST_TRI.execute(src)
            try assertCodec(out, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("RST_Band.execute defaults to DEFLATE and honours an incoming compression") {
        withRamp("band.tif") { src =>
            // RST_Band.execute(ds: Dataset, options: Map[String, String], bandIndex: Int):
            //     (Dataset, Map[String, String])
            val (out, meta) = RST_Band.execute(src, Map.empty, 1)
            try assertCodec(out, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(out)

            val (lzw, lzwMeta) = RST_Band.execute(src, Map("compression" -> "LZW"), 1)
            try assertCodec(lzw, lzwMeta, "LZW")
            finally RasterDriver.releaseDataset(lzw)
        }
    }

    test("one shared constant decides the default codec for appendOptions and the stamped metadata") {
        assert(OperatorOptions.DefaultCompression == DerivedDefault)
        withRamp("const.tif") { src =>
            // OperatorOptions.appendOptions(command: String, writeOptions: Map[String, String], ds: Dataset): String
            val cmd = OperatorOptions.appendOptions("gdal_translate", Map.empty, src)
            assert(cmd.contains(s"COMPRESS=$DerivedDefault"), cmd)
            // A tile whose metadata names no codec serializes with the default too.
            // RasterDriver.writeToBytes(ds: Dataset, options: Map[String, String]): Array[Byte]
            assert(storedCompression(RasterDriver.writeToBytes(src, Map.empty)) == DerivedDefault)
        }
    }

    test("an explicit compression option still wins over the default") {
        withRamp("explicit.tif") { src =>
            for (codec <- Seq("LZW", "ZSTD")) {
                // GDALTranslate.executeTranslate(outputPath: String, raster: Dataset, command: String,
                //                                options: Map[String, String]): (Dataset, Map[String, String])
                val (out, meta) = GDALTranslate.executeTranslate(
                  mem(s"${codec.toLowerCase}.tif"),
                  src,
                  "gdal_translate",
                  Map("compression" -> codec)
                )
                try assertCodec(out, meta, codec)
                finally RasterDriver.releaseDataset(out)
            }
        }
    }

    test("the grid rasterize tile writer emits the heavyweight default codec, not raw bytes") {
        // VectorRasterBridge.toGTiffBytes is the encoder of the grid rasterize aggregates: on a canvas of
        // 200 x 200 an explicit NONE stays raw, and the default is much smaller than raw
        def encoded(compression: Option[String]): Array[Byte] = {
            val canvas = VectorRasterBridge.buildEmptyRaster(0.0, 0.0, 200.0, 200.0, 200, 200, 4326)
            try compression.fold(VectorRasterBridge.toGTiffBytes(canvas))(VectorRasterBridge.toGTiffBytes(canvas, _))
            finally canvas.delete()
        }
        val default = encoded(None)
        assert(storedCompression(default) == DerivedDefault, s"the writer wrote ${storedCompression(default)}")
        val raw = encoded(Some("NONE"))
        assert(storedCompression(raw) == null, "an explicit NONE must stay raw")
        assert(default.length < raw.length / 4, s"${default.length} B vs raw ${raw.length} B")
    }

}
