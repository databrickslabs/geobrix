package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.RST_BinPoints
import com.databricks.labs.gbx.rasterx.expressions.dem.RST_TRI
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Band
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.{Convolve, RasterTessellate, WindowedExtract}
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * Derived heavy tiles (retile / tessellate / warp / translate / VRT / convolve / proximity /
  * band / DEM) must default to the documented DEFLATE codec (baseline TIFF, decodable by Java ImageIO and
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

    // Copied from WindowedExtractTest (private there).
    private def mem(path: String): String = s"/vsimem/dtctest_${UUID.randomUUID().toString.replace("-", "")}_$path"

    // Copied from WindowedExtractTest (private there).
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

    test("WindowedExtract.extract (retile fast path) defaults to DEFLATE") {
        withRamp("we.tif") { src =>
            // WindowedExtract.extract(ds: Dataset, options: Map[String, String], xStart: Int, yStart: Int,
            //                         xOffset: Int, yOffset: Int): (Dataset, Map[String, String])
            val (out, meta) = WindowedExtract.extract(src, Map.empty, 0, 0, 16, 16)
            try assertCodec(out, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("WindowedExtract.extract fallback (gdal_translate -srcwin) defaults to DEFLATE") {
        withRamp("wefb.tif") { src =>
            // applyScale + a non-identity scale routes to the executeTranslate fallback.
            src.GetRasterBand(1).SetScale(0.5)
            // WindowedExtract.extract(ds: Dataset, options: Map[String, String], xStart: Int, yStart: Int,
            //                         xOffset: Int, yOffset: Int): (Dataset, Map[String, String])
            val (out, meta) = WindowedExtract.extract(src, Map("applyScale" -> "true"), 0, 0, 16, 16)
            try {
                assert(meta("last_command").startsWith("gdal_translate"), "expected the translate fallback")
                assertCodec(out, meta, DerivedDefault)
            } finally RasterDriver.releaseDataset(out)
        }
    }

    test("RasterTessellate.tessellateH3Iter first chip defaults to DEFLATE") {
        // RasterTessellate.tessellateH3Iter(ds: Dataset, options: Map[String, String], resolution: Int,
        //     assignment: String = "centroid", coverage: String = "complete"):
        //     Iterator[(Long, Dataset, Map[String, String])]   (AutoCloseable at runtime)
        // The tessellation owns its input and releases it when closed.
        val iter = RasterTessellate.tessellateH3Iter(ramp("tess.tif"), Map.empty, 7)
        try {
            assert(iter.hasNext, "tessellation emitted no chips")
            val (_, chip, meta) = iter.next()
            try assertCodec(chip, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(chip)
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
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

    test("GDALBuildVRT.executeVRT defaults to DEFLATE (VRT serializes as a GTiff)") {
        withRamp("vrt_src.tif") { src =>
            // GDALBuildVRT.executeVRT(outputPath: String, dss: Array[Dataset], options: Map[String, String],
            //                         command: String): (Dataset, Map[String, String])
            val (vrt, meta) = GDALBuildVRT.executeVRT(mem("built.vrt"), Array(src), Map.empty, "gdalbuildvrt")
            try assertCodec(vrt, meta, DerivedDefault)
            finally RasterDriver.releaseDataset(vrt)
        }
    }

    test("Convolve.convolve defaults to DEFLATE") {
        withRamp("conv.tif") { src =>
            val kernel = Array(
              Array(0.0, 1.0, 0.0),
              Array(1.0, -4.0, 1.0),
              Array(0.0, 1.0, 0.0)
            )
            // Convolve.convolve(ds: Dataset, options: Map[String, String], kernel: Array[Array[Double]]):
            //     (Dataset, Map[String, String])
            val (out, meta) = Convolve.convolve(src, Map.empty, kernel)
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

    test("the rasterize and binpoints tile writers emit the heavyweight default codec, not raw bytes") {
        // RST_BinPoints.execute(x, y, z, xmin, ymin, xmax, ymax, w, h, srid, statistic): Array[Byte]
        val pts = RST_BinPoints.execute(
          Array(1.0, 2.0, 3.0), Array(1.0, 2.0, 3.0), Array(5.0, 6.0, 7.0), 0.0, 0.0, 64.0, 64.0, 64, 64, 4326, "mean")
        assert(storedCompression(pts) == DerivedDefault, s"binpoints wrote ${storedCompression(pts)}")
        // the other callers of VectorRasterBridge.toGTiffBytes (the rasterize aggregates) share its encoder: a larger
        // canvas, so an explicit NONE stays raw and the default is much smaller than raw
        val big = RST_BinPoints.execute(
          Array(1.0, 2.0, 3.0), Array(1.0, 2.0, 3.0), Array(5.0, 6.0, 7.0), 0.0, 0.0, 200.0, 200.0, 200, 200, 4326, "mean")
        assert(storedCompression(big) == DerivedDefault, s"binpoints wrote ${storedCompression(big)}")
        val raw = VectorRasterBridge.toGTiffBytes(
          { val p = mem("cells.tif"); gdal.FileFromMemBuffer(p, big); gdal.Open(p) }, "NONE")
        assert(storedCompression(raw) == null, "an explicit NONE must stay raw")
        assert(big.length < raw.length / 4, s"${big.length} B vs raw ${raw.length} B")
    }

}
