package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.{Convolve, RasterTessellate, WindowedExtract}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * Derived heavy tiles (retile / tessellate / warp / translate / VRT / convolve) must default to the
  * documented ZSTD codec when the caller passes no `compression` option. The default is decided in
  * two places that have to agree: the metadata each operation stamps on its tile (which
  * `RasterDriver.writeToBytes` feeds straight back into `OperatorOptions.appendOptions`) and the
  * `appendOptions` fallback itself. Each test therefore asserts BOTH the stamped `compression`
  * metadata and the codec GDAL actually recorded in the serialized bytes.
  */
class DerivedTileCompressionTest extends AnyFunSuite with BeforeAndAfterAll {

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
        val src = buildRamp(
          mem(name),
          32,
          32,
          1,
          gdalconstConstants.GDT_Float32,
          Array(-0.0160, 0.001, 0.0, 51.5160, 0.0, -0.001)
        )
        try f(src)
        finally RasterDriver.releaseDataset(src)
    }

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

    test("WindowedExtract.extract (retile fast path) defaults to ZSTD") {
        withRamp("we.tif") { src =>
            // WindowedExtract.extract(ds: Dataset, options: Map[String, String], xStart: Int, yStart: Int,
            //                         xOffset: Int, yOffset: Int): (Dataset, Map[String, String])
            val (out, meta) = WindowedExtract.extract(src, Map.empty, 0, 0, 16, 16)
            try assertCodec(out, meta, "ZSTD")
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("WindowedExtract.extract fallback (gdal_translate -srcwin) defaults to ZSTD") {
        withRamp("wefb.tif") { src =>
            // applyScale + a non-identity scale routes to the executeTranslate fallback.
            src.GetRasterBand(1).SetScale(0.5)
            // WindowedExtract.extract(ds: Dataset, options: Map[String, String], xStart: Int, yStart: Int,
            //                         xOffset: Int, yOffset: Int): (Dataset, Map[String, String])
            val (out, meta) = WindowedExtract.extract(src, Map("applyScale" -> "true"), 0, 0, 16, 16)
            try {
                assert(meta("last_command").startsWith("gdal_translate"), "expected the translate fallback")
                assertCodec(out, meta, "ZSTD")
            } finally RasterDriver.releaseDataset(out)
        }
    }

    test("RasterTessellate.tessellateH3Iter first chip defaults to ZSTD") {
        withRamp("tess.tif") { src =>
            // RasterTessellate.tessellateH3Iter(ds: Dataset, options: Map[String, String], resolution: Int,
            //     assignment: String = "centroid", coverage: String = "complete"):
            //     Iterator[(Long, Dataset, Map[String, String])]   (AutoCloseable at runtime)
            val iter = RasterTessellate.tessellateH3Iter(src, Map.empty, 7)
            try {
                assert(iter.hasNext, "tessellation emitted no chips")
                val (_, chip, meta) = iter.next()
                try assertCodec(chip, meta, "ZSTD")
                finally RasterDriver.releaseDataset(chip)
            } finally iter match {
                case ac: AutoCloseable => ac.close()
                case _                 =>
            }
        }
    }

    test("GDALWarp.executeWarp defaults to ZSTD") {
        withRamp("warp.tif") { src =>
            // GDALWarp.executeWarp(outputPath: String, dss: Array[Dataset], options: Map[String, String],
            //                      command: String): (Dataset, Map[String, String])
            val (out, meta) = GDALWarp.executeWarp(mem("warped.tif"), Array(src), Map.empty, command = "gdalwarp")
            try assertCodec(out, meta, "ZSTD")
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("GDALTranslate.executeTranslate defaults to ZSTD") {
        withRamp("trans.tif") { src =>
            // GDALTranslate.executeTranslate(outputPath: String, raster: Dataset, command: String,
            //                                options: Map[String, String]): (Dataset, Map[String, String])
            val (out, meta) = GDALTranslate.executeTranslate(mem("translated.tif"), src, "gdal_translate", Map.empty)
            try assertCodec(out, meta, "ZSTD")
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("GDALBuildVRT.executeVRT defaults to ZSTD (VRT serializes as a GTiff)") {
        withRamp("vrt_src.tif") { src =>
            // GDALBuildVRT.executeVRT(outputPath: String, dss: Array[Dataset], options: Map[String, String],
            //                         command: String): (Dataset, Map[String, String])
            val (vrt, meta) = GDALBuildVRT.executeVRT(mem("built.vrt"), Array(src), Map.empty, "gdalbuildvrt")
            try assertCodec(vrt, meta, "ZSTD")
            finally RasterDriver.releaseDataset(vrt)
        }
    }

    test("Convolve.convolve defaults to ZSTD") {
        withRamp("conv.tif") { src =>
            val kernel = Array(
              Array(0.0, 1.0, 0.0),
              Array(1.0, -4.0, 1.0),
              Array(0.0, 1.0, 0.0)
            )
            // Convolve.convolve(ds: Dataset, options: Map[String, String], kernel: Array[Array[Double]]):
            //     (Dataset, Map[String, String])
            val (out, meta) = Convolve.convolve(src, Map.empty, kernel)
            try assertCodec(out, meta, "ZSTD")
            finally RasterDriver.releaseDataset(out)
        }
    }

    test("one shared constant decides the default codec for appendOptions and the stamped metadata") {
        assert(OperatorOptions.DefaultCompression == "ZSTD")
        withRamp("const.tif") { src =>
            // OperatorOptions.appendOptions(command: String, writeOptions: Map[String, String], ds: Dataset): String
            val cmd = OperatorOptions.appendOptions("gdal_translate", Map.empty, src)
            assert(cmd.contains(s"COMPRESS=${OperatorOptions.DefaultCompression}"), cmd)
        }
    }

    test("an explicit compression option still wins over the default") {
        withRamp("explicit.tif") { src =>
            // GDALTranslate.executeTranslate(outputPath: String, raster: Dataset, command: String,
            //                                options: Map[String, String]): (Dataset, Map[String, String])
            val (out, meta) =
                GDALTranslate.executeTranslate(mem("lzw.tif"), src, "gdal_translate", Map("compression" -> "LZW"))
            try assertCodec(out, meta, "LZW")
            finally RasterDriver.releaseDataset(out)
        }
    }

}
