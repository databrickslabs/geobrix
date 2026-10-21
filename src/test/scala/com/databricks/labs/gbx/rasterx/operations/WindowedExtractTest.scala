package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{ColorTable, Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.awt.Color
import java.util.UUID

/**
  * Edge-condition coverage for [[WindowedExtract.extract]] — the windowed-read fast path under
  * [[ReTile.getTile]]. Fixtures are synthesized in /vsimem so every attribute under test is
  * controlled. Each test asserts either fast-path parity or that an unsupported attribute routes
  * to the `gdal.Translate` fallback while still producing a correct window.
  */
class WindowedExtractTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private def mem(path: String): String = s"/vsimem/wetest_${UUID.randomUUID().toString.replace("-", "")}_$path"

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

    private def readBandDoubles(ds: Dataset, b: Int, x: Int, y: Int, w: Int, h: Int): Array[Double] = {
        val out = new Array[Double](w * h)
        ds.GetRasterBand(b).ReadRaster(x, y, w, h, out)
        out
    }

    test("pixel parity vs direct windowed read (single + multi band)") {
        val src = buildRamp(mem("ramp.tif"), 64, 64, 3, gdalconstConstants.GDT_Float32,
          Array(100.0, 1.0, 0.0, 200.0, 0.0, -1.0))
        try {
            val (xs, ys, xo, yo) = (10, 20, 32, 24)
            val (out, _) = WindowedExtract.extract(src, Map.empty, xs, ys, xo, yo)
            try {
                assert(out.getRasterXSize == xo && out.getRasterYSize == yo)
                assert(out.getRasterCount == src.getRasterCount)
                for (b <- 1 to src.getRasterCount) {
                    val got = readBandDoubles(out, b, 0, 0, xo, yo)
                    val exp = readBandDoubles(src, b, xs, ys, xo, yo)
                    assert(got.sameElements(exp), s"band $b pixel mismatch")
                }
            } finally out.delete()
        } finally src.delete()
    }

    test("window-shifted geotransform origin, axis-aligned") {
        val gt = Array(500000.0, 30.0, 0.0, 4000000.0, 0.0, -30.0)
        val src = buildRamp(mem("gt.tif"), 50, 50, 1, gdalconstConstants.GDT_Int16, gt)
        try {
            val (xs, ys, xo, yo) = (7, 13, 20, 15)
            val (out, _) = WindowedExtract.extract(src, Map.empty, xs, ys, xo, yo)
            try {
                val ogt = new Array[Double](6)
                out.GetGeoTransform(ogt)
                assert(math.abs(ogt(0) - (gt(0) + xs * gt(1) + ys * gt(2))) < 1e-9)
                assert(math.abs(ogt(3) - (gt(3) + xs * gt(4) + ys * gt(5))) < 1e-9)
                assert(ogt(1) == gt(1) && ogt(5) == gt(5) && ogt(2) == gt(2) && ogt(4) == gt(4))
            } finally out.delete()
        } finally src.delete()
    }

    test("window-shifted geotransform origin, ROTATED (GT2/GT4 != 0)") {
        val gt = Array(1000.0, 10.0, 2.5, 5000.0, 1.5, -10.0) // sheared/rotated
        val src = buildRamp(mem("rot.tif"), 40, 40, 1, gdalconstConstants.GDT_Float32, gt)
        try {
            val (xs, ys, xo, yo) = (8, 9, 16, 16)
            val (out, _) = WindowedExtract.extract(src, Map.empty, xs, ys, xo, yo)
            try {
                val ogt = new Array[Double](6)
                out.GetGeoTransform(ogt)
                assert(math.abs(ogt(0) - (gt(0) + xs * gt(1) + ys * gt(2))) < 1e-9, "origin X must include GT(2) shear")
                assert(math.abs(ogt(3) - (gt(3) + xs * gt(4) + ys * gt(5))) < 1e-9, "origin Y must include GT(4) shear")
                assert(ogt(1) == gt(1) && ogt(2) == gt(2) && ogt(4) == gt(4) && ogt(5) == gt(5))
            } finally out.delete()
        } finally src.delete()
    }

    test("NoData value preserved across the window (multi-band)") {
        val src = buildRamp(mem("nd.tif"), 32, 32, 3, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0), nodata = Some(42.0))
        try {
            val (out, _) = WindowedExtract.extract(src, Map.empty, 4, 4, 16, 16)
            try {
                for (b <- 1 to 3) {
                    val nd = new Array[java.lang.Double](1); out.GetRasterBand(b).GetNoDataValue(nd)
                    assert(nd(0) != null && nd(0) == 42.0, s"band $b NoData must be preserved")
                }
            } finally out.delete()
        } finally src.delete()
    }

    test("NaN NoData round-trips") {
        val src = buildRamp(mem("nan.tif"), 32, 32, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0), nodata = Some(Double.NaN))
        try {
            val (out, _) = WindowedExtract.extract(src, Map.empty, 4, 4, 16, 16)
            try {
                val nd = new Array[java.lang.Double](1); out.GetRasterBand(1).GetNoDataValue(nd)
                assert(nd(0) != null && nd(0).isNaN, "NaN nodata must round-trip via SetNoDataValue")
            } finally out.delete()
        } finally src.delete()
    }

    test("source without NoData => output stays unset") {
        val src = buildRamp(mem("nond.tif"), 32, 32, 2, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0)) // no nodata
        try {
            assert({
                val nd = new Array[java.lang.Double](1); src.GetRasterBand(1).GetNoDataValue(nd); nd(0) == null
            }, "fixture must have no NoData")
            val (out, _) = WindowedExtract.extract(src, Map.empty, 4, 4, 16, 16)
            try {
                for (b <- 1 to 2) {
                    val nd = new Array[java.lang.Double](1); out.GetRasterBand(b).GetNoDataValue(nd)
                    assert(nd(0) == null, s"band $b must stay unset when source has no NoData")
                }
            } finally out.delete()
        } finally src.delete()
    }

    test("color table preserved (paletted raster keeps palette on fast path)") {
        val src = buildRamp(mem("pal.tif"), 32, 32, 1, gdalconstConstants.GDT_Byte,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val ct = new ColorTable()
            ct.SetColorEntry(0, new Color(10, 20, 30))
            ct.SetColorEntry(1, new Color(40, 50, 60))
            ct.SetColorEntry(2, new Color(70, 80, 90))
            src.GetRasterBand(1).SetColorInterpretation(gdalconstConstants.GCI_PaletteIndex)
            src.GetRasterBand(1).SetColorTable(ct)
            src.FlushCache()
            val (out, _) = WindowedExtract.extract(src, Map.empty, 2, 2, 16, 16)
            try {
                val oct = out.GetRasterBand(1).GetColorTable()
                assert(oct != null, "color table must be preserved")
                assert(oct.GetCount() == ct.GetCount())
                assert(oct.GetColorEntry(1) == new Color(40, 50, 60))
                assert(out.GetRasterBand(1).GetColorInterpretation() == gdalconstConstants.GCI_PaletteIndex)
            } finally out.delete()
        } finally src.delete()
    }

    test("scale / offset / unit type / band metadata / dataset metadata preserved") {
        val src = buildRamp(mem("meta.tif"), 32, 32, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val band = src.GetRasterBand(1)
            band.SetScale(0.5)
            band.SetOffset(3.25)
            band.SetUnitType("metre")
            band.SetMetadataItem("BAND_KEY", "band_val")
            src.SetMetadataItem("DS_KEY", "ds_val")
            src.FlushCache()
            val (out, _) = WindowedExtract.extract(src, Map.empty, 1, 1, 8, 8)
            try {
                val ob = out.GetRasterBand(1)
                val sc = new Array[java.lang.Double](1); ob.GetScale(sc)
                val of = new Array[java.lang.Double](1); ob.GetOffset(of)
                assert(sc(0) != null && sc(0) == 0.5)
                assert(of(0) != null && of(0) == 3.25)
                assert(ob.GetUnitType() == "metre")
                assert(ob.GetMetadataItem("BAND_KEY") == "band_val")
                assert(out.GetMetadataItem("DS_KEY") == "ds_val")
            } finally out.delete()
        } finally src.delete()
    }

    test("color interpretation preserved (multi-band RGB)") {
        val src = buildRamp(mem("rgb.tif"), 24, 24, 3, gdalconstConstants.GDT_Byte,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            src.GetRasterBand(1).SetColorInterpretation(gdalconstConstants.GCI_RedBand)
            src.GetRasterBand(2).SetColorInterpretation(gdalconstConstants.GCI_GreenBand)
            src.GetRasterBand(3).SetColorInterpretation(gdalconstConstants.GCI_BlueBand)
            src.FlushCache()
            val (out, _) = WindowedExtract.extract(src, Map.empty, 0, 0, 12, 12)
            try {
                assert(out.GetRasterBand(1).GetColorInterpretation() == gdalconstConstants.GCI_RedBand)
                assert(out.GetRasterBand(2).GetColorInterpretation() == gdalconstConstants.GCI_GreenBand)
                assert(out.GetRasterBand(3).GetColorInterpretation() == gdalconstConstants.GCI_BlueBand)
            } finally out.delete()
        } finally src.delete()
    }

    test("projection preserved") {
        val src = buildRamp(mem("proj.tif"), 32, 32, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val (out, _) = WindowedExtract.extract(src, Map.empty, 5, 5, 10, 10)
            try {
                assert(out.GetProjection() == src.GetProjection())
                assert(out.GetProjection().nonEmpty)
            } finally out.delete()
        } finally src.delete()
    }

    test("mixed-dtype bands => FALLBACK path, output still correct") {
        // A genuine mixed-dtype, file-backed source: two single-band GTiffs (Byte + Float32)
        // combined with `gdalbuildvrt -separate`. The VRT preserves each band's source dtype,
        // so Create (one dtype) cannot represent it => simpleEnough must return false. Real
        // temp files (not /vsimem) keep the VRT's source references resolvable at translate time.
        val tmp = java.nio.file.Files.createTempDirectory("wetest_mixed").toString
        val bytePath = s"$tmp/byte.tif"
        val floatPath = s"$tmp/float.tif"
        val b = buildRamp(bytePath, 32, 32, 1, gdalconstConstants.GDT_Byte,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        val f = buildRamp(floatPath, 32, 32, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        b.delete(); f.delete()
        val vrtPath = s"$tmp/mixed.vrt"
        val inputs = new java.util.Vector[String](); inputs.add(bytePath); inputs.add(floatPath)
        val opts = new java.util.Vector[String](); opts.add("-separate")
        val ds = gdal.BuildVRT(vrtPath, inputs, new org.gdal.gdal.BuildVRTOptions(opts))
        try {
            assert(ds != null, "BuildVRT must succeed")
            assert(ds.GetRasterBand(1).getDataType != ds.GetRasterBand(2).getDataType,
              "VRT must be genuinely mixed-dtype")
            // Mixed dtype cannot be written to GTiff; production retile carries the source
            // format in the tile metadata, so the fallback round-trips it via `-of VRT`.
            val (xs, ys, xo, yo) = (4, 6, 16, 12)
            val (out, meta) = WindowedExtract.extract(ds, Map("format" -> "VRT"), xs, ys, xo, yo)
            try {
                // fallback => last_command is a gdal_translate string, not windowed_extract
                assert(meta("last_command").contains("gdal_translate"), "mixed dtype must take fallback")
                assert(out.getRasterXSize == xo && out.getRasterYSize == yo)
                val got = readBandDoubles(out, 1, 0, 0, xo, yo)
                val exp = readBandDoubles(ds, 1, xs, ys, xo, yo)
                assert(got.sameElements(exp))
            } finally { out.delete(); gdal.Unlink(meta("path")) }
        } finally {
            ds.delete()
            Seq(vrtPath, bytePath, floatPath, tmp).foreach(p => new java.io.File(p).delete())
        }
    }

    test("real mask band (GMF_PER_DATASET) => FALLBACK, output correct") {
        val src = buildRamp(mem("mask.tif"), 32, 32, 2, gdalconstConstants.GDT_Byte,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            src.CreateMaskBand(gdalconstConstants.GMF_PER_DATASET)
            val maskArr = new Array[Byte](32 * 32)
            var i = 0; while (i < maskArr.length) { maskArr(i) = (if (i % 3 == 0) 0 else 255).toByte; i += 1 }
            src.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, 32, 32, maskArr)
            src.FlushCache()
            val flags = src.GetRasterBand(1).GetMaskFlags()
            assert((flags & gdalconstConstants.GMF_PER_DATASET) != 0, "fixture must have a per-dataset mask")
            val (xs, ys, xo, yo) = (3, 3, 16, 16)
            val (out, meta) = WindowedExtract.extract(src, Map.empty, xs, ys, xo, yo)
            try {
                assert(meta("last_command").contains("gdal_translate"), "mask band must take fallback")
                assert(out.getRasterXSize == xo && out.getRasterYSize == yo)
                val got = readBandDoubles(out, 1, 0, 0, xo, yo)
                val exp = readBandDoubles(src, 1, xs, ys, xo, yo)
                assert(got.sameElements(exp))
            } finally { out.delete(); gdal.Unlink(meta("path")) }
        } finally src.delete()
    }

    test("GCPs present => FALLBACK") {
        val src = buildRamp(mem("gcp.tif"), 32, 32, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val gcps = Array(
              new org.gdal.gdal.GCP(10.0, 20.0, 0.0, 0.0),
              new org.gdal.gdal.GCP(110.0, 20.0, 32.0, 0.0),
              new org.gdal.gdal.GCP(10.0, 120.0, 0.0, 32.0)
            )
            val srs = new org.gdal.osr.SpatialReference(); srs.ImportFromEPSG(4326)
            src.SetGCPs(gcps, srs.ExportToWkt())
            src.FlushCache()
            assert(src.GetGCPCount() > 0, "fixture must have GCPs")
            val (out, meta) = WindowedExtract.extract(src, Map.empty, 2, 2, 16, 16)
            try {
                assert(meta("last_command").contains("gdal_translate"), "GCPs must take fallback")
                assert(out.getRasterXSize == 16 && out.getRasterYSize == 16)
            } finally { out.delete(); gdal.Unlink(meta("path")) }
        } finally src.delete()
    }

    test("edge/partial window (size not divisible by tile) => exact partial dims, no over-read") {
        // 50x50 raster, the bottom-right tile of a 32x32 tiling is 18x18.
        val src = buildRamp(mem("partial.tif"), 50, 50, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val (xs, ys, xo, yo) = (32, 32, 18, 18)
            val (out, _) = WindowedExtract.extract(src, Map.empty, xs, ys, xo, yo)
            try {
                assert(out.getRasterXSize == 18 && out.getRasterYSize == 18)
                val got = readBandDoubles(out, 1, 0, 0, 18, 18)
                val exp = readBandDoubles(src, 1, 32, 32, 18, 18)
                assert(got.sameElements(exp))
            } finally out.delete()
        } finally src.delete()
    }

    test("reTileIter emits every window, including all-NoData windows") {
        val src = buildRamp(mem("keep.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0), nodata = Some(7.0))
        val nd = Array.fill(16 * 16)(7.0)
        src.GetRasterBand(1).WriteRaster(0, 0, 16, 16, nd)    // window (0,0) all-NoData
        src.GetRasterBand(1).WriteRaster(32, 48, 16, 16, nd)  // window (r3,c2) all-NoData
        src.FlushCache()
        val tiles = ReTile.reTileIter(src, Map.empty, 16, 16).toList // iterator releases src on exhaustion
        try assert(tiles.size == 16)
        finally tiles.foreach { case (d, m) => d.delete(); gdal.Unlink(m("path")) }
    }

    test("getTile returns a live tile for a window with data and a valid metadata path") {
        val src = buildRamp(mem("live.tif"), 40, 40, 2, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val tile = ReTile.getTile(src, Map.empty, 5, 5, 20, 20)
            assert(tile != null)
            try {
                assert(tile._2("path").startsWith("/vsimem/"))
                assert(tile._1.getRasterXSize == 20 && tile._1.getRasterYSize == 20)
            } finally { tile._1.delete(); gdal.Unlink(tile._2("path")) }
        } finally src.delete()
    }

    test("metadata-map shape parity with executeTranslate keys") {
        val src = buildRamp(mem("shape.tif"), 32, 32, 1, gdalconstConstants.GDT_Float32,
          Array(0.0, 1.0, 0.0, 0.0, 0.0, -1.0))
        try {
            val (out, meta) = WindowedExtract.extract(src, Map.empty, 1, 1, 8, 8)
            try {
                val required = Set("path", "sourcePath", "driver", "format", "last_command",
                  "last_error", "all_parents", "size", "compression", "isZipped", "isSubset")
                assert(required.subsetOf(meta.keySet), s"missing keys: ${required -- meta.keySet}")
                assert(meta("driver") == "GTiff" && meta("format") == "GTiff")
                assert(meta("size") == "-1")
                assert(meta("path").startsWith("/vsimem/"))
            } finally { out.delete(); gdal.Unlink(meta("path")) }
        } finally src.delete()
    }

}
