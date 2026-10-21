package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.UUID
import scala.jdk.CollectionConverters._

/**
  * [[WarpSource]], the one place a heavyweight warp of a source with its own mask band reads that mask
  * (the warp consumers each pin their own wiring): which sources get the mask view, the fill arguments
  * per kind of source, a fill-marking warp's output, and the view's release (it never unlinks the source).
  *
  * Sources are 40x40 EPSG:4326 GTiffs over Norfolk, reopened read-only as tiles are, warped to EPSG:27700
  * (which rotates them, so the warp fills the corners). Pixel `p` holds `p + 1` (no 0, so 7 is pixel 6),
  * row 3 holds 65535 (the uint16 fallback), and the own mask hides rows 10-11.
  */
class WarpSourceTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private val N = 40
    private val ToBng = "gdalwarp -t_srs EPSG:27700 -r near"
    private val values = Array.tabulate(N * N)(p => if (p / N == 3) 65535.0 else (p + 1).toDouble)
    private val maskedRow: Int => Boolean = r => r == 10 || r == 11

    private def vsimemFiles(): Set[String] =
        Option(gdal.ReadDir("/vsimem/")).map(_.asScala.map(_.toString).toSet).getOrElse(Set.empty)

    /** The source: `bands` uint16 bands (or RGBA uint8 with `rgba`), its own mask when `masked`, the NoData
      * `declared` if given. Returns it and its path. */
    private def source(masked: Boolean, declared: Option[Double] = None, bands: Int = 1, rgba: Boolean = false): (Dataset, String) = {
        val path = s"/vsimem/warpsource_${UUID.randomUUID().toString.replace("-", "")}.tif"
        val made =
            if (rgba) GDALManager.gtiffDriver().Create(path, N, N, 4, GDT_Byte, Array("PHOTOMETRIC=RGB", "ALPHA=YES"))
            else GDALManager.gtiffDriver().Create(path, N, N, bands, GDT_UInt16)
        made.SetGeoTransform(Array(1.00, 0.005, 0.0, 52.60, 0.0, -0.005))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        made.SetProjection(sr.ExportToWkt())
        sr.delete()
        (1 to made.GetRasterCount()).foreach { b =>
            made.GetRasterBand(b).WriteRaster(0, 0, N, N, if (rgba) Array.fill(N * N)(200.0) else values)
            declared.foreach(made.GetRasterBand(b).SetNoDataValue)
        }
        if (masked) {
            made.CreateMaskBand(GMF_PER_DATASET)
            val mask = Array.tabulate[Byte](N * N)(p => if (maskedRow(p / N)) 0.toByte else 255.toByte)
            made.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, N, N, mask)
        }
        made.FlushCache()
        made.delete()
        val ds = gdal.Open(path)
        if (masked) ds.GetRasterBand(1).GetMaskFlags() shouldBe GMF_PER_DATASET
        (ds, path)
    }

    private def release(ds: Dataset, path: String): Unit = {
        ds.delete()
        gdal.Unlink(path)
    }

    /** `(valid, zeros, masked, noDataValued)` over the valid pixels of band `b` of `out`: warp-fill zeros,
      * values of a masked source pixel, and values equal to `noData` (or, undeclared, 65535 moved to 65534). */
    private def census(out: Dataset, b: Int, noData: Double, declared: Boolean): (Int, Int, Int, Int) = {
        val (w, h) = (out.getRasterXSize, out.getRasterYSize)
        val vals = new Array[Double](w * h)
        val mask = new Array[Byte](w * h)
        out.GetRasterBand(b).ReadRaster(0, 0, w, h, vals)
        out.GetRasterBand(b).GetMaskBand().ReadRaster(0, 0, w, h, mask)
        val v = vals.indices.filter(mask(_) != 0).map(vals(_))
        (
          v.size,
          v.count(_ == 0.0),
          v.count(x => x >= 1 && x <= N * N && maskedRow(((x - 1) / N).toInt)),
          v.count(x => x == noData || (!declared && x == 65534.0))
        )
    }

    private def noDataOf(ds: Dataset, b: Int): Option[Double] = {
        val nd = new Array[java.lang.Double](1)
        ds.GetRasterBand(b).GetNoDataValue(nd)
        Option(nd(0)).map(_.doubleValue())
    }

    test("a source without a mask band is warped as it is, with the fill arguments of its NoData") {
        val (plain, p1) = source(masked = false)
        val (declared, p2) = source(masked = false, declared = Some(7.0))
        val built = WarpSource.viewsBuilt.get()
        val (a, b) = (new WarpSource(plain), new WarpSource(declared))
        try {
            a.masked shouldBe false
            (a.dataset eq plain) shouldBe true
            a.alphaArgs shouldBe empty
            a.fillNoDataArgs shouldBe Seq("-dstnodata", "65535.0", "-srcnodata", "65535.0", "-wo", "UNIFIED_SRC_NODATA=NO")
            b.fillNoDataArgs shouldBe empty // the warp carries the source NoData
            WarpSource.viewsBuilt.get() shouldBe built
        } finally {
            a.close(); b.close()
            release(plain, p1); release(declared, p2)
        }
    }

    test("an alpha source is warped as it is, with no fill arguments: its alpha band marks the fill") {
        val (rgba, path) = source(masked = false, rgba = true)
        val src = new WarpSource(rgba)
        try {
            src.masked shouldBe false
            src.fillNoDataArgs shouldBe empty
            val (out, _) = WarpSource.warpFillAsNoData(rgba, "/vsimem/warpsource_rgba_out.tif", ToBng)
            try {
                out.GetRasterCount shouldBe 4
                (1 to 4).map(noDataOf(out, _)) shouldBe Seq.fill(4)(None)
            } finally RasterDriver.releaseDataset(out)
        } finally {
            src.close()
            release(rgba, path)
        }
    }

    test("a source with its own mask band is read through one mask view, released by close() alone") {
        val (masked, path) = source(masked = true, bands = 3)
        val built = WarpSource.viewsBuilt.get()
        val before = vsimemFiles()
        val src = new WarpSource(masked)
        try {
            src.masked shouldBe true
            (src.dataset eq masked) shouldBe false
            src.dataset.GetRasterCount shouldBe 4
            src.alphaArgs shouldBe Seq("-srcalpha")
            src.fillNoDataArgs shouldBe Seq("-dstnodata", "65535.0", "-srcnodata", "65535.0", "-wo", "UNIFIED_SRC_NODATA=NO")
            WarpSource.viewsBuilt.get() shouldBe built + 1
        } finally src.close()
        src.close() // idempotent
        withClue("the view left /vsimem files behind or removed the source's: ") { vsimemFiles() shouldBe before }
        masked.GetRasterBand(1).ReadRaster(0, 0, 1, 1, new Array[Double](1)) shouldBe CE_None
        release(masked, path)
    }

    for ((what, declared) <- Seq(("without NoData", None), ("with NoData 7", Some(7.0)))) {
        test(s"a fill-marking warp of a source $what and its own mask: the fill, masked pixels and NoData-valued pixels are NoData") {
            val (masked, path) = source(masked = true, declared = declared, bands = 3)
            val outPath = s"/vsimem/warpsource_out_${UUID.randomUUID().toString.replace("-", "")}.tif"
            try {
                val (out, metadata) = WarpSource.warpFillAsNoData(masked, outPath, ToBng)
                try {
                    val noData = declared.getOrElse(65535.0)
                    out.GetRasterCount shouldBe 3
                    for (b <- 1 to 3) {
                        out.GetRasterBand(b).getDataType shouldBe GDT_UInt16
                        noDataOf(out, b) shouldBe Some(noData)
                        val (valid, zeros, maskedValid, noDataValid) = census(out, b, noData, declared.isDefined)
                        valid should be > 0
                        withClue(s"band $b warp-fill zeros: ") { zeros shouldBe 0 }
                        withClue(s"band $b masked source pixels valid: ") { maskedValid shouldBe 0 }
                        withClue(s"band $b pixels equal to the NoData $noData valid (or moved by one): ") { noDataValid shouldBe 0 }
                    }
                    metadata("last_command") should include("-srcalpha")
                    metadata("driver") shouldBe "GTiff"
                    metadata("format") shouldBe "GTiff"
                    metadata("all_parents") shouldBe path
                } finally RasterDriver.releaseDataset(out)
            } finally release(masked, path)
        }
    }
}
