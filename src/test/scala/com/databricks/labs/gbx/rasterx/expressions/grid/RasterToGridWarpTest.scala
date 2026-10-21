package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.{BNG, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.collection.mutable

/**
  * The raster-to-grid reducers on a source in another CRS ([[GridReprojection.toGridCrs]] warps it to the
  * grid's): only the source's valid pixels are aggregated. The warp fills the pixels outside the source
  * footprint; for a source without NoData that fill is NoData, not valid zeros, and source pixels equal to
  * the fallback NoData (65535 for uint16) are NoData too. A source's own mask band still masks its pixels,
  * also when the source declares NoData (which keeps its NoData, so its 65535s stay real values).
  *
  * The oracle warps an index raster of the source (pixel `p` holds `p + 1`) with the same reprojection,
  * so every output pixel names the source pixel nearest-neighbour put there; the expected values of a cell
  * are those of its output pixels whose source pixel is valid. Each cell's values must match exactly.
  */
class RasterToGridWarpTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private val N = 40
    private val FallbackRow = 3
    private val MaskedRows = Set(10, 11)

    /** EPSG:27700 over Norfolk at 100 m, for the grids in EPSG:4326 (h3, quadbin). */
    private val Gt27700 = Array(600000.0, 100.0, 0.0, 320000.0, 0.0, -100.0)

    /** EPSG:4326 over Norfolk at 0.005 degrees, for BNG. Far from the 27700 central meridian, so either
      * warp rotates the grid by about 2.4 degrees and fills the corners. */
    private val Gt4326 = Array(1.00, 0.005, 0.0, 52.60, 0.0, -0.005)

    /** Source values: pixel `p` holds `p + 1` (no 0, so 7 is pixel 6), row 3 holds 65535 (the uint16 fallback). */
    private val values: Array[Double] =
        Array.tabulate(N * N)(p => if (p / N == FallbackRow) 65535.0 else (p + 1).toDouble)

    private def mem(name: String): String = s"/vsimem/rtgwarp_${UUID.randomUUID().toString.replace("-", "")}_$name"

    private def georeference(ds: Dataset, epsg: Int, gt: Array[Double]): Unit = {
        ds.SetGeoTransform(gt)
        val sr = new SpatialReference()
        sr.ImportFromEPSG(epsg)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
    }

    /** A uint16 GTiff of [[values]], reopened read-only as a tile is; when `masked`, an internal per-dataset
      * mask hides [[MaskedRows]]; `declared` sets the source NoData. Returns it and its path. */
    private def source(epsg: Int, gt: Array[Double], masked: Boolean, declared: Option[Double]): (Dataset, String) = {
        val path = mem("src.tif")
        val made = GDALManager.gtiffDriver().Create(path, N, N, 1, gdalconstConstants.GDT_UInt16)
        georeference(made, epsg, gt)
        made.GetRasterBand(1).WriteRaster(0, 0, N, N, values)
        declared.foreach(made.GetRasterBand(1).SetNoDataValue)
        if (masked) {
            made.CreateMaskBand(gdalconstConstants.GMF_PER_DATASET)
            val mask = Array.tabulate[Byte](N * N)(p => if (MaskedRows.contains(p / N)) 0.toByte else 255.toByte)
            made.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, N, N, mask)
        }
        made.FlushCache()
        made.delete()
        val ds = gdal.Open(path)
        val flags = ds.GetRasterBand(1).GetMaskFlags()
        if (masked) assert(flags == gdalconstConstants.GMF_PER_DATASET, s"fixture needs a per-dataset mask, flags $flags")
        (ds, path)
    }

    private def validSource(p: Int, masked: Boolean, declared: Option[Double]): Boolean =
        !(masked && MaskedRows.contains(p / N)) && values(p) != declared.getOrElse(65535.0)

    /** The expected values per rendered cell: an index raster of the source warped like the source. */
    private def expected(
        grid: GridSystem,
        epsg: Int,
        gt: Array[Double],
        resolution: Int,
        masked: Boolean,
        declared: Option[Double],
        isCellValid: Long => Boolean
    ): Map[Any, Seq[Double]] = {
        val index = GDALManager.memDriver().Create("", N, N, 1, gdalconstConstants.GDT_UInt32)
        georeference(index, epsg, gt)
        index.GetRasterBand(1).WriteRaster(0, 0, N, N, Array.tabulate(N * N)(p => p + 1))
        val (warped, _) = GDALWarp.executeWarp(
          mem("index.tif"),
          Array(index),
          Map.empty[String, String],
          command = s"gdalwarp -t_srs EPSG:${grid.crsSrid} -r near -dstnodata 0"
        )
        try {
            val (w, h) = (warped.getRasterXSize, warped.getRasterYSize)
            val idx = new Array[Int](w * h)
            warped.GetRasterBand(1).ReadRaster(0, 0, w, h, idx)
            val wgt = warped.GetGeoTransform
            val out = mutable.Map.empty[Any, mutable.ArrayBuffer[Double]]
            for (k <- idx.indices if idx(k) > 0 && validSource(idx(k) - 1, masked, declared)) {
                val (x, y) = (k % w + 0.5, k / w + 0.5)
                val cell = grid.pointToCellID(wgt(0) + x * wgt(1) + y * wgt(2), wgt(3) + x * wgt(4) + y * wgt(5), resolution)
                if (isCellValid(cell)) out.getOrElseUpdate(grid.renderCellId(cell), mutable.ArrayBuffer.empty) += values(idx(k) - 1)
            }
            out.map { case (c, v) => c -> v.sorted.toSeq }.toMap
        } finally {
            RasterDriver.releaseDataset(warped)
            index.delete()
        }
    }

    private val collect: mutable.ArrayBuffer[Double] => Seq[Double] = _.sorted.toSeq

    private val grids: Seq[(String, GridSystem, Int, Int, Array[Double], Long => Boolean)] = Seq(
      ("h3", H3, 9, 27700, Gt27700, _ => true),
      ("quadbin", Quadbin, 15, 27700, Gt27700, _ => true),
      ("bng", BNG, 3, 4326, Gt4326, BNG.isValid)
    )

    private val sources: Seq[(String, Boolean, Option[Double])] = Seq(
      ("without NoData", false, None),
      ("without NoData, with its own mask", true, None),
      ("with NoData 7 and its own mask", true, Some(7.0))
    )

    for ((name, grid, res, epsg, gt, isCellValid) <- grids; (what, masked, declared) <- sources) {
        test(s"(a) $name raster-to-grid of a source in another CRS $what: only its valid pixels are aggregated") {
            val (ds, _) = source(epsg, gt, masked, declared)
            val want = expected(grid, epsg, gt, res, masked, declared, isCellValid)
            val got =
                try RasterToGridGeneric.execute[Seq[Double]](grid, ds, res, collect, isCellValid).head.toMap
                finally RasterDriver.releaseDataset(ds)
            val all = got.values.flatten.toSeq
            val noData = declared.getOrElse(65535.0)
            val zeros = all.count(_ == 0.0)
            val maskedSeen = all.count(v => masked && v >= 1 && v <= N * N && MaskedRows.contains(((v - 1) / N).toInt))
            val noDataSeen = all.count(v => v == noData || (declared.isEmpty && v == 65534.0))
            assert(want.nonEmpty, "the oracle found no valid pixel")
            assert(zeros == 0, s"$zeros warp-fill zeros were aggregated (of ${all.size} values)")
            assert(maskedSeen == 0, s"$maskedSeen masked source pixels were aggregated")
            assert(noDataSeen == 0, s"$noDataSeen source pixels equal to the NoData $noData (or moved by one) were aggregated")
            val total = want.values.map(_.size).sum
            assert(all.size == total, s"${all.size} values aggregated, expected $total")
            val diff = (want.keySet ++ got.keySet).filter(c => want.get(c) != got.get(c))
            assert(diff.isEmpty, s"${diff.size} cells differ from the oracle, e.g. ${diff.take(3).map(c => (c, want.get(c), got.get(c)))}")
        }
    }
}
