package com.databricks.labs.gbx.rasterx.operator

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_Proximity
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

/**
  * `rst_proximity` is the exact Euclidean distance, as the lightweight function is (GDAL's `ComputeProximity` is up to ~6% off).
  */
class ProximityExactTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private def mem(name: String): String = s"/vsimem/l1pf_${UUID.randomUUID().toString.replace("-", "")}_$name"

    /** A Float64 raster of `values` (row-major), top-left origin `(0, h * dy)`, `dx` by `dy` cells, in EPSG:32633. */
    private def raster(w: Int, h: Int, dx: Double, dy: Double, values: Array[Double]): Dataset = {
        val ds = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(0.0, dx, 0.0, h * dy, 0.0, -dy))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(32633)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, w, h, values)
        ds
    }

    private def pixels(ds: Dataset): Array[Double] = {
        val buf = new Array[Double](ds.GetRasterXSize * ds.GetRasterYSize)
        ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, buf)
        buf
    }

    // ---- rst_proximity: exact ------------------------------------------------------------------------------------

    /** Brute force: the distance from every pixel to its nearest target pixel, in ground units. */
    private def bruteForce(w: Int, h: Int, dx: Double, dy: Double, isTarget: Int => Boolean): Array[Double] = {
        val targets = (0 until w * h).filter(isTarget)
        Array.tabulate(w * h) { i =>
            val (x, y) = (i % w, i / w)
            targets.map { t => math.hypot((x - t % w) * dx, (y - t / w) * dy) }.min
        }
    }

    private def proximity(
        w: Int, h: Int, dx: Double, dy: Double, values: Array[Double],
        targetValues: Option[String], units: String, maxDistance: Option[Double]
    ): Array[Double] = {
        val src = raster(w, h, dx, dy, values)
        val (out, _) = RST_Proximity.execute(src, Map.empty, targetValues, units, maxDistance)
        try pixels(out)
        finally { out.delete(); src.delete() }
    }

    test("rst_proximity is the exact Euclidean distance (GEO units, non-square cells)") {
        val (w, h, dx, dy) = (47, 31, 30.0, 12.5)
        val rnd = new scala.util.Random(7)
        val values = Array.fill(w * h)(if (rnd.nextInt(60) == 0) 1.0 else 0.0)
        val expected = bruteForce(w, h, dx, dy, i => values(i) != 0.0)
        val got = proximity(w, h, dx, dy, values, None, "GEO", None)
        val worst = got.zip(expected).map { case (g, e) => math.abs(g - e) / math.max(e, 1e-12) }.max
        // Float32 output: an exact distance is within one float ulp (~6e-8); GDAL's approximation was off by ~6%.
        assert(worst < 1e-6, s"worst relative error $worst")
    }

    test("rst_proximity in PIXEL units, with target values and a distance cap") {
        val (w, h) = (40, 40)
        val rnd = new scala.util.Random(11)
        val values = Array.fill(w * h)(rnd.nextInt(90) match { case 0 => 2.0; case 1 => 3.4; case 2 => 5.0; case _ => 0.0 })
        // target 3 matches the pixel 3.4 (rounded, as GDAL reads it); 2 matches the 2s; 5 is not a target
        val expected = bruteForce(w, h, 1.0, 1.0, i => values(i) == 2.0 || values(i) == 3.4)
        val got = proximity(w, h, 30.0, 30.0, values, Some("2,3"), "PIXEL", Some(6.5))
        val cap = 6.5f
        got.indices.foreach { i =>
            val e = expected(i).toFloat
            if (e > cap) assert(got(i) == -1.0, s"pixel $i beyond the cap must be NoData; got ${got(i)}")
            else assert(math.abs(got(i) - e) <= 1e-6 * math.max(e, 1.0), s"pixel $i: ${got(i)} vs $e")
        }
    }

    test("rst_proximity with no source pixel is all NoData") {
        val got = proximity(8, 8, 1.0, 1.0, Array.fill(64)(0.0), None, "PIXEL", None)
        assert(got.forall(_ == -1.0))
    }

    test("rst_proximity refuses a target_values entry that is not a number, naming the argument and the value") {
        val src = raster(4, 4, 1.0, 1.0, Array.fill(16)(1.0))
        try {
            val e = intercept[IllegalArgumentException] {
                RST_Proximity.execute(src, Map.empty, Some("1, x ,3"), "PIXEL", None)
            }
            assert(e.getMessage.contains("target_values") && e.getMessage.contains("'1, x ,3'"), e.getMessage)
        } finally src.delete()
    }

}
