package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.bench.RequiresProjIsolation
import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.operations.{ProjGridRegistry, SpatialRefOps}
import com.databricks.labs.gbx.rasterx.expressions.RST_TransformCrs
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.expressions.ST_TransformCrs
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.util.control.NonFatal

/** End-to-end functional proof that a registered custom PROJ grid is actually
  * consulted by the HEAVY tier, plus cross-tier parity with the light result.
  *
  * ## The chain under test
  *
  *   ProjGridRegistry.set(dirs)  →  ExpressionConfig.apply(spark) folds them under
  *   `spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS`  →  GDALManager.configureGDAL
  *   PREPENDS them to PROJ_DATA/PROJ_LIB via in-JVM gdal.SetConfigOption  →  the
  *   in-JVM gdal.Warp (heavy raster RST_TransformCrs) and OGR CoordinateTransformation
  *   (heavy vector ST_TransformCrs) find the grid on that path.
  *
  * Heavy vector `ST_TransformCrs` is GDAL/OGR-backed (`org.gdal.osr.SpatialReference`
  * + `org.gdal.osr.CoordinateTransformation` + OGR `Geometry.Transform`, see
  * `TransformCrsCore`), so the same in-JVM `SetConfigOption("PROJ_DATA", …)` that
  * reaches `gdal.Warp` also reaches the vector transform — heavy vector honors the
  * registered grid, giving a genuine cross-tier parity assertion against the light
  * pyvx result.
  *
  * ## Fixture — the same synthetic NTv2 grid the light functional test uses
  *
  * `synthetic.gsb` applies a CONSTANT +30 arc-second latitude shift (0 in longitude)
  * everywhere inside its box (50..53 N, 1 E .. 1 W). The source CRS
  * `+proj=longlat +ellps=GRS80 +nadgrids=synthetic.gsb +no_defs` REQUIRES that grid to
  * build a datum transform to WGS84 — so "the CRS string finds the grid" is the thing
  * under test. A constant shift makes the expected output exact: a point at lat 51.5
  * becomes 51.508333… in WGS84 (identical to
  * `python/geobrix/test/pyvx/test_grid_shift_accuracy.py`, light Task 5), and a raster's
  * north extent shifts up by 0.008333 deg.
  *
  * ## Why RED and GREEN are structured the way they are (a real PROJ property)
  *
  * PROJ caches a grid lookup PROCESS-GLOBALLY. Verified empirically in the dev
  * container (osgeo.osr): once a `+nadgrids` transform for a given grid FILENAME has
  * either found or failed to find its grid, a later `gdal.SetConfigOption("PROJ_DATA", …)`
  * that adds/removes the grid dir does NOT change the outcome for that filename in the
  * same process — only the explicit `OSRSetPROJSearchPaths` API clears it. This is
  * exactly why the feature's contract is "register at session start, before the first
  * GDAL use": the config-option prepend takes effect on first use and is not meant to be
  * toggled live. Consequently a same-filename without-grid→with-grid toggle is NOT
  * expressible in one JVM through the feature's own mechanism. So:
  *
  *   - RED / GREEN at the CONFIG layer (no transform, no PROJ cache involved): an EMPTY
  *     registry leaves the grid dir OFF PROJ_DATA (the pre-feature behavior); a populated
  *     registry PREPENDS it. This is precisely Task 8's prepend and is the bit that would
  *     be RED before Task 8.
  *   - GREEN at the TRANSFORM layer: with the grid registered before first use, the exact
  *     +30 arc-second shift appears in both the raster warp and the vector transform
  *     (the latter equal to the light coordinate — cross-tier parity).
  *   - RED at the TRANSFORM layer: a CRS naming a grid NOT on the registered path
  *     (`__gbx_unregistered__.gsb`) cannot be resolved, so no shift is fabricated — the
  *     transform only applies a grid it actually finds on the registered path.
  */
class RST_TransformCrsGridSpec extends AnyFunSuite with BeforeAndAfterAll {

    // Source CRS that references the registered grid BY FILENAME (same as light Task 5).
    private val SRC_CRS = "+proj=longlat +ellps=GRS80 +nadgrids=synthetic.gsb +no_defs"
    // Source CRS naming a grid that is NOT on the registered path (transform-layer RED).
    private val UNREG_CRS = "+proj=longlat +ellps=GRS80 +nadgrids=__gbx_unregistered__.gsb +no_defs"
    private val SHIFT_DEG = 30.0 / 3600.0 // +30 arc-seconds of latitude = 0.008333…

    // Raster placed inside the grid box, centered on the light test point (0, 51.5).
    // Small pixels (0.001 deg ~ 3.6") so the 0.00833 deg shift is ~8 px — far above any
    // pixel-snapping in the warped output extent.
    private val R_ORIGIN_LON = -0.05
    private val R_ORIGIN_LAT = 51.55 // top-left latitude (north edge)
    private val R_PIXEL = 0.001
    private val R_SIZE = 100

    // Light-tier reference point + expected shifted latitude (parity target).
    private val PT_LON = 0.0
    private val PT_LAT = 51.5
    private val EXPECT_LAT = PT_LAT + SHIFT_DEG // 51.508333… — identical to light EXPECT_LAT

    private var gridDir: String = _
    private var spark: SparkSession = _
    private val gf = new GeometryFactory()

    override def beforeAll(): Unit = {
        // Resolve the committed fixture dir. Prefer the on-cluster /Volumes path (the
        // documented registered-grid location, mounted in the dev container); fall back
        // to the repo checkout so the test does not depend on the mount.
        val candidates = Seq(
          "/Volumes/main/geobrix_samples/geobrix-examples/proj-grids",
          "/root/geobrix/sample-data/Volumes/main/geobrix_samples/geobrix-examples/proj-grids"
        )
        gridDir = candidates.find(d => Files.isRegularFile(Paths.get(d, "synthetic.gsb")))
            .getOrElse(candidates.head)

        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp") // sets GDAL_SKIP before AllRegister; PROJ_DATA unset
        gdal.AllRegister()
        GDALManager.initOgr()

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("RST_TransformCrsGridSpec")
            .getOrCreate()

        // Register the grid and run the FEATURE fold+prepend EXACTLY ONCE, before any
        // transform (the feature's session-start contract). Re-running configureGDAL AFTER a
        // grid transform has loaded the grid transiently resets PROJ_LIB and breaks the next
        // transform's grid resolution — verified in the dev container — so no test below
        // re-configures; they all share this one grid-on-path setup.
        registerAndConfigure(Seq(gridDir))
    }

    override def afterAll(): Unit = {
        ProjGridRegistry.set(Seq.empty, replace = true)
        if (spark != null) spark.stop()
    }

    // --- Helpers -----------------------------------------------------------

    private val PROJ_GRID_DIRS_KEY = "spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS"

    /** Register `dirs` and run the FEATURE fold+prepend: ProjGridRegistry.set →
      * ExpressionConfig.apply(spark) → GDALManager.configureGDAL. Returns the folded config. */
    private def registerAndConfigure(dirs: Seq[String]): ExpressionConfig = {
        ProjGridRegistry.set(dirs, replace = true)
        val cfg = ExpressionConfig.apply(spark)
        GDALManager.configureGDAL(cfg)
        cfg
    }

    /** Build a tiny GTiff in /vsimem tagged with the given (grid-referencing) source CRS.
      * Throws if the CRS cannot be resolved (e.g. its `+nadgrids` grid is not on the path). */
    private def makeSourceRaster(path: String, srcCrs: String): Dataset = {
        val ds = gdal.GetDriverByName("GTiff").Create(path, R_SIZE, R_SIZE, 1, gdalconstConstants.GDT_Byte)
        // GeoTransform: (originLon, +pixel, 0, originLat, 0, -pixel)
        ds.SetGeoTransform(Array(R_ORIGIN_LON, R_PIXEL, 0.0, R_ORIGIN_LAT, 0.0, -R_PIXEL))
        val sr = SpatialRefOps.resolveCrs(srcCrs)
        ds.SetSpatialRef(sr)
        sr.delete()
        ds.GetRasterBand(1).Fill(100)
        ds.FlushCache()
        ds
    }

    /** Reproject the synthetic source raster `srcCrs` -> EPSG:4326 through the real heavy
      * expression path (RST_TransformCrs.execute -> RasterProject -> gdal.Warp). Returns the
      * north-edge latitude of the warped output, or None if the warp failed / raised (e.g.
      * the grid was required but not on the search path). */
    private def warpNorthEdgeLat(srcCrs: String): Option[Double] = {
        val srcPath = s"/vsimem/gridspec_src_${UUID.randomUUID().toString.replace("-", "")}.tif"
        try {
            val ds = makeSourceRaster(srcPath, srcCrs)
            try {
                val (resultDs, _) = RST_TransformCrs.execute(ds, Map.empty[String, String], "EPSG:4326")
                if (resultDs == null) None
                else {
                    val northLat = resultDs.GetGeoTransform()(3)
                    resultDs.delete()
                    Some(northLat)
                }
            } finally {
                ds.delete()
                gdal.Unlink(srcPath)
            }
        } catch {
            case NonFatal(_) => None
        }
    }

    // --- Tests -------------------------------------------------------------

    test("synthetic NTv2 grid fixture is present") {
        assert(Files.isRegularFile(Paths.get(gridDir, "synthetic.gsb")),
          s"synthetic.gsb missing at $gridDir; regenerate via gen_synthetic_gsb.py")
    }

    test("ExpressionConfig.apply folds ProjGridRegistry dirs under the PROJ_GRID_DIRS key (RED empty, GREEN populated)") {
        // RED: an empty registry contributes no key, so configureGDAL has nothing to prepend
        // (this is the pre-feature state).
        ProjGridRegistry.set(Seq.empty, replace = true)
        ExpressionConfig.apply(spark).configs.get(PROJ_GRID_DIRS_KEY) shouldBe None

        // GREEN: a populated registry folds the dirs under the key GDALManager prepends.
        ProjGridRegistry.set(Seq(gridDir), replace = true)
        ExpressionConfig.apply(spark).configs.get(PROJ_GRID_DIRS_KEY) shouldBe Some(gridDir)
    }

    test("GDALManager.configureGDAL prepended the registered grid dir to the PROJ search path") {
        // The single beforeAll registerAndConfigure ran the full chain
        // (ProjGridRegistry.set -> ExpressionConfig.apply fold -> configureGDAL prepend). Assert
        // the grid dir landed on PROJ_DATA/PROJ_LIB, ahead of the stock dir — Task 8's prepend,
        // verified end-to-end in a live session (not just the unit-level GDALManagerProjGridsSpec).
        val projData = gdal.GetConfigOption("PROJ_DATA")
        projData should include(gridDir)
        projData should include("/usr/share/proj")
        projData.indexOf(gridDir) should be < projData.indexOf("/usr/share/proj")
        gdal.GetConfigOption("PROJ_LIB") should include(gridDir)
    }

    test("heavy vector ST_TransformCrs applies the grid — cross-tier parity with light", RequiresProjIsolation) {
        val g = gf.createPoint(new Coordinate(PT_LON, PT_LAT)) // plain WKB, no SRID
        val result = ST_TransformCrs.eval(
          JTS.toWKB(g),
          UTF8String.fromString("EPSG:4326"),
          UTF8String.fromString(SRC_CRS)
        )
        assert(result != null, "ST_TransformCrs returned NULL — the registered grid was not consulted")
        assert(result.isInstanceOf[Array[Byte]], s"expected EWKB, got ${result.getClass.getSimpleName}")
        val out = JTS.fromWKB(result.asInstanceOf[Array[Byte]])

        // Exact +30 arc-second latitude shift, 0 in longitude — identical to the light-tier
        // assertion in test_grid_shift_accuracy.py (cross-tier parity: both tiers apply the grid).
        out.getCoordinate.getX shouldBe (PT_LON +- 1e-6)
        out.getCoordinate.getY shouldBe (EXPECT_LAT +- 1e-6)
    }

    test("heavy raster RST_TransformCrs consults the registered grid (RED unfindable, GREEN registered)", RequiresProjIsolation) {
        // RED: a CRS naming a grid NOT on the registered path cannot be resolved -> no output,
        // so no shift is fabricated.
        warpNorthEdgeLat(UNREG_CRS) shouldBe None

        // GREEN: the registered synthetic.gsb is found -> the north edge shifts up by the
        // grid's +30 arc-seconds.
        val greenLat = warpNorthEdgeLat(SRC_CRS)
        assert(greenLat.isDefined, "GREEN warp produced no output — the registered grid was not consulted")
        greenLat.get shouldBe (R_ORIGIN_LAT + SHIFT_DEG +- 0.0015)
    }
}
