package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_Crs, RST_SRID}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.operations.{ProjDatabase, ProjDatabaseException, SpatialRefOps}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, V2Tile}
import org.apache.spark.sql.types.BinaryType
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.osr.{SpatialReference, osr}
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.jdk.CollectionConverters._

/**
  * A Spark task thread whose PROJ context cannot read PROJ's database (proj.db) reads a GeoTIFF's CRS from its keys
  * alone: GDAL gives a `LOCAL_CS` with no authority code. `gbx_rst_srid` then answered NULL and `gbx_rst_crs` a
  * `LOCAL_CS` WKT, with no error, where the light level gives 27700 and `EPSG:27700`. The read must give the tile's CRS
  * (PROJ re-pointed at its database and the tile read again), or fail with the reason, never a silent NULL.
  */
class RST_ProjDatabaseTest extends PlanTest with SilentSparkSession {

    private def exprConf: UTF8String = UTF8String.fromString(ExpressionConfig(spark).toB64)

    /** A 16 x 16 GeoTIFF tile on the British National Grid. */
    private def bngTile(): InternalRow = {
        GDALManager.init(ExpressionConfig(spark))
        val ds = GDALManager.memDriver().Create("", 16, 16, 1)
        ds.SetGeoTransform(Array(400000.0, 10.0, 0.0, 300000.0, 0.0, -10.0))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        try V2Tile.row(cellid = 0L, raster = RasterDriver.writeToBytes(ds, Map("driver" -> "GTiff")),
              metadata = SerializationUtil.toMapData(Map("driver" -> "GTiff")))
        finally RasterDriver.releaseDataset(ds)
    }

    /** The directory PROJ reads now: its search path, else PROJ_DATA / PROJ_LIB. */
    private def projDir(): Path = {
        val set = Option(osr.GetPROJSearchPaths()).map(_.asScala.map(_.toString).toSeq).getOrElse(Nil)
        val dirs = set ++ Seq("PROJ_DATA", "PROJ_LIB").flatMap(k => Option(gdal.GetConfigOption(k))).flatMap(_.split(":"))
        dirs.map(Paths.get(_)).find(d => Files.isReadable(d.resolve("proj.db"))).getOrElse(Paths.get("/usr/share/proj"))
    }

    /** Run `body` with PROJ pointed at a copy of its data directory that is then deleted: a thread that creates its
      * PROJ context afterwards finds no proj.db (as an executor whose payload directory went away). */
    private def withProjDatabaseGone[T](body: => T): T = {
        val dir = projDir()
        val copy = Files.createTempDirectory("gbx_proj_gone")
        Files.list(dir).iterator().asScala.filter(Files.isRegularFile(_)).foreach(p => Files.copy(p, copy.resolve(p.getFileName)))
        osr.SetPROJSearchPaths(new java.util.Vector[String](java.util.List.of(copy.toString)))
        Files.list(copy).iterator().asScala.foreach(Files.delete(_))
        try body
        finally {
            osr.SetPROJSearchPaths(new java.util.Vector[String](java.util.List.of(dir.toString)))
            Files.deleteIfExists(copy)
        }
    }

    /** `f` on `n` new threads at once (each a fresh PROJ context, as new Spark task threads). */
    private def onFreshThreads[T](n: Int)(f: () => T): Seq[T] = {
        val pool = Executors.newFixedThreadPool(n)
        try pool.invokeAll((1 to n).map(_ => new Callable[T] { def call(): T = f() }).asJava).asScala.map(_.get()).toSeq
        finally { pool.shutdown(); pool.awaitTermination(1, TimeUnit.MINUTES) }
    }

    test("srid and crs on fresh threads whose PROJ database is gone are the tile's, not NULL") {
        val tile = bngTile()
        val c = exprConf
        RST_SRID.eval(tile, c) shouldBe 27700
        withProjDatabaseGone {
            val out = onFreshThreads(8)(() => (RST_SRID.eval(tile, c), RST_Crs.eval(tile, c)))
            out.map(_._1) shouldBe Seq.fill(8)(Integer.valueOf(27700))
            out.map(o => Option(o._2).map(_.toString).orNull) shouldBe Seq.fill(8)("EPSG:27700")
        }
    }

    test("an EPSG code resolves on fresh threads whose PROJ database is gone") {
        withProjDatabaseGone {
            onFreshThreads(4)(() => {
                val sr = SpatialRefOps.resolveCrs("27700")
                try SpatialRefOps.crsToCanonical(sr) finally sr.delete()
            }) shouldBe Seq.fill(4)("EPSG:27700")
        }
    }

    test("a PROJ database that stays unreadable fails the read with the reason, never NULL") {
        val e = intercept[ProjDatabaseException] {
            ProjDatabase.guardedWith[Int](() => 0, _ == 0, () => false, () => Seq("/gone/proj"))
        }
        e.getMessage should include("proj.db")
        e.getMessage should include("/gone/proj")
        e.getMessage should include("Fix:")
        // the scalar safeEval (NULL on a data error) lets it through
        intercept[ProjDatabaseException] {
            RST_ErrorHandler.safeEval(() => throw new ProjDatabaseException("gone"), bngTile(), BinaryType, exprConf)
        }
    }

    test("a thread checks PROJ's database once per search path; a failed check is checked again") {
        onFreshThreads(1)(() => {
            var calls = 0
            val pass = () => { calls += 1; true }
            ProjDatabase.cachedWith(() => { calls += 1; false }) shouldBe false
            ProjDatabase.cachedWith(pass) shouldBe true
            ProjDatabase.cachedWith(pass) shouldBe true
            val beforeChange = calls
            // a new search path: the thread checks again
            ProjDatabase.invalidate()
            ProjDatabase.cachedWith(pass) shouldBe true
            (beforeChange, calls)
        }) shouldBe Seq((2, 3))
    }

    test("a CRS with no authority code is the data's own when PROJ's database is readable") {
        var recovered = false
        ProjDatabase.guardedWith[Int](() => 0, _ == 0, () => true, () => { recovered = true; Nil }) shouldBe 0
        recovered shouldBe false
        // an answer that is not suspect is never checked
        ProjDatabase.guardedWith[Int](() => 27700, _ == 0, () => fail("checked"), () => fail("recovered")) shouldBe 27700
    }
}
