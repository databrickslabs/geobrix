package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.gdal.gdal.gdal
import org.gdal.osr.{SpatialReference, osr, osrConstants}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import scala.util.Try

class GDALManagerProjGridsSpec extends AnyFunSuite with BeforeAndAfterAll {

    // The PROJ data dir of the GDAL this JVM runs: the payload's share/proj (its JNI, or /usr/lib/libgdalalljni.so
    // linked into a payload, as in the dev container), else the system PROJ's /usr/share/proj.
    private def baseProjDir: String = GDALManager.ensureJniLoaded(None).projData.getOrElse("/usr/share/proj")

    /** The search path GDAL hands each thread's PROJ context. */
    private def searchPath(): Seq[String] = Option(osr.GetPROJSearchPaths()).map(_.asScala.map(_.toString).toSeq).getOrElse(Nil)

    /** ImportFromEPSG's result for `code` on a new thread: a new PROJ context, which opens proj.db at its first use. */
    private def epsgOnNewThread(code: Int): Int = {
        var out = -1
        val t = new Thread(() => {
            val sr = new SpatialReference()
            try out = Try(sr.ImportFromEPSG(code)).getOrElse(-99) finally sr.delete()
        })
        t.start()
        t.join()
        out
    }

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
    }

    test("configureGDAL prepends PROJ_GRID_DIRS to the PROJ search path when key is present") {
        val config = ExpressionConfig(
          Map("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> "/Volumes/a:/Volumes/b"),
          null
        )
        try {
            GDALManager.configureGDAL(config)
            // the user dirs, then the GDAL's own PROJ dir
            searchPath() shouldBe Seq("/Volumes/a", "/Volumes/b", baseProjDir)
            // PROJ_DATA and PROJ_LIB (legacy alias) name the one dir with proj.db: GDAL makes either one search-path entry
            Seq("PROJ_DATA", "PROJ_LIB").foreach(k => gdal.GetConfigOption(k) shouldBe baseProjDir)
        } finally {
            gdal.SetConfigOption("PROJ_DATA", null)
            GDALManager.configureGDAL("/tmp", "/tmp")
        }
    }

    test("configureGDAL does NOT set PROJ_GRID_DIRS as a raw GDAL config option") {
        // First make sure PROJ_GRID_DIRS is not set from a previous test
        gdal.SetConfigOption("PROJ_GRID_DIRS", null)
        val config = ExpressionConfig(
          Map("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> "/Volumes/x"),
          null
        )
        try {
            GDALManager.configureGDAL(config)
            // The raw GDAL option must not be set — it would be a bogus config key
            Option(gdal.GetConfigOption("PROJ_GRID_DIRS")) shouldBe None
        } finally {
            gdal.SetConfigOption("PROJ_DATA", null)
            gdal.SetConfigOption("PROJ_GRID_DIRS", null)
            GDALManager.configureGDAL("/tmp", "/tmp")
        }
    }

    test("configureGDAL skips PROJ_DATA prepend when PROJ_GRID_DIRS is absent") {
        val config = ExpressionConfig(Map.empty, null)
        GDALManager.configureGDAL(config)
        // PROJ_LIB ends at the GDAL's own PROJ dir after the base configureGDAL call (grid dirs an init applied earlier
        // in this JVM stay ahead of it)
        gdal.GetConfigOption("PROJ_LIB").split(":").last shouldBe baseProjDir
    }

    test("a later init's PROJ grid dirs reach PROJ, after an earlier init without them") {
        // the committed NTv2 fixture: a constant +30 arc-second latitude shift over 50..53 N, 1 W .. 1 E
        val fixture = Seq(
          "/root/geobrix/sample-data/Volumes/main/geobrix_samples/geobrix-examples/proj-grids/synthetic.gsb",
          "sample-data/Volumes/main/geobrix_samples/geobrix-examples/proj-grids/synthetic.gsb"
        ).map(Paths.get(_)).find(Files.isRegularFile(_))
        assume(fixture.isDefined, "PRECONDITION MISSING: the synthetic.gsb fixture; this row not run")
        GDALManager.init(ExpressionConfig(Map.empty, null)) // an earlier init, with no grid dirs
        // a grid file name PROJ has never looked up in this JVM (PROJ caches each name's outcome)
        val dir = Files.createTempDirectory("gbx_later_grids")
        val name = s"gbx_later_${System.nanoTime}.gsb"
        Files.copy(fixture.get, dir.resolve(name))
        GDALManager.init(ExpressionConfig(Map("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> dir.toString), null))
        val src = new SpatialReference()
        src.ImportFromProj4(s"+proj=longlat +ellps=GRS80 +nadgrids=$name +no_defs")
        val dst = new SpatialReference()
        dst.ImportFromEPSG(4326)
        Seq(src, dst).foreach(_.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER))
        // null (or a raise, with exceptions on) when PROJ cannot find the grid
        val ct = Try(osr.CreateCoordinateTransformation(src, dst)).toOption.flatMap(Option(_))
        withClue(s"no transform through $name on $dir: ") { ct should not be empty }
        ct.get.TransformPoint(0.0, 51.5)(1) shouldBe (51.5 + 30.0 / 3600.0 +- 1e-6)
        // a base reset keeps the applied grid dirs on the search path
        GDALManager.configureGDAL("/tmp", "/tmp")
        searchPath() should contain(dir.toString)
    }

    test("re-applying a PROJ path of several directories never leaves a new thread without PROJ's database") {
        // GDAL makes a PROJ_DATA or PROJ_LIB config option ONE search-path entry: a colon-joined value set while other
        // threads run is a path on which a new PROJ context finds no proj.db (an EPSG code does not resolve)
        val grid = Files.createTempDirectory("gbx_path_grids").toString
        GDALManager.init(ExpressionConfig(Map("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> grid), null))
        val stop = new AtomicBoolean(false)
        // a recovery on another thread, as a Spark task whose PROJ context lost its database runs one
        val writer = new Thread(() => while (!stop.get()) GDALManager.recoverProjData())
        writer.start()
        val failed =
            try (1 to 300).map(_ => epsgOnNewThread(27700)).count(_ != 0)
            finally { stop.set(true); writer.join() }
        failed shouldBe 0
        searchPath() should contain inOrder (grid, baseProjDir)
        Seq("PROJ_DATA", "PROJ_LIB").foreach(k => gdal.GetConfigOption(k) shouldBe baseProjDir)
    }

    test("a user's PROJ_DATA of several directories is a search path, not one directory") {
        val user = Files.createTempDirectory("gbx_user_proj").toString
        try {
            GDALManager.configureGDAL(ExpressionConfig(Map("spark.gdal.PROJ_DATA" -> s"$user:$baseProjDir"), null))
            searchPath() shouldBe Seq(user, baseProjDir)
            epsgOnNewThread(2154) shouldBe 0
        } finally GDALManager.configureGDAL("/tmp", "/tmp")
        searchPath() should not contain user
    }

    test("a user's PROJ_LIB of several directories is a search path after the grid directories") {
        val (user, grid) = (Files.createTempDirectory("gbx_user_proj").toString, Files.createTempDirectory("gbx_user_grid").toString)
        try {
            GDALManager.configureGDAL(ExpressionConfig(
              Map("spark.gdal.PROJ_LIB" -> s"$user:$baseProjDir", "spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> grid), null
            ))
            // grid directories, then the user's directories in their order; PROJ_DATA and PROJ_LIB name the one with proj.db
            searchPath() should contain inOrder (grid, user, baseProjDir)
            Seq("PROJ_DATA", "PROJ_LIB").foreach(k => gdal.GetConfigOption(k) shouldBe baseProjDir)
            epsgOnNewThread(2154) shouldBe 0
        } finally GDALManager.configureGDAL("/tmp", "/tmp")
        searchPath() should not contain user
    }

    test("a search path no directory of which holds proj.db is applied whole, and PROJ_DATA names its last directory") {
        val (a, b) = (Files.createTempDirectory("gbx_no_db_a").toString, Files.createTempDirectory("gbx_no_db_b").toString)
        val before = searchPath()
        try {
            GDALManager.setProjSearchPath(Seq(a, b))
            searchPath() shouldBe Seq(a, b)
            Seq("PROJ_DATA", "PROJ_LIB").foreach(k => gdal.GetConfigOption(k) shouldBe b)
        } finally GDALManager.setProjSearchPath(before)
        searchPath() shouldBe before
        gdal.GetConfigOption("PROJ_DATA") shouldBe baseProjDir
    }

    test("a search-path directory that is gone does not keep PROJ from its database") {
        val gone = Files.createTempDirectory("gbx_gone_dir")
        Files.delete(gone)
        val before = searchPath()
        try {
            osr.SetPROJSearchPaths(new java.util.Vector[String](Seq(gone.toString, baseProjDir).asJava))
            epsgOnNewThread(3857) shouldBe 0
        } finally osr.SetPROJSearchPaths(new java.util.Vector[String](before.asJava))
    }

}
