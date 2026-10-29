package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.gdal.gdal.gdal
import org.gdal.osr.{SpatialReference, osr, osrConstants}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}
import scala.util.Try

class GDALManagerProjGridsSpec extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
    }

    test("configureGDAL prepends PROJ_GRID_DIRS to PROJ_DATA and PROJ_LIB when key is present") {
        val config = ExpressionConfig(
          Map("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> "/Volumes/a:/Volumes/b"),
          null
        )
        try {
            GDALManager.configureGDAL(config)
            val projData = gdal.GetConfigOption("PROJ_DATA")
            projData should include("/Volumes/a")
            projData should include("/Volumes/b")
            // Standard PROJ dir must still be present
            projData should include("/usr/share/proj")
            // User dirs must appear before the standard dir
            projData.indexOf("/Volumes/a") should be < projData.indexOf("/usr/share/proj")
            // PROJ_LIB (legacy alias) must also carry the prepended value
            gdal.GetConfigOption("PROJ_LIB") should include("/Volumes/a")
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
        // PROJ_LIB is reset to /usr/share/proj by the base configureGDAL call
        gdal.GetConfigOption("PROJ_LIB") shouldBe "/usr/share/proj"
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
    }

}
