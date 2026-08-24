package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

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

}
