package tests.docs.scala.advanced

import com.databricks.labs.gbx.rasterx.expressions.accessors.{RST_BoundingBox, RST_GeoReference}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for code examples in docs/docs/advanced/overview.md
  *
  * These tests verify that documented code patterns are valid and compile correctly.
  */
class OverviewDocTest extends AnyFunSuite with BeforeAndAfterAll {

    var testDataset: Dataset = _

    override def beforeAll(): Unit = {
        super.beforeAll()
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            .toString.replace("file:/", "/")
        testDataset = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        if (testDataset != null) testDataset.delete()
        super.afterAll()
    }

    test("RST_GeoReference execute method") {
        val geoRef = RST_GeoReference.execute(testDataset)

        geoRef should not be null
        geoRef should contain key "scaleX"
        geoRef should contain key "scaleY"
        geoRef should contain key "upperLeftX"
        geoRef should contain key "upperLeftY"
        geoRef should contain key "skewX"
        geoRef should contain key "skewY"
    }

    test("RST_BoundingBox execute method (OverviewExamples.EXECUTE_METHODS_EXAMPLE)") {
        val bbox = RST_BoundingBox.execute(testDataset)
        bbox should not be null
    }

}
