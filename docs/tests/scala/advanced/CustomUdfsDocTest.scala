package tests.docs.scala.advanced

import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.apache.spark.sql.functions.udf
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for code examples in docs/docs/advanced/custom-udfs.md
  *
  * These tests verify that documented code patterns are valid and compile correctly.
  */
class CustomUdfsDocTest extends AnyFunSuite with BeforeAndAfterAll {

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

    test("RasterDriver.readFromBytes pattern") {
        val bytes = RasterDriver.writeToBytes(testDataset, Map.empty[String, String])

        // Pattern from docs: Read Dataset from bytes
        val dsFromBytes = RasterDriver.readFromBytes(bytes, Map.empty[String, String])

        // Use execute methods as shown in docs
        val width = RST_Width.execute(dsFromBytes)
        val height = RST_Height.execute(dsFromBytes)
        val numBands = RST_NumBands.execute(dsFromBytes)

        width should be > 0
        height should be > 0
        numBands should be > 0

        RasterDriver.releaseDataset(dsFromBytes)
    }

    test("execute methods in UDF") {
        val customStatsUDF = udf((tileBytes: Array[Byte]) => {
            val ds: Dataset = RasterDriver.readFromBytes(tileBytes, Map.empty[String, String])
            try {
                val width = RST_Width.execute(ds)
                val height = RST_Height.execute(ds)
                val numBands = RST_NumBands.execute(ds)

                Map(
                  "width" -> width.toString,
                  "height" -> height.toString,
                  "bands" -> numBands.toString
                )
            } finally {
                RasterDriver.releaseDataset(ds)
            }
        })

        customStatsUDF should not be null
        customStatsUDF.getClass.getName should include ("UserDefinedFunction")
    }

    test("RST_BoundingBox execute method") {
        val bbox = RST_BoundingBox.execute(testDataset)

        bbox should not be null
        bbox.isEmpty shouldBe false
        bbox.getGeometryType shouldBe "Polygon"
    }

    test("basic accessor execute methods") {
        val width = RST_Width.execute(testDataset)
        val height = RST_Height.execute(testDataset)
        val numBands = RST_NumBands.execute(testDataset)
        val format = RST_Format.execute(testDataset)
        val srid = RST_SRID.execute(testDataset)

        width shouldBe 2400
        height shouldBe 2400
        numBands shouldBe 1
        format shouldBe "GTiff"
        srid shouldBe a[Int]  // Verify method returns an Int (SRID can be 0 if not set)
    }

    test("pixel accessor execute methods") {
        val pixelWidth = RST_PixelWidth.execute(testDataset)
        val pixelHeight = RST_PixelHeight.execute(testDataset)
        val pixelCount = RST_PixelCount.execute(testDataset)

        pixelWidth should be > 0.0
        pixelHeight should be > 0.0
        pixelCount should not be empty
        pixelCount.head should be > 0L
    }

    test("coordinate accessor execute methods") {
        val upperLeftX = RST_UpperLeftX.execute(testDataset)
        val upperLeftY = RST_UpperLeftY.execute(testDataset)
        val scaleX = RST_ScaleX.execute(testDataset)
        val scaleY = RST_ScaleY.execute(testDataset)
        val rotation = RST_Rotation.execute(testDataset)

        upperLeftX should not be 0.0
        upperLeftY should not be 0.0
        scaleX should not be 0.0
        scaleY should not be 0.0
        rotation shouldBe a[Double]
    }

    test("resource cleanup pattern") {
        val bytes = RasterDriver.writeToBytes(testDataset, Map.empty[String, String])

        def safeExecute[T](f: Dataset => T)(bytes: Array[Byte]): T = {
            val ds: Dataset = RasterDriver.readFromBytes(bytes, Map.empty[String, String])
            try {
                f(ds)
            } finally {
                RasterDriver.releaseDataset(ds)
            }
        }

        val result = safeExecute(ds => RST_Width.execute(ds))(bytes)
        result should be > 0
    }

    test("import statements compile") {
        import com.databricks.labs.gbx.rasterx.expressions.accessors._
        import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
        import org.apache.spark.sql.expressions.UserDefinedFunction
        import org.apache.spark.sql.functions.udf
        import org.gdal.gdal.Dataset

        succeed
    }

    test("CustomUdfExamples constants exist for docs") {
        CustomUdfExamples.EXECUTE_METHODS_EXAMPLE should not be empty
        CustomUdfExamples.SCALA_UDF_EXAMPLE should not be empty
        CustomUdfExamples.RESOURCE_MANAGEMENT_PATTERN should not be empty
    }

}
