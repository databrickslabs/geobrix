package com.databricks.labs.gbx.rasterx.expressions.vector

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct tests for [[VectorRasterBridge]]. */
class VectorRasterBridgeTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    test("VectorRasterBridge.buildEmptyRaster rejects degenerate extents") {
        an[IllegalArgumentException] should be thrownBy {
            VectorRasterBridge.buildEmptyRaster(0, 0, 0, 10, 32, 32, 4326)
        }
        an[IllegalArgumentException] should be thrownBy {
            VectorRasterBridge.buildEmptyRaster(0, 0, 10, 10, 0, 32, 4326)
        }
    }
}
