package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.gdal
import org.locationtech.jts.geom.Point
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for GDALRasterize (empty, Point, Polygon, unsupported geometry). Requires GDAL (e.g. run in Docker). */
class GDALRasterizeTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    test("executeRasterize with empty geometries should return dataset and metadata with last_error") {
        val origin = JTS.point(0.0, 0.0).asInstanceOf[Point]
        origin.setSRID(4326)
        val (ds, meta) = GDALRasterize.executeRasterize(
            geoms = Seq.empty,
            values = None,
            origin = origin,
            xWidth = 10,
            yWidth = 10,
            xSize = 1.0,
            ySize = -1.0,
            noDataValue = -9999.0,
            options = Map.empty
        )
        ds should not be null
        meta should contain key "last_error"
        meta("last_error") should include("No geometries to rasterize")
        meta should contain key "path"
        RasterDriver.releaseDataset(ds)
    }

    test("executeRasterize with Point geometry should return dataset and metadata") {
        val origin = JTS.point(0.0, 0.0).asInstanceOf[Point]
        origin.setSRID(4326)
        val pt = JTS.point(2.0, 3.0)
        pt.setSRID(4326)
        val (ds, meta) = GDALRasterize.executeRasterize(
            geoms = Seq(pt),
            values = Some(Seq(5.0)),
            origin = origin,
            xWidth = 10,
            yWidth = 10,
            xSize = 1.0,
            ySize = -1.0,
            noDataValue = -9999.0,
            options = Map.empty
        )
        ds should not be null
        meta should contain key "path"
        meta should contain key "driver"
        RasterDriver.releaseDataset(ds)
    }

    test("executeRasterize with Polygon geometry should return dataset and metadata") {
        val origin = JTS.point(0.0, 0.0).asInstanceOf[Point]
        origin.setSRID(4326)
        val poly = JTS.polygonFromXYs(Array((1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)))
        poly.setSRID(4326)
        val (ds, meta) = GDALRasterize.executeRasterize(
            geoms = Seq(poly),
            values = Some(Seq(10.0)),
            origin = origin,
            xWidth = 20,
            yWidth = 20,
            xSize = 0.5,
            ySize = -0.5,
            noDataValue = -9999.0,
            options = Map.empty
        )
        ds should not be null
        meta should contain key "path"
        meta should contain key "driver"
        RasterDriver.releaseDataset(ds)
    }

    test("executeRasterize with unsupported geometry type should throw UnsupportedOperationException") {
        val origin = JTS.point(0.0, 0.0).asInstanceOf[Point]
        origin.setSRID(4326)
        val line = JTS.lineStringXYs(Array((0.0, 0.0), (1.0, 1.0)).toBuffer)
        line.setSRID(4326)
        assertThrows[UnsupportedOperationException] {
            GDALRasterize.executeRasterize(
                geoms = Seq(line),
                values = None,
                origin = origin,
                xWidth = 10,
                yWidth = 10,
                xSize = 1.0,
                ySize = -1.0,
                noDataValue = -9999.0,
                options = Map.empty
            )
        }
    }
}
