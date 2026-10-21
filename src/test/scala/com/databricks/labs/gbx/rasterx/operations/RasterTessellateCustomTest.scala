package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.expressions.grid.RST_Custom_RasterToGridAvg
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference, osrConstants}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for [[RasterTessellate.tessellateCustomIter]] — the Long-keyed custom-grid tessellate
  * wrapper. Exercises both the centroid and covering paths at a custom SRID (27700), and the
  * R3 up-front bounds-contain-extent guard.
  */
class RasterTessellateCustomTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    private val conf = GridConf(
        boundXMin     = 0,
        boundXMax     = 1000000,
        boundYMin     = 0,
        boundYMax     = 1000000,
        cellSplits    = 2,
        rootCellSizeX = 100000,
        rootCellSizeY = 100000,
        crsID         = Some(27700)
    )
    private val grid = CustomGridSystem(conf)

    /** A 32x32 EPSG:27700 Float32 raster filled with 42.0; extent 490_000–510_000 E/N. */
    private def raster27700(): Dataset = {
        val (minX, minY) = (490000.0, 490000.0)
        val (w, h)       = (20000.0, 20000.0)
        val size         = 32
        val path = s"/vsimem/custom_tess_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv  = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, size, size, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(minX, w / size, 0.0, minY + h, 0.0, -(h / size)))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(27700)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        band.WriteRaster(0, 0, size, size, Array.fill[Double](size * size)(42.0))
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    /** Drains and closes a tessellation, releasing its chips. The tessellation owns (and releases) its input `ds`. */
    private def drain(iter: Iterator[(Long, Dataset, Map[String, String])]): List[(Long, Dataset, Map[String, String])] = {
        val cells = try iter.toList finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
        cells.foreach { case (_, chip, _) => RasterDriver.releaseDataset(chip) }
        cells
    }

    test("centroid tessellation over a custom grid at 27700 emits Long-keyed chips") {
        val ds = raster27700()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid, ds, Map.empty, resolution = 3,
                assignment = "centroid", coverage = "sparse"))

        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
    }

    test("covering tessellation over a custom grid at 27700 emits Long-keyed chips") {
        val ds = raster27700()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid, ds, Map.empty, resolution = 3,
                assignment = "covering", coverage = "complete"))

        // Requires the custom-grid SR (27700) in the generic tessellate path: with a WGS84 SR the
        // raster bbox and the 27700 cell geometries would never overlap and the set would be empty.
        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
    }

    test("R3: a grid whose bounds do NOT contain the raster extent fails UP FRONT") {
        val tinyGrid = CustomGridSystem(conf.copy(boundXMax = 15000, boundYMax = 15000))
        val ds = raster27700()
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateCustomIter(tinyGrid, ds, Map.empty, resolution = 3,
                assignment = "centroid", coverage = "sparse").toList
        }
        ex.getMessage.toLowerCase should include("bound")
    }

    // -------------------------------------------------------------------------------------------
    // Non-27700 custom CRS (EPSG:3857): the per-pixel reprojection TARGET must be the GRID CRS, not
    // a hard-coded WGS84. Before the fix, centroid tessellation at 3857 transformed pixel centroids
    // to 4326 and fed them to a 3857 grid -> out-of-bounds throw / silent wrong cells.
    // -------------------------------------------------------------------------------------------

    /** Custom grid at EPSG:3857, bounds [0, 20_000_000] x [0, 20_000_000], 1000 km root cells (split 2). */
    private val grid3857 = CustomGridSystem(GridConf(0, 20000000, 0, 20000000, 2, 1000000, 1000000, Some(3857)))

    /** At resolution 1 (500 km cells) every raster below falls entirely inside this single cell. */
    private val expectedCell3857 = grid3857.pointToCellID(1050000.0, 6050000.0, 1)

    private def rasterWith(srid: Int, minX: Double, minY: Double, spanX: Double, spanY: Double, size: Int): Dataset = {
        val path = s"/vsimem/custom_tess_${srid}_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val d = gdal.GetDriverByName("GTiff").Create(path, size, size, 1, gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(minX, spanX / size, 0.0, minY + spanY, 0.0, -(spanY / size)))
        val srs = new SpatialReference()
        srs.ImportFromEPSG(srid)
        srs.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        band.WriteRaster(0, 0, size, size, Array.fill[Double](size * size)(42.0))
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    /** Native 3857 raster: x[1_000_000, 1_100_000], y[6_000_000, 6_100_000] -> all inside cell (2,12). */
    private def raster3857(): Dataset = rasterWith(3857, 1000000.0, 6000000.0, 100000.0, 100000.0, 32)

    /** 4326 raster: lon[10.0, 10.2], lat[50.0, 50.1] -> reprojects into 3857 cell (2,12). */
    private def raster4326(): Dataset = rasterWith(4326, 10.0, 50.0, 0.2, 0.1, 32)

    test("centroid tessellate at a NON-27700 custom CRS (3857) with a native-CRS raster bins into grid-CRS cells") {
        val ds = raster3857()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid3857, ds, Map.empty, resolution = 1,
                assignment = "centroid", coverage = "sparse"))

        // Before the fix this threw (centroids transformed to 4326, out of the 3857 grid bounds).
        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
        cells.map(_._1).distinct shouldBe List(expectedCell3857)
    }

    test("centroid tessellate at 3857 with a 4326 raster reprojects to the GRID CRS (not WGS84)") {
        val ds = raster4326()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid3857, ds, Map.empty, resolution = 1,
                assignment = "centroid", coverage = "sparse"))

        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
        // The 4326 footprint reprojects into the same 3857 cell as the native raster.
        cells.map(_._1).distinct shouldBe List(expectedCell3857)
    }

    test("covering tessellate at 3857 with a 4326 raster clips in the GRID CRS and covers the expected cell") {
        val ds = raster4326()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid3857, ds, Map.empty, resolution = 1,
                assignment = "covering", coverage = "complete"))

        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
        cells.map(_._1) should contain(expectedCell3857)
    }

    test("custom rastertogrid and custom tessellate agree on a mismatched-CRS (4326) raster over a 3857 grid") {
        val dsA = raster4326()
        val r2gKeys = RST_Custom_RasterToGridAvg.execute(grid3857, dsA, 1, "sparse", "centroid")(0)
            .map(_._1).toSet
        RasterDriver.releaseDataset(dsA)

        val dsB = raster4326()
        val tessKeys = drain(
            RasterTessellate.tessellateCustomIter(grid3857, dsB, Map.empty, resolution = 1,
                assignment = "centroid", coverage = "sparse")).map(_._1).toSet

        r2gKeys should not be empty
        r2gKeys shouldBe tessKeys
        r2gKeys shouldBe Set(expectedCell3857)
    }
}
