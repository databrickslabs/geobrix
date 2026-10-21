package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * Verifies that [[RasterTessellate]] routes BNG-specific behavior (cell-validity clipping via
  * [[com.databricks.labs.gbx.gridx.grid.BNG.isValid]] and warp-to-27700) off grid identity
  * (`grid.name == "BNG"`) rather than off `grid.crsSrid == 27700`.
  *
  * A [[CustomGridSystem]] configured at SRID 27700 must NOT inherit BNG validity clipping: custom
  * cell IDs use a different bit-encoding from BNG cell IDs, so calling [[com.databricks.labs.gbx.gridx.grid.BNG.isValid]]
  * on them throws [[java.util.NoSuchElementException]] (the digit-decoded resolution is absent from
  * BNG's `sizeMap`), crashing the tessellate call.  The fix routes the guard through
  * `grid.name == "BNG"` so custom grids at 27700 follow the generic non-BNG path.
  */
class RasterTessellateBngDiscriminatorTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    /**
      * A small EPSG:27700 Float32 raster entirely within the custom grid's bounds
      * [0, 1000000] x [0, 1000000].  Extent: 490000–510000 E, 490000–510000 N (20 km x 20 km
      * square; coordinates inside Britain but outside the GB landmass BNG validity mask).
      * All pixels are filled with 42.0 (no NoData) so the centroid path assigns every pixel.
      */
    private def customGrid27700Raster(): Dataset = {
        val (minX, minY) = (490000.0, 490000.0)
        val (w, h) = (20000.0, 20000.0)
        val size = 32
        val path = s"/vsimem/custom27700_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, size, size, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(minX, w / size, 0.0, minY + h, 0.0, -(h / size)))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(27700)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        val buf = Array.fill[Double](size * size)(42.0)
        band.WriteRaster(0, 0, size, size, buf)
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    test("custom grid at SRID 27700 is not treated as BNG (no isValid clip, no BNG warp)") {
        // Grid: [0, 1_000_000] x [0, 1_000_000] in EPSG:27700; 100 km root cells split by 2 each level.
        // Resolution 1: cells are 50 km x 50 km.  The raster extent (490_000–510_000) spans ≥1 cell.
        val conf = GridConf(
            boundXMin    = 0,
            boundXMax    = 1000000,
            boundYMin    = 0,
            boundYMax    = 1000000,
            cellSplits   = 2,
            rootCellSizeX = 100000,
            rootCellSizeY = 100000,
            crsID        = Some(27700)
        )
        val custom = CustomGridSystem(conf)

        // Sanity-check: identity does NOT look like BNG.
        assert(custom.name != "BNG",     s"CustomGridSystem.name must not be 'BNG'; got '${custom.name}'")
        assert(custom.crsSrid == 27700,  "Custom grid crsSrid must be 27700 (precondition for the discriminator test)")

        val ds = customGrid27700Raster()
        val iter = RasterTessellate.tessellate(
            custom, ds, Map.empty, resolution = 1,
            assignment = "centroid", coverage = "sparse"
        )
        val cells: List[(Any, Dataset, Map[String, String])] = try {
            iter.toList
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
        cells.foreach { case (_, chip, _) => RasterDriver.releaseDataset(chip) }

        // Before fix: isCellValid calls BNG.isValid(customCellId), which throws
        // NoSuchElementException because the digit-decoded resolution is absent from BNG's sizeMap.
        // After fix:  isBng(custom) == false -> isCellValid returns true -> cells is non-empty.
        assert(cells.nonEmpty,
            "Custom grid at 27700 must yield non-empty cells; got empty set " +
                "(BNG.isValid was incorrectly applied to custom cell IDs)")

        // Custom grid keys are Long (base GridSystem.renderCellId returns the Long cellId directly),
        // not String (BNG overrides renderCellId to return format(cellId) which is a String).
        val keyTypes = cells.map(_._1.getClass.getName).distinct
        assert(cells.forall(_._1.isInstanceOf[Long]),
            s"Custom grid keys must be Long, not String; got: $keyTypes")
    }
}
