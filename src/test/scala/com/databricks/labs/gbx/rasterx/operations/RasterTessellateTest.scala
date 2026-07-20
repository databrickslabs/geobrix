package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_Max
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.locationtech.jts.geom.Geometry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Covers the H3 covering path of [[RasterTessellate.tessellateH3Iter]]: the emitted cell set must be
  * exactly the cells whose H3 hexagon geometrically overlaps the raster bbox (no disjoint fringe).
  */
class RasterTessellateTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _
    private var tifPath: String = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        tifPath = this.getClass
            .getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            .toString
            .replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    /** Opens a fresh handle to the test raster. tessellateH3Iter.close() unlinks its input, so each
      * tessellation needs its own dataset (the suite-level `ds` is kept alive only for bbox checks). */
    private def freshDs(): Dataset = gdal.Open(tifPath)

    /** Collects the emitted cell IDs from the tessellation (fresh ds per call), releasing each chip dataset. */
    private def tessellateCells(resolution: Int, mode: String = "covering"): Seq[Long] = {
        val iter = RasterTessellate.tessellateH3Iter(freshDs(), Map.empty, resolution, mode)
        try {
            iter.map { case (cell, resDs, _) =>
                RasterDriver.releaseDataset(resDs)
                cell
            }.toList
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
    }

    /** Counts valid (non-nodata) pixels across all bands of a chip dataset. */
    private def validPixelCount(chip: Dataset): Long = {
        val xSize = chip.GetRasterXSize
        val ySize = chip.GetRasterYSize
        val nPix = xSize * ySize
        var total = 0L
        var b = 1
        while (b <= chip.getRasterCount) {
            val band = chip.GetRasterBand(b)
            val maskBuf = new Array[Byte](nPix)
            band.GetMaskBand().ReadRaster(0, 0, xSize, ySize, maskBuf)
            var i = 0
            while (i < nPix) { if (maskBuf(i) != 0) total += 1; i += 1 }
            b += 1
        }
        total
    }

    /** Counts valid (non-nodata) pixels across all bands of a fresh source dataset. */
    private def sourceValidPixelCount(): Long = {
        val src = freshDs()
        try validPixelCount(src)
        finally src.delete()
    }

    test("tessellateH3Iter covering emits only cells whose hexagon overlaps the raster bbox") {
        // MODIS tile footprint in WGS84 is roughly lon [-85..-71], lat [10..20] (see ClipToGeomTest).
        // Resolution 3 yields a handful of cells including border cells, so the old nodata keep-test
        // over-includes a disjoint fringe just outside the raster.
        val resolution = 3

        // Capture the bbox BEFORE consuming the iterator: tessellateH3Iter.close() unlinks `ds`,
        // after which BoundingBox.bbox(ds, ...) would read a dead dataset and return a degenerate
        // (0 0 ...) polygon.
        val bboxGeom: Geometry = BoundingBox.bbox(ds, GDAL.WSG84)
        bboxGeom.isValid shouldBe true
        bboxGeom.getArea should be > 0.0

        val cells = tessellateCells(resolution)
        cells should not be empty

        val disjoint = cells.filterNot { cell =>
            val hex = H3.cellIdToGeometry(cell)
            hex.intersects(bboxGeom)
        }

        withClue(
          s"${disjoint.length} of ${cells.length} emitted H3 cells are geometrically disjoint from the " +
              s"raster bbox (covering must emit only overlapping hexagons): ${disjoint.take(10).mkString(",")} "
        ) {
            disjoint shouldBe empty
        }
    }

    test("default mode equals explicit covering (same non-empty cell set); centroid emits cells") {
        val resolution = 3
        val defaultCells = tessellateCells(resolution).toSet
        val coveringCells = tessellateCells(resolution, "covering").toSet
        val centroidCells = tessellateCells(resolution, "centroid").toSet

        defaultCells should not be empty
        defaultCells shouldBe coveringCells
        centroidCells should not be empty
    }

    test("centroid mode single-assigns every valid pixel to exactly one cell (partition)") {
        val resolution = 3
        val totalValid = sourceValidPixelCount()
        totalValid should be > 0L

        val iter = RasterTessellate.tessellateH3Iter(freshDs(), Map.empty, resolution, "centroid")
        var emittedValid = 0L
        val emittedCells = scala.collection.mutable.ListBuffer.empty[Long]
        try {
            iter.foreach { case (cell, resDs, _) =>
                emittedCells += cell
                emittedValid += validPixelCount(resDs)
                RasterDriver.releaseDataset(resDs)
            }
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }

        // Each valid source pixel lands in exactly one cell's chip: the chips partition the
        // valid pixels, so the summed per-chip valid count equals the source valid count.
        withClue(s"emitted=$emittedValid expected=$totalValid across ${emittedCells.length} cells: ") {
            emittedValid shouldBe totalValid
        }
        // No cell emitted twice.
        emittedCells.length shouldBe emittedCells.distinct.length
    }

    /** 9x9 Float32 /vsimem raster (EPSG:4326, georeferenced) = 42.0 except a 3x3 interior NoData block. */
    private def interiorHoleDs(): Dataset = {
        val path = s"/vsimem/tess_hole_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, 9, 9, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(10.0, 0.05, 0.0, 50.0, 0.0, -0.05))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        val buf = Array.fill[Double](81)(42.0)
        for (r <- 3 to 5; c <- 3 to 5) buf(r * 9 + c) = -9999.0  // interior 3x3 hole
        band.WriteRaster(0, 0, 9, 9, buf)
        band.FlushCache()
        d.FlushCache()
        band.delete()
        d
    }

    test("covering emits all-nodata cells whose reducer is null (issue #59 emit+NULL)") {
        val iter = RasterTessellate.tessellateH3Iter(interiorHoleDs(), Map.empty, 7, "covering")
        var emptySeen = 0
        var dataSeen = 0
        try {
            iter.foreach { case (_, chip, _) =>
                val vc = validPixelCount(chip)
                val mx = RST_Max.execute(chip).headOption.orNull
                if (vc == 0L) { emptySeen += 1; mx shouldBe null }
                else { dataSeen += 1; mx should not be null }
                RasterDriver.releaseDataset(chip)
            }
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
        emptySeen should be > 0  // the hole must yield >=1 all-nodata covering cell
        dataSeen should be > 0
    }

}
