package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{BNG, H3, Quadbin}
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

    // -------------------------------------------------------------------------------------------------
    // Positive-area covering keep-test: on a GRID-ALIGNED tile (raster edges land exactly on cell
    // boundaries), edge-only-touching cells share just a 1-D boundary line with the raster (zero pixel
    // overlap) and must NOT be emitted (they would otherwise clip to spurious empty all-NoData chips).
    // A cell with real areal overlap IS emitted even when all-NoData (case A). Guards the 48-vs-36
    // heavy-vs-light BNG divergence at its root; applied identically to all three grids.
    // -------------------------------------------------------------------------------------------------

    /** A GB (London) EPSG:27700 raster whose edges land exactly on 1km BNG cell boundaries — a 2km x 2km
      * window over exactly 4 whole 1km cells. `fill` sets every pixel (Some(v) = data, None = all NoData). */
    private def bngAlignedDs(fill: Option[Double] = Some(42.0)): Dataset = {
        val (minX, minY) = (529000.0, 179000.0)
        val edge = 1000.0
        val (w, h) = (2 * edge, 2 * edge)
        val size = 64
        val path = s"/vsimem/tess_bng_aligned_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, size, size, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(minX, w / size, 0.0, minY + h, 0.0, -(h / size)))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(27700)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        val buf = Array.fill[Double](size * size)(fill.getOrElse(-9999.0))
        band.WriteRaster(0, 0, size, size, buf)
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    private def bngCells(ds: Dataset, resolution: Int, mode: String = "covering"): Seq[String] = {
        val iter = RasterTessellate.tessellateBngIter(ds, Map.empty, resolution, mode)
        try iter.map { case (cell, resDs, _) => RasterDriver.releaseDataset(resDs); cell }.toList
        finally iter match { case ac: AutoCloseable => ac.close(); case _ => }
    }

    test("BNG covering on a grid-aligned tile excludes edge-only-touching cells (positive-area keep-test)") {
        val res = BNG.getResolution("1km")
        val emitted = bngCells(bngAlignedDs(), res).toSet
        // A 2km x 2km grid-aligned window covers exactly 4 whole 1km cells; the edge-touch neighbours
        // (sharing only a boundary line) must be excluded.
        withClue(s"grid-aligned 2km window should emit exactly 4 cells, got ${emitted.toList.sorted}: ") {
            emitted.size shouldBe 4
        }
        // The cell immediately west of the window shares only the x=minX boundary line -> zero-area -> excluded.
        val westNeighbour = BNG.format(BNG.pointToCellID(529000.0 - 500.0, 179000.0 + 500.0, res))
        emitted should not contain westNeighbour
    }

    test("BNG covering keeps a within-extent all-NoData cell (case A: positive area, all NoData)") {
        val res = BNG.getResolution("1km")
        // Every pixel NoData but all 4 cells genuinely overlap the raster extent -> all 4 still emitted.
        val emitted = bngCells(bngAlignedDs(fill = None), res).toSet
        withClue(s"within-extent all-NoData cells must still be emitted, got ${emitted.toList.sorted}: ") {
            emitted.size shouldBe 4
        }
    }

    /** A quadbin-aligned EPSG:4326 raster: window == exactly one whole quadbin cell at `z` (edges on the
      * cell's own bbox). `fill` None => all NoData. */
    private def quadbinAlignedDs(z: Int, fill: Option[Double] = Some(42.0)): Dataset = {
        // Pick a cell near London and align the raster to a 2x2 block of its children (edges land on the
        // child boundaries, so the 4 children are whole and their outer neighbours only edge-touch).
        val parent = Quadbin.pointToCell(-0.1, 51.5, z - 1)
        val (lonMin, latMin, lonMax, latMax) = Quadbin.cellBbox(parent)
        val size = 64
        val (w, h) = (lonMax - lonMin, latMax - latMin)
        val path = s"/vsimem/tess_qb_aligned_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, size, size, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(lonMin, w / size, 0.0, latMax, 0.0, -(h / size)))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        val buf = Array.fill[Double](size * size)(fill.getOrElse(-9999.0))
        band.WriteRaster(0, 0, size, size, buf)
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    private def quadbinCells(ds: Dataset, z: Int, mode: String = "covering"): Seq[Long] = {
        val iter = RasterTessellate.tessellateQuadbinIter(ds, Map.empty, z, mode)
        try iter.map { case (cell, resDs, _) => RasterDriver.releaseDataset(resDs); cell }.toList
        finally iter match { case ac: AutoCloseable => ac.close(); case _ => }
    }

    test("quadbin covering on a grid-aligned tile excludes edge-only-touching cells (positive-area keep-test)") {
        val z = 12
        val emitted = quadbinCells(quadbinAlignedDs(z), z).toSet
        // The window == one parent cell at z-1 == exactly 4 whole child cells at z. Edge-touch neighbours excluded.
        withClue(s"grid-aligned parent window should emit exactly 4 child cells at z=$z, got ${emitted.size}: ") {
            emitted.size shouldBe 4
        }
    }

    test("quadbin covering keeps a within-extent all-NoData cell (case A: positive area, all NoData)") {
        val z = 12
        val emitted = quadbinCells(quadbinAlignedDs(z, fill = None), z).toSet
        withClue(s"within-extent all-NoData cells must still be emitted, got ${emitted.size}: ") {
            emitted.size shouldBe 4
        }
    }

}
