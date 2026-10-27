package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.GridSystem
import com.databricks.labs.gbx.rasterx.gdal.{RasterDriver, ValidMask}
import com.databricks.labs.gbx.rasterx.operations.{BoundingBox, GridOverlap}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.osr.{SpatialReference, osrConstants}
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable

/** Generic raster→grid pixel aggregation for any [[GridSystem]].
  *
  * Reprojects `ds` to the grid's native CRS (via [[GridReprojection]]), bins each valid
  * (non-NoData, opaque) pixel centroid to a cell id with `grid.pointToCellID`, optionally filters
  * cells with `isCellValid` (for grids like BNG that have coordinate-extent constraints),
  * accumulates pixel values per cell, and applies `fAgg`. Cells with no valid pixels are
  * dropped (sparse output). The caller owns `ds`; any internally reprojected dataset is
  * released before return.
  *
  * Output cell keys are produced by [[GridSystem.renderCellId]]: Long for H3/Quadbin,
  * String for BNG.
  */
object RasterToGridGeneric {

    /** Run pixel aggregation over `ds` for `grid`. Convenience overload: accepts all cells.
      * See the 5-param variant for the BNG use-case (where `BNG.isValid` must be passed).
      *
      * @param grid       the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds         source raster (any CRS; reprojected internally if needed)
      * @param resolution grid resolution
      * @param fAgg       reduces per-cell pixel values to an aggregated `T`
      * @return per-band array of `(cellKey, T)` pairs, where `cellKey` is
      *         `grid.renderCellId(cellID)` (Long for H3/Quadbin, String for BNG)
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(Any, T)]] =
        execute(grid, ds, resolution, fAgg, _ => true)

    /** Run pixel aggregation over `ds` for `grid` with explicit per-cell validity.
      *
      * @param grid        the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds          source raster (any CRS; reprojected internally if needed)
      * @param resolution  grid resolution
      * @param fAgg        reduces per-cell pixel values to an aggregated `T`
      * @param isCellValid per-cell validity predicate (use `BNG.isValid` for BNG, `_ => true` otherwise)
      * @return per-band array of `(cellKey, T)` pairs, where `cellKey` is
      *         `grid.renderCellId(cellID)` (Long for H3/Quadbin, String for BNG)
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T,
        isCellValid: Long => Boolean
    ): Array[Array[(Any, T)]] = {
        val (workDs, reprojected) = GridReprojection.toGridCrs(ds, grid.crsSrid)
        try executeOn(grid, workDs, resolution, fAgg, isCellValid)
        finally if (reprojected) RasterDriver.releaseDataset(workDs)
    }

    /** Run pixel aggregation over `ds` for `grid` with explicit coverage, assignment, and
      * per-cell validity params. This is the ONLY overload that defines a default argument
      * (`isCellValid`) — Scala 2 permits at most one overloaded variant to have defaults.
      *
      * @param grid        the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds          source raster (any CRS; reprojected internally if needed)
      * @param resolution  grid resolution
      * @param coverage    `"sparse"` — emit only cells with pixels (Stage-1 behaviour);
      *                    `"complete"` — additionally emit cells that overlap the raster bbox
      *                    but have no pixel centroids, using `emptyValue` as their measure
      * @param assignment  `"centroid"` — bin each valid pixel to the cell containing its centroid
      *                    (existing path). `"covering"` — distribute each pixel's value across all
      *                    cells it overlaps, weighted by intersection-area fraction.
      * @param fAgg        reduces per-cell pixel values to `T` (centroid path)
      * @param fAggW       reduces per-cell `(value, areaWeight)` pairs to `T` (covering path)
      * @param emptyValue  measure for cells added by `complete` coverage that have no pixels
      * @param isCellValid per-cell validity predicate; default accepts all cells (use `BNG.isValid` for BNG)
      * @return per-band array of `(cellKey, Option[T])` pairs:
      *         `Some(fAgg(buf))` / `Some(fAggW(buf))` for data cells;
      *         `emptyValue` for covered-but-empty cells (only with `coverage == "complete"`)
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        coverage: String,
        assignment: String,
        fAgg: mutable.ArrayBuffer[Double] => T,
        fAggW: mutable.ArrayBuffer[(Double, Double)] => T,
        emptyValue: Option[T],
        isCellValid: Long => Boolean = _ => true
    ): Array[Array[(Any, Option[T])]] = {
        require(Set("sparse", "complete").contains(coverage),
            s"coverage must be 'sparse' or 'complete'; got '$coverage'")
        require(Set("centroid", "covering").contains(assignment),
            s"assignment must be 'centroid' or 'covering'; got '$assignment'")

        val (workDs, reprojected) = GridReprojection.toGridCrs(ds, grid.crsSrid)
        try {
            assignment match {
                case "centroid" =>
                    val sparseOut = executeOn(grid, workDs, resolution, fAgg, isCellValid)
                    // Wrap T in Some to get the Option-valued shape shared with covering.
                    val sparseOptBands: Array[Array[(Any, Option[T])]] =
                        sparseOut.map(_.map { case (c, v) => (c, Option(v)): (Any, Option[T]) })
                    if (coverage == "sparse") sparseOptBands
                    else {
                        val (bboxGeom, candidates) = buildBboxAndCandidates(workDs, grid, resolution)
                        applyCompleteCoverage(sparseOptBands, bboxGeom, candidates, grid, isCellValid, emptyValue)
                    }

                case "covering" =>
                    val sparseOptBands = executeOnCovering(grid, workDs, resolution, fAggW, isCellValid)
                    if (coverage == "sparse") sparseOptBands
                    else {
                        val (bboxGeom, candidates) = buildBboxAndCandidates(workDs, grid, resolution)
                        applyCompleteCoverage(sparseOptBands, bboxGeom, candidates, grid, isCellValid, emptyValue)
                    }
            }
        } finally {
            if (reprojected) RasterDriver.releaseDataset(workDs)
        }
    }

    /** Builds a SpatialReference for the given EPSG code with traditional (easting/northing
      * before northing/easting) axis order, matching the convention used by [[BoundingBox]] and
      * [[com.databricks.labs.gbx.rasterx.operations.RasterTessellate]].
      */
    private def buildGridSR(crsSrid: Int): SpatialReference = {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(crsSrid)
        sr.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        sr
    }

    /** Builds the raster bounding-box geometry (in grid CRS) and the covering candidate cells
      * for `complete`-coverage post-pass. The native SpatialReference is released in `finally`
      * to prevent native-heap leaks on executors running many aggregations.
      */
    private def buildBboxAndCandidates(
        workDs: Dataset,
        grid: GridSystem,
        resolution: Int
    ): (Geometry, Seq[Long]) = {
        val gridSr = buildGridSR(grid.crsSrid)
        try {
            val bbox = BoundingBox.bbox(workDs, gridSr)
            (bbox, grid.coveringCandidateCells(bbox, resolution))
        } finally {
            gridSr.delete()
        }
    }

    /** Adds covered-but-empty cells (as `emptyValue`) to `sparseOptBands`.
      *
      * For each candidate cell from the raster bbox that
      * (a) is not already keyed in the sparse output,
      * (b) passes the validity guard, and
      * (c) has positive-area overlap with the bbox,
      * emits `(key, emptyValue)`.  Shared between the centroid and covering paths so the
      * enumeration/keep-test logic is not duplicated.
      */
    private def applyCompleteCoverage[T](
        sparseOptBands: Array[Array[(Any, Option[T])]],
        bboxGeom: Geometry,
        candidates: Seq[Long],
        grid: GridSystem,
        isCellValid: Long => Boolean,
        emptyValue: Option[T]
    ): Array[Array[(Any, Option[T])]] = {
        sparseOptBands.map { band =>
            val sparseRendered: Set[Any] = band.map(_._1).toSet
            val extraCells: Array[(Any, Option[T])] = candidates
                .flatMap { c =>
                    val key = grid.renderCellId(c)
                    if (!sparseRendered.contains(key) &&
                        isCellValid(c) &&
                        GridOverlap.hasPositiveAreaOverlap(grid.cellIdToGeometry(c), bboxGeom))
                        Some((key: Any, emptyValue))
                    else
                        None
                }
                .toArray
            band ++ extraCells
        }
    }

    private def executeOn[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T,
        isCellValid: Long => Boolean
    ): Array[Array[(Any, T)]] = {

        val gt     = ds.GetGeoTransform
        val xSize  = ds.getRasterXSize
        val ySize  = ds.getRasterYSize
        val nPix   = xSize * ySize
        val bands  = ds.getRasterCount

        val bandBuf  = new Array[Double](nPix)
        val maskBuf  = new Array[Byte](nPix)
        val validity = new ValidMask(ds)

        (1 to bands).iterator.map { bi =>
            val b = ds.GetRasterBand(bi)
            b.ReadRaster(0, 0, xSize, ySize, bandBuf)
            // Valid FOR THIS BAND: by its own mask and, for a raster with an alpha band, opaque (GDAL derives
            // a mask from the alpha band only for 2- and 4-band rasters).
            validity.readBand(bi, 0, 0, xSize, ySize, maskBuf)

            // Count valid pixels for LongMap pre-sizing.
            var valid = 0; var i = 0
            while (i < nPix) { if (maskBuf(i) != 0) valid += 1; i += 1 }

            val acc = new mutable.LongMap[mutable.ArrayBuffer[Double]](valid)
            var y = 0; var idx = 0
            while (y < ySize) {
                var x = 0
                while (x < xSize) {
                    if (maskBuf(idx) != 0) {
                        val xOff = 0.5 + x
                        val yOff = 0.5 + y
                        val cell = grid.pointToCellID(
                            gt(0) + xOff * gt(1) + yOff * gt(2),
                            gt(3) + xOff * gt(4) + yOff * gt(5),
                            resolution
                        )
                        if (isCellValid(cell)) {
                            val buf = acc.getOrElseUpdate(cell, new mutable.ArrayBuffer)
                            buf += bandBuf(idx)
                        }
                    }
                    idx += 1; x += 1
                }
                y += 1
            }

            val out = new Array[(Any, T)](acc.size)
            var j = 0
            acc.foreach { case (cell, buf) => out(j) = (grid.renderCellId(cell), fAgg(buf)); j += 1 }
            out
        }.toArray
    }

    /** Area-weighted pixel aggregation: each valid pixel distributes its value across all cells
      * whose footprint overlaps the pixel's rectangle, weighted by intersection-area fraction.
      *
      * For each valid pixel at raster offset (x, y):
      *  1. Interior fast-path, taken ONLY when [[GridSystem.coveringFastPathExact]] is true
      *     (analytic-square grids, where the point-partition equals the cell polygon): compute the
      *     four affine-geotransform corners and bin each via [[GridSystem.pointToCellID]]. If all
      *     four map to the SAME cell, the pixel's rectangle (the convex hull of its corners) lies
      *     wholly inside that convex cell: accumulate `(pixelValue, 1.0)` to that one cell and SKIP
      *     the polyfill/intersection. This makes the pass O(boundary). For non-exact grids (H3) the
      *     fast-path is skipped and every pixel takes step 2 — byte-identical to the prior behaviour.
      *  2. Otherwise (boundary pixel): build the pixel rectangle and enumerate candidate cells
      *     via [[GridSystem.coveringCandidateCells]] (a small set for a single pixel).
      *  3. For each candidate passing `isCellValid`: compute the JTS intersection; if the
      *     intersection area is positive, accumulate `(pixelValue, interArea/pixelArea)`.
      *  4. Emit `Some(fAggW(buf))` per cell.
      *
      * Mass is conserved: if the candidate cells tile the pixel completely, the sum of
      * weighted contributions equals the pixel value (all area-fraction weights sum to 1);
      * an interior pixel's single weight is exactly 1.
      */
    private def executeOnCovering[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAggW: mutable.ArrayBuffer[(Double, Double)] => T,
        isCellValid: Long => Boolean
    ): Array[Array[(Any, Option[T])]] = {

        val gt     = ds.GetGeoTransform
        val xSize  = ds.getRasterXSize
        val ySize  = ds.getRasterYSize
        val nPix   = xSize * ySize
        val bands  = ds.getRasterCount

        val bandBuf  = new Array[Double](nPix)
        val maskBuf  = new Array[Byte](nPix)
        val validity = new ValidMask(ds)

        (1 to bands).iterator.map { bi =>
            val b = ds.GetRasterBand(bi)
            b.ReadRaster(0, 0, xSize, ySize, bandBuf)
            validity.readBand(bi, 0, 0, xSize, ySize, maskBuf) // valid for this band: see executeOn

            val accW = new mutable.LongMap[mutable.ArrayBuffer[(Double, Double)]]()
            var y = 0; var idx = 0
            while (y < ySize) {
                var x = 0
                while (x < xSize) {
                    if (maskBuf(idx) != 0) {
                        val value = bandBuf(idx)
                        // Pixel corners from the affine geotransform (closes at the first corner).
                        val x0 = gt(0) + x       * gt(1) + y       * gt(2)
                        val y0 = gt(3) + x       * gt(4) + y       * gt(5)
                        val x1 = gt(0) + (x + 1) * gt(1) + y       * gt(2)
                        val y1 = gt(3) + (x + 1) * gt(4) + y       * gt(5)
                        val x2 = gt(0) + (x + 1) * gt(1) + (y + 1) * gt(2)
                        val y2 = gt(3) + (x + 1) * gt(4) + (y + 1) * gt(5)
                        val x3 = gt(0) + x       * gt(1) + (y + 1) * gt(2)
                        val y3 = gt(3) + x       * gt(4) + (y + 1) * gt(5)

                        // Interior fast-path (O(boundary)): ONLY for grids where pointToCellID's
                        // partition is exactly cellIdToGeometry's polygon (grid.coveringFastPathExact:
                        // the analytic-square grids). If all four pixel corners bin to the same cell,
                        // the pixel's rectangle — the convex hull of its corners — lies wholly inside
                        // that (convex) cell, so it contributes weight 1 with NO polyfill/intersection.
                        // For non-exact grids (H3: chord polygon vs geoToH3) the fast-path is NOT taken
                        // and every pixel uses the exact area split below, byte-identical to before.
                        var handled = false
                        if (grid.coveringFastPathExact) {
                            val c0 = grid.pointToCellID(x0, y0, resolution)
                            val c1 = grid.pointToCellID(x1, y1, resolution)
                            val c2 = grid.pointToCellID(x2, y2, resolution)
                            val c3 = grid.pointToCellID(x3, y3, resolution)
                            if (c0 == c1 && c1 == c2 && c2 == c3) {
                                // Mirrors the old path's per-cell validity guard: an interior pixel's
                                // only positive-area overlap is with its containing cell c0.
                                if (isCellValid(c0)) {
                                    accW.getOrElseUpdate(c0, new mutable.ArrayBuffer) += ((value, 1.0))
                                }
                                handled = true
                            }
                        }
                        if (!handled) {
                            val pixelRect = JTS.polygonFromXYs(
                                Array((x0, y0), (x1, y1), (x2, y2), (x3, y3), (x0, y0))
                            )
                            val pxArea = pixelRect.getArea

                            grid.coveringCandidateCells(pixelRect, resolution).foreach { c =>
                                if (isCellValid(c)) {
                                    val inter = grid.cellIdToGeometry(c).intersection(pixelRect)
                                    if (inter != null && inter.getArea > 0) {
                                        accW.getOrElseUpdate(c, new mutable.ArrayBuffer) +=
                                            ((value, inter.getArea / pxArea))
                                    }
                                }
                            }
                        }
                    }
                    idx += 1; x += 1
                }
                y += 1
            }

            val out = new Array[(Any, Option[T])](accW.size)
            var j = 0
            accW.foreach { case (cell, buf) =>
                out(j) = (grid.renderCellId(cell), Some(fAggW(buf)))
                j += 1
            }
            out
        }.toArray
    }
}
