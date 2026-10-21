package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{BNG, CustomGridSystem, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALWarp, OperatorOptions}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Tessellates a raster into grid cells: clips by cell geometry and yields (cellId, Dataset, metadata) per cell. */
object RasterTessellate {

    /** Supported assignment strategies.
      * `covering` keeps every cell whose polygon overlaps the raster bbox (chips may share pixels).
      * `centroid` single-assigns each valid pixel to the one cell whose polygon contains its centroid
      * (chips partition the valid pixels). */
    val Assignments: Set[String] = Set("covering", "centroid")

    /** Supported coverage strategies.
      * `complete` (default) emits a chip for every cell that passes the assignment keep-test, including
      * all-NoData chips (covering) or synthetic all-NoData chips for covered cells that caught no pixels
      * (centroid). `sparse` drops chips whose clipped result is entirely NoData (covering) or emits only
      * cells that received ≥1 pixel centroid (centroid). */
    val Coverages: Set[String] = Set("sparse", "complete")

    /**
      * Covering keep-test shared by all three grids (H3, quadbin, BNG): a cell is emitted iff its geometry
      * has POSITIVE-AREA overlap with the raster bbox — mere boundary touch (a shared edge line or corner
      * point) is NOT enough. On a grid-aligned tile (raster edges land exactly on cell boundaries) a fringe
      * cell just outside the data shares only a 1-D boundary with the raster: `intersects == true` but
      * `intersection(bbox).getArea == 0.0` and it holds ZERO source pixels, so clipping it yields a spurious
      * empty all-NoData chip. This test drops those (matches the light tier). It KEEPS cells with real areal
      * overlap even if they clip to all-NoData (e.g. a cloud hole or the raster's own NoData) — those fill
      * their position in covering mode and must not be dropped, else white gaps are punched into the mosaic.
      *
      * Delegates to [[GridOverlap.hasPositiveAreaOverlap]] — the shared canonical implementation that
      * [[com.databricks.labs.gbx.rasterx.expressions.grid.RasterToGridGeneric]] also uses.
      */
    private def hasPositiveAreaOverlap(cellGeom: Geometry, bbox: Geometry): Boolean =
        GridOverlap.hasPositiveAreaOverlap(cellGeom, bbox)

    // ------------------------------------------------------------------------------------------------
    // BNG-specific helpers shared by the generic tessellate path.
    // ------------------------------------------------------------------------------------------------

    /** EPSG:27700 (British National Grid) spatial reference, traditional (easting, northing) axis order. */
    private val BngSR: SpatialReference = {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(27700)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        sr
    }

    /**
      * Reproject `ds` to EPSG:27700 (nearest-neighbour) unless it is already 27700; returns
      * `(workDs, reprojected)`. When `reprojected` is true the caller owns `workDs` and must release it.
      * Mirrors `RST_BNG_RasterToGrid`'s warp-up-front behaviour (BNG has no lon/lat input path).
      */
    private def warpToBng(ds: Dataset): (Dataset, Boolean) = {
        val srcWkt = ds.GetProjection()
        val alreadyBng = srcWkt != null && srcWkt.nonEmpty && {
            val s = new SpatialReference()
            s.ImportFromWkt(srcWkt)
            val same = s.IsSame(BngSR) == 1
            s.delete()
            same
        }
        if (alreadyBng) (ds, false)
        else {
            val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
            val driver = ds.GetDriver()
            val extension = GDAL.getExtension(driver.getShortName)
            val resultPath = s"/vsimem/raster_bng_tess_$uuid.$extension"
            val (result, _) = GDALWarp.executeWarp(
              resultPath,
              Array(ds),
              Map.empty[String, String],
              command = "gdalwarp -t_srs EPSG:27700 -r near"
            )
            (result, true)
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Grid-identity helpers — placed here, shared by the generic tessellation paths below.
    // ------------------------------------------------------------------------------------------------

    /** True iff `grid` is the BNG singleton — the only grid that needs BNG-specific clipping and warp.
      * Keyed on grid identity (`grid.name == "BNG"`) rather than CRS (`crsSrid == 27700`) so that a
      * [[com.databricks.labs.gbx.gridx.grid.CustomGridSystem]] configured at SRID 27700 stays on the
      * generic path: custom cell IDs use a different bit-encoding from BNG cell IDs, and calling
      * [[BNG.isValid]] on them throws [[java.util.NoSuchElementException]]. */
    private def isBng(grid: GridSystem): Boolean = grid.name == "BNG"

    /** True iff `grid` is a [[CustomGridSystem]] — cell geometries live in the grid's own `crsSrid`,
      * which is arbitrary (not the fixed 4326/27700 of the built-in grids), so [[srForGrid]] must
      * build a per-call SpatialReference for it and the caller must release it (see [[srForGrid]]). */
    private def isCustom(grid: GridSystem): Boolean = grid.name == "CUSTOM"

    /** Returns the native spatial reference for the grid (the CRS its cell geometries live in).
      * BNG → the cached [[BngSR]] singleton.  H3/quadbin (SRID 4326) → the cached [[GDAL.WSG84]]
      * singleton.  Both are shared and MUST NOT be released.  A [[CustomGridSystem]] → a FRESH
      * SpatialReference at the grid's own `crsSrid` (traditional axis order): the caller MUST release
      * it (`.delete()`) when done — the covering iterator does so in `close()`, the centroid iterator
      * right after the bbox is built.  Use [[isCustom]] to decide whether a release is owed. */
    private def srForGrid(grid: GridSystem): SpatialReference =
        if (isBng(grid)) BngSR
        else if (isCustom(grid)) {
            val sr = new SpatialReference()
            sr.ImportFromEPSG(grid.crsSrid)
            sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
            sr
        } else GDAL.WSG84

    // ------------------------------------------------------------------------------------------------
    // Generic tessellation over GridSystem.
    // ------------------------------------------------------------------------------------------------

    /**
      * Validity guard: BNG rejects out-of-GB cells; all other grids accept every cell returned by
      * pointToCellID. Applied in BOTH covering and centroid paths to prevent invalid (out-of-extent)
      * cells from being emitted — mirrors the `if (!BNG.isValid(cell)) return null` guard that the
      * former per-grid covering helpers enforced via `getBngTile`.
      *
      * Routed off grid identity ([[isBng]]) not `crsSrid == 27700` — a custom grid at 27700 must
      * NOT trigger [[BNG.isValid]] (its cell IDs use a different bit-encoding).
      */
    private def isCellValid(grid: GridSystem, cellId: Long): Boolean =
        if (isBng(grid)) BNG.isValid(cellId) else true

    /**
      * Returns true iff every band of `chip` is entirely NoData (mask is all-zero). Used by
      * `covering+sparse` to drop chips that have no valid pixels after clipping.
      * GDAL resources: reads only (does not create or release any dataset).
      */
    private def isAllNoData(chip: Dataset): Boolean = {
        val xSize = chip.getRasterXSize
        val ySize = chip.getRasterYSize
        val nPix  = xSize * ySize
        val maskBuf = new Array[Byte](nPix)
        var b = 1
        while (b <= chip.getRasterCount) {
            chip.GetRasterBand(b).GetMaskBand().ReadRaster(0, 0, xSize, ySize, maskBuf)
            var i = 0
            while (i < nPix) { if (maskBuf(i) != 0) return false; i += 1 }
            b += 1
        }
        true
    }

    /**
      * Generic tessellation of a raster over any [[GridSystem]]. The per-grid enumeration differences
      * are captured in [[GridSystem.coveringCandidateCells]] (buffered polyfill for H3/BNG; raw bbox
      * lookup for quadbin), so the shared clip+keep-test logic here is grid-agnostic.
      *
      * Returns an `Iterator[(cellKey, Dataset, metadata)]` where `cellKey` is a `Long` for H3 and
      * quadbin, and a `String` for BNG (via [[GridSystem.renderCellId]]). The three named wrapper
      * methods ([[tessellateH3Iter]], [[tessellateQuadbinIter]], [[tessellateBngIter]]) delegate here
      * and cast the result to their specific return type.
      *
      * Caller must release each emitted Dataset. The iterator is AutoCloseable and releases the working
      * dataset when exhausted or explicitly closed.
      *
      * @param assignment `"covering"` — one chip per cell whose polygon overlaps the raster bbox;
      *                   `"centroid"` — pixel-centroid single-assignment. Default: `"centroid"`.
      * @param coverage   `"complete"` — always emit a chip for every candidate cell, including
      *                   all-NoData chips (covering) and synthetic all-NoData chips for uncaptured cells
      *                   (centroid); `"sparse"` — drop all-NoData chips (covering) / emit only cells
      *                   with ≥1 captured pixel (centroid). Default: `"complete"`.
      */
    def tessellate(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        assignment: String = "centroid",
        coverage: String = "complete"
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        require(
          Assignments.contains(assignment),
          s"gbx_rst_${grid.name.toLowerCase}_tessellate assignment must be one of ${Assignments.mkString(", ")}; got '$assignment'"
        )
        require(
          Coverages.contains(coverage),
          s"gbx_rst_${grid.name.toLowerCase}_tessellate coverage must be one of ${Coverages.mkString(", ")}; got '$coverage'"
        )
        if (assignment == "centroid") tessellateGenericCentroidIter(grid, ds, options, resolution, coverage)
        else tessellateGenericCoveringIter(grid, ds, options, resolution, coverage)
    }

    /**
      * Generic covering tessellation. Candidate cells come from [[GridSystem.coveringCandidateCells]],
      * which encodes each grid's buffering strategy (H3/BNG buffer; quadbin does not). The shared
      * positive-area keep-test, validity guard, and [[ClipToGeom.clip]] are applied identically for
      * every grid.
      *
      * When `coverage == "sparse"`, chips whose clipped result is entirely NoData are released and
      * skipped. When `coverage == "complete"` (default), all-NoData chips are emitted as-is.
      */
    private def tessellateGenericCoveringIter(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        coverage: String
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        val gridSR = srForGrid(grid)
        // BNG requires a pre-warp to EPSG:27700; all other grids (H3/quadbin/custom) use ds as-is.
        val (workDs, reprojected) = if (isBng(grid)) warpToBng(ds) else (ds, false)
        val bbox = BoundingBox.bbox(workDs, gridSR)
        val cells = grid.coveringCandidateCells(bbox, resolution).toArray

        new Iterator[(Any, Dataset, Map[String, String])] with AutoCloseable {
            private var closed   = false
            private var fetched  = false
            private var _ds      = workDs
            private var cc       = 0
            private var nextTile: (Any, Dataset, Map[String, String]) = _

            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (cc < cells.length && nextTile == null) {
                    val cellId = cells(cc)
                    cc += 1
                    // Guard against out-of-extent candidates (BNG isValid) before touching
                    // cellIdToGeometry — an invalid cell could produce a degenerate geometry and
                    // must not be emitted (mirrors the former getBngTile guard).
                    if (isCellValid(grid, cellId)) {
                        val cellGeom = grid.cellIdToGeometry(cellId)
                        if (hasPositiveAreaOverlap(cellGeom, bbox)) {
                            val (resDs, resMtd) = ClipToGeom.clip(_ds, options, cellGeom, gridSR)
                            if (resDs != null) {
                                // covering+sparse: drop chips whose clip is entirely NoData.
                                if (coverage == "sparse" && isAllNoData(resDs)) {
                                    RasterDriver.releaseDataset(resDs)
                                } else {
                                    val rendered = grid.renderCellId(cellId)
                                    resDs.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
                                    resDs.FlushCache()
                                    nextTile = (rendered, resDs, resMtd)
                                }
                            }
                        }
                    }
                }
                if (cc >= cells.length && nextTile == null) close()
            }

            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            override def next(): (Any, Dataset, Map[String, String]) = {
                if (!fetched && !closed) advance()
                fetched = false
                nextTile
            }

            override def close(): Unit = {
                if (!closed) {
                    closed = true
                    if (reprojected) RasterDriver.releaseDataset(_ds) else RasterAccessors.unlink(_ds)
                    _ds = null
                    if (isCustom(grid)) gridSR.delete() // fresh per-call SR (see srForGrid); shared SRs are not released
                }
            }
        }
    }

    /**
      * Generic centroid tessellation. Reads all pixel values up front, assigns each valid pixel to
      * the cell returned by [[GridSystem.pointToCellID]], then emits one chip per cell cropped to the
      * bounding pixel window of its assigned pixels (window pixels assigned to other cells are nodata).
      * BNG warps the raster to EPSG:27700 before reading so that pixel coordinates are already in the
      * grid's native CRS. 4326-native grids (H3/quadbin) optionally reproject pixel centroids per-pixel
      * when the raster CRS differs from 4326.
      *
      * When `coverage == "complete"`, also emits a synthetic all-NoData chip for each covered cell
      * (positive-area overlap, valid grid cell) that caught no pixel centroid: a 1x1 chip on the source
      * pixel holding the cell's centroid (see [[centroidPixel]]). When `coverage == "sparse"`
      * (default-equivalent to old centroid), only cells with >=1 assigned pixel are emitted.
      *
      * A band without a declared NoData blanks with [[GDAL.getNoDataConstant]] for its data type (the
      * same table the light tier uses), so a real 0 pixel is never mistaken for nodata.
      */
    private def tessellateGenericCentroidIter(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        coverage: String
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        // BNG requires a pre-warp so pixel coordinates are in EPSG:27700 (the grid's native CRS).
        // Other grids, including a custom grid at 27700, use ds as-is.
        val (workDs, reprojected) = if (isBng(grid)) warpToBng(ds) else (ds, false)

        val xSize     = workDs.getRasterXSize
        val ySize     = workDs.getRasterYSize
        val nPix      = xSize * ySize
        val bandCount = workDs.getRasterCount
        val dtype     = workDs.GetRasterBand(1).getDataType
        val gt        = workDs.GetGeoTransform

        // Capture projection WKT and source path before potentially releasing workDs (BNG case).
        val projWkt = workDs.GetProjection()
        val sourcePath = Option(workDs.GetFileList())
            .flatMap(_.asScala.headOption.map(_.toString))
            .getOrElse("unknown source path")

        // For centroid+complete: capture the covering cells BEFORE releasing workDs.
        val gridSR = srForGrid(grid)
        val (completeBbox, completeCoveredCells): (Geometry, Array[Long]) =
            if (coverage == "complete") {
                val b = BoundingBox.bbox(workDs, gridSR)
                (b, grid.coveringCandidateCells(b, resolution).toArray)
            } else (null, Array.empty[Long])
        // Per-pixel reprojection TARGET is the grid's OWN CRS (srForGrid), so pixel centroids are
        // transformed into the grid CRS before pointToCellID for ANY grid CRS — consistent with the
        // raster->grid path (which reprojects the raster to the grid CRS). BNG is pre-warped to its
        // native 27700, so it needs no per-pixel transform (else null). H3/quadbin: srForGrid is the
        // shared WGS84 SR, so the target stays 4326 and per-pixel results are unchanged (digest-neutral).
        // Custom: srForGrid is a fresh SR built from grid.crsSrid. gridSR is reused (also the bbox SR
        // above) as the transform target and released below (only when it is a fresh custom SR).
        val srcSR: SpatialReference = if (isBng(grid)) null else workDs.GetSpatialRef
        val needsTransform = srcSR != null && srcSR.IsSame(gridSR) != 1
        val tf: CoordinateTransformation = if (needsTransform) new CoordinateTransformation(srcSR, gridSR) else null
        // The inverse (grid CRS -> working-raster CRS) moves a covered-but-empty cell's centroid onto
        // the raster; only centroid+complete builds those chips. Created and released together with tf.
        val tfInv: CoordinateTransformation =
            if (needsTransform && coverage == "complete") new CoordinateTransformation(gridSR, srcSR) else null

        // Read every band's values + mask once; assign each valid pixel (by flat index) to its cell.
        val bandVals   = new Array[Array[Double]](bandCount)
        val bandMask   = new Array[Array[Byte]](bandCount)
        val bandNoData = new Array[Double](bandCount)
        var bi = 0
        while (bi < bandCount) {
            val band = workDs.GetRasterBand(bi + 1)
            val vals = new Array[Double](nPix)
            val mask = new Array[Byte](nPix)
            band.ReadRaster(0, 0, xSize, ySize, vals)
            band.GetMaskBand().ReadRaster(0, 0, xSize, ySize, mask)
            bandVals(bi) = vals
            bandMask(bi) = mask
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            bandNoData(bi) = if (nd(0) != null) nd(0).doubleValue() else GDAL.getNoDataConstant(dtype)
            bi += 1
        }

        // cell -> flat pixel indices; all valid pixels assigned to exactly one cell.
        val cellPixels = new mutable.LongMap[mutable.ArrayBuffer[Int]]()
        var y = 0
        var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                var anyValid = false
                var b = 0
                while (b < bandCount && !anyValid) { if (bandMask(b)(idx) != 0) anyValid = true; b += 1 }
                if (anyValid) {
                    val xOff = 0.5 + x
                    val yOff = 0.5 + y
                    val xGeo = gt(0) + xOff * gt(1) + yOff * gt(2)
                    val yGeo = gt(3) + xOff * gt(4) + yOff * gt(5)
                    val (cx, cy) = if (tf != null) {
                        val p = tf.TransformPoint(xGeo, yGeo)
                        (p(0), p(1))
                    } else (xGeo, yGeo)
                    val cellId = grid.pointToCellID(cx, cy, resolution)
                    if (isCellValid(grid, cellId))
                        cellPixels.getOrElseUpdate(cellId, new mutable.ArrayBuffer[Int]) += idx
                }
                idx += 1
                x += 1
            }
            y += 1
        }

        // For complete coverage: synthetic cells (valid, covered, but no centroid pixel assigned), each
        // with the source pixel its 1x1 chip sits on, resolved here while tfInv is still alive.
        val syntheticCells: Array[(Long, Int, Int)] = if (coverage == "complete") {
            lazy val invGt = {
                val inv = new Array[Double](6)
                require(gdal.InvGeoTransform(gt, inv) == 1, s"raster geotransform ${gt.mkString(",")} is not invertible")
                inv
            }
            completeCoveredCells.iterator
                .filter(cellId => isCellValid(grid, cellId) && !cellPixels.contains(cellId))
                .map(cellId => (cellId, grid.cellIdToGeometry(cellId)))
                .filter { case (_, cellGeom) => hasPositiveAreaOverlap(cellGeom, completeBbox) }
                .map { case (cellId, cellGeom) =>
                    val (col, row) = centroidPixel(cellGeom, tfInv, invGt, xSize, ySize)
                    (cellId, col, row)
                }
                .toArray
        } else Array.empty[(Long, Int, Int)]

        // For BNG the working dataset is a temporary warp; release it once all pixels are read.
        if (reprojected) RasterDriver.releaseDataset(workDs)
        // Release the per-pixel transform, its inverse and the fresh per-call custom grid SR; all are
        // done being used (tf in the pixel loop above, tfInv for the synthetic placements; gridSR in the
        // bbox and as the transform target/source). Shared SRs (GDAL.WSG84 for H3/quadbin, BngSR for
        // BNG) are never released.
        if (tf != null) tf.delete()
        if (tfInv != null) tfInv.delete()
        if (isCustom(grid)) gridSR.delete()

        val gridName = grid.name.toLowerCase
        val cellIter = cellPixels.iterator
        var synIdx   = 0

        new Iterator[(Any, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false

            override def hasNext: Boolean = !closed && (cellIter.hasNext || synIdx < syntheticCells.length)

            override def next(): (Any, Dataset, Map[String, String]) = {
                val tile = if (cellIter.hasNext) {
                    val (cellId, pixIdx) = cellIter.next()
                    buildGenericCentroidChip(
                      grid, gridName, projWkt, sourcePath, options, cellId, pixIdx,
                      xSize, bandCount, dtype, gt, bandVals, bandNoData
                    )
                } else {
                    // Synthetic all-NoData chip for a covered cell with no centroid pixel.
                    val (cellId, col, row) = syntheticCells(synIdx)
                    synIdx += 1
                    buildSyntheticEmptyChip(
                      grid, gridName, projWkt, sourcePath, options, cellId, col, row,
                      bandCount, dtype, gt, bandNoData
                    )
                }
                if (!hasNext) close()
                tile
            }

            override def close(): Unit = { closed = true }
        }
    }

    /** Source pixel `(col, row)` that a covered-but-empty cell's 1x1 chip sits on: the planar centroid of
      * the cell polygon (grid CRS), moved into the working-raster CRS by `toRaster` when the two differ
      * (null when they match, e.g. BNG's 27700 warp or an EPSG:4326 raster under H3/quadbin), mapped
      * through the inverse geotransform `invGt`, floored and clamped into the raster. Same rule as the
      * light tier, so both tiers place an empty cell on the same pixel. */
    private def centroidPixel(
        cellGeom: Geometry,
        toRaster: CoordinateTransformation,
        invGt: Array[Double],
        xSize: Int,
        ySize: Int
    ): (Int, Int) = {
        val cen = cellGeom.getCentroid
        val (x, y) = if (toRaster != null) {
            val p = toRaster.TransformPoint(cen.getX, cen.getY)
            (p(0), p(1))
        } else (cen.getX, cen.getY)
        val col = math.floor(invGt(0) + x * invGt(1) + y * invGt(2))
        val row = math.floor(invGt(3) + x * invGt(4) + y * invGt(5))
        // Clamp in Double before narrowing so far-off (or NaN) centroids cannot overflow Int.
        (math.min(math.max(col, 0.0), xSize - 1.0).toInt, math.min(math.max(row, 0.0), ySize - 1.0).toInt)
    }

    /** Creates the /vsimem GTiff for one centroid chip: `w` x `h` pixels whose top-left is source pixel
      * (`col`, `row`). The geotransform is the source's offset to that pixel (rotation and pixel-size
      * terms kept, so rotated and south-up rasters stay on the source grid). Returns (path, dataset). */
    private def createChip(
        gridName: String,
        cellId: Long,
        w: Int,
        h: Int,
        col: Int,
        row: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        projWkt: String
    ): (String, Dataset) = {
        val uuid       = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rasterPath = s"/vsimem/${gridName}_centroid_${cellId}_$uuid.tif"
        val out = GDALManager.gtiffDriver().Create(rasterPath, w, h, bandCount, dtype)
        out.SetGeoTransform(
          Array(gt(0) + col * gt(1) + row * gt(2), gt(1), gt(2), gt(3) + col * gt(4) + row * gt(5), gt(4), gt(5))
        )
        out.SetProjection(projWkt)
        (rasterPath, out)
    }

    /** Tile metadata shared by every centroid chip (assigned and synthetic). */
    private def chipMeta(
        gridName: String,
        rendered: Any,
        rasterPath: String,
        sourcePath: String,
        options: Map[String, String]
    ): Map[String, String] =
        Map(
          "path"         -> rasterPath,
          "parentPath"   -> options.getOrElse("path", sourcePath),
          "driver"       -> "GTiff",
          "format"       -> "GTiff",
          "last_command" -> s"${gridName}_centroid_tessellate cell=$rendered",
          "last_error"   -> "",
          "all_parents"  -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size"         -> "-1",
          "compression"  -> options.getOrElse("compression", OperatorOptions.DefaultCompression),
          "isZipped"     -> "false",
          "isSubset"     -> "false"
        )

    /** Builds one centroid chip cropped to the bounding pixel window of the cell's assigned pixels
      * (`pixIdx`: flat source indices, non-empty). The window starts as nodata and the assigned pixels
      * are restored to their source values, so window pixels assigned to other cells stay nodata.
      * Works for H3, quadbin, BNG and custom grids because the grid-specific rendering
      * ([[GridSystem.renderCellId]]) and name are supplied as parameters. */
    private def buildGenericCentroidChip(
        grid: GridSystem,
        gridName: String,
        projWkt: String,
        sourcePath: String,
        options: Map[String, String],
        cellId: Long,
        pixIdx: mutable.ArrayBuffer[Int],
        xSize: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandVals: Array[Array[Double]],
        bandNoData: Array[Double]
    ): (Any, Dataset, Map[String, String]) = {
        // Window (c0..c1, r0..r1) = bounding box of the assigned pixels.
        val n  = pixIdx.length
        var c0 = Int.MaxValue
        var c1 = Int.MinValue
        var r0 = Int.MaxValue
        var r1 = Int.MinValue
        var i  = 0
        while (i < n) {
            val p = pixIdx(i)
            val r = p / xSize
            val c = p - r * xSize
            if (c < c0) c0 = c
            if (c > c1) c1 = c
            if (r < r0) r0 = r
            if (r > r1) r1 = r
            i += 1
        }
        val w = c1 - c0 + 1
        val h = r1 - r0 + 1
        // Offset of each assigned pixel inside the w x h window buffer.
        val local = new Array[Int](n)
        i = 0
        while (i < n) {
            val p = pixIdx(i)
            val r = p / xSize
            local(i) = (r - r0) * w + (p - r * xSize - c0)
            i += 1
        }

        val rendered = grid.renderCellId(cellId)
        val (rasterPath, out) = createChip(gridName, cellId, w, h, c0, r0, bandCount, dtype, gt, projWkt)
        val buf = new Array[Double](w * h)
        var b = 0
        while (b < bandCount) {
            val nd  = bandNoData(b)
            val src = bandVals(b)
            java.util.Arrays.fill(buf, nd) // blank the window to nodata...
            i = 0
            while (i < n) { buf(local(i)) = src(pixIdx(i)); i += 1 } // ...then restore assigned pixels
            val db = out.GetRasterBand(b + 1)
            db.SetNoDataValue(nd)
            // Implicit Float64 buffer overload: GDAL converts double[] to the band's native dtype on write.
            db.WriteRaster(0, 0, w, h, buf)
            b += 1
        }
        out.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
        out.FlushCache()
        (rendered, out, chipMeta(gridName, rendered, rasterPath, sourcePath, options))
    }

    /** Builds the 1x1 all-NoData chip of a covered cell that caught no pixel centroid (used by
      * `centroid+complete`), on source pixel (`col`, `row`) from [[centroidPixel]]. Every band holds
      * its nodata value. */
    private def buildSyntheticEmptyChip(
        grid: GridSystem,
        gridName: String,
        projWkt: String,
        sourcePath: String,
        options: Map[String, String],
        cellId: Long,
        col: Int,
        row: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandNoData: Array[Double]
    ): (Any, Dataset, Map[String, String]) = {
        val rendered = grid.renderCellId(cellId)
        val (rasterPath, out) = createChip(gridName, cellId, 1, 1, col, row, bandCount, dtype, gt, projWkt)
        var b = 0
        while (b < bandCount) {
            val db = out.GetRasterBand(b + 1)
            db.SetNoDataValue(bandNoData(b))
            db.WriteRaster(0, 0, 1, 1, Array(bandNoData(b)))
            b += 1
        }
        out.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
        out.FlushCache()
        (rendered, out, chipMeta(gridName, rendered, rasterPath, sourcePath, options))
    }

    // ------------------------------------------------------------------------------------------------
    // Named wrappers — thin delegates to `tessellate`, preserving the exact per-grid return types
    // so all current callers (RST_H3_Tessellate, RST_Quadbin_Tessellate, RST_BNG_Tessellate) and
    // tests compile and run unchanged.
    // ------------------------------------------------------------------------------------------------

    /**
      * Iterator of (cellId Long, Dataset, metadata) per emitted H3 cell at resolution. Caller must release each
      * Dataset; iterator is AutoCloseable.
      *
      *  - `assignment="centroid"` (default): pixel-centroid single-assignment.
      *  - `assignment="covering"`: one chip per cell whose hexagon overlaps the raster bbox.
      *  - `coverage="complete"` (default): always emit a chip for every candidate cell.
      *  - `coverage="sparse"`: drop chips with no valid pixels (covering) or uncaptured cells (centroid).
      */
    def tessellateH3Iter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        assignment: String = "centroid",
        coverage: String = "complete"
    ): Iterator[(Long, Dataset, Map[String, String])] =
        tessellate(H3, ds, options, resolution, assignment, coverage)
            .asInstanceOf[Iterator[(Long, Dataset, Map[String, String])]]

    /**
      * Iterator of (cellId Long, Dataset, metadata) per emitted quadbin cell at `resolution` (zoom z).
      * Caller must release each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]].
      */
    def tessellateQuadbinIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        assignment: String = "centroid",
        coverage: String = "complete"
    ): Iterator[(Long, Dataset, Map[String, String])] =
        tessellate(Quadbin, ds, options, resolution, assignment, coverage)
            .asInstanceOf[Iterator[(Long, Dataset, Map[String, String])]]

    /**
      * Iterator of (BNG cellId String, Dataset, metadata) per emitted BNG cell at `resolution`.
      * Caller must release each Dataset; iterator is AutoCloseable.
      *
      * The raster is reprojected to EPSG:27700 first (skipped if already 27700). Cells are enumerated
      * via [[GridSystem.coveringCandidateCells]] (buffered polyfill) and geometrised via
      * [[GridSystem.cellIdToGeometry]] — the vector `bng_tessellate` codepath is never reached.
      */
    def tessellateBngIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        assignment: String = "centroid",
        coverage: String = "complete"
    ): Iterator[(String, Dataset, Map[String, String])] =
        tessellate(BNG, ds, options, resolution, assignment, coverage)
            .asInstanceOf[Iterator[(String, Dataset, Map[String, String])]]

    /**
      * Iterator of (cellId Long, Dataset, metadata) per emitted custom-grid cell at `resolution`.
      * Caller must release each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]].
      *
      * The raster is NOT pre-warped (custom cell geometries live in the grid's own `crsSrid`, and the
      * generic path clips/bins in that CRS via [[srForGrid]]); the raster is expected to be in the
      * grid's native CRS. Before enumerating, the R3 guard ([[CustomGridBounds.requireBoundsContainRaster]])
      * asserts the grid bounds contain the raster extent so no pixel centroid throws mid-aggregation.
      */
    def tessellateCustomIter(
        grid: CustomGridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        assignment: String = "centroid",
        coverage: String = "complete"
    ): Iterator[(Long, Dataset, Map[String, String])] = {
        CustomGridBounds.requireBoundsContainRaster(grid, ds)
        tessellate(grid, ds, options, resolution, assignment, coverage)
            .asInstanceOf[Iterator[(Long, Dataset, Map[String, String])]]
    }

}
