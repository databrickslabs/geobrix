package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{BNG, CustomGridSystem, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALWarp, OperatorOptions}
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants
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
      * the cell returned by [[GridSystem.pointToCellID]], then emits one chip per cell holding only
      * its assigned pixels (the rest set to nodata). BNG warps the raster to EPSG:27700 before
      * reading so that pixel coordinates are already in the grid's native CRS. 4326-native grids
      * (H3/quadbin) optionally reproject pixel centroids per-pixel when the raster CRS differs
      * from 4326.
      *
      * When `coverage == "complete"`, also emits a synthetic all-NoData chip for each covered cell
      * (positive-area overlap, valid grid cell) that caught no pixel centroid. The chip has the same
      * dimensions and geotransform as the source raster, with every pixel set to the source NoData.
      * When `coverage == "sparse"` (default-equivalent to old centroid), only cells with ≥1 assigned
      * pixel are emitted.
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
        val tf: CoordinateTransformation = if (isBng(grid)) null else {
            val srcSR = workDs.GetSpatialRef
            if (srcSR != null && srcSR.IsSame(gridSR) != 1) new CoordinateTransformation(srcSR, gridSR)
            else null
        }

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
            bandNoData(bi) = if (nd(0) != null) nd(0).doubleValue() else sentinelNoData(dtype)
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

        // For complete coverage: compute synthetic cells (covered but no centroid pixel assigned).
        val syntheticCells: Array[Long] = if (coverage == "complete") {
            completeCoveredCells.filter(cellId =>
                isCellValid(grid, cellId) &&
                hasPositiveAreaOverlap(grid.cellIdToGeometry(cellId), completeBbox) &&
                !cellPixels.contains(cellId)
            )
        } else Array.empty[Long]

        // For BNG the working dataset is a temporary warp; release it once all pixels are read.
        if (reprojected) RasterDriver.releaseDataset(workDs)
        // Release the per-pixel transform and the fresh per-call custom grid SR — both are done being
        // used (tf in the pixel loop above; gridSR in the bbox and as the tf target). Shared SRs
        // (GDAL.WSG84 for H3/quadbin, BngSR for BNG) are never released.
        if (tf != null) tf.delete()
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
                      xSize, ySize, bandCount, dtype, gt, bandVals, bandNoData
                    )
                } else {
                    // Synthetic all-NoData chip for a covered cell with no centroid pixel.
                    val cellId    = syntheticCells(synIdx)
                    synIdx += 1
                    val emptyIdx = new mutable.ArrayBuffer[Int]()
                    buildGenericCentroidChip(
                      grid, gridName, projWkt, sourcePath, options, cellId, emptyIdx,
                      xSize, ySize, bandCount, dtype, gt, bandVals, bandNoData
                    )
                }
                if (!hasNext) close()
                tile
            }

            override def close(): Unit = { closed = true }
        }
    }

    /** Builds one full-extent chip for centroid tessellation: every pixel starts as nodata, then the
      * `pixIdx` pixels are restored to their source values.  Works for H3, quadbin, and BNG because the
      * grid-specific rendering ([[GridSystem.renderCellId]]) and name are supplied as parameters.
      * When `pixIdx` is empty the result is a full-extent all-NoData chip (used by `centroid+complete`
      * to synthesise placeholder chips for covered cells that caught no pixel centroid). */
    private def buildGenericCentroidChip(
        grid: GridSystem,
        gridName: String,
        projWkt: String,
        sourcePath: String,
        options: Map[String, String],
        cellId: Long,
        pixIdx: mutable.ArrayBuffer[Int],
        xSize: Int,
        ySize: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandVals: Array[Array[Double]],
        bandNoData: Array[Double]
    ): (Any, Dataset, Map[String, String]) = {
        val rendered   = grid.renderCellId(cellId)
        val uuid       = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rasterPath = s"/vsimem/${gridName}_centroid_${cellId}_$uuid.tif"
        val drv = GDALManager.gtiffDriver()
        val out = drv.Create(rasterPath, xSize, ySize, bandCount, dtype)
        out.SetGeoTransform(gt)
        out.SetProjection(projWkt)

        val nPix = xSize * ySize
        var b = 0
        while (b < bandCount) {
            val nd  = bandNoData(b)
            val src = bandVals(b)
            val buf = new Array[Double](nPix)
            java.util.Arrays.fill(buf, nd) // blank everything to nodata...
            var i = 0
            while (i < pixIdx.length) { val p = pixIdx(i); buf(p) = src(p); i += 1 } // ...then restore assigned pixels
            val db = out.GetRasterBand(b + 1)
            db.SetNoDataValue(nd)
            // Implicit Float64 buffer overload: GDAL converts double[] to the band's native dtype on write.
            db.WriteRaster(0, 0, xSize, ySize, buf)
            b += 1
        }
        out.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
        out.FlushCache()

        val meta = Map(
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
        (rendered, out, meta)
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

    // ------------------------------------------------------------------------------------------------
    // Utility.
    // ------------------------------------------------------------------------------------------------

    /** A nodata sentinel for bands lacking an explicit nodata, by data type (used only to blank unassigned pixels). */
    private def sentinelNoData(dtype: Int): Double = {
        // Float types: NaN is the natural sentinel. Integer types: 0 (chips for centroid mode set it as nodata
        // so the mask treats it as invalid; collisions with real 0-valued data are acceptable for blanking only
        // when no explicit nodata exists, which is rare for the rasters this path serves).
        if (dtype == gdalconstConstants.GDT_Float32 || dtype == gdalconstConstants.GDT_Float64) Double.NaN else 0.0
    }

}
