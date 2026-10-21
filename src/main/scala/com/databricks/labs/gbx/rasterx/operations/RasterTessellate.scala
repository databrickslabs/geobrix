package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{BNG, CustomGridSystem, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{OperatorOptions, WarpSource}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Tessellates a raster into grid cells: clips by cell geometry and yields (cellId, Dataset, metadata) per cell. */
object RasterTessellate {

    /** Supported assignment strategies.
      * `covering` keeps every cell whose polygon overlaps the raster footprint ([[footprint]]; chips may
      * share pixels).
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
      * has POSITIVE-AREA overlap with the raster footprint ([[footprint]]) — mere boundary touch (a shared
      * edge line or corner point) is NOT enough. On a grid-aligned tile (raster edges land exactly on cell
      * boundaries) a fringe cell just outside the data shares only a 1-D boundary with the raster:
      * `intersects == true` but `intersection(footprint).getArea == 0.0` and it holds ZERO source pixels,
      * so clipping it yields a spurious empty all-NoData chip. This test drops those (matches the light
      * tier). It KEEPS cells with real areal overlap even if they clip to all-NoData (e.g. a cloud hole or
      * the raster's own NoData) — those fill their position in covering mode and must not be dropped, else
      * white gaps are punched into the mosaic.
      *
      * Delegates to [[GridOverlap.hasPositiveAreaOverlap]] — the shared canonical implementation that
      * [[com.databricks.labs.gbx.rasterx.expressions.grid.RasterToGridGeneric]] also uses.
      */
    private def hasPositiveAreaOverlap(cellGeom: Geometry, footprint: Geometry): Boolean =
        GridOverlap.hasPositiveAreaOverlap(cellGeom, footprint)

    /** Points per raster edge, corners included, walked when the footprint is reprojected into the grid
      * CRS: the lightweight tier's `_FOOTPRINT_EDGE_POINTS` (rasterio's `transform_bounds` default). */
    private val FootprintEdgePoints = 21

    /**
      * The raster footprint that the covering and centroid+complete keep-tests, and their candidate
      * enumeration, use: [[BoundingBox.densifiedBBox]] of `workDs` in the grid CRS. When the footprint is
      * reprojected (H3/quadbin on a raster not in EPSG:4326; a custom grid on a raster in another CRS),
      * each edge is walked at [[FootprintEdgePoints]] points, so the ring follows the curved edge and no
      * cell holding edge pixels is dropped. BNG (warped to 27700 first), same-CRS and CRS-less rasters keep
      * their four corners. Same rule as the lightweight tier.
      *
      * Throws IllegalArgumentException, before any chip is made, when a corner cannot be transformed, or
      * when a lon/lat footprint crosses the antimeridian or encloses a pole (its longitudes jump by more
      * than 180 degrees between consecutive points, closing pair included, or the ring is not a valid
      * polygon): cell enumeration and overlap tests on such a ring are meaningless.
      */
    private def footprint(grid: GridSystem, workDs: Dataset, gridSR: SpatialReference): Geometry = {
        val fn = s"rst_${grid.name.toLowerCase(java.util.Locale.ROOT)}_tessellate"
        val fp =
            try BoundingBox.densifiedBBox(workDs, gridSR, FootprintEdgePoints)
            catch { case e: IllegalArgumentException => throw new IllegalArgumentException(s"$fn: ${e.getMessage}", e) }
        if (BoundingBox.reprojects(workDs, gridSR) && gridSR.IsGeographic() == 1) {
            val ring = fp.getCoordinates // closed: the last point is the first
            val wraps = (1 until ring.length).exists(i => math.abs(ring(i).x - ring(i - 1).x) > 180.0)
            if (wraps || !fp.isValid) {
                throw new IllegalArgumentException(
                  s"$fn: the raster footprint crosses the antimeridian or encloses a pole in " +
                    s"${SpatialRefOps.crsToCanonical(workDs.GetSpatialRef)}; reproject it (e.g. to EPSG:4326) " +
                    "or split it before tessellating"
                )
            }
        }
        fp
    }

    /** Runs `check`, releasing the tessellation input `ds` if it throws: a tessellation owns `ds` from
      * the call on, whether it returns an iterator or fails. */
    private def releaseOnThrow[T](ds: Dataset)(check: => T): T =
        try check
        catch {
            case t: Throwable =>
                RasterAccessors.unlink(ds)
                throw t
        }

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
      * Mirrors `RST_BNG_RasterToGrid`'s warp-up-front behaviour (BNG has no lon/lat input path). The warp
      * fill is NoData and a source's own mask band is kept ([[WarpSource.warpFillAsNoData]]), as in the
      * lightweight tier.
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
            val (result, _) = WarpSource.warpFillAsNoData(ds, resultPath, "gdalwarp -t_srs EPSG:27700 -r near")
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
      * it (`.delete()`) when done — both iterators do so through [[TessellateNatives]].  Use [[isCustom]]
      * to decide whether a release is owed. */
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
      * Caller must release each emitted Dataset. The tessellation owns `ds`: the AutoCloseable iterator
      * releases it (and any BNG warp) when exhausted or explicitly closed, and a call that throws has
      * released it already, so the caller never releases `ds`. Centroid reads `ds` lazily, chip by chip,
      * so `ds` must stay open until the iterator is exhausted or closed.
      *
      * @param assignment `"covering"` — one chip per cell whose polygon overlaps the raster footprint;
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
        releaseOnThrow(ds) {
            require(
              Assignments.contains(assignment),
              s"gbx_rst_${grid.name.toLowerCase}_tessellate assignment must be one of ${Assignments.mkString(", ")}; got '$assignment'"
            )
            require(
              Coverages.contains(coverage),
              s"gbx_rst_${grid.name.toLowerCase}_tessellate coverage must be one of ${Coverages.mkString(", ")}; got '$coverage'"
            )
        }
        if (assignment == "centroid") tessellateGenericCentroidIter(grid, ds, options, resolution, coverage)
        else tessellateGenericCoveringIter(grid, ds, options, resolution, coverage)
    }

    /**
      * Generic covering tessellation. Candidate cells come from [[GridSystem.coveringCandidateCells]] on
      * the raster [[footprint]], which encodes each grid's buffering strategy (H3/BNG buffer; quadbin does
      * not). The shared positive-area keep-test against the footprint, validity guard, and
      * [[ClipToGeom.clip]] are applied identically for every grid.
      *
      * When `coverage == "sparse"`, chips whose clipped result is entirely NoData are released and
      * skipped. When `coverage == "complete"` (default), all-NoData chips are emitted as-is.
      *
      * The input, the BNG warp and a custom grid SR ([[TessellateNatives]]) live until the iterator is
      * exhausted or closed, and are released once; they are also released if setup fails.
      */
    private def tessellateGenericCoveringIter(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        coverage: String
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        val natives = new TessellateNatives(ds)
        try {
            natives.gridSR = srForGrid(grid)
            natives.ownsGridSR = isCustom(grid)
            // BNG requires a pre-warp to EPSG:27700; all other grids (H3/quadbin/custom) use ds as-is.
            if (isBng(grid)) natives.useBngWarp()
            val fp = footprint(grid, natives.workDs, natives.gridSR)
            val cells = grid.coveringCandidateCells(fp, resolution).toArray
            // One warp source (and mask view, for a source with its own mask) for every cell's clip.
            natives.clipSource = new WarpSource(natives.workDs)
            if (coveringSetupFault != null) throw coveringSetupFault
            new CoveringChipIterator(grid, natives, options, coverage, fp, cells)
        } catch {
            case t: Throwable =>
                natives.close()
                throw t
        }
    }

    /** Emits, in candidate order, one clipped chip per candidate cell that is valid and has positive-area
      * overlap with `rasterFootprint`. The natives are held until the candidates are exhausted (including
      * a tessellation with no chips at all) or the iterator is closed; `close()` is idempotent, releases a
      * chip that `hasNext` fetched but `next()` never handed out, and drops the footprint and candidates. */
    private final class CoveringChipIterator(
        grid: GridSystem,
        natives: TessellateNatives,
        options: Map[String, String],
        coverage: String,
        private var rasterFootprint: Geometry,
        private var cells: Array[Long]
    ) extends Iterator[(Any, Dataset, Map[String, String])]
          with AutoCloseable {
        private var closed   = false
        private var fetched  = false
        private var cc       = 0
        private var nextTile: (Any, Dataset, Map[String, String]) = _
        // A source without NoData gets the dtype fallback, so the clip fill outside each cell reads as
        // NoData rather than as valid zero pixels (the light tier does the same).
        private val fillNoData: Option[Double] = {
            val band = natives.workDs.GetRasterBand(1)
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            if (nd(0) != null) None else Some(GDAL.getNoDataConstant(band.getDataType))
        }

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
                    if (hasPositiveAreaOverlap(cellGeom, rasterFootprint)) {
                        val (resDs, resMtd) = ClipToGeom.clip(
                          natives.workDs,
                          options,
                          cellGeom,
                          natives.gridSR,
                          dstNoData = fillNoData,
                          source = Some(natives.clipSource)
                        )
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

        override def close(): Unit =
            if (!closed) {
                closed = true
                // A fetched chip that next() never handed out is still ours.
                if (fetched && nextTile != null) RasterDriver.releaseDataset(nextTile._2)
                nextTile = null
                cells = null
                rasterFootprint = null
                natives.close()
            }
    }

    // ------------------------------------------------------------------------------------------------
    // Centroid tessellation: assignment in row strips, chips read by window.
    // ------------------------------------------------------------------------------------------------

    /** Pixels per centroid-assignment strip (1 M), the cap the lightweight tier uses: each band's mask
      * strip stays about 1 MB whatever the raster size. */
    private val StripPixels = 1048576

    /** Initial capacity of the per-cell tables; doubled as cells are found. */
    private val InitialCells = 1024

    /** Most pixels the centroid label table can index (a JVM array holds a little under Int.MaxValue). */
    private val MaxCentroidPixels: Long = Int.MaxValue - 8L

    /** Test seam: when positive, the centroid assignment strip height in rows, instead of [[centroidStripRows]]. */
    @volatile private[operations] var centroidStripRowsOverride: Int = 0

    /** Test seam: the tallest strip, in rows, that the last centroid assignment read. */
    @volatile private[operations] var lastAssignmentMaxRows: Int = 0

    /** Test seam: when set, thrown at the end of the centroid setup, once every native resource exists. */
    @volatile private[operations] var centroidSetupFault: Throwable = null

    /** Test seam: when set, thrown at the end of the covering setup, once every native resource exists. */
    @volatile private[operations] var coveringSetupFault: Throwable = null

    /** Rows per centroid-assignment strip: at most [[StripPixels]] pixels, or one row if a single row is
      * wider than that. When the source blocks are shorter, the strip is a whole number of blocks (at
      * least one), so each block is decoded for one strip only. Same rule as the lightweight tier. */
    private[operations] def centroidStripRows(xSize: Int, ySize: Int, blockYSize: Int): Int = {
        var rows = math.max(1, math.min(ySize, StripPixels / xSize))
        if (blockYSize > 0 && blockYSize < rows) rows -= rows % blockYSize
        rows
    }

    /** Native resources one tessellation (covering or centroid) owns until its iterator ends: the `input`
      * dataset, always (a tessellation owns its input), the dataset it works on (`workDs`: the EPSG:27700
      * warp, owned when `reprojected`, else `input` itself), a fresh custom-grid SR (`gridSR`, owned when
      * `ownsGridSR`), for covering the [[WarpSource]] of `workDs` its clips warp through (`clipSource`) and,
      * for centroid, the per-pixel transform `tf` and its inverse `tfInv`. `close()` releases them once
      * (`clipSource` before `workDs`, whose bands its mask view reads); later calls do nothing. Shared SRs
      * (GDAL.WSG84, BngSR) are never released. */
    private final class TessellateNatives(val input: Dataset) extends AutoCloseable {
        var workDs: Dataset = input
        var reprojected: Boolean = false
        var gridSR: SpatialReference = null
        var ownsGridSR: Boolean = false
        var clipSource: WarpSource = null
        var tf: CoordinateTransformation = null
        var tfInv: CoordinateTransformation = null
        private var released = false

        /** Works on `input` warped to EPSG:27700 ([[warpToBng]]; `input` itself when already 27700). */
        def useBngWarp(): Unit = {
            val (warped, owned) = warpToBng(input)
            workDs = warped
            reprojected = owned
        }

        override def close(): Unit = synchronized {
            if (!released) {
                released = true
                if (tf != null) tf.delete()
                if (tfInv != null) tfInv.delete()
                if (ownsGridSR && gridSR != null) gridSR.delete()
                if (clipSource != null) clipSource.close()
                if (reprojected) RasterDriver.releaseDataset(workDs)
                RasterAccessors.unlink(input)
            }
        }
    }

    /** Pixel-to-cell assignment of one raster. `labels(p)` is the label of the cell holding source pixel
      * `p` (row-major flat index; -1 = none). Labels `0 until count` are numbered in first-seen pixel
      * order; label `k` is cell `cellIds(k)` with the inclusive pixel window rows `r0(k)..r1(k)`, columns
      * `c0(k)..c1(k)`. `cellIndex` maps each cell met to its label (-1 for a cell the grid rejects). */
    private final class CentroidAssignment(
        val labels: Array[Int],
        val count: Int,
        val cellIds: Array[Long],
        val r0: Array[Int],
        val r1: Array[Int],
        val c0: Array[Int],
        val c1: Array[Int],
        val cellIndex: mutable.LongMap[Int]
    )

    /** Assigns every valid pixel of `workDs` (valid in ANY band's mask) to the cell holding its centre,
      * one row strip at a time ([[centroidStripRows]]). Only the mask bands of one strip are read; band
      * values are read later, per chip window. Pixel centres go through the full geotransform `gt`, then
      * `tf` into the grid CRS (null when the raster is already in it). Memory is the 4 B/px label table,
      * one mask strip per band and five primitive per-cell tables. */
    private def assignCentroids(
        grid: GridSystem,
        workDs: Dataset,
        gt: Array[Double],
        tf: CoordinateTransformation,
        resolution: Int
    ): CentroidAssignment = {
        val xSize     = workDs.getRasterXSize
        val ySize     = workDs.getRasterYSize
        val bandCount = workDs.getRasterCount
        val rows =
            if (centroidStripRowsOverride > 0) math.min(centroidStripRowsOverride, ySize)
            else centroidStripRows(xSize, ySize, workDs.GetRasterBand(1).GetBlockYSize())
        val maskBands = Array.tabulate(bandCount)(b => workDs.GetRasterBand(b + 1).GetMaskBand())
        val masks     = Array.fill(bandCount)(new Array[Byte](xSize * rows))
        val labels    = new Array[Int](xSize * ySize)
        java.util.Arrays.fill(labels, -1)

        val cellIndex = new mutable.LongMap[Int]()
        var cellIds   = new Array[Long](InitialCells)
        var r0        = new Array[Int](InitialCells)
        var r1        = new Array[Int](InitialCells)
        var c0        = new Array[Int](InitialCells)
        var c1        = new Array[Int](InitialCells)
        var count     = 0
        // The previous pixel's cell and label: neighbouring pixels mostly share a cell, which skips the lookup.
        var lastCell  = 0L
        var lastLabel = Int.MinValue // no previous pixel yet
        var maxRows   = 0

        var y0 = 0
        while (y0 < ySize) {
            val n = math.min(rows, ySize - y0)
            var b = 0
            while (b < bandCount) {
                val rc = maskBands(b).ReadRaster(0, y0, xSize, n, masks(b))
                if (rc != gdalconstConstants.CE_None) {
                    throw new IllegalStateException(s"reading the band ${b + 1} mask, rows $y0..${y0 + n - 1}, failed: ${gdal.GetLastErrorMsg()}")
                }
                b += 1
            }
            if (n > maxRows) maxRows = n
            var r = 0
            while (r < n) {
                val y        = y0 + r
                val rowBase  = y * xSize
                val stripRow = r * xSize
                val yOff     = 0.5 + y
                var x = 0
                while (x < xSize) {
                    var anyValid = false
                    b = 0
                    while (b < bandCount && !anyValid) { if (masks(b)(stripRow + x) != 0) anyValid = true; b += 1 }
                    if (anyValid) {
                        val xOff = 0.5 + x
                        var cx   = gt(0) + xOff * gt(1) + yOff * gt(2)
                        var cy   = gt(3) + xOff * gt(4) + yOff * gt(5)
                        if (tf != null) {
                            val p = tf.TransformPoint(cx, cy)
                            cx = p(0)
                            cy = p(1)
                        }
                        val cellId = grid.pointToCellID(cx, cy, resolution)
                        val k =
                            if (lastLabel != Int.MinValue && cellId == lastCell) lastLabel
                            else {
                                val found = cellIndex.getOrElse(cellId, Int.MinValue)
                                val label =
                                    if (found != Int.MinValue) found
                                    else if (!isCellValid(grid, cellId)) {
                                        cellIndex.update(cellId, -1)
                                        -1
                                    } else {
                                        if (count == cellIds.length) {
                                            val cap = math.min(count.toLong * 2, MaxCentroidPixels).toInt
                                            cellIds = java.util.Arrays.copyOf(cellIds, cap)
                                            r0 = java.util.Arrays.copyOf(r0, cap)
                                            r1 = java.util.Arrays.copyOf(r1, cap)
                                            c0 = java.util.Arrays.copyOf(c0, cap)
                                            c1 = java.util.Arrays.copyOf(c1, cap)
                                        }
                                        cellIds(count) = cellId
                                        r0(count) = y
                                        r1(count) = y
                                        c0(count) = x
                                        c1(count) = x
                                        cellIndex.update(cellId, count)
                                        count += 1
                                        count - 1
                                    }
                                lastCell = cellId
                                lastLabel = label
                                label
                            }
                        if (k >= 0) {
                            labels(rowBase + x) = k
                            r1(k) = y // rows are visited in order, so the last row seen is the largest
                            if (x < c0(k)) c0(k) = x
                            if (x > c1(k)) c1(k) = x
                        }
                    }
                    x += 1
                }
                r += 1
            }
            y0 += n
        }
        lastAssignmentMaxRows = maxRows
        new CentroidAssignment(labels, count, cellIds, r0, r1, c0, c1, cellIndex)
    }

    /**
      * Generic centroid tessellation. Assigns each valid pixel to the cell returned by
      * [[GridSystem.pointToCellID]] for its centre ([[assignCentroids]], row strips into a primitive
      * label table), then emits one chip per cell, in first-seen pixel order (row-major), cropped to the
      * bounding pixel window of its assigned pixels and read from the raster by that window (window
      * pixels assigned to other cells are nodata). BNG warps the raster to EPSG:27700 first so that pixel
      * coordinates are already in the grid's native CRS. Other grids reproject pixel centres per pixel
      * when the raster CRS differs from the grid CRS.
      *
      * When `coverage == "complete"`, the assigned chips are followed by a synthetic all-NoData chip for
      * each covered cell (positive-area overlap, valid grid cell) that caught no pixel centroid: a 1x1
      * chip on the source pixel holding the cell's centroid (see [[centroidPixel]]). When
      * `coverage == "sparse"`, only cells with >=1 assigned pixel are emitted.
      *
      * A band without a declared NoData blanks with [[GDAL.getNoDataConstant]] for its data type (the
      * same table the light tier uses), so a real 0 pixel is never mistaken for nodata.
      *
      * Covered cells are kept against the raster [[footprint]] (the reprojected, densified edge), as in
      * covering.
      *
      * The input, the BNG warp, the transforms and a custom grid SR ([[TessellateNatives]]) live until the
      * iterator is exhausted or closed, and are released once; they are also released if setup fails.
      */
    private def tessellateGenericCentroidIter(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        coverage: String
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        val natives = new TessellateNatives(ds)
        try {
            // BNG requires a pre-warp so pixel coordinates are in EPSG:27700 (the grid's native CRS).
            // Other grids, including a custom grid at 27700, use ds as-is.
            if (isBng(grid)) natives.useBngWarp()
            val workDs = natives.workDs
            val xSize = workDs.getRasterXSize
            val ySize = workDs.getRasterYSize
            require(
              xSize.toLong * ySize <= MaxCentroidPixels,
              s"centroid tessellation supports at most $MaxCentroidPixels pixels; the raster is $xSize x $ySize. " +
                "Retile it first (gbx_rst_retile)."
            )
            val bandCount = workDs.getRasterCount
            val dtype     = workDs.GetRasterBand(1).getDataType
            val gt        = workDs.GetGeoTransform
            val projWkt   = workDs.GetProjection()
            val sourcePath = Option(workDs.GetFileList())
                .flatMap(_.asScala.headOption.map(_.toString))
                .getOrElse("unknown source path")

            natives.gridSR = srForGrid(grid)
            natives.ownsGridSR = isCustom(grid)
            val gridSR = natives.gridSR
            // For centroid+complete: the covered cells, kept against the raster footprint in the grid CRS.
            val (completeFootprint, completeCoveredCells): (Geometry, Array[Long]) =
                if (coverage == "complete") {
                    val fp = footprint(grid, workDs, gridSR)
                    (fp, grid.coveringCandidateCells(fp, resolution).toArray)
                } else (null, Array.empty[Long])
            // Per-pixel reprojection TARGET is the grid's OWN CRS (srForGrid), so pixel centroids are
            // transformed into the grid CRS before pointToCellID for ANY grid CRS, consistent with the
            // raster->grid path (which reprojects the raster to the grid CRS). BNG is pre-warped to its
            // native 27700, so it needs no per-pixel transform. H3/quadbin: srForGrid is the shared WGS84
            // SR, so the target stays 4326. Custom: a fresh SR built from grid.crsSrid.
            val srcSR: SpatialReference = if (isBng(grid)) null else workDs.GetSpatialRef
            val needsTransform = srcSR != null && srcSR.IsSame(gridSR) != 1
            if (needsTransform) natives.tf = new CoordinateTransformation(srcSR, gridSR)
            // The inverse (grid CRS -> working-raster CRS) moves a covered-but-empty cell's centroid onto
            // the raster; only centroid+complete builds those chips.
            if (needsTransform && coverage == "complete") natives.tfInv = new CoordinateTransformation(gridSR, srcSR)

            val bandNoData = Array.tabulate(bandCount) { bi =>
                val nd = new Array[java.lang.Double](1)
                workDs.GetRasterBand(bi + 1).GetNoDataValue(nd)
                if (nd(0) != null) nd(0).doubleValue() else GDAL.getNoDataConstant(dtype)
            }

            val assigned = assignCentroids(grid, workDs, gt, natives.tf, resolution)

            // For complete coverage: synthetic cells (valid, covered, but no centroid pixel assigned), each
            // with the source pixel its 1x1 chip sits on.
            val syntheticCells: Array[(Long, Int, Int)] = if (coverage == "complete") {
                lazy val invGt = {
                    val inv = new Array[Double](6)
                    require(gdal.InvGeoTransform(gt, inv) == 1, s"raster geotransform ${gt.mkString(",")} is not invertible")
                    inv
                }
                completeCoveredCells.iterator
                    .filter(cellId => isCellValid(grid, cellId) && !assigned.cellIndex.contains(cellId))
                    .map(cellId => (cellId, grid.cellIdToGeometry(cellId)))
                    .filter { case (_, cellGeom) => hasPositiveAreaOverlap(cellGeom, completeFootprint) }
                    .map { case (cellId, cellGeom) =>
                        val (col, row) = centroidPixel(cellGeom, natives.tfInv, invGt, xSize, ySize)
                        (cellId, col, row)
                    }
                    .toArray
            } else Array.empty[(Long, Int, Int)]

            if (centroidSetupFault != null) throw centroidSetupFault
            val ctx = ChipContext(grid, grid.name.toLowerCase, projWkt, sourcePath, options, bandCount, dtype, gt, bandNoData)
            new CentroidChipIterator(ctx, natives, assigned, syntheticCells, xSize)
        } catch {
            case t: Throwable =>
                natives.close()
                throw t
        }
    }

    /** Emits the assigned chips (labels `0 until count`, first-seen pixel order), then the synthetic
      * empties in `syntheticCells` order. Assigned chips read their window from `natives.workDs`, so the
      * natives and the label table are held until the iterator is exhausted (including a tessellation
      * with no chips at all) or closed; `close()` is idempotent and drops the label table, the synthetic
      * cells and the chip context, so a task-lifetime cleanup listener holding the iterator pins none. */
    private final class CentroidChipIterator(
        private var ctx: ChipContext,
        natives: TessellateNatives,
        private var assigned: CentroidAssignment,
        private var syntheticCells: Array[(Long, Int, Int)],
        xSize: Int
    ) extends Iterator[(Any, Dataset, Map[String, String])]
          with AutoCloseable {
        private var closed = false
        private var k      = 0
        private var synIdx = 0

        /** Releases everything as soon as the last chip has been handed out. */
        private def closeIfExhausted(): Unit =
            if (!closed && k >= assigned.count && synIdx >= syntheticCells.length) close()

        override def hasNext: Boolean = {
            closeIfExhausted()
            !closed
        }

        override def next(): (Any, Dataset, Map[String, String]) = {
            if (!hasNext) throw new NoSuchElementException("the centroid tessellation has no more chips")
            val tile = if (k < assigned.count) {
                val t = buildGenericCentroidChip(ctx, natives.workDs, assigned, k, xSize)
                k += 1
                t
            } else {
                // Synthetic all-NoData chip for a covered cell with no centroid pixel.
                val (cellId, col, row) = syntheticCells(synIdx)
                synIdx += 1
                buildSyntheticEmptyChip(ctx, cellId, col, row)
            }
            closeIfExhausted()
            tile
        }

        override def close(): Unit =
            if (!closed) {
                closed = true
                assigned = null
                syntheticCells = null
                ctx = null
                natives.close()
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

    /** Per-call values shared by every centroid chip (assigned and synthetic) of one tessellation. */
    private final case class ChipContext(
        grid: GridSystem,
        gridName: String,
        projWkt: String,
        sourcePath: String,
        options: Map[String, String],
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandNoData: Array[Double]
    )

    /** Creates the /vsimem GTiff for one centroid chip: `w` x `h` pixels whose top-left is source pixel
      * (`col`, `row`). The geotransform is the source's offset to that pixel (rotation and pixel-size
      * terms kept, so rotated and south-up rasters stay on the source grid). Returns (path, dataset). */
    private def createChip(ctx: ChipContext, cellId: Long, w: Int, h: Int, col: Int, row: Int): (String, Dataset) = {
        val gt         = ctx.gt
        val uuid       = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rasterPath = s"/vsimem/${ctx.gridName}_centroid_${cellId}_$uuid.tif"
        val out = GDALManager.gtiffDriver().Create(rasterPath, w, h, ctx.bandCount, ctx.dtype)
        out.SetGeoTransform(
          Array(gt(0) + col * gt(1) + row * gt(2), gt(1), gt(2), gt(3) + col * gt(4) + row * gt(5), gt(4), gt(5))
        )
        out.SetProjection(ctx.projWkt)
        (rasterPath, out)
    }

    /** Tile metadata shared by every centroid chip (assigned and synthetic). */
    private def chipMeta(ctx: ChipContext, rendered: Any, rasterPath: String): Map[String, String] =
        Map(
          "path"         -> rasterPath,
          "parentPath"   -> ctx.options.getOrElse("path", ctx.sourcePath),
          "driver"       -> "GTiff",
          "format"       -> "GTiff",
          "last_command" -> s"${ctx.gridName}_centroid_tessellate cell=$rendered",
          "last_error"   -> "",
          "all_parents"  -> s"${ctx.sourcePath};${ctx.options.getOrElse("all_parents", "")}",
          "size"         -> "-1",
          "compression"  -> ctx.options.getOrElse("compression", OperatorOptions.DefaultCompression),
          "isZipped"     -> "false",
          "isSubset"     -> "false"
        )

    /** Builds the chip of label `k`: its pixel window, read from `workDs` band by band, with the window
      * pixels assigned to other cells (or to none) set to nodata. Works for H3, quadbin, BNG and custom
      * grids because the grid-specific rendering ([[GridSystem.renderCellId]]) and name come from `ctx`.
      * The chip is released if building it fails. */
    private def buildGenericCentroidChip(
        ctx: ChipContext,
        workDs: Dataset,
        a: CentroidAssignment,
        k: Int,
        xSize: Int
    ): (Any, Dataset, Map[String, String]) = {
        val (c0, r0) = (a.c0(k), a.r0(k))
        val w        = a.c1(k) - c0 + 1
        val h        = a.r1(k) - r0 + 1
        val cellId   = a.cellIds(k)
        val labels   = a.labels
        val rendered = ctx.grid.renderCellId(cellId)
        val (rasterPath, out) = createChip(ctx, cellId, w, h, c0, r0)
        try {
            val buf = new Array[Double](w * h)
            var b = 0
            while (b < ctx.bandCount) {
                val nd = ctx.bandNoData(b)
                val rc = workDs.GetRasterBand(b + 1).ReadRaster(c0, r0, w, h, buf)
                if (rc != gdalconstConstants.CE_None) {
                    throw new IllegalStateException(s"reading the band ${b + 1} window of cell $rendered failed: ${gdal.GetLastErrorMsg()}")
                }
                var j = 0
                var r = 0
                while (r < h) {
                    var p = (r0 + r) * xSize + c0
                    var c = 0
                    while (c < w) {
                        if (labels(p) != k) buf(j) = nd
                        p += 1
                        j += 1
                        c += 1
                    }
                    r += 1
                }
                val db = out.GetRasterBand(b + 1)
                db.SetNoDataValue(nd)
                // Implicit Float64 buffer overload: GDAL converts double[] to the band's native dtype on write.
                db.WriteRaster(0, 0, w, h, buf)
                b += 1
            }
            out.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
            out.FlushCache()
        } catch {
            case t: Throwable =>
                RasterDriver.releaseDataset(out)
                throw t
        }
        (rendered, out, chipMeta(ctx, rendered, rasterPath))
    }

    /** Builds the 1x1 all-NoData chip of a covered cell that caught no pixel centroid (used by
      * `centroid+complete`), on source pixel (`col`, `row`) from [[centroidPixel]]. Every band holds
      * its nodata value. The chip is released if building it fails. */
    private def buildSyntheticEmptyChip(ctx: ChipContext, cellId: Long, col: Int, row: Int): (Any, Dataset, Map[String, String]) = {
        val rendered = ctx.grid.renderCellId(cellId)
        val (rasterPath, out) = createChip(ctx, cellId, 1, 1, col, row)
        try {
            var b = 0
            while (b < ctx.bandCount) {
                val db = out.GetRasterBand(b + 1)
                db.SetNoDataValue(ctx.bandNoData(b))
                db.WriteRaster(0, 0, 1, 1, Array(ctx.bandNoData(b)))
                b += 1
            }
            out.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
            out.FlushCache()
        } catch {
            case t: Throwable =>
                RasterDriver.releaseDataset(out)
                throw t
        }
        (rendered, out, chipMeta(ctx, rendered, rasterPath))
    }

    // ------------------------------------------------------------------------------------------------
    // Named wrappers — thin delegates to `tessellate`, preserving the exact per-grid return types
    // so all current callers (RST_H3_Tessellate, RST_Quadbin_Tessellate, RST_BNG_Tessellate) and
    // tests compile and run unchanged.
    // ------------------------------------------------------------------------------------------------

    /**
      * Iterator of (cellId Long, Dataset, metadata) per emitted H3 cell at resolution. Caller must release
      * each Dataset; iterator is AutoCloseable.
      *
      * The iterator owns `ds` and releases it when exhausted or closed (a call that throws has released it
      * already), so the caller never releases `ds`. Centroid reads `ds` lazily, chip by chip: the caller
      * must keep `ds` open until the iterator is exhausted or closed.
      *
      *  - `assignment="centroid"` (default): pixel-centroid single-assignment.
      *  - `assignment="covering"`: one chip per cell whose hexagon overlaps the raster footprint (reprojected
      *    to lon/lat and densified, see [[footprint]]).
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
      * Caller must release each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]],
      * including its ownership of `ds`: centroid reads `ds` lazily, so the caller must keep `ds` open until
      * the iterator is exhausted or closed, and never releases it.
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
      * Caller must release each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]],
      * including its ownership of `ds` (the EPSG:27700 warp is released with it): centroid reads the
      * raster lazily, so the caller must keep `ds` open until the iterator is exhausted or closed, and
      * never releases it.
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
      * Caller must release each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]],
      * including its ownership of `ds` (also released when the R3 guard below rejects the raster):
      * centroid reads `ds` lazily, so the caller must keep `ds` open until the iterator is exhausted or
      * closed, and never releases it.
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
        releaseOnThrow(ds)(CustomGridBounds.requireBoundsContainRaster(grid, ds))
        tessellate(grid, ds, options, resolution, assignment, coverage)
            .asInstanceOf[Iterator[(Long, Dataset, Map[String, String])]]
    }

}
