package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.Geometry

/**
 * Common interface for GeoBrix discrete grid systems (H3, BNG, quadbin, custom).
 * Instance-based so a custom grid can carry its own configuration; H3/BNG/Quadbin
 * are singleton-backed instances. Defines the members consumed by raster
 * tessellation/aggregation and neighbourhood/formatting paths.
 */
trait GridSystem extends Serializable {
  /** Stable grid name, e.g. "H3", "BNG", "QUADBIN", "CUSTOM". */
  def name: String
  /** SRID the grid's cell geometries are expressed in (4326 for H3/quadbin, 27700 for BNG). */
  def crsSrid: Int
  /** Valid resolution keys for this grid. */
  def resolutions: Set[Int]
  /** Index a point (in the grid's native CRS) to a cell id. */
  def pointToCellID(x: Double, y: Double, resolution: Int): Long
  /** Cell id -> its polygon in `crsSrid`. */
  def cellIdToGeometry(cellID: Long): Geometry
  /** Cells (by id) whose geometry the input geometry covers, at `resolution`. */
  def polyfill(geometry: Geometry, resolution: Int): Seq[Long]
  /**
   * Candidate cells for COVERING tessellation of a raster bbox — the enumeration step
   * BEFORE the positive-area keep-test. Grid-specific by necessity: quadbin enumerates
   * the bbox directly (its two-corner tile lookup already includes every overlapping cell);
   * H3 and BNG buffer the bbox because their polyfill is centroid-based and would otherwise
   * miss cells that overlap but whose centroid lies outside. The generic tessellate path
   * applies one shared keep-test to whatever this method returns.
   */
  def coveringCandidateCells(bbox: Geometry, resolution: Int): Seq[Long]
  /** All cells within Chebyshev grid-ring distance ≤ k of cellID (inclusive of center).
    * Distance is defined as the minimum k such that b ∈ kRing(a, k) — equivalently,
    * max(|dx|, |dy|) in cell-position units for rectangular grids, or the H3 hex distance.
    * k=0 → Seq(cellID). All four grid implementations MUST honour this contract.
    */
  def kRing(cellID: Long, k: Int): Seq[Long]
  /** Cells at EXACTLY Chebyshev grid-ring distance k from cellID (hollow ring).
    * k=0 → Seq(cellID) for all grid implementations.
    */
  def kLoop(cellID: Long, k: Int): Seq[Long]
  /**
   * How this grid renders a cell id in raster->grid OUTPUT.
   * H3/quadbin emit the Long; BNG emits its formatted string. Default: the Long.
   */
  def renderCellId(cellID: Long): Any = cellID

  /**
   * Geometry-aware k-ring with dilation mode + coverage basis. Default implementation routes
   * through [[GeomDilation.expand]] so quadbin and custom grids get this for free. `coverage`
   * ∈ {"coveras","polyfill","core"} selects the belongs-to basis (default "coveras"). BNG
   * overrides the 5-arg form to additionally drop out-of-bounds cells via `BNG.isValid`.
   */
  def geometryKRing(geom: Geometry, resolution: Int, k: Int, mode: String, coverage: String): Set[Long] =
    GeomDilation.expand("ring", k, mode, this, geom, resolution, coverage)

  /** 4-arg overload: default coverage "coveras". Delegates virtually to the 5-arg form so a
    * grid overriding only the 5-arg form (e.g. BNG's isValid filter) is honoured here too. */
  def geometryKRing(geom: Geometry, resolution: Int, k: Int, mode: String): Set[Long] =
    geometryKRing(geom, resolution, k, mode, GeomDilation.DEFAULT_COVERAGE)

  /**
   * Geometry-aware k-loop with dilation mode + coverage basis. Default implementation routes
   * through [[GeomDilation.expand]] so quadbin and custom grids get this for free.
   */
  def geometryKLoop(geom: Geometry, resolution: Int, k: Int, mode: String, coverage: String): Set[Long] =
    GeomDilation.expand("loop", k, mode, this, geom, resolution, coverage)

  /** 4-arg overload: default coverage "coveras". Delegates virtually to the 5-arg form. */
  def geometryKLoop(geom: Geometry, resolution: Int, k: Int, mode: String): Set[Long] =
    geometryKLoop(geom, resolution, k, mode, GeomDilation.DEFAULT_COVERAGE)

  /**
   * True iff this grid's point-partition (`pointToCellID`) coincides EXACTLY with the polygon
   * `cellIdToGeometry` returns — so a pixel whose four corners all bin to one cell provably lies
   * wholly inside that (convex) cell polygon. When true, the covering raster→grid path may assign
   * such an interior pixel weight 1.0 directly (skipping the per-candidate JTS area split) and stay
   * BIT-IDENTICAL to the intersection result. When false, the interior fast-path is NOT taken and
   * every pixel uses the exact candidate+intersection split.
   *
   * Analytic-square grids (BNG, Quadbin, Custom) satisfy this: `pointToCellID` floor-bins to the
   * same square `cellIdToGeometry` draws. H3 does NOT: `pointToCellID` is `geoToH3` (the true H3
   * partition) while `cellIdToGeometry` is the `h3ToGeoBoundary` chord polygon, which under-shoots
   * the geodesic hex edge — a pixel binning to a hex can leave a sliver in the chord gap, so its
   * exact intersection weight is slightly < 1.0. H3 therefore keeps the intersection path.
   *
   * Defaults to false so a new grid must opt in explicitly after proving exactness.
   */
  def coveringFastPathExact: Boolean = false
}

object GridSystem {
  /**
   * Returns the GridSystem instance for the given name.
   * Recognised names (case-insensitive): H3, BNG, QUADBIN, CUSTOM.
   * CUSTOM requires a GridConf; supplying conf=None for CUSTOM throws IllegalArgumentException.
   * An unrecognised name throws IllegalArgumentException.
   */
  def forName(name: String, conf: Option[GridConf] = None): GridSystem =
    name.toUpperCase match {
      case "H3"      => H3
      case "BNG"     => BNG
      case "QUADBIN" => Quadbin
      case "CUSTOM"  =>
        val c = conf.getOrElse(
          throw new IllegalArgumentException(
            "GridConf is required for a CUSTOM grid; pass conf=Some(GridConf(...))"
          )
        )
        CustomGridSystem(c)
      case n         => throw new IllegalArgumentException(s"Unknown grid: $n")
    }
}
