package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.{
  Geometry, GeometryCollection, GeometryFactory, LineString, MultiLineString,
  MultiPoint, MultiPolygon, Point, Polygon
}
import org.locationtech.jts.linearref.LengthIndexedLine
import scala.collection.mutable
import scala.util.Try

object GeomDilation {
  val DEFAULT_MODE = "boundary-out"
  val MODES: Seq[String] = Seq(
    "boundary-out", "boundary-in", "boundary-in-ignore-holes",
    "hole-in", "hole-out", "hole-out-ignore-geom")

  // Coverage basis selector (LOCKED design, "COVERAGE PARAM"). Three nested predicates
  // decide which cells "belong to" a region P/S/H:
  //   coveras  -> cover    (*Cover)    : cell overlaps the region
  //   polyfill -> centroid (*Centroid) : cell CENTROID is inside the region
  //   core     -> core     (*Core)     : cell is fully contained by the region
  // Nested: cover ⊇ centroid ⊇ core. The chosen basis drives the seed (perimeter),
  // the admit bound, and boundary-ring inclusion.  Mirrors light `_dilate.COVERAGE`.
  val DEFAULT_COVERAGE = "coveras"
  val COVERAGE: Seq[String] = Seq("coveras", "polyfill", "core")

  final case class Classification(
      pCover: Set[Long], pCore: Set[Long],
      sCover: Set[Long], sCore: Set[Long],
      hCover: Set[Long], hCore: Set[Long],
      // Centroid basis (cell CENTROID inside the region) — the "polyfill" coverage.
      // Nested strictly between core and cover: core ⊆ centroid ⊆ cover.  Defaulted so
      // legacy 6-arg positional construction still compiles.
      pCentroid: Set[Long] = Set.empty, sCentroid: Set[Long] = Set.empty, hCentroid: Set[Long] = Set.empty) {
    def pBorder: Set[Long] = pCover diff pCore
    def hBorder: Set[Long] = hCover diff hCore

    /** (P_X, S_X, H_X) — belongs-to sets for the chosen coverage basis; validates `coverage`. */
    def basisSets(coverage: String): (Set[Long], Set[Long], Set[Long]) = coverage match {
      case "coveras"  => (pCover, sCover, hCover)
      case "polyfill" => (pCentroid, sCentroid, hCentroid)
      case "core"     => (pCore, sCore, hCore)
      case other =>
        throw new IllegalArgumentException(s"unknown coverage '$other'; expected ${COVERAGE.mkString(", ")}")
    }
  }

  private def solidAndHoles(geom: Geometry): (Geometry, Option[Geometry]) = {
    val gf = new GeometryFactory()
    val polys: Seq[Polygon] = (0 until geom.getNumGeometries).map(geom.getGeometryN)
      .collect { case p: Polygon => p }
    val solids: Seq[Geometry] = polys.map(p => gf.createPolygon(p.getExteriorRing.getCoordinates))
    val holes: Seq[Geometry] = polys.flatMap(p =>
      (0 until p.getNumInteriorRing).map(i => gf.createPolygon(p.getInteriorRingN(i).getCoordinates)))
    val solid: Geometry = if (solids.isEmpty) geom else solids.reduce(_ union _)
    val hole: Option[Geometry] = if (holes.isEmpty) None else Some(holes.reduce(_ union _))
    (solid, hole)
  }

  /** Non-empty, non-collection leaf members of a GeometryCollection, recursing
    * into nested collections. A Multi* member is returned as-is (expanded later
    * by groupByDimension). Tests the type STRING, not isInstanceOf — JTS
    * MultiPolygon extends GeometryCollection. */
  private def flattenMembers(geom: Geometry): Seq[Geometry] = {
    val buf = mutable.ArrayBuffer.empty[Geometry]
    (0 until geom.getNumGeometries).map(geom.getGeometryN).foreach { g =>
      if (!g.isEmpty) {
        if (g.getGeometryType == "GeometryCollection") buf ++= flattenMembers(g)
        else buf += g
      }
    }
    buf.toSeq
  }

  /** Group leaf members into (polygons, lines, points), expanding Multi* parts. */
  private def groupByDimension(
      members: Seq[Geometry]): (Seq[Polygon], Seq[LineString], Seq[Point]) = {
    val polys  = mutable.ArrayBuffer.empty[Polygon]
    val lines  = mutable.ArrayBuffer.empty[LineString]
    val points = mutable.ArrayBuffer.empty[Point]
    members.foreach { g =>
      (0 until g.getNumGeometries).map(g.getGeometryN).foreach {
        case p: Polygon    => polys  += p
        case l: LineString => lines  += l
        case pt: Point     => points += pt
        case _             => ()
      }
    }
    (polys.toSeq, lines.toSeq, points.toSeq)
  }

  /** Union the nine cell-sets across a sequence of Classifications. */
  private def unionClassifications(cs: Seq[Classification]): Classification =
    Classification(
      cs.flatMap(_.pCover).toSet,    cs.flatMap(_.pCore).toSet,
      cs.flatMap(_.sCover).toSet,    cs.flatMap(_.sCore).toSet,
      cs.flatMap(_.hCover).toSet,    cs.flatMap(_.hCore).toSet,
      cs.flatMap(_.pCentroid).toSet, cs.flatMap(_.sCentroid).toSet,
      cs.flatMap(_.hCentroid).toSet)

  /** Topological dimension: 0=point, 1=line/ring, 2=surface/other. Mirrors `_geom_dimension`. */
  private def geomDimension(geom: Geometry): Int = geom.getGeometryType match {
    case "Point" | "MultiPoint"                       => 0
    case "LineString" | "LinearRing" | "MultiLineString" => 1
    case _                                            => 2
  }

  /** Coordinate samples along a point/line geometry, mirroring light `_sample_coords`
    * (centroid + explicit vertices + `nSamples-1` interior fractions for lines). Used only
    * as the point/line fallback when centroid-based polyfill yields no candidates. */
  private def sampleCoords(geom: Geometry, nSamples: Int = 16): Seq[(Double, Double)] = {
    val buf = mutable.ArrayBuffer.empty[(Double, Double)]
    def recurse(g: Geometry): Unit = g match {
      case gc: GeometryCollection => // Multi*/GeometryCollection (mirror hasattr(geom,'geoms'))
        (0 until gc.getNumGeometries).foreach(i => recurse(gc.getGeometryN(i)))
      case _ =>
        val c = g.getCentroid
        if (!c.isEmpty) buf += ((c.getX, c.getY))
        g.getCoordinates.foreach(coord => buf += ((coord.getX, coord.getY)))
        g match {
          case ls: LineString if nSamples > 0 =>
            val len = ls.getLength
            if (len > 0) {
              val lil = new LengthIndexedLine(ls)
              (1 until nSamples).foreach { i =>
                val p = lil.extractPoint(i.toDouble / nSamples * len)
                buf += ((p.x, p.y))
              }
            }
          case _ =>
        }
    }
    recurse(geom)
    buf.toSeq
  }

  def classify(grid: GridSystem, geom: Geometry, res: Int): Classification = {
    // GeometryCollection: mixed-dimension. Flatten, group members by dimension
    // into Multi*, classify each via the existing path, and union. Non-collection
    // and homogeneous Multi* inputs skip this (getGeometryType != "GeometryCollection").
    if (geom.getGeometryType == "GeometryCollection") {
      val members = flattenMembers(geom)
      if (members.isEmpty)
        return Classification(
          Set.empty, Set.empty, Set.empty, Set.empty, Set.empty, Set.empty)
      val (polys, lines, points) = groupByDimension(members)
      val gf = geom.getFactory
      val groups: Seq[Geometry] = Seq(
        if (polys.nonEmpty)  Some(gf.createMultiPolygon(polys.toArray))         else None,
        if (lines.nonEmpty)  Some(gf.createMultiLineString(lines.toArray))       else None,
        if (points.nonEmpty) Some(gf.createMultiPoint(points.toArray))           else None
      ).flatten
      return unionClassifications(groups.map(g => classify(grid, g, res)))
    }
    val (solid, holeOpt) = solidAndHoles(geom)
    val dim = geomDimension(geom)
    // polyfill the SOLID so hole-interior cells are classified (hole modes need hCore).
    val cands = mutable.Set.empty[Long] ++ grid.polyfill(solid, res)
    if (cands.isEmpty) {
      // Fallback when the polyfill yields no candidates: BNG/custom centroid-membership
      // polyfill returns nothing for (a) points/lines and (b) a SUB-CELL polygon (smaller
      // than a cell — its interior holds no cell centroid) even though it overlaps a cell.
      // Sample representative coordinates and map each to its containing cell (matches light).
      val seen = mutable.Set.empty[(Double, Double)]
      sampleCoords(geom).foreach { case (x, y) =>
        if (!seen.contains((x, y))) {
          seen += ((x, y))
          Try(grid.pointToCellID(x, y, res)).foreach(cands += _)
        }
      }
    }

    val pCover, pCore, sCover, sCore, hCover, hCore = mutable.Set.empty[Long]
    val pCentroid, sCentroid, hCentroid = mutable.Set.empty[Long]
    val is2d = dim == 2
    cands.foreach { c =>
      val g = grid.cellIdToGeometry(c)
      val cen = g.getCentroid // cell centroid — the "polyfill" (centroid) basis probe

      // Dimension-aware coverage: polygon uses area>0, line uses length>0, point uses intersects.
      val (pInCover, sInCover) = dim match {
        case 0 =>
          (geom.intersects(g), solid.intersects(g))
        case 1 =>
          val pl = geom.intersects(g) && geom.intersection(g).getLength > 0
          val sl = solid.intersects(g) && solid.intersection(g).getLength > 0
          (pl, sl)
        case _ =>
          val pa = geom.intersects(g) && geom.intersection(g).getArea > 0
          val sa = solid.intersects(g) && solid.intersection(g).getArea > 0
          (pa, sa)
      }
      // centroid and core bases are 2D-only: for a point/line region a cell centroid is
      // "inside" only by measure-zero coincidence, so polyfill/core coverage is empty by
      // construction (matches light — coherent and documented).
      if (pInCover) {
        pCover += c
        if (is2d && geom.contains(cen)) pCentroid += c
        if (is2d && geom.contains(g)) pCore += c
      }
      if (sInCover) {
        sCover += c
        if (is2d && solid.contains(cen)) sCentroid += c
        if (is2d && solid.contains(g)) sCore += c
      }
      holeOpt.foreach { h =>
        if (h.intersects(g) && h.intersection(g).getArea > 0) {
          hCover += c
          if (is2d && h.contains(cen)) hCentroid += c
          if (is2d && h.contains(g)) hCore += c
        }
      }
    }
    Classification(
      pCover.toSet, pCore.toSet, sCover.toSet, sCore.toSet, hCover.toSet, hCore.toSet,
      pCentroid.toSet, sCentroid.toSet, hCentroid.toSet)
  }

  /** Outer perimeter of a cell set: cells in `set` with at least one immediate neighbour
    * (kLoop distance 1) outside `set`. Alignment-robust — any non-empty set has a perimeter
    * even when the geometry aligns exactly to cell boundaries (zero straddling cells). For a
    * holed polygon, hole-interior cells are surrounded by other set cells and never appear. */
  def outerPerimeter(set: Set[Long], grid: GridSystem): Set[Long] =
    set.filter(c => grid.kLoop(c, 1).exists(n => !set.contains(n)))

  // ---------------------------------------------------------------------------
  // Lazy O(perimeter) seed path — mirrors light _dilate.py lazy builders.
  // Used for Polygon/MultiPolygon inputs; point/line/GC fall back to classify+setup.
  // ---------------------------------------------------------------------------

  /** Membership bits for ONE cell.  Mirrors light `_classify_cell`.
    *
    * Returns a mutable.Map with keys p_cover/p_centroid/p_core, s_cover/s_centroid/s_core,
    * h_cover/h_centroid/h_core.  Same dimension-aware coverage semantics as classify(). */
  private def classifyCell(
      c: Long, grid: GridSystem,
      geom: Geometry, solid: Geometry, holeOpt: Option[Geometry], dim: Int
  ): mutable.Map[String, Boolean] = {
    val g   = grid.cellIdToGeometry(c)
    val cen = g.getCentroid
    val (pIn, sIn) = dim match {
      case 0 => (geom.intersects(g), solid.intersects(g))
      case 1 =>
        val pl = geom.intersects(g) && geom.intersection(g).getLength > 0
        val sl = solid.intersects(g) && solid.intersection(g).getLength > 0
        (pl, sl)
      case _ =>
        val pa = geom.intersects(g) && geom.intersection(g).getArea > 0
        val sa = solid.intersects(g) && solid.intersection(g).getArea > 0
        (pa, sa)
    }
    val is2d = dim == 2
    val m = mutable.Map(
      "p_cover" -> false, "p_centroid" -> false, "p_core" -> false,
      "s_cover" -> false, "s_centroid" -> false, "s_core" -> false,
      "h_cover" -> false, "h_centroid" -> false, "h_core" -> false
    )
    if (pIn) {
      m("p_cover") = true
      if (is2d && geom.contains(cen)) m("p_centroid") = true
      if (is2d && geom.contains(g))   m("p_core")     = true
    }
    if (sIn) {
      m("s_cover") = true
      if (is2d && solid.contains(cen)) m("s_centroid") = true
      if (is2d && solid.contains(g))   m("s_core")     = true
    }
    holeOpt.foreach { h =>
      if (h.intersects(g) && h.intersection(g).getArea > 0) {
        m("h_cover") = true
        if (is2d && h.contains(cen)) m("h_centroid") = true
        if (is2d && h.contains(g))   m("h_core")     = true
      }
    }
    m
  }

  /** Cells the boundary `rings` pass through (O(perimeter)).  Mirrors light `_boundary_cells`.
    *
    * `cellStep` = the grid's cell edge length at the target resolution (density guard).
    * Iterates segment-by-segment: each segment is sampled at ≤ cellStep intervals so
    * no boundary cell is skipped. */
  private def boundaryCells(rings: Seq[Geometry], grid: GridSystem, res: Int, cellStep: Double): Set[Long] = {
    val cells = mutable.Set.empty[Long]
    rings.foreach { ring =>
      val coords = ring.getCoordinates
      var i = 0
      while (i < coords.length - 1) {
        val x0 = coords(i).getX;   val y0 = coords(i).getY
        val x1 = coords(i + 1).getX; val y1 = coords(i + 1).getY
        val dx     = x1 - x0; val dy = y1 - y0
        val segLen = math.sqrt(dx * dx + dy * dy)
        val n      = math.max(1, (segLen / cellStep).toInt + 1)
        var j = 0
        while (j <= n) {
          val t  = j.toDouble / n
          val px = x0 + t * dx
          val py = y0 + t * dy
          Try(grid.pointToCellID(px, py, res)).foreach(cells += _)
          j += 1
        }
        i += 1
      }
    }
    cells.toSet
  }

  /** Cells in band's neighbourhood that are in-region and have an out-of-region neighbour.
    * Mirrors light `_local_perimeter`.  C = band ∪ {neighbors of each cell in band}. */
  private def localPerimeter(band: Set[Long], grid: GridSystem, inRegion: Long => Boolean): Set[Long] = {
    val C = mutable.Set.empty[Long] ++ band
    band.foreach(c => C ++= grid.kLoop(c, 1))
    C.filter(c => inRegion(c) && grid.kLoop(c, 1).exists(n => !inRegion(n))).toSet
  }

  /** Extract exterior rings from a solid geometry (Polygon or MultiPolygon). */
  private def extRingsOf(solid: Geometry): Seq[Geometry] = solid match {
    case p: Polygon       => Seq(p.getExteriorRing)
    case mp: MultiPolygon =>
      (0 until mp.getNumGeometries).map(i => mp.getGeometryN(i).asInstanceOf[Polygon].getExteriorRing)
    case g                => Seq(g.getBoundary)
  }

  /** Convert a coverage name to its basis string suffix.  Validates and throws on unknown. */
  private def basisStr(coverage: String): String = coverage match {
    case "coveras"  => "cover"
    case "polyfill" => "centroid"
    case "core"     => "core"
    case other      =>
      throw new IllegalArgumentException(s"unknown coverage '$other'; expected ${COVERAGE.mkString(", ")}")
  }

  /** Lazy (frontier0, visited0, admit, k0) for boundary-out mode.  Mirrors `_lazy_boundary_out`.
    *
    * Avoids the O(area) polyfill by tracing the exterior ring.
    * `admit = n => !s_cover(n)` replaces `visited0 = s_cov` (provably equivalent for k>=1). */
  private def lazyBoundaryOut(
      geom: Geometry, solid: Geometry, holeOpt: Option[Geometry], dim: Int,
      grid: GridSystem, res: Int, coverage: String
  ): (Set[Long], Set[Long], Long => Boolean, Set[Long]) = {
    val cs   = grid.geomCellStep(geom, res)
    val band = boundaryCells(extRingsOf(solid), grid, res, cs)

    val cache = mutable.Map.empty[Long, mutable.Map[String, Boolean]]
    def m(c: Long) = cache.getOrElseUpdate(c, classifyCell(c, grid, geom, solid, holeOpt, dim))
    def sCover(c: Long)   = m(c)("s_cover")
    def sCore(c: Long)    = m(c)("s_core")
    def sCentroid(c: Long)= m(c)("s_centroid")

    // C = band ∪ neighbors(band) — lazy analogue of the classify() candidate set near the ring
    val C = mutable.Set.empty[Long] ++ band
    band.foreach(c => C ++= grid.kLoop(c, 1))

    // Straddling band: overlap S but not fully inside (mirrors light full_band)
    val fullBand = C.filter(c => sCover(c) && !sCore(c)).toSet
    // Lazy outer perimeter: s_cover cells in C with at least one non-s_cover neighbour
    val op = C.filter(c => sCover(c) && grid.kLoop(c, 1).exists(n => !sCover(n))).toSet

    val frontier = if (fullBand.nonEmpty) fullBand else op

    val k0: Set[Long] = coverage match {
      case "polyfill" =>
        // centroid-out: overlap S but centroid outside S (mirrors light centroid_out)
        val centroidOut = C.filter(c => sCover(c) && !sCentroid(c)).toSet
        if (centroidOut.nonEmpty) centroidOut else op
      case _ => frontier
    }

    // admit = not s_cover(n): replaces visited0 = s_cov (BFS never enters the solid)
    val admit: Long => Boolean = n => !sCover(n)
    (frontier, frontier, admit, k0)
  }

  /** Lazy (frontier0, visited0, admit, k0) for boundary-in / boundary-in-ignore-holes.
    * Mirrors `_lazy_boundary_in`. */
  private def lazyBoundaryIn(
      mode: String, geom: Geometry, solid: Geometry, holeOpt: Option[Geometry], dim: Int,
      grid: GridSystem, res: Int, coverage: String
  ): (Set[Long], Set[Long], Long => Boolean, Set[Long]) = {
    val cs   = grid.geomCellStep(geom, res)
    val band = boundaryCells(extRingsOf(solid), grid, res, cs)

    val cache = mutable.Map.empty[Long, mutable.Map[String, Boolean]]
    def m(c: Long) = cache.getOrElseUpdate(c, classifyCell(c, grid, geom, solid, holeOpt, dim))
    def sCover(c: Long) = m(c)("s_cover")

    val op = localPerimeter(band, grid, sCover)

    val basis    = basisStr(coverage)
    val admitKey = (if (mode == "boundary-in") "p_" else "s_") + basis
    val admit: Long => Boolean = n => m(n)(admitKey)
    val k0 = op.filter(c => m(c)(admitKey))
    (op, op, admit, k0)
  }

  /** Lazy (frontier0, visited0, admit, k0) for hole-in / hole-out / hole-out-ignore-geom.
    * Mirrors `_lazy_hole`.
    *
    * Seeds from the interior rings (O(hole perimeter)).  Returns empty for geometries with no holes.
    * visited0 = solid_edge only (not the full h_cov): admit rejects deep hole cells so
    * pre-visiting the entire hole interior is unnecessary. */
  private def lazyHole(
      mode: String, geom: Geometry, solid: Geometry, holeOpt: Option[Geometry], dim: Int,
      grid: GridSystem, res: Int, coverage: String
  ): (Set[Long], Set[Long], Long => Boolean, Set[Long]) = {
    val emptyAdmit: Long => Boolean = _ => false
    val empty = (Set.empty[Long], Set.empty[Long], emptyAdmit, Set.empty[Long])

    if (holeOpt.isEmpty) return empty

    val intRings: Seq[Geometry] = geom match {
      case mp: MultiPolygon =>
        (0 until mp.getNumGeometries).flatMap { i =>
          val p = mp.getGeometryN(i).asInstanceOf[Polygon]
          (0 until p.getNumInteriorRing).map(j => p.getInteriorRingN(j))
        }
      case p: Polygon =>
        (0 until p.getNumInteriorRing).map(j => p.getInteriorRingN(j))
      case _ => Seq.empty
    }
    if (intRings.isEmpty) return empty

    val cs       = grid.geomCellStep(geom, res)
    val holeBand = boundaryCells(intRings, grid, res, cs)
    if (holeBand.isEmpty) return empty

    val cache = mutable.Map.empty[Long, mutable.Map[String, Boolean]]
    def m(c: Long) = cache.getOrElseUpdate(c, classifyCell(c, grid, geom, solid, holeOpt, dim))

    val C = mutable.Set.empty[Long] ++ holeBand
    holeBand.foreach(c => C ++= grid.kLoop(c, 1))

    val voidEdge  = C.filter(c => m(c)("h_cover") && grid.kLoop(c, 1).exists(n => !m(n)("h_cover"))).toSet
    val solidEdge = C.filter(c => m(c)("p_cover") && grid.kLoop(c, 1).exists(n => m(n)("h_cover"))).toSet

    val basis = basisStr(coverage)
    val hKey  = "h_" + basis
    val pKey  = "p_" + basis

    mode match {
      case "hole-in" =>
        // Seed void-side; expand INTO the hole (admit H_basis).
        val admit: Long => Boolean = n => m(n)(hKey)
        val k0 = voidEdge.filter(c => m(c)(hKey))
        (voidEdge, voidEdge, admit, k0)
      case "hole-out" =>
        // Seed solid-side; expand into the solid (admit P_basis).
        // visited0 = solidEdge: admit rejects deep hole cells (not in P), so
        // pre-visiting the whole h_cov is unnecessary (mirrors _lazy_hole).
        val admit: Long => Boolean = n => m(n)(pKey)
        val k0 = solidEdge.filter(c => m(c)(pKey))
        (solidEdge, solidEdge, admit, k0)
      case _ => // hole-out-ignore-geom
        // Seed solid-side; expand unbounded away from the hole (admit not H_basis).
        // visited0 = solidEdge: admit rejects H_basis cells (mirrors _lazy_hole).
        val admit: Long => Boolean = n => !m(n)(hKey)
        (solidEdge, solidEdge, admit, solidEdge)
    }
  }

  /** BFS expansion given the four parameters from a setup / lazy-builder call.
    * Extracted so both expand() and expandLazy() share the same loop. */
  private def dilate(
      kind: String, k: Int,
      frontier0: Set[Long], visited0: Set[Long],
      admit: Long => Boolean, k0: Set[Long],
      grid: GridSystem
  ): Set[Long] = {
    if (k == 0) return k0
    val visited = mutable.Set.empty[Long] ++ visited0
    var frontier: Set[Long] = frontier0
    val acc = mutable.Set.empty[Long] ++ (if (kind == "ring") k0 else Set.empty[Long])
    var kk = 0
    var shellK = Set.empty[Long]
    while (frontier.nonEmpty && kk < k) {
      kk += 1
      val nxt = frontier.flatMap(c => grid.kLoop(c, 1)).filter(n => !visited.contains(n) && admit(n))
      if (nxt.isEmpty) { frontier = Set.empty }
      else {
        visited ++= nxt; frontier = nxt
        if (kind == "ring") acc ++= nxt
        if (kk == k) shellK = nxt
      }
    }
    if (kind == "ring") acc.toSet else shellK
  }

  /** Lazy O(perimeter) expand for Polygon/MultiPolygon inputs.
    * Mirrors light `geom_expand_lazy`.  Selects the appropriate lazy builder by mode
    * and runs the shared BFS loop. */
  def expandLazy(kind: String, k: Int, mode: String, grid: GridSystem, geom: Geometry, res: Int,
                 coverage: String = DEFAULT_COVERAGE): Set[Long] = {
    require(kind == "ring" || kind == "loop", s"kind must be 'ring' or 'loop'; got '$kind'")
    if (!MODES.contains(mode))
      throw new IllegalArgumentException(s"unknown mode '$mode'; expected ${MODES.mkString(", ")}")
    val (solid, holeOpt) = solidAndHoles(geom)
    val dim = geomDimension(geom)
    val (frontier0, visited0, admit, k0) = mode match {
      case "boundary-out" =>
        lazyBoundaryOut(geom, solid, holeOpt, dim, grid, res, coverage)
      case "boundary-in" | "boundary-in-ignore-holes" =>
        lazyBoundaryIn(mode, geom, solid, holeOpt, dim, grid, res, coverage)
      case _ =>
        lazyHole(mode, geom, solid, holeOpt, dim, grid, res, coverage)
    }
    dilate(kind, k, frontier0, visited0, admit, k0, grid)
  }

  // `grid` computes perimeters; `coverage` selects the belongs-to basis X (cover/centroid/core).
  private def setup(mode: String, cls: Classification, grid: GridSystem, coverage: String)
      : (Set[Long], Set[Long], Long => Boolean, Set[Long]) = {
    if (!MODES.contains(mode))
      throw new IllegalArgumentException(s"unknown mode '$mode'; expected ${MODES.mkString(", ")}")
    // LOCKED model (see spec "LOCKED SEMANTICS"): SEEDS/frontier are the coveras (overlap)
    // topology — physical, non-empty, correctly placed (a fully-contained "core" cell can
    // never sit on a boundary).  Coverage governs only the ADMIT predicate (k>=1) and the
    // RETURNED k0 (= frontier ∩ admit, with two exceptions noted below).  Expanding from the
    // full coveras frontier keeps the k>=1 rings gap-free and identical across coverages.
    val (pX, sX, hX) = cls.basisSets(coverage) // validates coverage
    val sCov = cls.sCover; val pCov = cls.pCover; val hCov = cls.hCover

    if (mode.startsWith("boundary-")) {
      val op = outerPerimeter(sCov, grid) // coveras outer ring (alignment-robust fallback)
      mode match {
        case "boundary-out" =>
          // frontier = FULL coveras straddling band (expand OUTWARD, gap-free); visited = S ∪ band.
          // k0 = full band for coveras/core; polyfill returns only the centroid-OUT cells.
          val fullBand = sCov -- cls.sCore
          val frontier = if (fullBand.nonEmpty) fullBand else op
          val k0 =
            if (coverage == "polyfill") { val co = sCov -- sX; if (co.nonEmpty) co else op }
            else frontier
          (frontier, sCov ++ frontier, (_: Long) => true, k0)
        // boundary-in: INWARD from the coveras ring; admit pX (respect holes); k0 = op ∩ pX.
        case "boundary-in"              => (op, op, pX.contains, op.filter(pX.contains))
        // boundary-in-ignore-holes: INWARD; admit sX (crosses holes); k0 = op ∩ sX.
        case "boundary-in-ignore-holes" => (op, op, sX.contains, op.filter(sX.contains))
      }
    } else {
      // hole-* modes — coveras hole-edge seeds (alignment-robust), coverage as admit:
      //   voidEdge  = { c ∈ h_cover : ∃ n ∉ h_cover }  (hole-side edge)
      //   solidEdge = { c ∈ p_cover : ∃ n ∈ h_cover }  (solid-side edge)
      val voidEdge  = hCov.filter(c => grid.kLoop(c, 1).exists(n => !hCov.contains(n)))
      val solidEdge = pCov.filter(c => grid.kLoop(c, 1).exists(n => hCov.contains(n)))
      mode match {
        // hole-in: expand INTO the hole (admit hX); k0 = voidEdge ∩ hX (core drops the straddling edge).
        case "hole-in"              => (voidEdge, voidEdge, hX.contains, voidEdge.filter(hX.contains))
        // hole-out: expand INTO the solid (admit pX); visited = h_cover ∪ seed; k0 = solidEdge ∩ pX
        // (core ⇒ solid CORE band around the hole, straddling boundary dropped).
        case "hole-out"             => (solidEdge, hCov ++ solidEdge, pX.contains, solidEdge.filter(pX.contains))
        // hole-out-ignore-geom: expand unbounded away from the hole (admit not-in-hX).  admit is
        // exclusionary (not a belongs-to region), so k0 = the full coveras solid edge (not filtered).
        case "hole-out-ignore-geom" => (solidEdge, hCov ++ solidEdge, (n: Long) => !hX.contains(n), solidEdge)
      }
    }
  }

  def expand(kind: String, k: Int, mode: String, grid: GridSystem, geom: Geometry, res: Int,
             coverage: String = DEFAULT_COVERAGE): Set[Long] = {
    require(kind == "ring" || kind == "loop", s"kind must be 'ring' or 'loop'; got '$kind'")
    if (!MODES.contains(mode))
      throw new IllegalArgumentException(s"unknown mode '$mode'; expected ${MODES.mkString(", ")}")
    // Polygon / MultiPolygon → lazy O(perimeter) seed path (avoids O(area) polyfill).
    // Line, point, and GeometryCollection → existing classify+setup path.
    val geomType = geom.getGeometryType
    if (geomType == "Polygon" || geomType == "MultiPolygon") {
      return expandLazy(kind, k, mode, grid, geom, res, coverage)
    }
    val cls = classify(grid, geom, res)
    val (frontier0, visited0, admit, k0) = setup(mode, cls, grid, coverage)
    dilate(kind, k, frontier0, visited0, admit, k0, grid)
  }
}
