"""Grid-agnostic geometry dilation engine (frontier BFS) + covering classifier.

Shared by every light-tier geom-aware kring/kloop (quadbin, custom, BNG-light, h3).
See .superpowers/specs/2026-09-11-geom-aware-kring-kloop-design.md §4/§5.
"""

from dataclasses import dataclass, field

from shapely.geometry import Polygon

MODES = (
    "boundary-out",
    "boundary-in",
    "boundary-in-ignore-holes",
    "hole-in",
    "hole-out",
    "hole-out-ignore-geom",
)
DEFAULT_MODE = "boundary-out"

# Coverage basis selector (LOCKED design, "COVERAGE PARAM").  Three nested
# predicates decide which cells "belong to" a region P/S/H:
#   coveras  -> cover    (*_cover)    : cell overlaps the region
#   polyfill -> centroid (*_centroid) : cell CENTROID is inside the region
#   core     -> core     (*_core)     : cell is fully contained by the region
# Nested: cover ⊇ centroid ⊇ core.  The chosen basis drives the seed
# (perimeter), the admit bound, and boundary-ring inclusion.
COVERAGE = ("coveras", "polyfill", "core")
DEFAULT_COVERAGE = "coveras"
_COVERAGE_BASIS = {"coveras": "cover", "polyfill": "centroid", "core": "core"}


def _basis_sets(cls, coverage):
    """Return (P_X, S_X, H_X) — the belongs-to sets for the chosen coverage basis.

    Validates ``coverage`` (raises ValueError on an unknown value).
    """
    if coverage not in _COVERAGE_BASIS:
        raise ValueError(f"unknown coverage {coverage!r}; expected one of {COVERAGE}")
    b = _COVERAGE_BASIS[coverage]
    return (
        getattr(cls, f"p_{b}"),
        getattr(cls, f"s_{b}"),
        getattr(cls, f"h_{b}"),
    )


def outer_perimeter(s_cover, neighbors):
    """Covering-set perimeter: cells in s_cover with at least one neighbor outside s_cover.

    outer_perimeter(S) = { c ∈ s_cover : ∃ n ∈ neighbors(c) with n ∉ s_cover }

    Properties:
    - Alignment-robust: any non-empty covering set has a perimeter, regardless of whether
      any cell straddles the geometry boundary (fixes the grid-aligned empty-seed bug).
    - Excludes hole-rim cells: for a holed polygon, hole-rim cells lie inside the filled
      solid S, so all their neighbors are in s_cover → not on the perimeter.  Subsumes
      Layer-1's s_border approach while fixing the aligned-solid case.
    - Non-empty whenever s_cover is non-empty.
    """
    return frozenset(c for c in s_cover if any(n not in s_cover for n in neighbors(c)))


def dilate(frontier0, visited0, neighbors, admit):
    """Yield (k, shell) for k=1,2,...; shell = cells first reached at step k.

    visited0 blocks re-entry (direction); admit filters+prunes (region bound).
    Each cell is recorded once → total work O(|output|).
    """
    visited = set(visited0)
    frontier = set(frontier0)
    k = 0
    while frontier:
        k += 1
        nxt = {
            n for c in frontier for n in neighbors(c) if n not in visited and admit(n)
        }
        if not nxt:
            return
        visited |= nxt
        frontier = nxt
        yield k, nxt


@dataclass
class Classification:
    p_cover: set  # overlaps P (geom with holes)
    p_core: set  # fully inside P
    s_cover: set  # overlaps S (outer ring, holes filled)
    s_core: set  # fully inside S
    h_cover: set  # overlaps holes union H
    h_core: set  # fully inside H
    # Centroid basis (cell CENTROID inside the region) — the "polyfill" coverage.
    # Nested strictly between core and cover: core ⊆ centroid ⊆ cover.  Defaulted
    # so legacy 6-arg positional construction (e.g. an empty Classification) works.
    p_centroid: set = field(default_factory=set)  # centroid inside P
    s_centroid: set = field(default_factory=set)  # centroid inside S
    h_centroid: set = field(default_factory=set)  # centroid inside H

    @property
    def p_border(self):
        return self.p_cover - self.p_core

    @property
    def s_border(self):
        """Outer-boundary cells only: overlaps S but not fully inside S.

        For a polygon with no holes, S = P so s_border == p_border.
        For a holed polygon, s_border excludes hole-rim cells (which ARE in
        p_border but are fully inside S and therefore in s_core, not s_border).
        This is the correct frontier seed for boundary-* modes.
        """
        return self.s_cover - self.s_core

    @property
    def h_border(self):
        return self.h_cover - self.h_core


def _solid_and_holes(geom):
    """Return (S, H) shapely geoms: S = outer ring filled; H = union of holes."""
    from shapely.geometry import MultiPolygon
    from shapely.ops import unary_union

    polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    solids, holes = [], []
    for p in polys:
        if p.geom_type != "Polygon":
            continue
        solids.append(Polygon(p.exterior))
        holes.extend(Polygon(r) for r in p.interiors)
    S = unary_union(solids) if solids else geom
    H = unary_union(holes) if holes else None
    return S, H


def _geom_dimension(geom) -> int:
    """Topological dimension of a geometry: 0=point, 1=line/ring, 2=surface/other."""
    t = geom.geom_type
    if t in ("Point", "MultiPoint"):
        return 0
    if t in ("LineString", "LinearRing", "MultiLineString"):
        return 1
    return 2  # Polygon, MultiPolygon, GeometryCollection, etc.


def _sample_coords(geom, n_samples: int = 16):
    """Yield (x, y) coordinate pairs sampled from a point or line geometry.

    Used for generating candidate cells from a grid's point_to_cell hook when
    polyfill_fn returns nothing for non-polygon geometries (e.g. BNG centroid-BFS
    returns empty for a point or line input).

    - Point / MultiPoint: yield the point coordinate(s).
    - LineString / LinearRing: yield endpoints + n_samples interior samples + centroid.
    - Multi-geometries: recurse into each part.
    """
    if hasattr(geom, "geoms"):
        for part in geom.geoms:
            yield from _sample_coords(part, n_samples)
        return
    # Single geometry: centroid first (always available)
    c = geom.centroid
    yield (c.x, c.y)
    # Explicit vertex coordinates for 0-/1-dim types (a Polygon's `.coords` raises,
    # so it is handled separately below).
    if geom.geom_type in ("Point", "LineString", "LinearRing"):
        for xy in geom.coords:
            yield (xy[0], xy[1])
    # Polygon: sample the boundary vertices (a Polygon has no .coords). Lets a
    # sub-cell polygon — one whose centroid-membership polyfill is empty — still
    # seed candidate cells from its containing cell(s).
    if geom.geom_type == "Polygon":
        for xy in geom.exterior.coords:
            yield (xy[0], xy[1])
        for ring in geom.interiors:
            for xy in ring.coords:
                yield (xy[0], xy[1])
    # Densify lines with n_samples-1 interior fractions
    if geom.geom_type in ("LineString", "LinearRing") and n_samples > 0:
        ln = geom.length
        if ln > 0:
            for i in range(1, n_samples):
                pt = geom.interpolate(i / n_samples, normalized=True)
                yield (pt.x, pt.y)


def classify(geom, res, polyfill_fn, cell_geom_fn, point_to_cell_fn=None):
    """Partition polyfill candidate cells vs P (geom), S (solid), H (holes).

    Parameters
    ----------
    geom : shapely geometry
    res : resolution (grid-specific)
    polyfill_fn : callable(g, res) -> list[cell_id]
        Grid's polygon polyfill.  Must be called with the SOLID (hole-filled)
        geometry so that hole-interior cells become candidates for hole-* modes.
        For non-polygon geoms (point/line) this may return an empty list (e.g.
        BNG centroid-BFS, custom centroid-containment).
    cell_geom_fn : callable(cell_id) -> shapely polygon
        Inverse map: grid cell → its bounding polygon.
    point_to_cell_fn : callable(x, y) -> cell_id | None, optional
        Per-grid hook that returns the cell containing a coordinate pair.
        Required for grids whose polyfill_fn cannot handle point/line inputs
        (BNG, custom).  When provided, it is used as a fallback only when
        polyfill_fn returns an empty candidate set for a non-polygon geometry.
        Quadbin (bbox-based polyfill) does not need this hook.

    Coverage semantics (dimension-aware, replaces the old uniform area>0 test):
    - Polygon (dim 2): cell ∈ cover iff intersection area > 0.
    - Line    (dim 1): cell ∈ cover iff intersection length > 0.
    - Point   (dim 0): cell ∈ cover iff they intersect (any shared point suffices).
    Core sets: cell ∈ core iff geom.contains(cell) — always empty for point/line
    because no polygon cell can be contained by a 0D or 1D geometry.

    Centroid sets (the "polyfill" coverage basis): cell ∈ centroid iff the cell's
    centroid lies inside the region (region.contains(cell.centroid)).  Nested
    strictly between core and cover.  Always empty for point/line (a cell centroid
    almost never lies exactly on a 0D/1D geometry) — so polyfill/core coverage of a
    point or line is naturally empty, which is the intended behaviour.
    """
    S, H = _solid_and_holes(geom)
    dim = _geom_dimension(geom)

    # Polyfill the filled SOLID so hole-interior cells are candidates for hole-*
    # modes.  Quadbin uses a bbox polyfill (returns cells for any input); BNG/custom
    # use centroid-membership, which returns NOTHING for (a) points/lines and (b) a
    # sub-cell polygon — one smaller than a cell, whose interior contains no cell
    # centroid — even though it overlaps a cell.  In both cases fall back to the
    # per-grid point_to_cell hook: sample representative coordinates and map each to
    # its containing cell.  classify then assigns cover/centroid/core as usual, so
    # this only adds candidates the polyfill missed (it never fires when polyfill
    # already returned cells).
    cands = set(polyfill_fn(S, res))
    if not cands and point_to_cell_fn is not None:
        seen: set = set()
        for x, y in _sample_coords(geom):
            if (x, y) in seen:
                continue
            seen.add((x, y))
            try:
                c = point_to_cell_fn(x, y)
                if c is not None:
                    cands.add(c)
            except Exception:
                pass

    p_cover, p_core, s_cover, s_core, h_cover, h_core = (set() for _ in range(6))
    p_centroid, s_centroid, h_centroid = set(), set(), set()

    for c in cands:
        g = cell_geom_fn(c)
        cen = g.centroid  # cell centroid — the "polyfill" (centroid) basis probe

        # Dimension-aware coverage test for P (the original geometry, may have holes)
        # and S (the hole-filled solid, always a polygon when dim==2).
        if dim == 0:
            p_in_cover = geom.intersects(g)
            s_in_cover = S.intersects(g)  # S == geom for non-polygon
        elif dim == 1:
            ix_p = geom.intersection(g)
            p_in_cover = geom.intersects(g) and ix_p.length > 0
            ix_s = S.intersection(g)
            s_in_cover = S.intersects(g) and ix_s.length > 0
        else:
            p_in_cover = geom.intersects(g) and geom.intersection(g).area > 0
            s_in_cover = S.intersects(g) and S.intersection(g).area > 0

        # The centroid basis (and core basis) is 2D-only.  For a point/line region a
        # cell centroid is "inside" only by measure-zero coincidence (e.g. a point
        # placed exactly at a cell centre) — which is not a meaningful polyfill hit,
        # so polyfill/core coverage of a 0/1-dim geom is empty by construction.
        is_2d = dim == 2
        if p_in_cover:
            p_cover.add(c)
            if is_2d and geom.contains(cen):
                p_centroid.add(c)
            if is_2d and geom.contains(g):
                p_core.add(c)
        if s_in_cover:
            s_cover.add(c)
            if is_2d and S.contains(cen):
                s_centroid.add(c)
            if is_2d and S.contains(g):
                s_core.add(c)
        if H is not None and H.intersects(g) and H.intersection(g).area > 0:
            h_cover.add(c)
            if is_2d and H.contains(cen):
                h_centroid.add(c)
            if is_2d and H.contains(g):
                h_core.add(c)

    return Classification(
        p_cover,
        p_core,
        s_cover,
        s_core,
        h_cover,
        h_core,
        p_centroid=p_centroid,
        s_centroid=s_centroid,
        h_centroid=h_centroid,
    )


def mode_setup(mode, cls, neighbors=None, coverage=DEFAULT_COVERAGE):
    """Return (frontier0, visited0, admit, k0) for a traversal mode + coverage.

    ``coverage`` ∈ {"coveras","polyfill","core"} selects the belongs-to basis X
    (cover/centroid/core).  Seeds (perimeters), admit bounds and boundary-ring
    inclusion are all computed against the region sets P_X / S_X / H_X.

    For boundary-* modes `neighbors` must be provided so that outer_perimeter can
    be computed.  hole-* modes also require `neighbors` for alignment-robust
    hole-edge perimeter seeds (void_hole_edge / solid_hole_edge).
    """
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {MODES}")
    if neighbors is None:
        raise ValueError(f"mode {mode!r} requires `neighbors`")

    # Coverage governs which cells are ADMITTED (the k>=1 expansion) and RETURNED
    # (the k0 filter) — NOT where the seed sits.  A fully-contained ("core") cell can
    # never lie on a boundary, so seeding on the narrow basis would misplace the ring
    # (boundary-out landed one cell inside under polyfill) or make it vanish
    # (hole-out/-ignore-geom returned nothing under core).  So SEEDS are always the
    # coveras (overlap) topology — the physical boundary/hole edge — and a boundary
    # cell is RETURNED at k0 only if it also belongs under the coverage basis
    # (k0 = seed ∩ admit).  Under coveras every seed cell belongs, so behaviour is
    # unchanged; under core the straddling boundary drops out and only core cells
    # (reached at k>=1, plus any core cells already on the seed) are returned.
    p_x, s_x, h_x = (frozenset(x) for x in _basis_sets(cls, coverage))  # validates
    s_cov, p_cov, h_cov = cls.s_cover, cls.p_cover, cls.h_cover

    if mode.startswith("boundary-"):
        # Coveras outer boundary ring (alignment-robust: non-empty for any non-empty
        # cover; excludes hole-rim cells, which lie inside the filled solid).
        op = outer_perimeter(s_cov, neighbors)
        if mode == "boundary-out":
            # TRAVERSAL frontier = the FULL coveras straddling band (cells overlapping
            # S but not fully contained).  We always expand OUTWARD from the full band
            # so the k>=1 outward ring has NO GAPS (outward cells reachable only from a
            # centroid-in band cell would otherwise be missed under polyfill).  The
            # RETURNED k0 is then narrowed by the coverage: coveras & core return the
            # whole band; polyfill subtracts the centroid-IN cells (returns only the
            # centroid-OUT boundary cells).  Fall back to the outer perimeter when a set
            # is empty (grid-aligned geom: no straddling cells, non-empty outer ring).
            full_band = frozenset(s_cov) - frozenset(cls.s_core)
            frontier = full_band if full_band else op
            if coverage == "polyfill":
                centroid_out = frozenset(s_cov) - s_x  # s_x == s_centroid
                k0 = centroid_out if centroid_out else op
            else:  # coveras / core → the full physical boundary band
                k0 = frontier
            return frontier, frozenset(s_cov) | frontier, (lambda n: True), k0
        if mode == "boundary-in":
            # INWARD, admit P_X (respect holes).  k0 = op ∩ P_X (core drops the ring).
            admit = lambda n: n in p_x  # noqa: E731
            return op, op, admit, frozenset(c for c in op if c in p_x)
        # boundary-in-ignore-holes: INWARD, admit S_X (marches across the hole).
        admit = lambda n: n in s_x  # noqa: E731
        return op, op, admit, frozenset(c for c in op if c in s_x)

    # ------------------------------------------------------------------
    # hole-* modes — coveras hole-edge seeds (alignment-robust), coverage as admit.
    #   void_edge  = { c ∈ h_cover : ∃ n ∉ h_cover }  (hole-side edge)
    #   solid_edge = { c ∈ p_cover : ∃ n ∈ h_cover }  (solid-side edge)
    # Both are non-empty for a non-empty hole region regardless of grid alignment.
    # ------------------------------------------------------------------
    void_edge = frozenset(c for c in h_cov if any(n not in h_cov for n in neighbors(c)))
    solid_edge = frozenset(c for c in p_cov if any(n in h_cov for n in neighbors(c)))

    if mode == "hole-in":
        # Seed void-side; expand INTO the hole (admit H_X).  k0 = void_edge ∩ H_X.
        admit = lambda n: n in h_x  # noqa: E731
        return void_edge, void_edge, admit, frozenset(c for c in void_edge if c in h_x)
    if mode == "hole-out":
        # Seed solid-side; expand into the solid (admit P_X).  visited = H ∪ seed.
        # k0 = solid_edge ∩ P_X → under core the straddling boundary drops, leaving
        # the solid CORE band around the hole.
        admit = lambda n: n in p_x  # noqa: E731
        vis = frozenset(h_cov) | solid_edge
        return solid_edge, vis, admit, frozenset(c for c in solid_edge if c in p_x)
    # hole-out-ignore-geom: solid-side seed, expand unbounded away from the hole
    # (admit not-in-H_X).  visited = H_cover ∪ seed blocks the WHOLE hole (topological)
    # so the band never leaks inward regardless of coverage.  admit is exclusionary
    # (not a belongs-to region), so k0 = the full coveras solid edge (not filtered).
    return (
        solid_edge,
        frozenset(h_cov) | solid_edge,
        (lambda n: n not in h_x),
        solid_edge,
    )


def geom_expand(kind, k, mode, cls, neighbors, coverage=DEFAULT_COVERAGE):
    """kind='ring' (filled <=k) or 'loop' (shell at exactly k). k>=0.

    ``coverage`` selects the belongs-to basis (see :func:`mode_setup`).
    """
    if kind not in ("ring", "loop"):
        raise ValueError(f"kind must be 'ring' or 'loop'; got {kind!r}")
    frontier0, visited0, admit, k0 = mode_setup(mode, cls, neighbors, coverage)
    if k == 0:
        return set(k0)
    acc = set(k0) if kind == "ring" else set()
    shell_k = set()
    for kk, shell in dilate(frontier0, visited0, neighbors, admit):
        if kk > k:
            break
        if kind == "ring":
            acc |= shell
        if kk == k:
            shell_k = shell
            break
    return acc if kind == "ring" else shell_k
