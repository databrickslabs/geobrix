"""Grid-agnostic geometry dilation engine (frontier BFS) + covering classifier.

Shared by every light-tier geom-aware kring/kloop (quadbin, custom, BNG-light, h3).
See .superpowers/specs/2026-09-11-geom-aware-kring-kloop-design.md §4/§5.
"""

from dataclasses import dataclass

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


def classify(geom, res, polyfill_fn, cell_geom_fn):
    """Partition polyfill candidate cells vs P (geom), S (solid), H (holes)."""
    S, H = _solid_and_holes(geom)
    # polyfill the SOLID so hole-interior cells are classified (hole modes need h_core)
    cands = set(polyfill_fn(S, res))
    p_cover, p_core, s_cover, s_core, h_cover, h_core = (set() for _ in range(6))
    for c in cands:
        g = cell_geom_fn(c)
        if geom.intersects(g) and geom.intersection(g).area > 0:
            p_cover.add(c)
            if geom.contains(g):
                p_core.add(c)
        if S.intersects(g) and S.intersection(g).area > 0:
            s_cover.add(c)
            if S.contains(g):
                s_core.add(c)
        if H is not None and H.intersects(g) and H.intersection(g).area > 0:
            h_cover.add(c)
            if H.contains(g):
                h_core.add(c)
    return Classification(p_cover, p_core, s_cover, s_core, h_cover, h_core)


def mode_setup(mode, cls):
    """Return (frontier0, visited0, admit, k0) for a traversal mode."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {MODES}")
    # sb = outer-boundary cells only (s_cover - s_core).
    # For a no-hole polygon S = P, so sb == pb — holeless behavior is unchanged.
    # For a holed polygon, hole-rim cells are inside S (in s_core), so they are
    # correctly excluded from sb.  This means boundary-* modes operate on the outer
    # ring only; hole-rim traversal belongs to hole-* modes.
    sb, hb = cls.s_border, cls.h_border
    if mode == "boundary-out":
        return (
            frozenset(sb),
            frozenset(cls.p_cover),
            (lambda n: True),
            frozenset(cls.p_cover),
        )
    if mode == "boundary-in":
        return frozenset(sb), frozenset(sb), (lambda n: n in cls.p_core), frozenset(sb)
    if mode == "boundary-in-ignore-holes":
        return frozenset(sb), frozenset(sb), (lambda n: n in cls.s_core), frozenset(sb)
    if mode == "hole-in":
        return frozenset(hb), frozenset(hb), (lambda n: n in cls.h_core), frozenset(hb)
    if mode == "hole-out":
        return (
            frozenset(hb),
            frozenset(cls.h_cover),
            (lambda n: n in cls.p_core),
            frozenset(hb),
        )
    # hole-out-ignore-geom
    return (
        frozenset(hb),
        frozenset(cls.h_cover),
        (lambda n: n not in cls.h_core),
        frozenset(hb),
    )


def geom_expand(kind, k, mode, cls, neighbors):
    """kind='ring' (filled <=k) or 'loop' (shell at exactly k). k>=0."""
    if kind not in ("ring", "loop"):
        raise ValueError(f"kind must be 'ring' or 'loop'; got {kind!r}")
    frontier0, visited0, admit, k0 = mode_setup(mode, cls)
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
