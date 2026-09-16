"""h3 geometry-aware expansion — self-contained, geom-taking (light-only).

Unlike the earlier product-columnar approach, h3 now behaves like the other
grids: a single geom-taking function does the whole job in pure Python, so it
runs anywhere (local + Databricks) with no dependency on Databricks product
functions.

Both halves use the ``h3`` library:
  - polyfill / cover: ``h3.polygon_to_cells_experimental`` — the performant
    experimental H3-core polyfill, whose ``contain=`` modes give exactly the two
    classifications the dilation engine needs: ``'overlap'`` = cover (cells whose
    hexagon overlaps the geometry) and ``'full'`` = core (cells fully inside).
  - neighbours: ``h3.grid_disk`` (topological, index math only).

The 6-set covering Classification (§4 of the design) is built by polyfilling the
donut P (exterior with holes), the solid S (exterior only), and each hole H, then
fed to the shared ``_dilate`` engine. h3 uses (lat, lng) order; shapely uses
(lng, lat) — rings are swapped when building ``h3.LatLngPoly``.
"""

import h3
from shapely import from_wkb, from_wkt
from shapely.geometry import MultiLineString, MultiPoint, MultiPolygon, Polygon, box

from databricks.labs.gbx.pygx import _dilate

_CONTAIN_COVER = "overlap"  # cells overlapping the shape       -> p_cover
_CONTAIN_CENTROID = "center"  # cells whose CENTER is inside     -> p_centroid
_CONTAIN_CORE = "full"  # cells fully inside the shape           -> p_core


def _parse_geom(g):
    """Parse a geometry from WKB bytes or a WKT string; None/empty -> None."""
    if g is None:
        return None
    geom = from_wkb(bytes(g)) if isinstance(g, (bytes, bytearray)) else from_wkt(str(g))
    return None if (geom is None or geom.is_empty) else geom


def _latlng_ring(coords):
    """shapely (lng, lat) ring -> h3 (lat, lng) ring."""
    return [(y, x) for (x, y) in coords]


def _shapes(geom):
    """Return (P, S, H): h3.LatLngPoly lists for the donut, the solid, and holes.

    P = exterior with interior rings (the geometry itself);
    S = exterior only (solid, holes filled);
    H = one LatLngPoly per interior ring (the holes).
    """
    parts = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    p_shapes, s_shapes, h_shapes = [], [], []
    for part in parts:
        if part.geom_type != "Polygon" or part.is_empty:
            continue
        ext = _latlng_ring(part.exterior.coords)
        holes = [_latlng_ring(r.coords) for r in part.interiors]
        p_shapes.append(h3.LatLngPoly(ext, *holes))
        s_shapes.append(h3.LatLngPoly(ext))
        for hole in holes:
            h_shapes.append(h3.LatLngPoly(hole))
    return p_shapes, s_shapes, h_shapes


def _to_int(cell):
    return int(cell, 16) if isinstance(cell, str) else int(cell)


def _fill(shapes, res, contain):
    """Union of h3 polyfill (given containment mode) over a list of LatLngPoly."""
    out = set()
    for shape in shapes:
        out |= {
            _to_int(c) for c in h3.polygon_to_cells_experimental(shape, res, contain)
        }
    return out


def _h3_cell_for_lnglat(lng, lat, res):
    """H3 integer cell id for a (longitude, latitude) coordinate pair."""
    return _to_int(h3.latlng_to_cell(lat, lng, res))


def _h3_covering_cells(geom, res):
    """Integer H3 cells covering a non-polygon geometry (point or line).

    For a point: the single cell containing the point.
    For a line: cells for each vertex + n_samples interior fractions + centroid.
    For multi-geometries: union over parts.

    Returns a frozenset of int cell ids.
    """
    cands: set = set()
    parts = list(geom.geoms) if hasattr(geom, "geoms") else [geom]
    for part in parts:
        if part.geom_type in ("Point", "MultiPoint"):
            pts = list(part.geoms) if hasattr(part, "geoms") else [part]
            for pt in pts:
                cands.add(_h3_cell_for_lnglat(pt.x, pt.y, res))
        elif part.geom_type in ("LineString", "LinearRing"):
            # Vertices
            for xy in part.coords:
                cands.add(_h3_cell_for_lnglat(xy[0], xy[1], res))
            # Centroid
            c = part.centroid
            cands.add(_h3_cell_for_lnglat(c.x, c.y, res))
            # Densified interior samples (16 fractions)
            if part.length > 0:
                for i in range(1, 16):
                    pt = part.interpolate(i / 16, normalized=True)
                    cands.add(_h3_cell_for_lnglat(pt.x, pt.y, res))
        elif part.geom_type == "MultiLineString":
            for seg in part.geoms:
                cands |= _h3_covering_cells(seg, res)
    return frozenset(cands)


def _classify_polyfill(geom, res):
    """Build the 6-set covering Classification for a geometry via h3-lib polyfill.

    RETAINED AS THE CORRECTNESS ORACLE (formerly ``classify``).  The performant
    ``geom_expand`` path for polygon boundary/hole modes is now lazy
    (O(perimeter), see :func:`geom_expand_lazy`); this full O(area) polyfill
    classifier is kept as the single source of truth the lazy path is proven
    byte-identical against (``test_h3_boundary_lazy_parity.py``), and is still
    used directly for non-polygon geometries and GeometryCollections (which have
    no lazy boundary-ring path).

    For polygon inputs (including multi-polygons) the h3 polygon_to_cells_experimental
    polyfill is used with 'overlap' (cover) and 'full' (core) containment modes.

    For non-polygon inputs (points, lines) the h3 polyfill API is polygon-specific
    and cannot be used directly.  Instead, candidate cells are generated by mapping
    representative coordinates to their H3 cells (via latlng_to_cell), and coverage
    is set to the union of those cells (p_cover = s_cover = cells; centroid = core =
    empty since no polygon cell has its centre inside — or is contained by — a point
    or line).

    For GeometryCollection inputs (mixed-dimension), members are flattened and grouped
    by dimension into Multi* geometries, each group is classified via the corresponding
    path, and the results are unioned.  This mirrors _dilate.classify's GC handling.

    Three bases are built for the polygon path via the h3 native containment modes:
    'overlap' → cover, 'center' → centroid (the "polyfill" coverage), 'full' → core.
    """
    if geom.geom_type == "GeometryCollection":
        members = list(_dilate._flatten_members(geom))
        if not members:
            return _dilate.Classification(*(set() for _ in range(6)))
        polys, lines, points = _dilate._group_by_dimension(members)
        groups = []
        if polys:
            groups.append(MultiPolygon(polys) if len(polys) > 1 else polys[0])
        if lines:
            groups.append(MultiLineString(lines) if len(lines) > 1 else lines[0])
        if points:
            groups.append(MultiPoint(points) if len(points) > 1 else points[0])
        return _dilate._union_classifications(
            _classify_polyfill(g, res) for g in groups
        )

    dim = _dilate._geom_dimension(geom)

    if dim != 2:
        # Non-polygon: derive covering cells from coordinate samples.
        # A point/line has no 2D interior → centroid/core/hole sets all empty.
        cover = _h3_covering_cells(geom, res)
        return _dilate.Classification(
            p_cover=set(cover),
            p_core=set(),
            s_cover=set(cover),
            s_core=set(),
            h_cover=set(),
            h_core=set(),
            p_centroid=set(),
            s_centroid=set(),
            h_centroid=set(),
        )

    p_shapes, s_shapes, h_shapes = _shapes(geom)
    return _dilate.Classification(
        p_cover=_fill(p_shapes, res, _CONTAIN_COVER),
        p_core=_fill(p_shapes, res, _CONTAIN_CORE),
        s_cover=_fill(s_shapes, res, _CONTAIN_COVER),
        s_core=_fill(s_shapes, res, _CONTAIN_CORE),
        h_cover=_fill(h_shapes, res, _CONTAIN_COVER),
        h_core=_fill(h_shapes, res, _CONTAIN_CORE),
        p_centroid=_fill(p_shapes, res, _CONTAIN_CENTROID),
        s_centroid=_fill(s_shapes, res, _CONTAIN_CENTROID),
        h_centroid=_fill(h_shapes, res, _CONTAIN_CENTROID),
    )


def _neighbors(c):
    """6 (or 5) H3 neighbours of integer cell id c (grid_disk topology, ring 1)."""
    c_str = h3.int_to_str(c)
    return [int(n, 16) for n in h3.grid_disk(c_str, 1) if n != c_str]


def _h3_cell_step_deg(res):
    """Ring-sampling step in DEGREES for the density guard at resolution ``res``.

    h3 boundary coordinates are lat/lng, so ``_boundary_cells`` measures segment
    length and samples in degree units.  A cell's *latitude* extent is ~constant
    at ``edge_km / 111.32`` degrees regardless of latitude (its longitude extent
    only grows toward the poles, which makes sampling denser, never coarser), so
    the latitude extent is the binding constraint.  A hexagon's minimum diameter
    (across flats) is ~``sqrt(3) * edge``; using one edge-length in degrees
    (``edge_km / 111.32``) is ~0.58× that minimum — comfortably below a cell
    width, satisfying the density guard (step <= cell width → no cell skipped).
    """
    edge_km = h3.average_hexagon_edge_length(int(res), unit="km")
    return edge_km / 111.32


def _shp_to_latlngpolys(geom):
    """shapely (lng, lat) Polygon/MultiPolygon -> list of h3.LatLngPoly (lat, lng).

    Empty / non-areal geometries yield an empty list.  Interior rings are carried
    through as holes so the h3 polyfill sees the same region topology shapely does.
    """
    if geom is None or geom.is_empty:
        return []
    parts = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    out = []
    for p in parts:
        if p.geom_type != "Polygon" or p.is_empty:
            continue
        ext = [(y, x) for (x, y) in p.exterior.coords]
        holes = [[(y, x) for (x, y) in r.coords] for r in p.interiors]
        out.append(h3.LatLngPoly(ext, *holes))
    return out


def _local_contains(cell, region, res, contain):
    """h3-NATIVE membership: is ``cell`` in ``polygon_to_cells_experimental(region, contain)``?

    Computed on a LOCAL clip of ``region`` — the window is 3× the cell's hexagon
    extent, centred on it — rather than by polyfilling the whole area.  h3's
    containment decision for a single cell is a LOCAL predicate (cell hexagon vs
    the polygon boundary passing through it), so clipping the region to a window
    that strictly contains the cell's hexagon (with a full cell-width margin on
    every side) preserves the region's edges near the cell exactly and therefore
    yields h3's identical per-cell verdict:

      - overlap: cell overlaps region ⟺ cell overlaps (region ∩ window)  [cell ⊂ window]
      - center:  cell centre in region ⟺ centre in (region ∩ window)     [centre ∈ window]
      - full:    cell ⊆ region        ⟺ cell ⊆ (region ∩ window)         [cell ⊆ window]

    This is why the lazy path is byte-identical to the polyfill oracle even for
    grid-aligned polygons, where planar shapely tests on a reconstructed hexagon
    disagree with h3's geodesic polyfill (shared cell/region edges flip
    cover/core).  ``region`` is a shapely (lng, lat) geometry (geom, solid S, or
    holes H); ``None``/empty regions (e.g. H for a holeless polygon) are not
    members of anything.
    """
    if region is None or region.is_empty:
        return False
    hexg = _h3_cell_geom_shp(cell)
    minx, miny, maxx, maxy = hexg.bounds
    w, h = maxx - minx, maxy - miny
    window = box(minx - w, miny - h, maxx + w, maxy + h)
    local = region.intersection(window)
    if local.is_empty:
        return False
    got: set = set()
    for shape in _shp_to_latlngpolys(local):
        got |= {
            _to_int(x)
            for x in h3.polygon_to_cells_experimental(shape, int(res), contain)
        }
    return cell in got


def _h3_cell_geom_shp(cell):
    """Shapely polygon of integer H3 cell ``cell`` in (lng, lat) order."""
    boundary = h3.cell_to_boundary(h3.int_to_str(cell))
    return Polygon([(lng, lat) for (lat, lng) in boundary])


def _membership_fn(geom, res):
    """Return a memoized ``_m(cell) -> {9 boolean bits}`` using h3-native membership.

    Mirrors the dict contract of :func:`_dilate._classify_cell` (the same nine
    keys) so the lazy seed/admit logic reads identically, but each bit is decided
    by h3's own ``polygon_to_cells_experimental`` (via :func:`_local_contains`)
    instead of planar shapely tests — the ONLY way to stay byte-identical to the
    ``_classify_polyfill`` oracle on cell-edge-aligned polygons.
    """
    S, H = _dilate._solid_and_holes(geom)
    cache: dict = {}

    def _m(cell):
        m = cache.get(cell)
        if m is None:
            m = {
                "p_cover": _local_contains(cell, geom, res, _CONTAIN_COVER),
                "p_centroid": _local_contains(cell, geom, res, _CONTAIN_CENTROID),
                "p_core": _local_contains(cell, geom, res, _CONTAIN_CORE),
                "s_cover": _local_contains(cell, S, res, _CONTAIN_COVER),
                "s_centroid": _local_contains(cell, S, res, _CONTAIN_CENTROID),
                "s_core": _local_contains(cell, S, res, _CONTAIN_CORE),
                "h_cover": _local_contains(cell, H, res, _CONTAIN_COVER),
                "h_centroid": _local_contains(cell, H, res, _CONTAIN_CENTROID),
                "h_core": _local_contains(cell, H, res, _CONTAIN_CORE),
            }
            cache[cell] = m
        return m

    return _m, S, H


def _lazy_setup(mode, geom, res, coverage):
    """Return ``(frontier0, visited0, admit, k0)`` for a lazy h3 polygon mode.

    h3-local analogue of :func:`_dilate.mode_setup`'s lazy branches
    (``_lazy_boundary_out`` / ``_lazy_boundary_in`` / ``_lazy_hole``): same seeds,
    admit predicates and k0 filters, but every membership test runs through the
    h3-NATIVE :func:`_membership_fn` (not shapely ``_classify_cell``), because on
    cell-edge-aligned polygons shapely and h3 disagree at shared edges and only
    h3-native membership reproduces the ``_classify_polyfill`` oracle exactly.

    Reachability widenings over the plain ``band ∪ neighbours(band)`` universe
    (needed on the hexagonal lattice, where a straddle/rim cell can sit two rings
    from the traced ring):
      - boundary-*: widen by neighbours of the straddle band before the full-band
        / centroid-out scan.
      - hole-*: widen by neighbours of ``void_edge`` before computing
        ``solid_edge`` (every solid-side rim cell adjoins a captured void_edge cell).
    """
    if coverage not in _dilate._COVERAGE_BASIS:
        raise ValueError(
            f"unknown coverage {coverage!r}; expected one of {_dilate.COVERAGE}"
        )
    basis = _dilate._COVERAGE_BASIS[coverage]
    step = _h3_cell_step_deg(res)
    point_to_cell = lambda x, y: _h3_cell_for_lnglat(x, y, int(res))  # noqa: E731
    _m, S, H = _membership_fn(geom, int(res))
    empty: frozenset = frozenset()

    if mode.startswith("boundary-"):
        ext_rings = (
            [S.exterior] if S.geom_type == "Polygon" else [g.exterior for g in S.geoms]
        )
        band = _dilate._boundary_cells(ext_rings, point_to_cell, step)

        def s_cover(c):
            return _m(c)["s_cover"]

        op = _dilate._local_perimeter(band, _neighbors, s_cover)

        if mode == "boundary-out":
            # Band neighbourhood, widened by neighbours of the straddle band so a
            # straddle cell two rings from the traced ring is still classified.
            C: set = set(band)
            for c in band:
                C.update(_neighbors(c))
            straddle = [c for c in C if _m(c)["s_cover"] and not _m(c)["s_core"]]
            for c in straddle:
                C.update(_neighbors(c))
            full_band = frozenset(
                c for c in C if _m(c)["s_cover"] and not _m(c)["s_core"]
            )
            frontier = full_band if full_band else op
            if coverage == "polyfill":
                centroid_out = frozenset(
                    c for c in C if _m(c)["s_cover"] and not _m(c)["s_centroid"]
                )
                k0 = centroid_out if centroid_out else op
            else:
                k0 = frontier
            admit = lambda n: not _m(n)["s_cover"]  # noqa: E731
            return frontier, set(frontier), admit, k0

        # boundary-in / boundary-in-ignore-holes: seed the outer perimeter, march
        # inward admitting p_<basis> (respect holes) or s_<basis> (ignore holes).
        key = ("p_" if mode == "boundary-in" else "s_") + basis
        admit = lambda n: _m(n)[key]  # noqa: E731
        return op, op, admit, frozenset(c for c in op if _m(c)[key])

    # ------------------------------------------------------------------ hole-*
    if H is None:
        return empty, empty, (lambda n: False), empty  # noqa: E731
    polys = list(geom.geoms) if isinstance(geom, MultiPolygon) else [geom]
    int_rings = [r for p in polys if p.geom_type == "Polygon" for r in p.interiors]
    if not int_rings:
        return empty, empty, (lambda n: False), empty  # noqa: E731
    hole_band = _dilate._boundary_cells(int_rings, point_to_cell, step)
    if not hole_band:
        return empty, empty, (lambda n: False), empty  # noqa: E731

    C = set(hole_band)
    for c in hole_band:
        C.update(_neighbors(c))
    void_edge = frozenset(
        c
        for c in C
        if _m(c)["h_cover"] and any(not _m(n)["h_cover"] for n in _neighbors(c))
    )
    C2 = set(C)
    for c in void_edge:
        C2.update(_neighbors(c))
    solid_edge = frozenset(
        c
        for c in C2
        if _m(c)["p_cover"] and any(_m(n)["h_cover"] for n in _neighbors(c))
    )

    h_key, p_key = "h_" + basis, "p_" + basis
    if mode == "hole-in":
        admit = lambda n: _m(n)[h_key]  # noqa: E731
        return (
            void_edge,
            void_edge,
            admit,
            frozenset(c for c in void_edge if _m(c)[h_key]),
        )
    if mode == "hole-out":
        admit = lambda n: _m(n)[p_key]  # noqa: E731
        return (
            solid_edge,
            frozenset(solid_edge),
            admit,
            frozenset(c for c in solid_edge if _m(c)[p_key]),
        )
    # hole-out-ignore-geom
    admit = lambda n: not _m(n)[h_key]  # noqa: E731
    return solid_edge, frozenset(solid_edge), admit, frozenset(solid_edge)


def geom_expand_lazy(kind, k, mode, geom, res, coverage=_dilate.DEFAULT_COVERAGE):
    """Lazy O(perimeter) h3 geom-aware expand for polygon boundary/hole modes.

    Drop-in replacement for
    ``_dilate.geom_expand(kind, k, mode, _classify_polyfill(...), _neighbors, cov)``
    that avoids the full O(area) polyfill by tracing the polygon boundary ring(s)
    and classifying only cells near the perimeter — with h3-NATIVE membership (see
    :func:`_lazy_setup`), so the output is byte-identical to the
    ``_classify_polyfill`` oracle across all modes, coverages and fixtures
    (including cell-edge-aligned polygons).
    """
    if mode not in _dilate._LAZY_MODES:
        raise ValueError(
            f"geom_expand_lazy only handles {sorted(_dilate._LAZY_MODES)}; got {mode!r}"
        )
    if kind not in ("ring", "loop"):
        raise ValueError(f"kind must be 'ring' or 'loop'; got {kind!r}")

    frontier0, visited0, admit, k0 = _lazy_setup(mode, geom, int(res), coverage)
    if k == 0:
        return set(k0)
    acc = set(k0) if kind == "ring" else set()
    shell_k: set = set()
    for kk, shell in _dilate.dilate(frontier0, visited0, _neighbors, admit):
        if kk > k:
            break
        if kind == "ring":
            acc |= shell
        if kk == k:
            shell_k = shell
            break
    return acc if kind == "ring" else shell_k


def geom_expand(kind, geom, resolution, k, mode, coverage=_dilate.DEFAULT_COVERAGE):
    """Geometry-aware h3 ring/loop from a geometry (WKB bytes or WKT string).

    kind='ring' (filled <=k) or 'loop' (shell at exactly k). ``coverage`` selects
    the belongs-to basis (coveras/polyfill/core).  Returns a set of int cell ids;
    empty for a null/empty geometry.

    Polygon/MultiPolygon inputs on a lazy mode take the O(perimeter)
    :func:`geom_expand_lazy` boundary-ring path; lines, points, and
    GeometryCollections (no lazy boundary path) fall back to the full
    :func:`_classify_polyfill` classifier fed to ``_dilate.geom_expand`` — the
    same split the other light grids use.
    """
    g = _parse_geom(geom)
    if g is None:
        return set()
    if mode in _dilate._LAZY_MODES and g.geom_type in ("Polygon", "MultiPolygon"):
        return geom_expand_lazy(kind, int(k), mode, g, int(resolution), coverage)
    cls = _classify_polyfill(g, int(resolution))
    return _dilate.geom_expand(kind, int(k), mode, cls, _neighbors, coverage)
