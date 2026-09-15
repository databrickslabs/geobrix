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
from shapely.geometry import MultiPolygon

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


def classify(geom, res):
    """Build the 6-set covering Classification for a geometry via h3-lib polyfill.

    For polygon inputs (including multi-polygons) the h3 polygon_to_cells_experimental
    polyfill is used with 'overlap' (cover) and 'full' (core) containment modes.

    For non-polygon inputs (points, lines) the h3 polyfill API is polygon-specific
    and cannot be used directly.  Instead, candidate cells are generated by mapping
    representative coordinates to their H3 cells (via latlng_to_cell), and coverage
    is set to the union of those cells (p_cover = s_cover = cells; centroid = core =
    empty since no polygon cell has its centre inside — or is contained by — a point
    or line).

    Three bases are built for the polygon path via the h3 native containment modes:
    'overlap' → cover, 'center' → centroid (the "polyfill" coverage), 'full' → core.
    """
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


def geom_expand(kind, geom, resolution, k, mode, coverage=_dilate.DEFAULT_COVERAGE):
    """Geometry-aware h3 ring/loop from a geometry (WKB bytes or WKT string).

    kind='ring' (filled <=k) or 'loop' (shell at exactly k). ``coverage`` selects
    the belongs-to basis (coveras/polyfill/core).  Returns a set of int cell ids;
    empty for a null/empty geometry.
    """
    g = _parse_geom(geom)
    if g is None:
        return set()
    cls = classify(g, int(resolution))
    return _dilate.geom_expand(kind, int(k), mode, cls, _neighbors, coverage)
