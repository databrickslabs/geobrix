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
from shapely.geometry import MultiLineString, MultiPoint, MultiPolygon

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
        return _dilate._union_classifications(classify(g, res) for g in groups)

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


def h3_los_visible(
    tower, observer_z, targets, surface_z, ground_z, target_height, eps=1.0
):
    """Exact H3 line-of-sight. blocker = surface_z; target base = ground_z + target_height.
    Surface-relative model: call with ground_z = surface_z (target is above the surface).
    NoData-safe: None/missing surface is transparent; a target with no base is skipped.

    Args:
        tower:         H3 cell string of the observer.
        observer_z:    observer elevation in metres (e.g. DTM@tower + tower_height).
        targets:       iterable of H3 cell strings to test.
        surface_z:     ``{cell_str: float|None}`` — blocker surface elevation (DSM/CHM).
                       Missing or None entries are transparent (no blocker at that cell).
        ground_z:      ``{cell_str: float|None}`` — bare-earth base elevation (DTM).
                       A target with None or missing ground_z is skipped (not fabricated).
        target_height: height of the target receiver above the ground, metres.
        eps:           sight-line tolerance in metres; a cell does not block its own
                       grazing ray (default 1.0 m).

    Returns:
        ``set`` of visible H3 cell strings (subset of *targets*).
    """
    visible = set()
    for c in targets:
        if c == tower:
            visible.add(c)
            continue
        g = ground_z.get(c)
        if g is None:
            continue
        path = h3.grid_path_cells(tower, c)
        n = len(path) - 1
        if n <= 0:
            visible.add(c)
            continue
        z_t = g + target_height
        blocked = False
        for i in range(1, n):
            s = surface_z.get(path[i])
            if s is not None and s > observer_z + (z_t - observer_z) * (i / n) + eps:
                blocked = True
                break
        if not blocked:
            visible.add(c)
    return visible


def h3_viewshed_towers(
    towers_df,
    surface_df,
    ground_df,
    *,
    radius_m,
    viewshed_res,
    target_height=1.6,
    tower_id_col="tower_cellid",
    observer_z_col="observer_z",
    cell_col="cellid",
    surface_z_col="z",
    ground_z_col="z",
    num_partitions=None,
):
    """Per-tower H3 line-of-sight viewshed — the H3 sibling of ``rst_viewshed_towers``.

    For each tower, returns the subset of in-buffer H3 cells visible from the
    tower top, computed with the exact :func:`h3_los_visible` line-of-sight
    (blocker = surface/DSM, observer = tower-top absolute elevation, target =
    ground/DTM + ``target_height``). This is the distributed sibling of the
    raster ``rst_viewshed_towers``: it lets the Wireless Coverage tower siting
    scale across Spark while staying entirely in H3 cell space.

    Non-columnar DataFrame function (no SQL binding): two surface/ground maps and
    a towers frame in, one row per ``(tower, visible cell)`` out. The two maps are
    collected once to driver dicts (bounded at ``viewshed_res``) and the per-tower
    line-of-sight is fanned one-tower-per-task with ``mapInPandas`` over the
    towers frame repartitioned by ``tower_id_col``. That path uses no
    ``sparkContext`` / ``.rdd`` / ``_jvm``, so it runs on Databricks Serverless
    Connect as well as on classic clusters and ``local`` test sessions.

    H3 cell ids flow as ``BIGINT`` (the integer H3 representation) in every
    column — matching the Databricks product H3 convention — and are converted to
    and from the ``h3`` string form internally.

    Args:
        towers_df:     DataFrame of towers; must carry ``tower_id_col`` (``BIGINT``
                       H3 cell) and ``observer_z_col`` (observer-top absolute
                       elevation in metres, e.g. DTM@tower + mast height).
        surface_df:    Blocker-surface (DSM / DSM+CHM) map at ``viewshed_res``;
                       must carry ``cell_col`` (``BIGINT`` H3 cell) and
                       ``surface_z_col`` (metres). Missing or NULL surface is
                       transparent (no blocker at that cell).
        ground_df:     Bare-earth (DTM) map at ``viewshed_res``; must carry
                       ``cell_col`` and ``ground_z_col`` (metres). A target with a
                       NULL/missing ground is skipped, not fabricated.
        radius_m:      Analysis radius in metres (> 0). Sizes the per-tower H3
                       ``grid_disk`` buffer; the line-of-sight is still bounded by
                       the surface/ground maps.
        viewshed_res:  H3 resolution of the surface/ground maps and the output
                       cells.
        target_height: Receiver height above the ground at each tested cell,
                       metres. Defaults to 1.6.
        tower_id_col:  Tower identity column. Defaults to "tower_cellid".
        observer_z_col: Observer-elevation column. Defaults to "observer_z".
        cell_col:      Cell-id column shared by ``surface_df`` / ``ground_df``.
                       Defaults to "cellid".
        surface_z_col: Surface-elevation column of ``surface_df``. Defaults to "z".
        ground_z_col:  Ground-elevation column of ``ground_df``. Defaults to "z".
        num_partitions: Fan-out for the per-tower stage (``repartition`` by
                       ``tower_id_col``). Defaults to the distinct tower count
                       (~one tower per task).

    Returns:
        DataFrame with one row per ``(tower, visible cell)``: ``tower_id_col``
        (``BIGINT``) and ``cell_col`` (``BIGINT`` H3 cell visible from the tower).
    """
    import math

    # Collect the two bounded viewshed-res maps to driver dicts keyed by the h3
    # string cell (the h3 library is string-native). At viewshed_res over a city
    # these are bounded (hundreds of thousands of cells) and small enough to
    # serialize into the mapInPandas closure. NULL/None elevations are preserved:
    # a None surface is a transparent blocker, a None ground skips that target.
    surf = {
        h3.int_to_str(int(r[cell_col])): (
            None if r[surface_z_col] is None else float(r[surface_z_col])
        )
        for r in surface_df.select(cell_col, surface_z_col).collect()
    }
    grnd = {
        h3.int_to_str(int(r[cell_col])): (
            None if r[ground_z_col] is None else float(r[ground_z_col])
        )
        for r in ground_df.select(cell_col, ground_z_col).collect()
    }

    # Buffer radius in ring steps. Hex centres are ~sqrt(3)*edge (~1.73*edge)
    # apart, so dividing by 1.5 (< 1.73) yields a safe over-estimate of k — the
    # buffer always covers radius_m; the line-of-sight stays bounded by the map,
    # so an over-large k only widens the (cheap) grid_disk membership test.
    edge_m = h3.average_hexagon_edge_length(int(viewshed_res), unit="m")
    k = max(1, int(math.ceil(float(radius_m) / edge_m / 1.5)))
    th = float(target_height)

    def _fan(batches):
        # Worker-side: one tower per task. Closes over the two bounded maps, k,
        # and target_height (serialized with the UDF). No sparkContext / .rdd.
        import pandas as pd

        for pdf in batches:
            towers_out, cells_out = [], []
            for raw_tower, raw_oz in zip(pdf[tower_id_col], pdf[observer_z_col]):
                if raw_tower is None or raw_oz is None:
                    continue
                tcell = int(raw_tower)
                t_str = h3.int_to_str(tcell)
                targets = [c for c in h3.grid_disk(t_str, k) if c in surf]
                for vc in h3_los_visible(
                    t_str, float(raw_oz), targets, surf, grnd, target_height=th
                ):
                    towers_out.append(tcell)
                    cells_out.append(int(vc, 16))
            yield pd.DataFrame(
                {
                    tower_id_col: pd.Series(towers_out, dtype="int64"),
                    cell_col: pd.Series(cells_out, dtype="int64"),
                }
            )

    n = num_partitions or towers_df.select(tower_id_col).distinct().count() or 1
    fanned = towers_df.select(tower_id_col, observer_z_col).repartition(
        int(n), tower_id_col
    )
    return fanned.mapInPandas(_fan, schema=f"{tower_id_col} long, {cell_col} long")


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
