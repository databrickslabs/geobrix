"""Spark-free TIN / IDW interpolation to raster tiles — pure-Python counterparts
to the heavyweight rasterx ``RST_GridFromPoints`` (IDW via ``gdal.Grid``) and
``RST_DTMFromGeoms`` (Delaunay-TIN DTM) constructors / aggregators.

Both builders take plain Python inputs (point coordinate arrays + extent / size /
srid params) and return the result raster's single-band Float64 GTiff ``bytes``.

  * ``idw_grid``      -> RST_GridFromPoints (inverse-distance-weighted grid)
  * ``delaunay_dtm``  -> RST_DTMFromGeoms (Delaunay TIN elevation surface)

PARITY DIVERGENCES vs heavyweight (documented inline):
  * IDW: the heavyweight delegates to ``gdal.Grid(invdist:power=p:max_points=m)``.
    This reimplements the same inverse-distance formula directly with a KD-tree
    nearest-neighbour query (``scipy.spatial.cKDTree``). The math matches
    gdal_grid's ``invdist`` mode (no smoothing, no search radius); ``max_pts``
    caps the neighbours considered per output cell.
  * DTM: the heavyweight builds a CONSTRAINED-Delaunay TIN (breaklines enforced
    as hard edges; ``merge_tolerance`` / ``snap_tolerance`` control vertex
    coalescing). scipy's ``Delaunay`` is UNCONSTRAINED — it cannot enforce
    breaklines as edges and has no analogue for those tolerances. They are
    accepted for signature parity but NOT enforced; breakline vertices are
    folded in as additional triangulation points only.
"""

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from scipy.spatial import Delaunay, cKDTree
from scipy.spatial.distance import cdist

from databricks.labs.gbx._geom import parse_geom

_NODATA = -9999.0


def _parse_geom_elem(raw):
    """Decode one user-geom element (WKB/EWKB/WKT/EWKT) -> shapely geom | None.

    Skips ``None`` and empty bytes/string elements (so a null or zero-length
    entry drops out rather than raising), then routes through the shared
    ``gbx._geom.parse_geom`` so every encoding is accepted uniformly.
    """
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)) and len(raw) == 0:
        return None
    return parse_geom(raw)


# Output cells processed per chunk in the all-points IDW path. Bounds the
# transient (chunk x npts) distance matrix so memory stays modest regardless of
# grid size or point count.
_IDW_CELL_CHUNK = 65_536


def _resolve_out_crs(out_srid, out_crs):
    """Rule 2 output CRS -> canonical string (or None). out_crs wins over out_srid;
    both set -> error; neither -> None (CRS-less output)."""
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs

    if out_srid is not None and out_crs is not None:
        raise ValueError("provide out_srid OR out_crs, not both")
    if out_crs is not None:
        return crs_to_canonical(resolve_crs(out_crs))
    if out_srid is not None:
        return crs_to_canonical(resolve_crs(out_srid))
    return None


def _write_float64_grid(arr, xmin, ymin, xmax, ymax, width_px, height_px, crs, nodata):
    """Write a (height_px, width_px) Float64 array as single-band GTiff bytes.

    The transform is derived from the bounds + size; ``crs`` is a canonical CRS
    string (or None -> CRS-less); the given nodata is stamped on the band. Shared
    by ``idw_grid`` and ``delaunay_dtm``.
    """
    transform = from_bounds(
        float(xmin),
        float(ymin),
        float(xmax),
        float(ymax),
        int(width_px),
        int(height_px),
    )
    profile = dict(
        driver="GTiff",
        width=int(width_px),
        height=int(height_px),
        count=1,
        dtype="float64",
        crs=crs,
        transform=transform,
        nodata=float(nodata),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.asarray(arr, dtype="float64"), 1)
        return mf.read()


def _cell_center_coords(xmin, ymin, xmax, ymax, width_px, height_px):
    """Return an ``(height_px*width_px, 2)`` array of cell-CENTER world coords.

    Row-major (row 0 = top), matching GTiff raster ordering. Cell (row, col)
    center is at ``(xmin + (col+0.5)*xres, ymax - (row+0.5)*yres)``.
    """
    width_px = int(width_px)
    height_px = int(height_px)
    xres = (float(xmax) - float(xmin)) / width_px
    yres = (float(ymax) - float(ymin)) / height_px
    cols = (np.arange(width_px) + 0.5) * xres + float(xmin)
    rows = float(ymax) - (np.arange(height_px) + 0.5) * yres
    gx, gy = np.meshgrid(cols, rows)  # shape (height_px, width_px)
    return np.column_stack([gx.ravel(), gy.ravel()])


def _idw_all_points(centers, pts, vals, power):
    """All-point inverse-distance-weighted value per cell center.

    Computes, for every center, ``sum(v_i/d_i**power)/sum(1/d_i**power)`` over
    ALL points (no neighbour cap) — the same result as ``cKDTree.query`` with
    ``k == npts`` but without building/traversing the tree. Distances are formed
    with ``cdist`` in chunks of ``_IDW_CELL_CHUNK`` cells to bound memory.

    Exact hits (a center coincident with a point, ``d == 0``) return that point's
    value, matching the cKDTree path (which returns the nearest neighbour's value
    when ``dist[:, 0] == 0``). Cells with no usable weight become ``_NODATA``.
    """
    n_cells = centers.shape[0]
    out = np.full(n_cells, _NODATA, dtype="float64")
    for start in range(0, n_cells, _IDW_CELL_CHUNK):
        stop = min(start + _IDW_CELL_CHUNK, n_cells)
        d = cdist(centers[start:stop], pts)  # (chunk, npts)

        # Exact hits: center sits on a point (min distance 0) => that value.
        exact_pt = d.argmin(axis=1)
        exact = d[np.arange(d.shape[0]), exact_pt] == 0.0

        res = np.full(d.shape[0], _NODATA, dtype="float64")
        interp = ~exact
        if np.any(interp):
            di = d[interp]
            w = 1.0 / np.power(di, power)
            wsum = w.sum(axis=1)
            vsum = (w * vals).sum(axis=1)
            good = np.isfinite(wsum) & (wsum > 0)
            res_i = np.full(di.shape[0], _NODATA, dtype="float64")
            res_i[good] = vsum[good] / wsum[good]
            res[interp] = res_i
        # Exact hits return the coincident point's value directly.
        res[exact] = vals[exact_pt[exact]]
        out[start:stop] = res
    return out


def idw_grid(
    points_xy,
    values,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    out_srid=None,
    power=2.0,
    max_pts=12,
    out_crs=None,
):
    """Inverse-distance-weighted grid from scattered points (GTiff bytes).

    For each output cell CENTER, the value is the inverse-distance-weighted mean
    of the nearest ``max_pts`` input points::

        v(cell) = sum(v_i / d_i**power) / sum(1 / d_i**power)

    where ``d_i`` is the distance from the cell center to neighbour ``i``. If a
    neighbour coincides with the cell center (``d_i == 0``) its value is returned
    directly. Cells with no usable neighbour become NoData (-9999.0).

    Mirrors RST_GridFromPoints (``gdal.Grid`` invdist). ``power`` defaults to
    2.0; ``max_pts`` defaults to 12 (matching the heavyweight defaults).
    """
    pts = np.asarray(points_xy, dtype="float64")
    vals = np.asarray(values, dtype="float64")
    width_px = int(width_px)
    height_px = int(height_px)
    if width_px <= 0 or height_px <= 0:
        raise ValueError("rst_gridfrompoints: width_px and height_px must be positive")
    if float(power) <= 0.0:
        raise ValueError("rst_gridfrompoints: power must be positive")
    if int(max_pts) <= 0:
        raise ValueError("rst_gridfrompoints: max_pts must be positive")
    # Rule 2 output CRS (points are assumed already in it; label-only).
    crs_str = _resolve_out_crs(out_srid, out_crs)

    if pts.ndim != 2 or pts.shape[0] == 0:
        # No points -> all-NoData raster of the requested shape.
        out = np.full((height_px, width_px), _NODATA, dtype="float64")
        return _write_float64_grid(
            out, xmin, ymin, xmax, ymax, width_px, height_px, crs_str, _NODATA
        )
    if pts.shape[0] != vals.shape[0]:
        raise ValueError(
            "rst_gridfrompoints: points "
            f"({pts.shape[0]}) and values ({vals.shape[0]}) length mismatch"
        )

    power = float(power)
    k = min(int(max_pts), pts.shape[0])
    centers = _cell_center_coords(xmin, ymin, xmax, ymax, width_px, height_px)

    if k >= pts.shape[0]:
        # All-points mode (gdal_grid invdist uses every point). cKDTree.query
        # with k == npts degenerates to a full per-cell traversal/sort and is
        # pathologically slow (~36x slower than heavyweight). Compute the same
        # all-point IDW directly with cdist, chunked over output cells to bound
        # memory. Output is numerically identical to the cKDTree-k=npts path.
        out = _idw_all_points(centers, pts, vals, power)
        out = out.reshape(height_px, width_px)
        return _write_float64_grid(
            out, xmin, ymin, xmax, ymax, width_px, height_px, crs_str, _NODATA
        )

    tree = cKDTree(pts)
    dist, idx = tree.query(centers, k=k)
    # cKDTree.query returns 1-D arrays when k == 1; normalise to 2-D.
    if k == 1:
        dist = dist[:, None]
        idx = idx[:, None]

    neigh_vals = vals[idx]  # (n_cells, k)
    out = np.full(centers.shape[0], _NODATA, dtype="float64")

    # Exact hits (distance 0): take that neighbour's value directly.
    exact = dist[:, 0] == 0.0
    if np.any(exact):
        out[exact] = neigh_vals[exact, 0]

    interp_mask = ~exact
    if np.any(interp_mask):
        d = dist[interp_mask]
        nv = neigh_vals[interp_mask]
        w = 1.0 / np.power(d, power)
        wsum = w.sum(axis=1)
        vsum = (w * nv).sum(axis=1)
        # wsum is always > 0 here (all distances are finite and > 0), but guard
        # against degenerate inf weights anyway.
        good = np.isfinite(wsum) & (wsum > 0)
        res = np.full(d.shape[0], _NODATA, dtype="float64")
        res[good] = vsum[good] / wsum[good]
        out[interp_mask] = res

    out = out.reshape(height_px, width_px)
    return _write_float64_grid(
        out, xmin, ymin, xmax, ymax, width_px, height_px, crs_str, _NODATA
    )


def _coords_with_z(geom):
    """Yield (x, y, z) tuples for every coordinate of a shapely geometry.

    Z defaults to NaN where a coordinate lacks an elevation.
    """
    if geom is None or geom.is_empty:
        return
    if geom.geom_type == "Point":
        c = geom.coords[0]
        yield (c[0], c[1], c[2] if len(c) > 2 else float("nan"))
    elif geom.geom_type in ("MultiPoint", "GeometryCollection"):
        for g in geom.geoms:
            yield from _coords_with_z(g)
    else:
        for c in geom.coords:
            yield (c[0], c[1], c[2] if len(c) > 2 else float("nan"))


def delaunay_dtm(
    points_xyz,
    breaklines,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    out_srid=None,
    no_data=_NODATA,
    out_crs=None,
):
    """Delaunay-TIN DTM from Z-valued points (GTiff bytes).

    Builds an (unconstrained) Delaunay triangulation of the points' (x, y) and
    interpolates Z at each output cell CENTER by barycentric interpolation within
    the containing triangle::

        z(cell) = b0*z_a + b1*z_b + b2*z_c

    where ``(b0, b1, b2)`` are the barycentric coordinates of the cell center in
    its containing triangle (``a``, ``b``, ``c`` the triangle vertices). Cells
    OUTSIDE the convex hull (``Delaunay.find_simplex == -1``) become ``no_data``.

    Mirrors RST_DTMFromGeoms. ``points_xyz`` is an ``(n, 3)`` array (Z required).
    ``breaklines`` is a list of WKB linestring bytes (may be empty/None).

    PARITY DIVERGENCE: scipy's Delaunay is UNCONSTRAINED. ``breaklines`` are NOT
    enforced as hard edges; their vertices are folded in as extra triangulation
    points only. ``merge_tolerance`` / ``snap_tolerance`` (handled by the caller)
    have no scipy analogue and are not applied.
    """
    width_px = int(width_px)
    height_px = int(height_px)
    if width_px <= 0 or height_px <= 0:
        raise ValueError("rst_dtmfromgeoms: width_px and height_px must be positive")
    # Rule 2 output CRS (points assumed already in it; label-only).
    crs_str = _resolve_out_crs(out_srid, out_crs)

    pts = np.asarray(points_xyz, dtype="float64")
    if pts.ndim != 2 or pts.shape[0] == 0:
        raise ValueError("rst_dtmfromgeoms: at least one point is required")
    if pts.shape[1] < 3:
        raise ValueError("rst_dtmfromgeoms: points must carry a Z coordinate")

    # Fold breakline vertices in as additional triangulation points (NOT as
    # constraint edges — scipy Delaunay is unconstrained). Drop vertices without
    # a usable Z.
    extra = []
    for raw in breaklines or []:
        geom = _parse_geom_elem(raw)
        if geom is None or geom.is_empty:
            continue
        for x, y, z in _coords_with_z(geom):
            if not np.isnan(z):
                extra.append((x, y, z))
    if extra:
        pts = np.vstack([pts, np.asarray(extra, dtype="float64")])

    xy = pts[:, :2]
    z = pts[:, 2]

    out = np.full(height_px * width_px, float(no_data), dtype="float64")
    # Delaunay needs >= 3 non-collinear points; fall back to all-NoData otherwise.
    if xy.shape[0] >= 3:
        try:
            tri = Delaunay(xy)
        except Exception:  # noqa: BLE001 — degenerate (collinear) input
            tri = None
        if tri is not None:
            centers = _cell_center_coords(xmin, ymin, xmax, ymax, width_px, height_px)
            simplex = tri.find_simplex(centers)
            inside = simplex >= 0
            if np.any(inside):
                s = simplex[inside]
                # Barycentric coordinates via the precomputed transform.
                T = tri.transform[s]  # (m, 3, 2): rows 0-1 = inv affine, row 2 = r
                delta = centers[inside] - T[:, 2, :]
                b01 = np.einsum("mij,mj->mi", T[:, :2, :], delta)  # b0, b1
                b2 = 1.0 - b01.sum(axis=1)
                bary = np.column_stack([b01, b2])  # (m, 3)
                vert = tri.simplices[s]  # (m, 3) vertex indices
                zvals = (bary * z[vert]).sum(axis=1)
                flat = np.where(inside)[0]
                out[flat] = zvals

    out = out.reshape(height_px, width_px)
    return _write_float64_grid(
        out, xmin, ymin, xmax, ymax, width_px, height_px, crs_str, float(no_data)
    )


def points_xy_from_wkb(wkbs):
    """Decode a list of POINT WKB bytes into an ``(n, 2)`` (x, y) float array.

    Empty / None elements are dropped. Used by the IDW constructor / aggregator
    where only planar coordinates matter.
    """
    out = []
    for raw in wkbs:
        g = _parse_geom_elem(raw)
        if g is None or g.is_empty:
            continue
        out.append((g.x, g.y))
    return (
        np.asarray(out, dtype="float64") if out else np.empty((0, 2), dtype="float64")
    )


def points_xyz_from_wkb(wkbs):
    """Decode a list of POINT-with-Z WKB bytes into an ``(n, 3)`` (x, y, z) array.

    Empty / None elements are dropped. Raises if any point lacks a Z value
    (mirrors the heavyweight RST_DTMFromGeoms requirement).
    """
    out = []
    for raw in wkbs:
        g = _parse_geom_elem(raw)
        if g is None or g.is_empty:
            continue
        if not g.has_z:
            raise ValueError(
                "rst_dtmfromgeoms: point has no Z coordinate — supply 3D WKB "
                "(e.g. 'POINT Z (x y z)')"
            )
        c = g.coords[0]
        out.append((c[0], c[1], c[2]))
    return (
        np.asarray(out, dtype="float64") if out else np.empty((0, 3), dtype="float64")
    )


# Re-export for symmetry with other core modules (struct unused here but keeps
# the import set explicit for callers that build cellid envelopes elsewhere).
__all__ = [
    "idw_grid",
    "delaunay_dtm",
    "points_xy_from_wkb",
    "points_xyz_from_wkb",
]
