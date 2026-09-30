"""pyrx.ortho — dense-photogrammetry orchestration (Sim(3) alignment, shared-ENU
cluster placement, dense-cloud -> ortho/DSM/LAZ products).

The Sim(3) primitives (``umeyama_sim3``, ``apply_sim3``) and the ENU-cloud
rasterizer (``rasterize_enu_ortho``) are pure numpy and import cleanly with no
optional dependency. Later cluster-placement + product functions
(``place_clusters_shared_enu``, ``dense_clusters_to_products``) import
``pycolmap`` lazily, only inside their own bodies, and raise a clear error
when it is absent — this module itself never imports pycolmap.
"""

from __future__ import annotations

import gc
import os
import shutil
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.core.crs import enu_to_lonlat, resolve_crs
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_file
from databricks.labs.gbx.pyrx.imagery import zbuffer_ortho


def umeyama_sim3(src, dst):
    """Least-squares Sim(3) (scale, rotation, translation) mapping src -> dst
    point sets (Umeyama 1991). Returns ``(scale, R(3,3), t(3,))``.

    Raises:
        ValueError: if fewer than 3 point pairs are given, or if `src` has
            zero variance (all source points coincident) — either case makes
            the scale estimate ``0/0`` (NaN) rather than a real fit.
    """
    src = np.asarray(src, dtype="float64")
    dst = np.asarray(dst, dtype="float64")
    if src.shape[0] < 3:
        raise ValueError("umeyama_sim3 requires >=3 non-degenerate point pairs")
    mu_s, mu_d = src.mean(0), dst.mean(0)
    sc, dc = src - mu_s, dst - mu_d
    var_s = (sc**2).sum() / src.shape[0]
    if var_s == 0.0:
        raise ValueError("umeyama_sim3 requires >=3 non-degenerate point pairs")
    cov = (dc.T @ sc) / src.shape[0]
    U, D, Vt = np.linalg.svd(cov)
    S = np.eye(3)
    if np.linalg.det(U) * np.linalg.det(Vt) < 0:
        S[2, 2] = -1
    R = U @ S @ Vt
    scale = float((D * np.diag(S)).sum() / var_s)
    t = mu_d - scale * (R @ mu_s)
    return scale, R, t


def apply_sim3(T, pts):
    """Apply ``T=(scale, R, t)`` to an ``(N,3)`` point array."""
    scale, R, t = T
    return scale * (np.asarray(pts, dtype="float64") @ np.asarray(R).T) + np.asarray(t)


def rasterize_enu_ortho(
    xe,
    ye,
    ze,
    r,
    g,
    b,
    ref_lat,
    ref_lon,
    ref_alt,
    gps_tf,
    out_ortho,
    out_dsm,
    gsd_cm=3.0,
):
    """Top-surface-rasterize an ENU-metre colored point cloud into a georeferenced
    ortho + DSM (EPSG:4326 GeoTIFFs).

    ``ref_lat``/``ref_lon`` anchor the ENU origin used to convert the raster's
    top-left corner back to lon/lat via
    :func:`databricks.labs.gbx.core.crs.enu_to_lonlat` — the tier-neutral
    equirectangular (local-tangent-plane) approximation, the same one
    ``accumulate_orthomosaic`` uses. ``ref_alt`` and ``gps_tf`` are accepted for
    call-site compatibility with callers that carry a ``pycolmap.GPSTransform``
    (e.g. ``place_clusters_shared_enu``'s return value) but are unused here:
    georeferencing never touches pycolmap, so ``gps_tf=None`` runs this
    function with no optional dependency at all.

    Returns ``(ortho_path, dsm_path)``.
    """
    del ref_alt, gps_tf  # accepted for call-site compatibility only; see docstring

    # Robust to a thin/degenerate cloud: drop non-finite points, require a usable
    # count, and if the ground band (±20 m of median z) removes everything, fall
    # back to all finite points instead of raising on an empty array.
    xe = np.asarray(xe, dtype="float64")
    ye = np.asarray(ye, dtype="float64")
    ze = np.asarray(ze, dtype="float64")
    finite = np.isfinite(xe) & np.isfinite(ye) & np.isfinite(ze)
    n_finite = int(finite.sum())
    if n_finite < 100:
        raise RuntimeError(
            f"rasterize_enu_ortho: only {n_finite} finite points — reconstruction "
            "too sparse to rasterize; raise dense quality (DENSE_GEOM_CONSIST=True, "
            "more DENSE_SRC_IMAGES, larger DENSE_MAX_IMAGE_SIZE)."
        )
    xe, ye, ze = xe[finite], ye[finite], ze[finite]
    r = np.asarray(r)[finite]
    g = np.asarray(g)[finite]
    b = np.asarray(b)[finite]
    z_med = float(np.median(ze))
    ground_mask = np.abs(ze - z_med) < 20.0
    if int(ground_mask.sum()) < max(10, int(0.01 * n_finite)):
        ground_mask = np.ones_like(ze, dtype=bool)  # band too thin — use all finite
    xe_g, ye_g = xe[ground_mask], ye[ground_mask]
    me = (xe_g.max() - xe_g.min()) * 0.05
    mn = (ye_g.max() - ye_g.min()) * 0.05
    e_min = xe_g.min() - me
    e_max = xe_g.max() + me
    n_min = ye_g.min() - mn
    n_max = ye_g.max() + mn
    gsd_m = gsd_cm / 100.0

    rgb, dsm, _ = zbuffer_ortho(
        xe,
        ye,
        ze,
        r,
        g,
        b,
        xmin=e_min,
        ymin=n_min,
        xmax=e_max,
        ymax=n_max,
        px=gsd_m,
    )

    # Georeference: same equirectangular approximation as accumulate_orthomosaic,
    # sourced entirely from the tier-neutral core.crs helper (one east/north pair
    # for the top-left corner, one more one-pixel-diagonal step for pixel size) —
    # no inline 111320/cos(lat) math and no pycolmap.
    lons, lats = enu_to_lonlat(
        np.array([e_min, e_min + gsd_m]),
        np.array([n_max, n_max - gsd_m]),
        ref_lat,
        ref_lon,
    )
    lon_tl, lat_tl = float(lons[0]), float(lats[0])
    px_dlon = float(lons[1] - lons[0])
    px_dlat = float(lats[0] - lats[1])
    geo_tf = from_origin(lon_tl, lat_tl, px_dlon, px_dlat)

    H, W = dsm.shape
    Path(out_ortho).parent.mkdir(parents=True, exist_ok=True)
    Path(out_dsm).parent.mkdir(parents=True, exist_ok=True)
    crs_4326 = resolve_crs(4326)

    tmp_ortho = new_local_temp_file(suffix=".tif")
    tmp_dsm = new_local_temp_file(suffix=".tif")
    try:
        with rasterio.open(
            tmp_ortho,
            "w",
            driver="GTiff",
            height=H,
            width=W,
            count=3,
            dtype="uint8",
            crs=crs_4326,
            transform=geo_tf,
            compress="lzw",
            nodata=0,
        ) as dst_f:
            for band in range(3):
                dst_f.write(rgb[band], band + 1)
        shutil.copy(tmp_ortho, str(out_ortho))

        with rasterio.open(
            tmp_dsm,
            "w",
            driver="GTiff",
            height=H,
            width=W,
            count=1,
            dtype="float32",
            crs=crs_4326,
            transform=geo_tf,
            compress="lzw",
        ) as dst_f:
            dst_f.write(dsm, 1)
        shutil.copy(tmp_dsm, str(out_dsm))
    finally:
        for _t in (tmp_ortho, tmp_dsm):
            if os.path.exists(_t):
                os.remove(_t)

    print(f"Dense ortho → {out_ortho}  DSM → {out_dsm}  ({H}×{W}px @ {gsd_cm:.1f}cm)")
    return str(out_ortho), str(out_dsm)


def _import_pycolmap():
    """Lazily import pycolmap, raising a clear error when the photogrammetry
    extra is not installed. Never imported at module top — this module must
    import cleanly with no optional dependency."""
    try:
        import pycolmap
    except ImportError as exc:
        raise ImportError(
            "pyrx.ortho.place_clusters_shared_enu requires pycolmap "
            "(install the photogrammetry extra)"
        ) from exc
    return pycolmap


def _read_gps_priors(gps_json):
    """{image_name: np.array([lat, lon, alt])} from a persisted GPS-priors JSON.

    GPS priors are persisted as JSON (not read from the SQLite master DB) so the
    orthomosaic step works from durable Volume artifacts on FORCE_RELOAD reuse.
    """
    import json

    with open(gps_json) as _f:
        return {k: np.asarray(v, dtype=float) for k, v in json.load(_f).items()}


def _load_largest_model(sparse_dir):
    """Load the COLMAP model with the most images from a cluster's sparse dir."""
    pycolmap = _import_pycolmap()
    sparse_dir = Path(sparse_dir)
    if not sparse_dir.exists():
        return None

    def _n(d):
        try:
            return len(pycolmap.Reconstruction(d).images)
        except Exception:
            return 0

    dirs = sorted(
        [d for d in sparse_dir.iterdir() if d.is_dir() and (d / "images.bin").exists()],
        key=_n,
        reverse=True,
    )
    return pycolmap.Reconstruction(dirs[0]) if dirs else None


def place_clusters_shared_enu(cluster_models):
    """Place every cluster's COLMAP frame into ONE shared ENU frame and return the
    per-cluster Sim(3) transforms (COLMAP frame -> shared ENU metres) plus the shared
    GPS reference. The anchor cluster is georeferenced to GPS (Umeyama); each
    remaining cluster is tied to the already-placed set through its SHARED overlap
    cameras (same image names), falling back to its own GPS fit when it shares no
    cameras.

    This is the single alignment source of truth: a single-cluster input (one
    entry in ``cluster_models``) degenerates to the group-of-one case — the sole
    cluster becomes the GPS anchor and there is nothing left to tie — which
    produces the same anchor-fit result as fitting that one cluster to its own
    GPS mean directly.

    This mirrors accumulate_orthomosaic's Passes A+B (the sparse projector). It is
    kept as a SEPARATE function so the GPU dense-cloud merge places its clusters in
    the SAME frame the sparse ortho uses, without modifying the validated sparse
    projector. (Unify with accumulate_orthomosaic when that path is next touched.)

    cluster_models: {cid: (sparse_dir, gps_json)}. Returns a dict with:
      T {cid: (scale, R, t)}, ref_lat/ref_lon/ref_alt, gps_tf, aligned (cids placed,
      largest first), n_tied, anchor, centers, gps, sizes.
    """
    pycolmap = _import_pycolmap()

    centers = {}  # cid -> {name: np.array([x, y, z]) in that cluster's COLMAP frame}
    gps = {}  # cid -> {name: np.array([lat, lon, alt])}
    sizes = {}
    for cid, (sparse_dir, gps_json) in cluster_models.items():
        rec = _load_largest_model(sparse_dir)
        if rec is None or not rec.images:
            print(f"[place] cluster {cid}: no model — skipped", flush=True)
            continue
        c = {
            im.name: np.asarray(im.projection_center(), dtype=float)
            for _, im in rec.images.items()
            if im.has_pose
        }
        if len(c) < 3:
            print(f"[place] cluster {cid}: <3 posed images — skipped", flush=True)
            del rec
            gc.collect()
            continue
        centers[cid] = c
        gps[cid] = {k: v for k, v in _read_gps_priors(gps_json).items() if k in c}
        sizes[cid] = len(c)
        del rec
        gc.collect()
    if not centers:
        raise RuntimeError("No cluster produced a usable model.")
    order = sorted(centers, key=lambda c: sizes[c], reverse=True)

    # Shared global ENU reference (origin) = mean of ALL clusters' GPS priors.
    all_gps = {}
    for cid in centers:
        all_gps.update(gps[cid])
    if not all_gps:
        raise RuntimeError("No GPS priors across clusters — cannot georeference.")
    ref_lat, ref_lon, ref_alt = np.array(list(all_gps.values())).mean(0)
    gps_tf = pycolmap.GPSTransform()

    def _gps_fit(cid):
        names = [n for n in centers[cid] if n in gps[cid]]
        if len(names) < 3:
            return None
        src = np.array([centers[cid][n] for n in names])
        dst = np.array(
            [
                gps_tf.ellipsoid_to_enu(
                    np.array([gps[cid][n]]), ref_lat, ref_lon, ref_alt
                )[0]
                for n in names
            ]
        )
        return umeyama_sim3(src, dst)

    # Anchor by GPS; tie neighbours through shared cameras; GPS-fallback the rest.
    T = {}
    placed_enu = {}  # image_name -> ENU centre (from already-placed clusters)
    n_tied = 0
    anchor = order[0]
    Ta = _gps_fit(anchor)
    if Ta is None:
        raise RuntimeError(
            f"Anchor cluster {anchor} lacks >=3 GPS priors for georeferencing."
        )
    T[anchor] = Ta
    for name, c in centers[anchor].items():
        placed_enu[name] = apply_sim3(Ta, c)

    remaining = [c for c in order if c != anchor]
    progress = True
    while remaining and progress:
        progress = False
        for cid in list(remaining):
            shared = [n for n in centers[cid] if n in placed_enu]
            if len(shared) >= 3:
                src = np.array([centers[cid][n] for n in shared])
                dst = np.array([placed_enu[n] for n in shared])
                T[cid] = umeyama_sim3(src, dst)
                for name, c in centers[cid].items():
                    placed_enu.setdefault(name, apply_sim3(T[cid], c))
                remaining.remove(cid)
                progress = True
                n_tied += 1
                print(
                    f"[place] cluster {cid}: tied via {len(shared)} shared cameras",
                    flush=True,
                )
    for cid in list(remaining):  # not shared-camera-reachable -> own GPS fit
        Tg = _gps_fit(cid)
        if Tg is None:
            print(
                f"[place] cluster {cid}: no shared tie + <3 GPS — skipped", flush=True
            )
            continue
        T[cid] = Tg
        for name, c in centers[cid].items():
            placed_enu.setdefault(name, apply_sim3(Tg, c))
        print(f"[place] cluster {cid}: GPS fallback (no shared-camera tie)", flush=True)

    aligned = [cid for cid in order if cid in T]
    print(
        f"[place] {len(aligned)}/{len(centers)} clusters placed in shared ENU "
        f"({n_tied} via shared cameras, anchor {anchor})",
        flush=True,
    )
    return {
        "T": T,
        "ref_lat": ref_lat,
        "ref_lon": ref_lon,
        "ref_alt": ref_alt,
        "gps_tf": gps_tf,
        "aligned": aligned,
        "n_tied": n_tied,
        "anchor": anchor,
        "centers": centers,
        "gps": gps,
        "sizes": sizes,
    }
