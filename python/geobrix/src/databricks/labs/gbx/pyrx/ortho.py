"""pyrx.ortho — dense-photogrammetry orchestration (Sim(3) alignment, shared-ENU
cluster placement, dense-cloud -> ortho/DSM/LAZ products), plus the lighter-weight
``sparse_orthomosaic`` blender that skips the dense point cloud entirely.

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
import time
from pathlib import Path

import numpy as np
import rasterio
from PIL import Image as PILImage
from rasterio.transform import from_origin

from databricks.labs.gbx.core.crs import enu_to_lonlat, resolve_crs, utm_epsg_for
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_file
from databricks.labs.gbx.pyrx.imagery import read_fused_ply, zbuffer_ortho
from databricks.labs.gbx.pyrx.mvs import dense_reconstruct_clusters


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

    Georeferencing anchors the raster's top-left corner two ways, matching
    ``_rasterize_enu_ortho`` in ``config_nb`` cell ``1ee46ad5`` exactly:

    - ``gps_tf`` given (a ``pycolmap.GPSTransform``, e.g. from
      ``place_clusters_shared_enu``): the corner is ``gps_tf.enu_to_ellipsoid``
      — the ellipsoidal conversion, for fidelity. This is the path real
      callers use and requires pycolmap (supplied by the caller; this module
      never imports it).
    - ``gps_tf=None``: the corner falls back to
      :func:`databricks.labs.gbx.core.crs.enu_to_lonlat` — the tier-neutral
      equirectangular (local-tangent-plane) approximation — so this function
      runs with no optional dependency at all. This is an approximation, not
      equivalent to the ``gps_tf`` path; it exists for local testability.

    Either way, the per-pixel GSD scale (``dpm_lat``/``dpm_lon``) uses the same
    naive equirectangular constant (``1/111320``, ``1/(111320*cos(ref_lat))``)
    as the source — only the anchor corner differs between the two paths.

    Returns ``(ortho_path, dsm_path)``.
    """
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

    # Georeference. Per-pixel GSD scale: naive equirectangular constant, same in
    # both branches (matches the source; this is a coarse fixed scale, not the
    # fidelity-sensitive part).
    dpm_lat = 1.0 / 111320.0
    dpm_lon = 1.0 / (111320.0 * np.cos(np.radians(ref_lat)))
    # Anchor corner: gps_tf's ellipsoidal enu_to_ellipsoid (fidelity — matches
    # _rasterize_enu_ortho verbatim) when a pycolmap GPSTransform is supplied;
    # core.crs.enu_to_lonlat (equirectangular approximation, pycolmap-free) only
    # as the gps_tf=None fallback. These are NOT equivalent — do not merge them.
    if gps_tf is not None:
        tl = gps_tf.enu_to_ellipsoid(
            np.array([[e_min, n_max, z_med]]), ref_lat, ref_lon, ref_alt
        )[0]
        lon_tl, lat_tl = float(tl[1]), float(tl[0])
    else:
        lon_tl_arr, lat_tl_arr = enu_to_lonlat(e_min, n_max, ref_lat, ref_lon)
        lon_tl, lat_tl = float(lon_tl_arr), float(lat_tl_arr)
    geo_tf = from_origin(lon_tl, lat_tl, gsd_m * dpm_lon, gsd_m * dpm_lat)

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


def _ortho_canvas_dims(e_min, e_max, n_min, n_max, gsd_cm):
    """Pure extent→canvas-size math: apply 5% margin to union ground extent,
    compute pixel dimensions from GSD.

    Used by sparse orthomosaic to size the shared canvas before projection.
    Returns ``(e_min_margined, e_max_margined, n_min_margined, n_max_margined,
    out_w, out_h)`` where ``out_w``/``out_h`` ≥ 1 even for degenerate zero
    extents.

    Args:
        e_min, e_max: easting bounds (metres).
        n_min, n_max: northing bounds (metres).
        gsd_cm: ground-sample distance in centimetres.

    Returns:
        Tuple of (e_min_margined, e_max_margined, n_min_margined, n_max_margined,
        out_w, out_h) where out_w and out_h are ints ≥ 1.
    """
    me = (e_max - e_min) * 0.05
    mn = (n_max - n_min) * 0.05
    e_min -= me
    e_max += me
    n_min -= mn
    n_max += mn
    gsd_m = gsd_cm / 100.0
    out_w = max(1, int((e_max - e_min) / gsd_m))
    out_h = max(1, int((n_max - n_min) / gsd_m))
    return e_min, e_max, n_min, n_max, out_w, out_h


def _backproject_image(
    src_rgb,
    *,
    cam_f,
    cam_cx,
    cam_cy,
    cam_w,
    cam_h,
    R_cw,
    t_cw,
    T_cid,
    C_enu,
    z_med,
    e_min,
    n_max,
    gsd_m,
    out_w,
    out_h,
    blend_gamma,
):
    """Pure-numpy per-camera back-projection core of the sparse orthomosaic:
    project one camera's already-decoded RGB image onto the shared ENU ground
    plane ``z=z_med`` and radially weight the result.

    This is the numeric body of ``accumulate_orthomosaic``'s
    ``_project_cluster._one`` (config_nb cell ``9e68917e``), carved out
    verbatim so it is unit-testable with a synthetic camera and no pycolmap.
    The caller (a pycolmap-aware wrapper) reads ``cam_f``/``cam_cx``/``cam_cy``/
    ``cam_w``/``cam_h`` off the COLMAP camera, ``R_cw``/``t_cw`` off
    ``image.cam_from_world()``, and ``C_enu`` via ``apply_sim3(T_cid,
    image.projection_center())`` — this function itself never imports
    pycolmap.

    ``T_cid = (scale, R(3,3), t(3,))`` is the cluster's Sim(3) COLMAP-frame ->
    shared-ENU transform (from ``place_clusters_shared_enu``); ``R_cw``(3,3) /
    ``t_cw``(3,) are the COLMAP ``cam_from_world`` pose for this image.

    Returns ``(px0, px1, py0, py1, colors, weights)`` — ``colors`` is
    ``float32`` ``(py1-py0, px1-px0, 3)``, ``weights`` is ``float32``
    ``(py1-py0, px1-px0)`` — or ``None`` when the camera is <1 m above the
    ground plane (``C_enu[2]-z_med < 1.0``) or its footprint bbox is
    empty/off-canvas (``px0>=px1 or py0>=py1``).
    """

    def _to_colmap(pe):
        s, R, t = T_cid
        return ((R.T @ (np.atleast_2d(pe) - t).T) / s).T

    f = cam_f
    cx = cam_cx
    cy = cam_cy
    cw = cam_w
    ch = cam_h
    h_above = float(C_enu[2] - z_med)
    if h_above < 1.0:
        return None
    hep = int(h_above * (cw / 2) / f / gsd_m) + 4
    hnp = int(h_above * (ch / 2) / f / gsd_m) + 4
    fc_px = (C_enu[0] - e_min) / gsd_m
    fc_py = (n_max - C_enu[1]) / gsd_m
    px0 = max(0, int(fc_px - hep))
    px1 = min(out_w, int(fc_px + hep))
    py0 = max(0, int(fc_py - hnp))
    py1 = min(out_h, int(fc_py + hnp))
    if px0 >= px1 or py0 >= py1:
        return None
    src = src_rgb
    out_e = (e_min + (np.arange(px0, px1) + 0.5) * gsd_m).astype(np.float32)
    out_n = (n_max - (np.arange(py0, py1) + 0.5) * gsd_m).astype(np.float32)
    ge, gn = np.meshgrid(out_e, out_n)
    ht, wt = ge.shape
    P_enu = np.column_stack([ge.ravel(), gn.ravel(), np.full(ht * wt, z_med, np.float32)])
    P_cam = (
        R_cw.astype(np.float32) @ _to_colmap(P_enu).astype(np.float32).T
        + t_cw.astype(np.float32)[:, None]
    ).T
    valid = P_cam[:, 2] > 1e-6
    dz = np.where(valid, P_cam[:, 2], 1.0)
    u = np.where(valid, f * P_cam[:, 0] / dz + cx, -1.0).reshape(ht, wt)
    v = np.where(valid, f * P_cam[:, 1] / dz + cy, -1.0).reshape(ht, wt)
    in_img = (u >= 0) & (u < cw - 1) & (v >= 0) & (v < ch - 1)
    u0 = np.clip(u.astype(np.int32), 0, cw - 2)
    v0 = np.clip(v.astype(np.int32), 0, ch - 2)
    uf = (u - u0).clip(0, 1)
    vf = (v - v0).clip(0, 1)
    colors = (
        src[v0, u0] * ((1 - uf) * (1 - vf))[..., None]
        + src[v0, u0 + 1] * (uf * (1 - vf))[..., None]
        + src[v0 + 1, u0] * ((1 - uf) * vf)[..., None]
        + src[v0 + 1, u0 + 1] * (uf * vf)[..., None]
    )
    de = (u - cx) / (cw / 2)
    dn = (v - cy) / (ch / 2)
    w = np.maximum(0.0, 1.0 - np.sqrt(de**2 + dn**2)) ** blend_gamma * in_img
    return px0, px1, py0, py1, colors.astype(np.float32), w.astype(np.float32)


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


def sparse_orthomosaic(
    cluster_models, image_dir, out_tiff, *, gsd_cm=3.0, max_workers=8, blend_gamma=4.0
):
    """Blend per-cluster sparse reconstructions into ONE georeferenced orthomosaic
    GeoTIFF.

    Clusters are placed in a single shared ENU frame via
    :func:`place_clusters_shared_enu`: the largest cluster is georeferenced to GPS
    (Umeyama), and each remaining cluster is tied to the already-placed set through
    its SHARED overlap cameras (same image names) via a similarity fit on their
    projection centres — restoring cross-cluster consistency without a monolithic
    global bundle adjustment. Clusters with no shared-camera tie fall back to their
    own GPS fit. Each cluster projects onto its own local ground plane (z_med). One
    cluster reconstruction is held at a time; peak RAM = largest cluster + the canvas.

    Driver-side (no ``spark`` param). ``pycolmap`` is imported only lazily — via
    :func:`_load_largest_model` and the ``place_clusters_shared_enu`` reconstruction
    reloads below — this module itself never imports it at top level. The per-camera
    numeric core is :func:`_backproject_image`; the canvas sizing is
    :func:`_ortho_canvas_dims`.

    ``cluster_models``: ``{cid: (sparse_dir, gps_json)}``. Returns the written
    ``out_tiff`` path (as ``str``).
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    t0 = time.perf_counter()
    image_dir = Path(image_dir)
    out_tiff = Path(out_tiff)

    # ---- Pass A+B: place every cluster in ONE shared ENU frame ----
    placement = place_clusters_shared_enu(cluster_models)
    T = placement["T"]  # cid -> (scale, R, t): COLMAP frame -> shared ENU
    aligned = placement["aligned"]  # cids placed, largest first
    ref_lat, ref_lon = placement["ref_lat"], placement["ref_lon"]

    # ---- Pass C: union ground extent + per-cluster z_med (one reload per cluster) ----
    cluster_z = {}
    e_min = n_min = np.inf
    e_max = n_max = -np.inf
    for cid in aligned:
        rec = _load_largest_model(cluster_models[cid][0])
        if rec is None:
            continue
        Tc = T[cid]
        if rec.points3D:
            pe = apply_sim3(Tc, np.array([p.xyz for p in rec.points3D.values()]))
            zc = float(np.median(pe[:, 2]))
            ground = pe[np.abs(pe[:, 2] - zc) < 20]
        else:
            cen = apply_sim3(
                Tc,
                np.array(
                    [im.projection_center() for _, im in rec.images.items() if im.has_pose]
                ),
            )
            zc = float(cen[:, 2].mean() - 250)
            ground = cen
        cluster_z[cid] = zc
        e_min = min(e_min, ground[:, 0].min())
        e_max = max(e_max, ground[:, 0].max())
        n_min = min(n_min, ground[:, 1].min())
        n_max = max(n_max, ground[:, 1].max())
        del rec
        gc.collect()

    e_min, e_max, n_min, n_max, out_w, out_h = _ortho_canvas_dims(
        e_min, e_max, n_min, n_max, gsd_cm
    )
    gsd_m = gsd_cm / 100.0
    _canvas_gb = (out_h * out_w * 3 * 2 + out_h * out_w * 2) / 1024 ** 3
    if _canvas_gb > 4.0:
        print(f"[WARN] Canvas {_canvas_gb:.1f} GB > 4 GB — raise GSD_CM to reduce.", flush=True)
    print(f"Canvas: {out_w}x{out_h}px @ {gsd_cm:.1f}cm ({_canvas_gb:.2f} GB float16) "
          f"from {len(aligned)} cluster(s)", flush=True)

    canvas = np.zeros((out_h, out_w, 3), dtype=np.float16)
    weights = np.zeros((out_h, out_w), dtype=np.float16)
    _lock = threading.Lock()
    n_proj = [0]

    def _project_cluster(rec, T_cid, z_med):
        def _one(kv):
            _, image = kv
            if not image.has_pose:
                return
            ip = image_dir / image.name
            if not ip.exists():
                return
            cam = rec.cameras[image.camera_id]
            cw, ch = cam.width, cam.height
            cfw = image.cam_from_world()
            R_cw = cfw.rotation.matrix()
            t_cw = cfw.translation
            C_enu = apply_sim3(T_cid, image.projection_center())
            with PILImage.open(ip) as pim:
                pim.draft("RGB", (cw, ch))
                pim.load()
                if pim.width != cw or pim.height != ch:
                    pim = pim.resize((cw, ch), PILImage.BILINEAR)
                src_rgb = np.array(pim.convert("RGB"), dtype=np.float32)
            result = _backproject_image(
                src_rgb,
                cam_f=cam.params[0], cam_cx=cam.params[1], cam_cy=cam.params[2],
                cam_w=cw, cam_h=ch,
                R_cw=R_cw, t_cw=t_cw,
                T_cid=T_cid, C_enu=C_enu, z_med=z_med,
                e_min=e_min, n_max=n_max, gsd_m=gsd_m,
                out_w=out_w, out_h=out_h, blend_gamma=blend_gamma,
            )
            if result is None:
                return
            px0, px1, py0, py1, colors, w = result
            with _lock:
                canvas[py0:py1, px0:px1] += (colors * w[..., None]).astype(np.float16)
                weights[py0:py1, px0:px1] += w.astype(np.float16)
                n_proj[0] += 1

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            list(pool.map(_one, sorted(rec.images.items(), key=lambda kv: kv[1].name)))

    # ---- Pass D: reload each cluster and project into the shared canvas ----
    for cid in aligned:
        rec = _load_largest_model(cluster_models[cid][0])
        if rec is None:
            continue
        _project_cluster(rec, T[cid], cluster_z[cid])
        del rec
        gc.collect()
        print(f"[ortho] cluster {cid}: projected ({n_proj[0]} images total)", flush=True)

    has = weights > 0
    ortho = np.zeros((out_h, out_w, 3), dtype=np.uint8)
    ortho[has] = np.clip(canvas[has] / weights[has, None], 0, 255).astype(np.uint8)
    print(f"Coverage: {100.0 * has.sum() / (out_h * out_w):.1f}% ({n_proj[0]} images projected)")

    # ---- Georef: equirectangular corner + per-pixel degree size, both via core.crs
    # (single consistent source — replaces the notebook's separate inline dpm_lat/
    # dpm_lon scale + gps_tf.enu_to_ellipsoid corner). ----
    lon_tl_arr, lat_tl_arr = enu_to_lonlat(e_min, n_max, ref_lat, ref_lon)
    lon_tl, lat_tl = float(lon_tl_arr), float(lat_tl_arr)
    dlon_px = float(enu_to_lonlat(gsd_m, 0.0, ref_lat, ref_lon)[0]) - ref_lon
    dlat_px = float(enu_to_lonlat(0.0, gsd_m, ref_lat, ref_lon)[1]) - ref_lat
    transform = from_origin(lon_tl, lat_tl, dlon_px, dlat_px)

    out_tiff.parent.mkdir(parents=True, exist_ok=True)
    local_tif = new_local_temp_file(suffix=".tif")
    try:
        with rasterio.open(
            local_tif, "w", driver="GTiff", height=out_h, width=out_w, count=3,
            dtype="uint8", crs=resolve_crs(4326), transform=transform, compress="lzw",
        ) as dst:
            for b in range(3):
                dst.write(ortho[:, :, b], b + 1)
        shutil.copy(local_tif, str(out_tiff))
    finally:
        if os.path.exists(local_tif):
            os.remove(local_tif)
    print(f"Orthomosaic written: {out_tiff} ({time.perf_counter() - t0:.0f}s)")
    return str(out_tiff)


def _enu_to_utm(xe, ye, ze, ref_lat, ref_lon):
    """Reproject shared-ENU-metre points to the local UTM zone so the exported LAZ
    carries a real *metric* projected CRS (a geographic CRS would force a coarse
    LAS scale in degrees). ENU->lon/lat uses the same equirectangular
    linearisation as the ortho georeferencing; lon/lat->UTM via pyproj.

    UTM zone selection is :func:`databricks.labs.gbx.core.crs.utm_epsg_for`
    (Task 1) rather than inline zone arithmetic — this is the one adaptation
    from ``_enu_to_utm`` in ``config_nb`` cell ``1ee46ad5``; the reprojection
    itself (``pyproj.Transformer.from_crs(4326, epsg, always_xy=True)``) is
    unchanged. Returns ``(easting, northing, up, epsg)``.
    """
    import pyproj

    xe = np.asarray(xe, dtype="float64")
    ye = np.asarray(ye, dtype="float64")
    dpm_lat = 1.0 / 111320.0
    dpm_lon = 1.0 / (111320.0 * np.cos(np.radians(ref_lat)))
    lon = ref_lon + xe * dpm_lon
    lat = ref_lat + ye * dpm_lat
    epsg = utm_epsg_for(ref_lon, ref_lat)
    tf = pyproj.Transformer.from_crs(4326, epsg, always_xy=True)
    east, north = tf.transform(lon, lat)
    return np.asarray(east), np.asarray(north), np.asarray(ze, dtype="float64"), epsg


def _write_dense_laz_dataset(spark, cluster_points, out_dir, crs=None):
    """cluster_points: list of (group, cluster, xe, ye, ze, r, g, b) numpy tuples.
    Assemble a points DataFrame, repartitionByRange by (group, cluster) to
    guarantee exactly one (group,cluster) key per partition, and write sharded
    *_<group>_<cluster>.laz via lidar_gbx. Per-cluster numpy arrays are
    concatenated into a single pandas DataFrame (Arrow-backed), avoiding a
    monolithic per-point Row list that OOMs the driver for large clouds."""
    import numpy as _np
    import pandas as _pd

    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    n_parts = max(1, len(cluster_points))
    per_cluster = []
    for group, cluster, xe, ye, ze, r, g, b in cluster_points:
        n = len(xe)
        if n == 0:
            continue
        per_cluster.append(
            _pd.DataFrame(
                {
                    "x": _np.asarray(xe, dtype=_np.float64),
                    "y": _np.asarray(ye, dtype=_np.float64),
                    "z": _np.asarray(ze, dtype=_np.float64),
                    "r": _np.asarray(r, dtype=_np.int32),
                    "g": _np.asarray(g, dtype=_np.int32),
                    "b": _np.asarray(b, dtype=_np.int32),
                    "group": _np.full(n, str(group)),
                    "cluster": _np.full(n, int(cluster), dtype=_np.int64),
                }
            )
        )
    if not per_cluster:
        return out_dir
    pdf = _pd.concat(per_cluster, ignore_index=True)
    df = spark.createDataFrame(pdf)
    writer = (
        df.repartitionByRange(n_parts, "group", "cluster")
        .write.format("lidar_gbx")
        .option("groupCol", "group")
        .option("clusterCol", "cluster")
        .option("fileName", "dense")
        .mode("overwrite")
    )
    if crs is not None:
        writer = writer.option("crs", str(crs))
    writer.save(out_dir)
    return out_dir


def _merge_dense_laz(spark, out_dir, file_name="dense_merged", crs=None):
    """Phase 2: fold the sharded *_<group>_<cluster>.laz parts under out_dir into
    one merged <file_name>.laz, KEEPING the per-cluster parts (keepParts). merge
    ignores the DataFrame rows (it folds the on-disk parts). overlap="drop"
    makes the merge seam-de-duplicated: the writer assigns each seam point to
    exactly one owning cluster (nearest-cluster ownership) instead of a raw
    concatenation that duplicates overlap-band points under overlap="keep" (the
    writer default) — so the merged cloud is a non-redundant union. The merge
    budget is sized to the executing node's available RAM (compute-aware): an
    AIR or classic-GPU node with large host RAM handles large clouds; a
    minimal CPU node falls back to a conservative 512 MiB floor. The merge only
    skips gracefully (sharded parts remain the canonical artifact) if the cloud
    exceeds even that measured budget. Pass crs= to tag the merged file
    (belt-and-suspenders over the writer-side CRS inference from the first part
    header)."""
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    writer = (
        spark.createDataFrame([(1,)], ["_"])  # merge ignores rows; folds parts
        .write.format("lidar_gbx")
        .option("merge", "true")
        .option("keepParts", "true")
        .option("overlap", "drop")  # seam-de-duplicated union (v2 seam-unification)
        .option("fileName", file_name)
        .mode("append")
    )
    if crs is not None:
        writer = writer.option("crs", str(crs))
    writer.save(out_dir)
    return out_dir


def dense_clusters_to_products(
    spark,
    cluster_models,
    dense_plys,
    *,
    merged_ortho,
    merged_dsm,
    merged_laz,
    cluster_paths,
    gsd_cm=3.0,
    group="all",
):
    """Place every cluster's fused.ply in ONE shared ENU frame (anchor GPS +
    shared-camera ties, mirroring accumulate_orthomosaic), then write:
      * per-cluster ortho/DSM  (cluster_paths[cid] -> {'ortho', 'dsm'})
      * sharded per-cluster LAZ parts via lidar_gbx (phase 1)
      * dense_merged.laz merged from all parts via lidar_gbx (phase 2,
        keepParts, overlap="drop" — seam-de-duplicated non-redundant union)
      * one MERGED, co-registered ortho/DSM over all clusters (merged_ortho/dsm).

    Rasters are EPSG:4326; LAZ clouds are reprojected to the shared local UTM
    zone (metric CRS) and written via the lidar_gbx DataSource writer.
    cluster_models: {cid: (sparse_dir, gps_json)}; dense_plys: {cid: fused.ply}.
    ``spark`` is an explicit parameter (it was a notebook global in
    ``config_nb`` cell ``1ee46ad5``) and is threaded to the LAZ-writer helpers.
    Returns ``(used_cids, path_to_dense_merged_laz)``.
    """
    placement = place_clusters_shared_enu(cluster_models)
    ref_lat = placement["ref_lat"]
    ref_lon = placement["ref_lon"]
    ref_alt = placement["ref_alt"]
    gps_tf = placement["gps_tf"]

    agg = {k: [] for k in ("x", "y", "z", "r", "g", "b")}
    used = []
    used_points = []  # (group, cluster, ux, uy, uz, r, g, b) for phase-1 write
    last_epsg = None
    for cid in placement["aligned"]:
        ply = dense_plys.get(cid)
        if not ply or not Path(ply).exists():
            print(f"[dense-merge] cluster {cid}: no fused.ply — skipped", flush=True)
            continue
        try:
            cloud = read_fused_ply(str(ply))
            pts = apply_sim3(
                placement["T"][cid],
                np.column_stack([cloud["x"], cloud["y"], cloud["z"]]),
            )
            xe, ye, ze = pts[:, 0], pts[:, 1], pts[:, 2]
            r, g, b = cloud["r"], cloud["g"], cloud["b"]
            cp = cluster_paths[cid]
            rasterize_enu_ortho(
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
                cp["ortho"],
                cp["dsm"],
                gsd_cm,
            )
            ux, uy, uz, epsg = _enu_to_utm(xe, ye, ze, ref_lat, ref_lon)
            last_epsg = epsg
            used_points.append((group, int(cid), ux, uy, uz, r, g, b))
            for k, v in zip(("x", "y", "z", "r", "g", "b"), (xe, ye, ze, r, g, b)):
                agg[k].append(v)
            used.append(cid)
        except Exception as e:
            print(f"[dense][skip] cluster {cid}: {e}", flush=True)
            continue
    if not used:
        raise RuntimeError("dense_clusters_to_products: no cluster produced points")

    X, Y, Z = (np.concatenate(agg[k]) for k in ("x", "y", "z"))
    R, G, B = (np.concatenate(agg[k]) for k in ("r", "g", "b"))
    print(
        f"[dense-merge] merging {len(used)} clusters -> {len(X):,} points", flush=True
    )
    rasterize_enu_ortho(
        X,
        Y,
        Z,
        R,
        G,
        B,
        ref_lat,
        ref_lon,
        ref_alt,
        gps_tf,
        merged_ortho,
        merged_dsm,
        gsd_cm,
    )
    # Phase 1: assemble all clusters' UTM points into a Spark DataFrame and write
    # sharded *_<group>_<cluster>.laz via the lidar_gbx DataSource writer.
    # repartitionByRange on (group, cluster) gives deterministic one-partition-
    # per-cluster isolation over the fixed key-set, so each shard carries
    # exactly one cluster's points and the filename is guaranteed correct.
    dense_laz_dir = str(Path(merged_laz).parent)
    _write_dense_laz_dataset(spark, used_points, dense_laz_dir, crs=last_epsg)
    # Phase 2 (best-effort): fold the sharded parts into dense_merged.laz with
    # overlap="drop" (seam-de-duplicated, non-redundant union). The merge gate
    # raises when the estimated cloud exceeds driver RAM (~256 MiB); on failure
    # the sharded parts (phase 1) are already written and remain readable as a
    # directory of parts via the lidar_gbx reader.
    try:
        _merge_dense_laz(spark, dense_laz_dir, crs=last_epsg)
        merged = str(Path(dense_laz_dir) / "dense_merged.laz")
    except Exception as e:
        print(
            f"[dense] phase-2 merge skipped ({e}); sharded parts remain the "
            "canonical artifact",
            flush=True,
        )
        merged = dense_laz_dir  # directory of parts — lidar_gbx reader accepts this
    return used, merged


def dense_orthomosaic(
    spark,
    cluster_models,
    image_dir,
    *,
    merged_ortho,
    merged_dsm,
    merged_laz,
    cluster_paths,
    work_root,
    ply_root=None,
    checkpoint=None,
    force=False,
    gsd_cm=3.0,
    group="all",
    max_image_size=1600,
    src_images=None,
    geom_consistency=False,
    num_iterations=None,
    window_step=None,
    allocation=None,
    on_event=None,
):
    """One-call dense orthomosaic: reconstruct (``dense_reconstruct_clusters``)
    then georef + products (``dense_clusters_to_products``). Collapses nb1b
    cell ``127b9903``'s hand-wired per-cluster reconstruction + fuse + georef
    loop into a single call for net-new data.

    ``cluster_models`` is ``{cid: (sparse_dir, gps_json)}``. ``spark`` is an
    explicit parameter (never a notebook global, matching
    ``dense_clusters_to_products``) and is threaded only to that function —
    the reconstruction half needs no Spark session. Raises ``RuntimeError``
    when every cluster's patch_match failed (no plys produced) rather than
    calling ``dense_clusters_to_products`` with an empty set. Returns
    ``(used_cids, path_to_dense_merged_laz)`` — the same shape as
    ``dense_clusters_to_products``.
    """
    plys = dense_reconstruct_clusters(
        cluster_models,
        image_dir,
        work_root=work_root,
        ply_root=ply_root,
        checkpoint=checkpoint,
        force=force,
        max_image_size=max_image_size,
        src_images=src_images,
        geom_consistency=geom_consistency,
        num_iterations=num_iterations,
        window_step=window_step,
        allocation=allocation,
        on_event=on_event,
    )
    if not plys:
        raise RuntimeError("dense_orthomosaic: no cluster produced points")
    return dense_clusters_to_products(
        spark,
        {cid: cluster_models[cid] for cid in plys},
        plys,
        merged_ortho=merged_ortho,
        merged_dsm=merged_dsm,
        merged_laz=merged_laz,
        cluster_paths=cluster_paths,
        gsd_cm=gsd_cm,
        group=group,
    )
