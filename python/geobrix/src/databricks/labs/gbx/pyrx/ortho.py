"""pyrx.ortho — dense-photogrammetry orchestration (Sim(3) alignment, shared-ENU
cluster placement, dense-cloud -> ortho/DSM/LAZ products).

The Sim(3) primitives (``umeyama_sim3``, ``apply_sim3``) are pure numpy and
import cleanly with no optional dependency. Later cluster-placement + product
functions (``place_clusters_shared_enu``, ``dense_clusters_to_products``)
import ``pycolmap`` lazily, only inside their own bodies, and raise a clear
error when it is absent — this module itself never imports pycolmap.
"""

from __future__ import annotations

import numpy as np


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
