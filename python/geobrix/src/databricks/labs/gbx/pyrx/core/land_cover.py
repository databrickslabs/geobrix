"""Spark-free land-cover classification (NumPy band math). Classifies an
HxWxbands RGB(+NIR) array into land-cover labels. Implements spectral-threshold
features + the ``spectral`` classify branch, plus ``kmeans``/``hybrid``
branches via ``scipy.cluster.vq.kmeans2`` with spectral cluster-labeling."""

import numpy as np
from scipy.cluster.vq import kmeans2

# Class scheme: index = class id; -1 = nodata/unclassified.
DEFAULT_CLASSES = ["vegetation", "bare", "impervious", "dark"]


def features(arr) -> dict:
    """Derive per-pixel spectral features from an HxWxbands uint8/float array.

    Assumes bands 0-2 are R, G, B. Returns a dict with ``"exg"`` (excess
    green: 2G-R-B), ``"brightness"`` ((R+G+B)/3), and a ``"nodata"`` boolean
    mask (near-black pixels). If a 4th band is present it is treated as NIR
    and an ``"ndvi"`` entry is added.
    """
    a = arr.astype("float32")
    R, G, B = a[..., 0], a[..., 1], a[..., 2]
    f = {
        "exg": 2 * G - R - B,
        "brightness": (R + G + B) / 3.0,
        "nodata": (R + G + B) < 12,
    }
    if a.shape[-1] >= 4:  # NIR present
        nir = a[..., 3]
        f["ndvi"] = (nir - R) / np.clip(nir + R, 1e-6, None)
    return f


def _spectral(f, names):
    """Threshold-based classifier: excess-green -> vegetation; else split
    the remainder on brightness into bare/dark/impervious."""
    veg = names.index("vegetation")
    bare = names.index("bare")
    imp = names.index("impervious")
    dark = names.index("dark")
    lab = np.full(f["exg"].shape, -1, np.int32)
    green = f["exg"] > 6
    lab[green] = veg
    rest = ~green
    b = f["brightness"]
    lab[rest & (b >= 150)] = bare
    lab[rest & (b < 55)] = dark
    lab[(lab == -1)] = imp
    lab[f["nodata"]] = -1
    return lab


def _label_cluster(cf, names):
    """Label a k-means cluster centroid via the spectral thresholds:
    greenest -> vegetation, brightest -> bare, darkest -> dark, else ->
    impervious. ``cf`` is a dict of centroid feature scalars (``exg``,
    ``brightness``)."""
    if cf["exg"] > 6:
        return names.index("vegetation")
    if cf["brightness"] >= 150:
        return names.index("bare")
    if cf["brightness"] < 55:
        return names.index("dark")
    return names.index("impervious")


def _kmeans(arr, f, names, n_clusters):
    """K-means partition (RGB + exg + brightness features) via
    ``scipy.cluster.vq.kmeans2``, with clusters labeled by spectral
    thresholds on their centroids. Guards the degenerate case where there
    are fewer distinct colors than ``n_clusters`` so ``kmeans2`` doesn't
    crash or emit empty clusters."""
    R, G, B = [arr[..., i].astype("float32") for i in range(3)]
    feats = np.stack([R, G, B, f["exg"], f["brightness"]], -1)  # HxWx5
    valid = ~f["nodata"]
    X = feats[valid]
    lab = np.full(arr.shape[:2], -1, np.int32)
    if X.shape[0] == 0:
        return lab
    k = min(n_clusters, max(1, np.unique(X, axis=0).shape[0]))  # guard degenerate
    cent, assign = kmeans2(X, k, minit="++", seed=0)
    flat = np.full(valid.sum(), -1, np.int32)
    for cid in range(k):
        cf = {"exg": cent[cid][3], "brightness": cent[cid][4]}
        flat[assign == cid] = _label_cluster(cf, names)
    lab[valid] = flat
    return lab


def classify(arr, *, method="hybrid", classes=None, n_clusters=6, smooth=3):
    """Classify an HxWxbands array into land-cover labels.

    Args:
        arr:        HxWxbands uint8/float array (RGB, optionally +NIR).
        method:     ``"spectral"`` (threshold-based); ``"kmeans"`` (scipy
                    k-means partition + spectral cluster-labeling);
                    ``"hybrid"`` (k-means partition + spectral
                    cluster-labeling; same as ``"kmeans"`` today, kept as a
                    distinct branch so a later phase can diverge it).
        classes:    Optional override of the class-name scheme; defaults to
                    ``DEFAULT_CLASSES``.
        n_clusters: Number of k-means clusters (``kmeans``/``hybrid`` only).
        smooth:     Reserved for the ``kmeans``/``hybrid`` branches.

    Returns:
        ``(labels, class_names)`` where ``labels`` is an HxW int32 array
        (``-1`` = nodata/unclassified) and ``class_names`` is the list of
        class names indexed by label.
    """
    names = list(classes) if classes else list(DEFAULT_CLASSES)
    f = features(arr)
    if method == "spectral":
        lab = _spectral(f, names)
    elif method == "kmeans":
        lab = _kmeans(arr, f, names, n_clusters)
    elif method == "hybrid":
        lab = _kmeans(arr, f, names, n_clusters)  # k-means partition, spectral labeling
    else:
        raise NotImplementedError(method)
    return lab.astype(np.int32), names
