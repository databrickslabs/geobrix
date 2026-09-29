"""Spark-free land-cover classification (NumPy band math). Classifies an
HxWxbands RGB(+NIR) array into land-cover labels. Implements spectral-threshold
features + the ``spectral`` classify branch, plus ``kmeans``/``hybrid``
branches via ``scipy.cluster.vq.kmeans2`` with spectral cluster-labeling."""

import numpy as np
from scipy import ndimage as ndi
from scipy.cluster.vq import kmeans2
from skimage.morphology import disk

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
        # NOTE: computed but not yet consumed by any classifier -- greenness
        # is always ExG (RGB-only) in phase-1. Reserved for a phase-2
        # NIR-aware greenness signal; wiring it in now would change
        # classification behavior without NIR test data to validate it.
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


def _kmeans(arr, f, names, n_clusters):
    """K-means partition (RGB + exg + brightness features) via
    ``scipy.cluster.vq.kmeans2``, then label each cluster by the MAJORITY
    per-pixel spectral class of its members (not its centroid).

    Two deliberate choices vs. a naive centroid-label:
    - **Standardized features** (z-score) so no single axis dominates the
      Euclidean distance -- excess-green (~+-510) would otherwise swamp the
      0-255 bands and brightness, distorting the clusters.
    - **Majority-vote labeling**: a cluster inherits the dominant per-pixel
      spectral class of its members, so a mixed / marginal region (e.g. a
      mowed field whose mean excess-green straddles the vegetation threshold)
      takes its majority class instead of flipping on the cluster mean.

    Guards the degenerate case (fewer distinct colors than ``n_clusters``) so
    ``kmeans2`` doesn't crash or emit empty clusters."""
    R, G, B = [arr[..., i].astype("float32") for i in range(3)]
    feats = np.stack([R, G, B, f["exg"], f["brightness"]], -1)  # HxWx5
    valid = ~f["nodata"]
    X = feats[valid]
    lab = np.full(arr.shape[:2], -1, np.int32)
    if X.shape[0] == 0:
        return lab
    # Standardize (z-score) each feature so exg's large range doesn't dominate.
    std = X.std(axis=0)
    std[std == 0] = 1.0
    Xz = (X - X.mean(axis=0)) / std
    k = min(n_clusters, max(1, np.unique(Xz, axis=0).shape[0]))  # guard degenerate
    _, assign = kmeans2(Xz, k, minit="++", seed=0)
    # Label each cluster by the majority per-pixel spectral class of its members.
    px = _spectral(f, names)[valid]  # per-pixel spectral labels (>= 0 for valid pixels)
    flat = np.full(X.shape[0], -1, np.int32)
    for cid in range(k):
        members = assign == cid
        cls = px[members]
        cls = cls[cls >= 0]
        if cls.size:
            flat[members] = int(np.bincount(cls).argmax())  # majority class
    lab[valid] = flat
    return lab


def _despeckle(lab, window):
    """Drop small connected specks of each class by binary-opening its mask.

    Pixels removed from a class's mask by the opening are reset to ``-1``
    (unclassified) rather than reassigned, so class semantics are preserved
    and only isolated noise pixels are dropped. ``window <= 0`` is a no-op.
    """
    if window <= 0:
        return lab
    out = lab.copy()
    for c in np.unique(lab):
        if c < 0:
            continue
        m = ndi.binary_opening(lab == c, structure=disk(max(1, window // 2)))
        out[(lab == c) & ~m] = -1  # drop specks of class c
    return out


def classify(arr, *, method="hybrid", classes=None, n_clusters=6, smooth=3):
    """Classify an HxWxbands array into land-cover labels.

    Args:
        arr:        HxWxbands uint8/float array (RGB, optionally +NIR).
        method:     ``"spectral"`` (threshold-based); ``"kmeans"`` (scipy
                    k-means partition + spectral cluster-labeling);
                    ``"hybrid"`` (k-means partition + spectral
                    cluster-labeling; same as ``"kmeans"`` today, kept as a
                    distinct branch so a later phase can diverge it).
        classes:    Optional class-name list to use instead of
                    ``DEFAULT_CLASSES`` (``["vegetation", "bare",
                    "impervious", "dark"]``). Every classifier resolves ids
                    by literal name lookup (``names.index("vegetation")``,
                    ``names.index("bare")``, etc.), so any ``classes`` list
                    must contain those four exact (case-sensitive) strings
                    -- reordering them is safe, since ids track position --
                    but renaming, translating, or dropping any of the four
                    canonical identities raises ``ValueError`` (e.g.
                    ``'vegetation' is not in list``). Arbitrary/renamed-away
                    class schemes are unsupported in phase-1. Defaults to
                    ``DEFAULT_CLASSES``.
        n_clusters: Number of k-means clusters (``kmeans``/``hybrid`` only).
        smooth:     De-speckle window size. When ``> 0``, a binary-opening
                    de-speckle pass (see ``_despeckle``) is applied to the
                    label array before the final nodata re-mask, dropping
                    isolated salt-and-pepper specks of each class. ``0``
                    leaves labels unchanged.

    Returns:
        ``(labels, class_names)`` where ``labels`` is an HxW int32 array
        (``-1`` = nodata/unclassified) and ``class_names`` is the list of
        class names indexed by label.
    """
    if arr.shape[-1] < 3:
        raise ValueError(
            f"land_cover requires a >=3-band (RGB) tile, got {arr.shape[-1]}"
        )
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
    if smooth > 0:
        lab = _despeckle(lab, smooth)
        lab[f["nodata"]] = -1  # re-mask nodata after de-speckle
    return lab.astype(np.int32), names
