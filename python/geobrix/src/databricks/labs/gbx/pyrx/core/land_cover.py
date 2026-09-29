"""Spark-free land-cover classification (NumPy band math). Classifies an
HxWxbands RGB(+NIR) array into land-cover labels. Phase 1 (this module)
implements spectral-threshold features + the ``spectral`` classify branch;
``kmeans``/``hybrid`` branches land in later tasks."""

import numpy as np

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


def classify(arr, *, method="hybrid", classes=None, n_clusters=6, smooth=3):
    """Classify an HxWxbands array into land-cover labels.

    Args:
        arr:        HxWxbands uint8/float array (RGB, optionally +NIR).
        method:     ``"spectral"`` (implemented here); ``"kmeans"``/``"hybrid"``
                    are later tasks and raise ``NotImplementedError``.
        classes:    Optional override of the class-name scheme; defaults to
                    ``DEFAULT_CLASSES``.
        n_clusters: Reserved for the ``kmeans``/``hybrid`` branches.
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
    else:
        raise NotImplementedError(method)  # kmeans/hybrid in Task 3
    return lab.astype(np.int32), names
