import numpy as np

from databricks.labs.gbx.pyrx.core import land_cover as lc


def _rgb(h=32, w=32):
    a = np.zeros((h, w, 3), np.uint8)
    a[:, :16] = (30, 200, 40)  # left half: green -> vegetation
    a[:, 16:] = (210, 205, 200)  # right half: bright/gray -> bare
    return a


def test_spectral_labels_vegetation_and_bare():
    labels, names = lc.classify(_rgb(), method="spectral", smooth=0)
    veg = names.index("vegetation")
    bare = names.index("bare")
    # left half predominantly vegetation, right half predominantly bare
    assert (labels[:, :16] == veg).mean() > 0.8
    assert (labels[:, 16:] == bare).mean() > 0.8


def test_spectral_runs_without_nir():
    labels, _ = lc.classify(_rgb(), method="spectral", smooth=0)
    assert (
        labels.shape == (32, 32) and labels.dtype == np.int32
    )  # Review Focus: RGB-only
