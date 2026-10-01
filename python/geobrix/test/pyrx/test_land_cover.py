import numpy as np
import pytest
from rasterio import features as rfeat

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


def test_kmeans_partitions_and_labels(_rgb=_rgb):
    labels, names = lc.classify(_rgb(), method="kmeans", n_clusters=4, smooth=0)
    assert set(np.unique(labels)) - {-1}  # some classes assigned
    assert (labels[:, :16] == names.index("vegetation")).mean() > 0.6


def test_kmeans_degenerate_fewer_colors_than_clusters():
    flat = np.full((16, 16, 3), (120, 120, 120), np.uint8)  # one color
    labels, _ = lc.classify(flat, method="kmeans", n_clusters=6, smooth=0)
    assert labels.shape == (16, 16)  # Review Focus: no crash on empty clusters


def _components(lab):
    return sum(
        1
        for _, v in rfeat.shapes((lab >= 0).astype("uint8"), mask=(lab >= 0))
        if v == 1
    )


def test_despeckle_reduces_fragments():
    # Note: unlike the brief's literal fixture (noise sprinkled onto an
    # already fully-classified _rgb(), with no nodata anywhere), the
    # (lab >= 0) connected-component count used by _components is
    # insensitive to holes carved into one solid classified blob -- a
    # blob riddled with holes is still ONE polygon (interior rings), so
    # component count can never drop that way (verified independently via
    # rasterio.features.shapes and scipy.ndimage.label, 4- and
    # 8-connectivity: all report 1 before and after, regardless of
    # de-speckle quality). To exercise the despeckle removing genuine
    # salt-and-pepper *islands*, this array is mostly nodata (near-black)
    # with one real 10x10 classified blob plus several isolated 1px
    # speckles far apart -- each speckle is its own fragment pre-despeckle
    # (surrounded by nodata, not by other classified pixels).
    a = np.zeros((32, 32, 3), np.uint8)
    a[2:12, 2:12] = (30, 200, 40)  # real vegetation blob
    for r, c in [(20, 4), (24, 10), (28, 20), (5, 25), (15, 28)]:
        a[r, c] = (30, 200, 40)  # isolated single-pixel speckles, far apart
    n0 = _components(lc.classify(a, method="spectral", smooth=0)[0])
    n3 = _components(lc.classify(a, method="spectral", smooth=3)[0])
    assert n3 < n0  # Review Focus: salt-and-pepper


def test_all_nodata_tile_all_unclassified():
    black = np.zeros((16, 16, 3), np.uint8)
    labels, _ = lc.classify(black, method="hybrid", smooth=3)
    assert (labels == -1).all()  # Review Focus: nodata


def test_classify_raises_clear_error_on_too_few_bands():
    single_band = np.zeros((16, 16, 1), np.uint8)
    with pytest.raises(ValueError, match="requires a >=3-band"):
        lc.classify(single_band, method="spectral", smooth=0)
