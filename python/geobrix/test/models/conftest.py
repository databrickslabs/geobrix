"""Shared fixtures for gbx.models tests: small in-memory rasters built with rasterio's
MemoryFile, used to exercise runner.segment_raster's chip/infer/stitch/polygonize path
with fake (non-GPU) segmenters.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

_CRS = "EPSG:32633"


def _open_rgb(width, height, data, nodata=None):
    """Return an open (write-then-reopen-for-read) in-memory HxWx3 uint8 GTiff
    MemoryFile + dataset pair; caller drives the fixture's yield/teardown."""
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=3,
        dtype="uint8",
        crs=_CRS,
        transform=from_origin(0, height, 1, 1),
        nodata=nodata,
    )
    mf = MemoryFile()
    with mf.open(**profile) as dst:
        dst.write(data)
    return mf


@pytest.fixture
def synthetic_rgb_tile():
    """64x64 RGB tile -- exactly one tile_px=64 chip, so a fake segmenter that draws
    the same per-chip objects every call is not duplicated across chips."""
    data = np.zeros((3, 64, 64), dtype="uint8")
    mf = _open_rgb(64, 64, data)
    with mf:
        with mf.open() as ds:
            yield ds


@pytest.fixture
def nodata_rgb_tile():
    """All-NoData 64x64 RGB tile."""
    data = np.zeros((3, 64, 64), dtype="uint8")
    mf = _open_rgb(64, 64, data, nodata=0)
    with mf:
        with mf.open() as ds:
            yield ds


@pytest.fixture
def wide_rgb_tile():
    """48x20 RGB tile -- wider than a tile_px=32 chip -- with a red object baked in at
    x:[24,34) y:[5,15), straddling the seam between the two windows a tile_px=32,
    overlap=16(%) grid plans (windows at col 0..32 and col 26..48; see
    _straddling_object, which finds the object by its marker color, not by chip
    position)."""
    data = np.zeros((3, 20, 48), dtype="uint8")
    data[0, 5:15, 24:34] = 255  # red channel -> the object's marker color
    mf = _open_rgb(48, 20, data)
    with mf:
        with mf.open() as ds:
            yield ds


def _straddling_object(image):
    """Fake segmenter: label 1 wherever a pixel matches the red marker color baked
    into wide_rgb_tile, else 0. Content-based on purpose -- a real segmenter has no
    notion of chip offsets, only pixel content, so this mirrors that contract."""
    red = np.all(image == [255, 0, 0], axis=-1)
    return red.astype(np.int32)
