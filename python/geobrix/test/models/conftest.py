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


@pytest.fixture
def multi_chip_rgb_tile():
    """96x32 RGB tile -- a tile_px=32, overlap=0 grid plans exactly THREE non-touching
    chips over it (col 0..32, 32..64, 64..96; see plan_grid_windows). A red marker
    object sits well inside chip 0 (x:[4,10) y:[4,10)), chip 1 (x:[32,64)) is pure
    background (no object), and a green marker object sits well inside chip 2
    (x:[70,80) y:[10,20)) -- far from every tile edge, so neither object touches a
    seam and no seam-merge is involved. Exercises the genuinely-multi-chip,
    multi-object aggregation path (see _two_color_objects), which a single-chip
    fixture (e.g. synthetic_rgb_tile at tile_px==its own size) cannot: gpu_pool_map
    must fan >1 chip across >1 device slot and segment_raster must still recombine
    all of them, in the same result, regardless of gpus."""
    data = np.zeros((3, 32, 96), dtype="uint8")
    data[0, 4:10, 4:10] = 255  # red channel -> object A, inside chip 0
    data[1, 10:20, 70:80] = 255  # green channel -> object B, inside chip 2
    mf = _open_rgb(96, 32, data)
    with mf:
        with mf.open() as ds:
            yield ds


@pytest.fixture
def geographic_rgb_tile():
    """64x64 RGB tile in a GEOGRAPHIC CRS (EPSG:4326) with a red marker object at
    x:[10,40) y:[10,40). Pixel size is 1e-5 deg (~1 m near lat 43), so the object is a
    few hundred SQUARE METERS but only ~1e-7 square DEGREES. This is the case that
    exposes the min_area-in-CRS-units bug: measured in degrees the object is far under
    any metric min_area and gets dropped (the real 0-objects failure on the 4326
    orthomosaic COG); measured metrically it survives. The projected fixtures above
    never catch it because EPSG:32633's .area is already in m^2."""
    data = np.zeros((3, 64, 64), dtype="uint8")
    data[0, 10:40, 10:40] = 255  # red marker object -> _straddling_object labels it
    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(-77.968, 43.233, 1e-5, 1e-5),
    )
    mf = MemoryFile()
    with mf.open(**profile) as dst:
        dst.write(data)
    with mf:
        with mf.open() as ds:
            yield ds


def _two_color_objects(image):
    """Fake segmenter: label 1 wherever a pixel matches the red marker color, label 2
    wherever it matches the green marker color (multi_chip_rgb_tile), else 0.
    Content-based, like _straddling_object -- deterministic and chip-position-blind,
    so it faithfully exercises real per-chip inference: a chip with neither color
    (e.g. the middle chip) yields an all-zero mask, same as a real segmenter would on
    an empty crop."""
    m = np.zeros(image.shape[:2], np.int32)
    m[np.all(image == [255, 0, 0], axis=-1)] = 1
    m[np.all(image == [0, 255, 0], axis=-1)] = 2
    return m
