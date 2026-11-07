"""Source rasters and read-backs shared by the compression tests.

``test_compression`` pins the canonical writer once; ``test_compression_sites`` runs every
site that writes a raster through one table and reads the result back with these.
"""

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

from databricks.labs.gbx.pyrx.core.compression import predictor_for


def lzw_src(dtype="float32", nodata=-999.0):
    """A 64x64 tiled LZW GTiff with no predictor: the codec a derived raster must NOT inherit."""
    a = (np.random.default_rng(7).random((64, 64)) * 300).astype(dtype)
    prof = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype=dtype,
        nodata=nodata,
        crs="EPSG:32618",
        transform=from_bounds(500000, 4490000, 500640, 4490640, 64, 64),
        compress="lzw",
        tiled=True,
        blockxsize=32,
        blockysize=32,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a, 1)
        return mf.read()


def ycbcr_src(x0=500000, crs="EPSG:32618", seed=7):
    """A 64x64 tiled 3-band uint8 JPEG/YCbCr GTiff: its photometric must not be inherited."""
    a = (np.random.default_rng(seed).random((3, 64, 64)) * 255).astype("uint8")
    prof = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=3,
        dtype="uint8",
        crs=crs,
        transform=from_bounds(x0, 4490000, x0 + 640, 4490640, 64, 64),
        compress="jpeg",
        photometric="ycbcr",
        tiled=True,
        blockxsize=32,
        blockysize=32,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a)
        return mf.read()


def assert_canonical(b, dtype="float32"):
    """b is ZSTD with the predictor creation_opts derives for dtype (1 = tag omitted)."""
    with MemoryFile(bytes(b)) as m, m.open() as c:
        img = c.tags(ns="IMAGE_STRUCTURE")
        assert img.get("COMPRESSION") == "ZSTD", img
        want = predictor_for(dtype)
        if want == 1:
            assert img.get("PREDICTOR") in (None, "1"), img
        else:
            assert img.get("PREDICTOR") == str(want), img


def assert_zstd_without_ycbcr(b, count=3):
    with MemoryFile(bytes(b)) as m, m.open() as c:
        img = c.tags(ns="IMAGE_STRUCTURE")
        assert img.get("COMPRESSION") == "ZSTD", img
        assert "YCbCr" not in img.get("SOURCE_COLOR_SPACE", ""), img
        assert str(c.profile.get("photometric")).lower() != "ycbcr", c.profile
        assert c.count == count
