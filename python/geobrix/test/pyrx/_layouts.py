"""Synthetic 3-layout raster corpus for windowed-read tests.

All three writers emit identical pixels + georeference so 'read window W from
any layout' yields the same slice. Origin (10, 50), 0.001 deg pixels (EPSG:4326).
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin

_PX = 0.001


def PIXELS(width, height):
    return np.arange(width * height, dtype="float32").reshape(height, width)


def _base_profile(width, height, epsg):
    return dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, _PX, _PX),
        nodata=-9999.0,
    )


def write_striped_gtiff(dst_path, width=1024, height=1024, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(tiled=False)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(PIXELS(width, height), 1)
    return dst_path


def write_tiled_gtiff(dst_path, width=1024, height=1024, blocksize=256, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(tiled=True, blockxsize=blocksize, blockysize=blocksize)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(PIXELS(width, height), 1)
    return dst_path


def write_cog(dst_path, width=1024, height=1024, blocksize=256, epsg=4326):
    prof = _base_profile(width, height, epsg)
    # COG driver accepts: driver, dtype, count, crs, transform, width, height,
    # nodata, blocksize, overview_resampling.
    # It rejects GTiff-only keys: tiled, blockxsize, blockysize.
    prof.update(driver="COG", blocksize=blocksize, overview_resampling="nearest")
    prof.pop("tiled", None)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(PIXELS(width, height), 1)
    return dst_path
