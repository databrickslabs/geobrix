"""A synthetic cloud-optimized GeoTIFF, shared by the cog_ranges / FILE-window tests."""

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin


def cog_bytes(
    width=2000,
    height=1500,
    count=3,
    dtype="uint16",
    interleave="pixel",
    nodata=0,
    sparse=False,
    crs="EPSG:32614",
):
    """A real COG (512 px blocks, overviews 2/4/8) as bytes."""
    rng = np.random.default_rng(7)
    data = rng.integers(1, 5000, size=(count, height, width)).astype(dtype)
    if sparse:
        data[:, :512, :512] = (
            nodata  # one all-nodata block -> SPARSE_OK leaves it unwritten
        )
    prof = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype=dtype,
        crs=crs,
        transform=from_origin(400000, 4000000, 10, 10),
        nodata=nodata,
        tiled=True,
        blockxsize=512,
        blockysize=512,
        compress="deflate",
        interleave=interleave,
    )
    with MemoryFile() as tmp:
        with tmp.open(**prof) as ds:
            ds.write(data)
            ds.build_overviews([2, 4, 8])
        with tmp.open() as src, MemoryFile() as out:
            from rasterio.shutil import copy

            copy(
                src,
                out.name,
                driver="COG",
                BLOCKSIZE=512,
                COMPRESS="DEFLATE",
                SPARSE_OK="TRUE" if sparse else "FALSE",
                INTERLEAVE="BAND" if interleave == "band" else "PIXEL",
            )
            return out.read()
