# python/geobrix/test/ds/test_encode_cog.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds import _encode
from databricks.labs.gbx.pyrx.core import cog


def _open(w=512, h=512):
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=rasterio.Affine.identity(),
    )
    mf = MemoryFile()
    with mf.open(**profile) as dst:
        dst.write(np.zeros((1, h, w), dtype="uint8"))
    return mf.open()


def test_encode_tile_gtiff_stamps_gtiff():
    with _open() as ds:
        _, b, md = _encode.encode_tile(
            ds, (0, 0, 512, 512), "/x.tif", "", tile_format="gtiff"
        )
    assert md[cog.GBX_FORMAT] == "gtiff"
    assert cog.sniff_header(b).is_cog is False


def test_encode_tile_cog_emits_and_stamps_cog():
    with _open() as ds:
        _, b, md = _encode.encode_tile(
            ds, (0, 0, 512, 512), "/x.tif", "", tile_format="cog", cog_blocksize=256
        )
    assert md[cog.GBX_FORMAT] == "cog"
    info = cog.sniff_header(b)
    assert info.is_cog is True and info.overview_levels >= 1
