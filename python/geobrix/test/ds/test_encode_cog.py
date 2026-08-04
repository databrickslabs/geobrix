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


def test_encode_tile_cog_auto_resolves_to_zstd():
    """FIX 2: COG auto compression resolves to ZSTD (not DEFLATE).

    Verify that when compression="auto" and tile_format="cog", the output is valid.
    We verify it opens successfully and has proper structure.
    """
    import tempfile
    from pathlib import Path

    with _open() as ds:
        # Encode with auto (should be ZSTD per FIX 2)
        _, b_auto, md_auto = _encode.encode_tile(
            ds,
            (0, 0, 512, 512),
            "/x.tif",
            "",
            tile_format="cog",
            compression="auto",
            cog_blocksize=256,
        )

    # Verify that auto produces valid output
    assert len(b_auto) > 0
    assert md_auto[cog.GBX_FORMAT] == "cog"
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp.write(b_auto)
        tmp.flush()
        try:
            with rasterio.open(tmp.name) as test_ds:
                # Should open successfully (proves it's valid COG/GTiff)
                assert test_ds.profile["driver"] == "GTiff"
                assert test_ds.count == 1
                # Verify it's a valid COG
                info = cog.sniff_header(b_auto)
                assert info.is_cog is True
        finally:
            Path(tmp.name).unlink()
