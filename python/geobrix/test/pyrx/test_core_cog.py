import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import cog


def _plain_gtiff_bytes(w=256, h=256, tiled=False):
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=rasterio.Affine.identity(),
    )
    if tiled:
        profile.update(tiled=True, blockxsize=128, blockysize=128)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, h, w), dtype="uint8"))
        return mf.read()


def _cog_bytes(w=512, h=512):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert

    with MemoryFile(_plain_gtiff_bytes(w, h, tiled=False)) as mf, mf.open() as ds:
        return cog_convert(ds, "DEFLATE", 256, "AVERAGE")


def test_sniff_plain_striped_gtiff_is_not_cog():
    info = cog.sniff_header(_plain_gtiff_bytes(tiled=False))
    assert info.is_cog is False
    assert info.tiled is False
    assert info.overview_levels == 0


def test_sniff_tiled_no_overview_is_not_cog():
    info = cog.sniff_header(_plain_gtiff_bytes(tiled=True))
    assert info.tiled is True
    assert info.is_cog is False  # tiled but no overviews != COG


def test_sniff_cog_is_cog():
    info = cog.sniff_header(_cog_bytes())
    assert info.is_cog is True
    assert info.tiled is True
    assert info.overview_levels >= 1
    assert info.blocksize == 256


def test_sniff_corrupt_bytes_defaults_non_cog():
    info = cog.sniff_header(b"not a tiff at all")
    assert info.is_cog is False


def test_detect_cog_metadata_fast_path_no_decode():
    # gbx_format present -> trusted verbatim; bytes are garbage on purpose to
    # prove no sniff/decode happened.
    md = {cog.GBX_FORMAT: "cog", cog.GBX_OVERVIEW_LEVELS: "3", cog.GBX_BLOCKSIZE: "512"}
    info = cog.detect_cog(md, b"garbage-not-a-tiff")
    assert info.is_cog is True
    assert info.overview_levels == 3
    assert info.blocksize == 512


def test_detect_cog_fallback_sniffs_when_no_metadata():
    info = cog.detect_cog(None, _cog_bytes())
    assert info.is_cog is True


def test_detect_cog_fallback_plain_gtiff():
    info = cog.detect_cog({}, _plain_gtiff_bytes())
    assert info.is_cog is False


def test_stamp_writes_gbx_keys_from_bytes():
    md = cog.stamp_format_metadata(_cog_bytes(), {"driver": "GTiff"})
    assert md["driver"] == "GTiff"  # existing keys preserved
    assert md[cog.GBX_FORMAT] == "cog"
    assert int(md[cog.GBX_OVERVIEW_LEVELS]) >= 1
    assert int(md[cog.GBX_BLOCKSIZE]) == 256


def test_stamp_plain_gtiff_marks_gtiff():
    md = cog.stamp_format_metadata(_plain_gtiff_bytes(), None)
    assert md[cog.GBX_FORMAT] == "gtiff"


def test_detect_and_stamp_agree():
    b = _cog_bytes()
    stamped = cog.stamp_format_metadata(b, None)
    assert cog.detect_cog(stamped, b"garbage").is_cog is True
    assert cog.detect_cog(None, b).is_cog is True
