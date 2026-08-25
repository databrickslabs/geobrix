# python/geobrix/test/pyrx/test_metadata_carry.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import cog
from databricks.labs.gbx.pyrx.core.analysis import cog_convert


def _cog_tile():
    profile = dict(
        driver="GTiff",
        width=512,
        height=512,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=rasterio.Affine.identity(),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, 512, 512), dtype="uint8"))
        with mf.open() as ds:
            b = cog_convert(ds, "DEFLATE", 256, "AVERAGE")
    md = cog.stamp_format_metadata(b, {"driver": "GTiff"})
    return b, md


def test_gbx_format_survives_op_chain():
    b, md = _cog_tile()
    assert md[cog.GBX_FORMAT] == "cog"
    # Simulate a non-COG-aware op that rebuilds bytes but preserves format:
    # it MUST re-stamp from its output bytes (R2). Here the op is identity.
    out_bytes = b
    md2 = cog.stamp_format_metadata(out_bytes, md)
    assert md2[cog.GBX_FORMAT] == "cog"
    # A final COG-aware op detects correctly from carried metadata (fast path).
    assert cog.detect_cog(md2, b"garbage").is_cog is True
