from databricks.labs.gbx.pyrx import _serde


def test_build_error_tile_shape():
    t = _serde.build_error_tile("RST_ReTile: unreadable raster")
    assert t["raster"] is None
    assert t["cellid"] == -1
    assert t["metadata"]["last_error"] == "RST_ReTile: unreadable raster"


# The `raster`-nullable assertion formerly here targeted the removed legacy
# TILE_SCHEMA constant. The v2 schema's field contract (raster nullable) is now
# asserted by G1 (test_v2_tile_output_invariant.py) against V2_TILE_SCHEMA.
