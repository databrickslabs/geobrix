from databricks.labs.gbx.pyrx import _serde

from .conftest import make_geotiff_bytes

# NOTE: the tile-output schema contract is asserted by G1
# (test_v2_tile_output_invariant.py::test_v2_schema_field_contract) and the
# light≡heavy parity guard (G2, test_rasterx/test_tile_schema_parity.py). The
# former per-function 3-field assertion here was removed when build_tile moved
# to the 8-field v2 struct.


def test_build_tile_populates_metadata():
    tile = _serde.build_tile(make_geotiff_bytes(), driver="GTiff", cellid=7)
    assert tile["cellid"] == 7
    assert isinstance(tile["raster"], (bytes, bytearray))
    assert tile["metadata"]["driver"] == "GTiff"


def test_open_tile_yields_readable_dataset():
    raster = make_geotiff_bytes(width=4, height=3)
    with _serde.open_tile(raster) as ds:
        assert ds.width == 4
        assert ds.height == 3
