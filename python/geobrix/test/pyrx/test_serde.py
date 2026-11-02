from databricks.labs.gbx.pyrx import _serde

from .conftest import make_geotiff_bytes

# NOTE: the tile-output schema contract is asserted by the v2 tile-output invariant
# (test_v2_tile_output_invariant.py::test_v2_schema_field_contract) and the
# light≡heavy parity guard (test_rasterx/test_tile_schema_parity.py). The
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


def test_build_tile_grid_system_key_present():
    """build_tile with grid_system=<str> adds gridSystem to metadata."""
    tile = _serde.build_tile(
        make_geotiff_bytes(), driver="GTiff", cellid=7, grid_system="h3"
    )
    assert tile["metadata"]["gridSystem"] == "h3"


def test_build_tile_no_grid_system_key_absent():
    """build_tile without grid_system (default None) must NOT add gridSystem key."""
    tile = _serde.build_tile(make_geotiff_bytes(), driver="GTiff", cellid=7)
    assert "gridSystem" not in tile["metadata"]


def test_open_tile_refuses_a_tile_decoded_over_the_carried_read_cap():
    """The light tile-bytes open checks the decoded size from the header against the read cap the registration
    carried, with the reason every level gives."""
    import numpy as np
    import pytest

    from databricks.labs.gbx import _gdal_policy

    raw = make_geotiff_bytes()
    with _serde.open_tile(raw) as ds:
        decoded = ds.width * ds.height * ds.count * np.dtype(ds.dtypes[0]).itemsize
    with _gdal_policy.carried(None, decoded):
        with _serde.open_tile(raw):
            pass
    with _gdal_policy.carried(None, decoded - 1):
        with pytest.raises(
            ValueError, match="over this compute's read cap.*Fix: retile"
        ):
            with _serde.open_tile(raw):
                pass
