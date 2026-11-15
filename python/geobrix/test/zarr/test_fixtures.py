"""The synthetic ERA5-like stores: shape, CF coordinates, encodings per layout."""

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

CHUNKS = (12, 37, 72)


def _open(path):
    return zarr.open_group(str(path), mode="r")


def test_coordinates_are_cf_encoded(era5_v3_sharded):
    g = _open(era5_v3_sharded)
    assert g["time"].shape == (48,) and g["time"].dtype == np.dtype("int64")
    assert g["time"].attrs["units"] == "hours since 1900-01-01"
    assert g["time"].attrs["calendar"] == "standard"
    assert g["latitude"].attrs["units"] == "degrees_north"
    assert g["longitude"].attrs["units"] == "degrees_east"


def test_grid_values(era5_v3_sharded):
    g = _open(era5_v3_sharded)
    lat, lon = g["latitude"][:], g["longitude"][:]
    assert lat.shape == (73,) and lat[0] == 90.0 and lat[-1] == -90.0
    assert np.all(np.diff(lat) < 0)
    assert lon.shape == (144,) and lon[0] == 0.0 and lon[-1] == 357.5


def test_time_decodes_to_hourly_from_2023(era5_v3_sharded):
    import xarray as xr

    ds = xr.open_zarr(str(era5_v3_sharded), consolidated=False, chunks=None)
    t = ds["time"].values
    assert str(t[0])[:13] == "2023-01-01T00" and str(t[-1])[:13] == "2023-01-02T23"
    assert set(np.diff(t).astype("timedelta64[h]").astype(int)) == {1}


def test_variables_are_float32_with_units(era5_v3_sharded):
    g = _open(era5_v3_sharded)
    assert g["t2m"].attrs["units"] == "K" and g["u10"].attrs["units"] == "m s-1"
    for name in ("t2m", "u10"):
        assert g[name].dtype == np.dtype("float32")
        assert np.isfinite(g[name][:]).all()


def test_fixture_is_deterministic(tmp_path):
    from databricks.labs.gbx.zarr._testing import make_era5_like

    a = _open(make_era5_like(tmp_path / "a.zarr", zarr_format=3, sharded=False))
    b = _open(make_era5_like(tmp_path / "b.zarr", zarr_format=3, sharded=False))
    assert np.array_equal(a["t2m"][:], b["t2m"][:])
    assert np.array_equal(a["u10"][:], b["u10"][:])


@pytest.mark.parametrize(
    "fixture,sharded",
    [("era5_v3_sharded", True), ("era5_v3_unsharded", False)],
)
def test_v3_layouts(request, fixture, sharded):
    arr = _open(request.getfixturevalue(fixture))["t2m"]
    assert arr.metadata.zarr_format == 3 and arr.chunks == CHUNKS
    assert (arr.shards == (24, 74, 144)) if sharded else (arr.shards is None)
    assert "zstd" in repr(arr.compressors).lower()


def test_v2_layout(era5_v2):
    arr = _open(era5_v2)["t2m"]
    assert arr.metadata.zarr_format == 2 and arr.chunks == CHUNKS
    assert "lz4" in repr(arr.compressors).lower()
    assert arr.metadata.dimension_separator == "."
    keys = sorted(p.name for p in (era5_v2 / "t2m").iterdir())
    assert "0.0.0" in keys and ".zarray" in keys


def test_three_layouts_hold_the_same_data(era5_v3_sharded, era5_v3_unsharded, era5_v2):
    ref = _open(era5_v3_sharded)["t2m"][:]
    assert np.array_equal(ref, _open(era5_v3_unsharded)["t2m"][:])
    assert np.array_equal(ref, _open(era5_v2)["t2m"][:])


def test_sharded_defaults_to_sharded_for_v3_and_unsharded_for_v2(tmp_path):
    from databricks.labs.gbx.zarr._testing import make_era5_like

    v3 = _open(make_era5_like(tmp_path / "v3.zarr"))["t2m"]
    assert v3.metadata.zarr_format == 3 and v3.shards == (24, 74, 144)
    v2 = _open(make_era5_like(tmp_path / "v2.zarr", zarr_format=2))["t2m"]
    assert v2.metadata.zarr_format == 2


def test_v2_cannot_be_sharded(tmp_path):
    from databricks.labs.gbx.zarr._testing import make_era5_like

    with pytest.raises(ValueError, match="no sharding"):
        make_era5_like(tmp_path / "x.zarr", zarr_format=2, sharded=True)
    with pytest.raises(ValueError, match="zarr_format"):
        make_era5_like(tmp_path / "y.zarr", zarr_format=4)
