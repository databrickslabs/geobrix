"""The small ERA5-like Zarr cube of the sample bundle: deterministic, small, both layouts."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from databricks.labs.gbx.sample.zarr_era5 import (  # noqa: E402
    BUNDLE_SUBDIR,
    V2_NAME,
    V3_NAME,
    tree_digest,
    write_era5_cube,
)

VARIABLES = ("t2m", "u10", "v10", "msl")
MAX_TOTAL_BYTES = 10 * 1024 * 1024


def _size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


@pytest.fixture(scope="module")
def cube(tmp_path_factory):
    return write_era5_cube(tmp_path_factory.mktemp("cube"))


def test_generator_is_deterministic_and_small(cube, tmp_path):
    again = write_era5_cube(tmp_path)

    assert tree_digest(cube[V3_NAME]) == tree_digest(again[V3_NAME])
    assert tree_digest(cube[V2_NAME]) == tree_digest(again[V2_NAME])
    assert _size(cube[V3_NAME]) + _size(cube[V2_NAME]) < MAX_TOTAL_BYTES


def test_both_layouts_hold_the_same_cube(cube):
    from databricks.labs.gbx.zarr._testing import plain_store_rows

    _, _, v3 = plain_store_rows(cube[V3_NAME])
    _, _, v2 = plain_store_rows(cube[V2_NAME])
    assert all(v3[name].shard is not None for name in VARIABLES)  # sharded v3
    assert all(v2[name].shard is None for name in VARIABLES)  # v2 has no shards
    assert (cube[V2_NAME] / "t2m" / ".zarray").is_file()
    assert (cube[V3_NAME] / "t2m" / "zarr.json").is_file()

    a = xr.open_zarr(cube[V3_NAME], consolidated=False).load()
    b = xr.open_zarr(cube[V2_NAME], consolidated=False, zarr_format=2).load()
    assert set(a.data_vars) == set(VARIABLES)
    assert dict(a.sizes) == {"time": 48, "latitude": 181, "longitude": 360}
    assert float(a.latitude[0]) == 90.0 and float(a.latitude[-1]) == -90.0
    assert str(a.time.values[0])[:13] == "2023-01-01T00" and a.time.size == 48
    assert 200.0 < float(a.t2m.min()) and float(a.t2m.max()) < 330.0  # kelvin
    xr.testing.assert_identical(a, b)


def test_minimal_bundle_script_writes_the_cube_into_the_bundle_layout(tmp_path):
    script = (
        Path(__file__).resolve().parents[4] / "sample-data/generate-minimal-bundle.py"
    )
    if not script.is_file():
        pytest.skip("sample-data/generate-minimal-bundle.py is not in this checkout")
    spec = importlib.util.spec_from_file_location("generate_minimal_bundle", script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert mod.main(["--zarr-only", "--out", str(tmp_path)]) == 0

    base = tmp_path / BUNDLE_SUBDIR
    assert (base / V3_NAME / "zarr.json").is_file()
    assert (base / V2_NAME / ".zgroup").is_file()
