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


def _bundle_script():
    script = (
        Path(__file__).resolve().parents[4] / "sample-data/generate-minimal-bundle.py"
    )
    if not script.is_file():
        pytest.skip("sample-data/generate-minimal-bundle.py is not in this checkout")
    spec = importlib.util.spec_from_file_location("generate_minimal_bundle", script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize(
    "clipped, cube, exit_code",
    [
        (True, True, 0),
        (
            True,
            False,
            0,
        ),  # no xarray or zarr: the cube is skipped, the bundle is complete
        (False, True, 1),  # a clipped dataset failed
        (False, False, 1),
    ],
    ids=["all-written", "cube-skipped", "clip-failed", "both"],
)
def test_a_normal_bundle_run_fails_only_when_a_clipped_dataset_does(
    tmp_path, monkeypatch, clipped, cube, exit_code
):
    mod = _bundle_script()
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(mod, "run", lambda *args: clipped)
    monkeypatch.setattr(mod, "run_zarr_era5", lambda out: cube)

    assert (
        mod.main(["--source", str(source), "--out", str(tmp_path / "out")]) == exit_code
    )


# --- the default call is the sample cube, file for file; other sizes are streamed -----------------

# Recorded from the cube before write_era5_cube took size and layout arguments: the names of its 102 files,
# the digest of its metadata documents, and its size in bytes (compression may differ a little by version).
_SAMPLE_d21ecbcb1974fd82ab84c42ddddb786e076295cefef71c3c469e6ff9705390e3 = (
    "d21ecbcb1974fd82ab84c42ddddb786e076295cefef71c3c469e6ff9705390e3"
)
_SAMPLE_META_SHA = "e4e4ddaa14a98a7c1a781cee16ed96b578e20f4579af7d270e4d1c83665317f3"
_SAMPLE_BYTES = 6_829_350


def test_default_call_still_writes_the_same_files(cube):
    import hashlib

    files, meta, total = [], hashlib.sha256(), 0
    for name, root in sorted(cube.items()):
        for f in sorted(p for p in root.rglob("*") if p.is_file()):
            rel = f"{name}/{f.relative_to(root).as_posix()}"
            files.append(rel)
            total += f.stat().st_size
            if f.name in ("zarr.json", ".zarray", ".zattrs", ".zgroup"):
                meta.update(rel.encode())
                meta.update(f.read_bytes())
    assert len(files) == 102
    assert (
        hashlib.sha256("\n".join(files).encode()).hexdigest()
        == _SAMPLE_d21ecbcb1974fd82ab84c42ddddb786e076295cefef71c3c469e6ff9705390e3
    )
    assert meta.hexdigest() == _SAMPLE_META_SHA
    assert abs(total - _SAMPLE_BYTES) < 0.03 * _SAMPLE_BYTES


def test_other_sizes_are_one_v3_store_with_the_notebooks_variables(tmp_path):
    out = write_era5_cube(
        tmp_path,
        hours=30,
        step=0.5,
        box=(50.0, 40.0, 10.0, 20.0),
        chunks=(24, 11, 11),
        shards=(24, 22, 22),
        v3_name="small.zarr",
        workers=1,
    )
    assert list(out) == ["small.zarr"]
    ds = xr.open_zarr(out["small.zarr"], consolidated=False).load()
    assert set(ds.data_vars) == set(VARIABLES)
    assert dict(ds.sizes) == {"time": 30, "latitude": 21, "longitude": 21}
    assert float(ds.latitude[0]) == 50.0 and float(ds.latitude[-1]) == 40.0
    assert float(ds.longitude[0]) == 10.0 and float(ds.longitude[-1]) == 20.0
    assert str(ds.time.values[0])[:13] == "2023-01-01T00" and ds.time.size == 30
    assert ds.t2m.attrs["units"] == "K" and ds.attrs["Conventions"] == "CF-1.8"
    assert ds.t2m.encoding["chunks"] == (24, 11, 11) and ds.t2m.encoding["shards"] == (
        24,
        22,
        22,
    )
    with pytest.raises(ValueError, match="Zarr v3 only"):
        write_era5_cube(tmp_path, hours=2, v2=True)


def test_a_block_holds_the_same_numbers_whatever_it_is_cut_from(tmp_path):
    """The stored cube equals ``era5_block`` over the whole, and any sub-block of it, cell for cell."""
    import numpy as np

    from databricks.labs.gbx.sample.zarr_era5 import era5_block

    out = write_era5_cube(
        tmp_path,
        hours=26,
        step=1.0,
        box=(60.0, 30.0, 0.0, 40.0),
        chunks=(24, 16, 21),
        shards=(24, 32, 42),
        v3_name="b.zarr",
        workers=2,  # shards written by separate processes
    )
    ds = xr.open_zarr(out["b.zarr"], consolidated=False).load()
    lat, lon = ds.latitude.values, ds.longitude.values
    whole = era5_block(VARIABLES, 0, 26, lat, lon)
    part = era5_block(("u10",), 5, 9, lat[3:11], lon[7:30])["u10"]
    for name in VARIABLES:
        assert np.array_equal(ds[name].values, whole[name]), name
    assert np.array_equal(part, whole["u10"][5:9, 3:11, 7:30])
    # the noise is real (not rounded away) and the fields are bounded
    assert 200.0 < float(ds.t2m.min()) and float(ds.t2m.max()) < 330.0
