"""The Zarr size ladder: the stores' shape math, the legs' wiring and the gates (no Spark, no big data)."""

import json

import pytest

pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from databricks.labs.gbx.bench import zarr_ladder as zl  # noqa: E402
from databricks.labs.gbx.bench import zarr_ladder_bench as zlb  # noqa: E402

HOUR_BYTES = {"county_wind": 101 * 101 * 16, "global_11km": 1801 * 3600 * 16}


@pytest.mark.parametrize("family", list(zl.FAMILIES))
def test_every_rung_hits_its_size_by_growing_along_time(family):
    hour = HOUR_BYTES[family]
    last = 0
    for rung in zl.RUNGS:
        g = zl.geometry(family, rung)
        target = zl.parse_rung(rung)
        assert g.logical_bytes > last  # the ladder climbs
        last = g.logical_bytes
        if target >= hour:
            # whole hours of the whole extent: within half an hour of the target
            assert not g.regional and (g.ny, g.nx) == zl.FAMILIES[family].grid
            assert abs(g.logical_bytes - target) <= hour / 2, rung
        else:
            # below one hour: one hour of a centred window with the extent's shape, within 0.1% of the target
            assert g.regional and g.hours == 1
            assert abs(g.logical_bytes - target) <= 0.001 * target, rung
            ey, ex = zl.FAMILIES[family].grid
            assert abs(g.ny / g.nx - ey / ex) < 0.01
    assert zl.parse_rung("10GB") == 1e10 and zl.parse_rung("100MB") == 1e8


def test_the_two_families_have_the_documented_grids_and_layouts():
    c, g = zl.FAMILIES["county_wind"], zl.FAMILIES["global_11km"]
    assert c.grid == (101, 101) and g.grid == (1801, 3600)
    assert (c.chunks, c.shards) == ((24, 101, 101), (168, 101, 101))
    assert (g.chunks, g.shards) == ((24, 90, 90), (168, 360, 360))
    # 10 GB of county is 61k hours; of the globe 96 hours; one globe hour is 103.7 MB
    assert zl.geometry("county_wind", "10GB").hours == round(
        1e10 / HOUR_BYTES["county_wind"]
    )
    assert zl.geometry("global_11km", "10GB").hours == 96
    assert zl.geometry("global_11km", "500MB").hours == 5
    with pytest.raises(ValueError, match="not <number>MB or <number>GB"):
        zl.parse_rung("10 apples")
    with pytest.raises(ValueError, match="unknown family"):
        zl.geometry("nope", "10MB")


@pytest.mark.parametrize("family", list(zl.FAMILIES))
def test_a_rung_is_a_v3_sharded_zstd_store_with_the_notebooks_variables(
    tmp_path, family
):
    rec = zl.generate(str(tmp_path), family, "10MB", workers=2)
    g = zl.geometry(family, "10MB")
    path = rec["path"]
    assert rec["shape"] == list(g.shape) and rec["n_chunks"] == g.n_chunks()
    assert abs(rec["logical_bytes"] - 1e7) < 0.01 * 1e7 and rec["disk_bytes"] > 0
    # compresses like weather data (real ERA5: about 1.5 fold), not like the sample cube (about 50)
    assert 1.4 < rec["logical_bytes"] / rec["disk_bytes"] < 3.2
    assert set(zl.VARIABLES) == {"t2m", "u10", "v10", "msl"}
    for v in zl.VARIABLES:
        doc = json.load(open(f"{path}/{v}/zarr.json"))
        assert doc["zarr_format"] == 3 and doc["data_type"] == "float32"
        assert tuple(doc["shape"]) == g.shape
        assert tuple(doc["chunk_grid"]["configuration"]["chunk_shape"]) == g.fam.shards
        assert doc["dimension_names"] == ["time", "latitude", "longitude"]
        (shard,) = [c for c in doc["codecs"] if c["name"] == "sharding_indexed"]
        assert tuple(shard["configuration"]["chunk_shape"]) == g.fam.chunks
        assert [c["name"] for c in shard["configuration"]["codecs"]] == [
            "bytes",
            "zstd",
        ]
    # store_layout reads the same facts back
    lay = zlb.store_layout(path, "t2m")
    assert lay["chunks"] == g.fam.chunks and lay["shards"] == g.fam.shards
    assert lay["n_chunks"] * 4 == g.n_chunks() and lay["n_shards"] * 4 == g.n_shards()
    # a store that exists is kept
    assert zl.generate(str(tmp_path), family, "10MB")["kept"] is True


def test_stored_values_are_the_generators_and_a_rerun_is_identical(tmp_path):
    import numpy as np

    from databricks.labs.gbx.sample.zarr_era5 import era5_block, tree_digest

    a = zl.generate(str(tmp_path / "a"), "county_wind", "10MB", workers=1)
    b = zl.generate(str(tmp_path / "b"), "county_wind", "10MB", workers=2)
    assert tree_digest(a["path"]) == tree_digest(
        b["path"]
    )  # processes do not change a byte
    ds = xr.open_zarr(a["path"], consolidated=False).load()
    want = era5_block(
        zl.VARIABLES,
        0,
        ds.time.size,
        ds.latitude.values,
        ds.longitude.values,
        noise=zl.LADDER_NOISE,
        precision=zl.LADDER_PRECISION,
    )
    for v in zl.VARIABLES:
        assert np.array_equal(ds[v].values, want[v]), v
    # CF time and the notebook's conventions
    assert str(ds.time.values[0])[:13] == "2023-01-01T00" and float(
        ds.latitude[0]
    ) > float(ds.latitude[-1])
    assert 255.0 <= float(ds.longitude[0]) < float(ds.longitude[-1]) <= 256.0


def test_the_skeleton_plans_a_big_rung_without_its_data(tmp_path):
    g = zl.geometry("global_11km", "10GB")
    zl.write_skeleton(str(tmp_path / "s.zarr"), g)
    lay = zlb.store_layout(str(tmp_path / "s.zarr"), "u10")
    assert lay["shape"] == g.shape and lay["n_chunks"] * 4 == g.n_chunks()
    assert (
        sum(f.stat().st_size for f in (tmp_path / "s.zarr").rglob("*") if f.is_file())
        < 5_000_000
    )


# --- the legs and their wiring ------------------------------------------------------------------


def test_cases_are_registered_with_the_notebooks_steps():
    assert list(zlb.LADDER_CASES) == [
        "ZL-R1", "ZL-R2", "ZL-R3", "ZL-R4", "ZL-R5",
        "ZL-W1", "ZL-W2", "ZL-W3", "ZL-W4", "ZL-W5",
        "ZL-W1s", "ZL-W2s", "ZL-W4s", "ZL-W5s",
    ]  # fmt: skip
    names = [zlb.ladder_case(c)[0] for c in zlb.LADDER_CASES]
    assert names == [
        "full_table",
        "box_all_time",
        "sample_100pts",
        "wind_speed_hourly_mean",
        "one_hour_tile",
        "table_to_zarr_managed",
        "table_to_zarr_plain",
        "table_to_zarr_tasks",
        "append_day_managed",
        "append_day_plain",
        "table_to_zarr_managed_staged",
        "table_to_zarr_plain_staged",
        "append_day_managed_staged",
        "append_day_plain_staged",
    ]
    with pytest.raises(KeyError, match="no Zarr ladder case"):
        zlb.ladder_case("ZL-R9")
    # the ladder cases do not disturb the classic ones (spec.ZARR_CASES is R1..W2)
    from databricks.labs.gbx.bench import spec

    assert sorted(c.args["case"] for c in spec.ZARR_CASES.values()) == [
        "R1",
        "R2",
        "R3",
        "W1",
        "W2",
    ]


def test_the_tap_restores_the_planners_functions():
    from databricks.labs.gbx.zarr import _read, _sample

    before = (
        _read._work_frame,
        _read._tiles_frame,
        _sample._through_spark,
        _sample._sample_on_driver,
    )
    with zlb._tapped((7, 1, 1)):
        assert _read._work_frame is not before[0]
    assert (
        _read._work_frame,
        _read._tiles_frame,
        _sample._through_spark,
        _sample._sample_on_driver,
    ) == before
    with zlb._unit_budget(16):
        assert _read._budget_bytes() == 16 << 20
    assert _read._budget_bytes() != 16 << 20


def _m(
    family,
    workload,
    rung,
    gb,
    wall,
    tasks=8,
    chunks=80,
    plan=0.02,
    check="ok",
    route="spark",
):
    return {
        "family": family, "workload": workload, "rung": rung, "store_bytes": gb * 1e9, "bytes_touched": gb * 1e9,
        "wall_s": wall, "plan_s": plan, "plan_share": plan / wall, "tasks": tasks, "chunks_touched_ref": chunks,
        "s_per_gb": wall / gb, "route": route, "check": check,
    }  # fmt: skip


def test_the_gates_flag_a_slow_rung_few_tasks_and_a_failed_check():
    ok = [
        _m("f", "full_table", "10MB", 0.01, 0.4),
        _m("f", "full_table", "100MB", 0.1, 0.55),
        _m("f", "full_table", "1GB", 1.0, 1.5),
    ]
    assert zlb.verdicts(ok, 8)[0].startswith("ok")
    # 1 GB takes 2x the 100 MB rung's time per GB: flagged from the reference rung up
    slow = ok[:2] + [_m("f", "full_table", "1GB", 1.0, 9.0)]
    out = "\n".join(zlb.verdicts(slow, 8))
    assert out.startswith("FLAG") and "1GB" in out and "x the 100MB rung" in out
    # fewer tasks than min(cores, chunks, MB / 8) is flagged; a read the driver answered is not
    few = [_m("f", "full_table", "100MB", 0.1, 0.5, tasks=2, chunks=26)]
    assert "2 tasks" in "\n".join(zlb.verdicts(few, 8))
    drv = [_m("f", "sample", "100MB", 0.1, 0.5, tasks=0, route="driver")]
    assert zlb.verdicts(drv, 8)[0].startswith("ok")
    bad = [_m("f", "full_table", "100MB", 0.1, 0.5, check="count 1 != 2")]
    assert "check failed" in "\n".join(zlb.verdicts(bad, 8))
    # a slope that more than doubles from one segment to the next is superlinear
    sup = [
        _m("f", "w", "10MB", 0.01, 0.3),
        _m("f", "w", "100MB", 0.1, 0.4),
        _m("f", "w", "1GB", 1.0, 4.0),
    ]
    assert "superlinear" in "\n".join(zlb.verdicts(sup, 8))
    # one hour of a tile must not grow with the store
    tile = [
        _m("f", zlb.CONSTANT_WORK, "100MB", 0.1, 0.3),
        _m("f", zlb.CONSTANT_WORK, "1GB", 1.0, 3.0),
    ]
    assert zlb.verdicts(tile, 8)[0].startswith("FLAG")


def test_local_timing_runs_the_zl_set_and_the_cluster_launcher_takes_the_ladder_flags():
    from databricks.labs.gbx.bench import local_timing

    assert local_timing.main(["--set", "ZL", "--needs"]) == 0  # no JAR, no Rust
    from databricks.labs.gbx.bench import cluster as cl

    cell = cl._zarr_cell(
        dict(
            zarr_store="s", zarr_cog_dir="c", zarr_scratch="x", zarr_schema="a.b", run_id="r", zarr_cases="",
            zarr_ladder="100MB,1GB", zarr_ladder_family="county_wind", zarr_ladder_root="/Volumes/c/s/v/zl",
            zarr_ladder_cases="ZL-R1,ZL-R5,ZL-W2", zarr_ladder_budget="16",
            zarr_ladder_scratch="/Volumes/c/s/v/out", zarr_ladder_generate=True,
        )
    )  # fmt: skip
    compile(cell, "zarr_cell", "exec")
    assert (
        "'rungs': ('100MB', '1GB')" in cell
        and "'cases': ('ZL-R1', 'ZL-R5', 'ZL-W2')" in cell
    )
    assert "'scratch': '/Volumes/c/s/v/out'" in cell and "'generate': True" in cell
    assert "'schema': 'a.b'" in cell and "_zlg.generate_to" in cell
    assert "'budget_mib': 16" in cell and "'root': '/Volumes/c/s/v/zl'" in cell
    assert "if _zcfg.cases:" in cell and "run_ladder" in cell
    none = cl._zarr_cell(
        dict(
            zarr_store="s",
            zarr_cog_dir="c",
            zarr_scratch="x",
            zarr_schema="a.b",
            run_id="r",
            zarr_cases="R1",
        )
    )
    assert "_ZLADDER = None" in none


def test_the_packer_cuts_cells_into_blocks_on_the_shard_grid():
    """Cells in any order within a shard key become one block per variable, cropped to the cells that came."""
    import numpy as np
    import pyarrow as pa

    from databricks.labs.gbx.bench import zarr_ladder_write as zw

    names = ("a", "b")
    shard = (4, 3, 3)
    # two shards along time (ts 0 and 1), cells of the second only for hours 5 and 6 (an append into a shard)
    ti, yi, xi, k = [], [], [], []
    for t in (0, 1, 2, 3, 5, 6):
        for y in range(3):
            for x in range(3):
                ti.append(t), yi.append(y), xi.append(x), k.append((t // 4) << 20)
    a = np.arange(len(ti), dtype=np.float32)
    batch = pa.RecordBatch.from_pydict(
        {"__k": k, "__ti": ti, "__yi": yi, "__xi": xi, "a": a, "b": -a}
    )
    out = list(
        zw.make_packer(names, shard, (8, 3, 3))(
            iter([batch.slice(0, 20), batch.slice(20)])
        )
    )
    rows = [r for b in out for r in b.to_pylist()]
    assert [(r["variable"], r["start"], r["stop"]) for r in rows] == [
        ("a", [0, 0, 0], [4, 3, 3]), ("b", [0, 0, 0], [4, 3, 3]),
        ("a", [5, 0, 0], [7, 3, 3]), ("b", [5, 0, 0], [7, 3, 3]),
    ]  # fmt: skip
    first = np.frombuffer(rows[0]["data"], rows[0]["dtype"]).reshape(rows[0]["shape"])
    assert first[2, 1, 1] == a[2 * 9 + 1 * 3 + 1]
    second = np.frombuffer(rows[3]["data"], rows[3]["dtype"]).reshape(rows[3]["shape"])
    assert (
        second.shape == (2, 3, 3) and second[1, 0, 0] == -a[5 * 9 - 9 + 9]
    )  # hour 6, cell (0,0)


def test_write_gates_flag_a_failed_check_and_a_slow_rung():
    from databricks.labs.gbx.bench import zarr_ladder_write as zw

    def m(rung, size, wall, check="ok"):
        return dict(family="f", workload="w", case="ZL-W1", rung=rung, store_bytes=size,
                    wall_s=wall, cold_s=wall, s_per_gb=round(wall / (size / 1e9), 2), check=check)  # fmt: skip

    ok = [m("100MB", 1e8, 10), m("1GB", 1e9, 90)]
    assert zw.write_verdicts(ok)[0].startswith("ok")
    slow = [m("100MB", 1e8, 10), m("1GB", 1e9, 200)]
    assert "x the 100MB" in zw.write_verdicts(slow)[0]
    bad = [m("100MB", 1e8, 10, check="cells 3 != 4")]
    assert zw.write_verdicts(bad)[0].startswith("FLAG")


def test_cells_cases_hand_the_table_to_the_writer_and_the_staged_cases_do_not(
    monkeypatch,
):
    """ZL-W1/W2/W4/W5 pass ``dim_cols`` (the table of cells is the writer's input); the ``s`` cases write blocks."""
    from databricks.labs.gbx.bench import zarr_ladder as zl
    from databricks.labs.gbx.bench import zarr_ladder_write as zw
    from databricks.labs.gbx.zarr import _write

    calls = []
    monkeypatch.setattr(
        "databricks.labs.gbx.zarr.gbx_zarr_write",
        lambda source, target, **kw: calls.append(kw),
    )
    g = zl.geometry("county_wind", "10MB")
    zw._write_call(None, "managed", "src", "t", "/r", g, "create", True)
    zw._write_call(None, "plain", "src", "/p", None, g, "append", True)
    zw._write_call(None, "managed", "src", "t", "/r", g, "create", False)
    assert calls[0]["dim_cols"] == {d: d for d in zw._DIMS} and calls[0]["root"] == "/r"
    assert calls[1]["dim_cols"] and calls[1]["transactional"] is False
    assert "dim_cols" not in calls[2]
    assert [zw.WRITE_CASES[c][0] for c in ("ZL-W1s", "ZL-W5s")] == [
        "table_to_zarr_managed_staged",
        "append_day_plain_staged",
    ]
    assert _write  # the writer module the cells path lives in
