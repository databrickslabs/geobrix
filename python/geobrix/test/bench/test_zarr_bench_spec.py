"""The Zarr benchmark cases: R1-R3 reads and W1-W2 writes (light tier only).

They sit beside the function REGISTRY, not in it: the Zarr API has no ``gbx_*`` SQL
binding, so the function-coverage check (``REGISTRY == registered_rst()``) is unchanged.
"""

import types

import pytest

from databricks.labs.gbx.bench import spec as s


def test_zarr_cases_declared():
    cases = {c.args["case"]: c for c in s.ZARR_CASES.values()}
    assert sorted(cases) == ["R1", "R2", "R3", "W1", "W2"]
    # a function's own case is named after it; a second case of it is a variant
    want = {
        "R1": ("gbx_zarr_read", None),
        "R2": ("gbx_zarr_sample", None),
        "R3": ("gbx_zarr_read_box", "gbx_zarr_read"),
        "W1": ("gbx_zarr_write", None),
        "W2": ("gbx_zarr_write_append", "gbx_zarr_write"),
    }
    for case, (name, variant_of) in want.items():
        c = cases[case]
        assert (c.name, c.variant_of) == (name, variant_of), case
        assert c.category == "zarr" and c.modes == ("spark-path",)
        assert c.name not in s.REGISTRY
        assert c.args["baseline"], case  # every case is timed against something
    assert s.benched_functions(s.ZARR_CASES.values()) == {
        "gbx_zarr_read",
        "gbx_zarr_sample",
        "gbx_zarr_write",
    }
    # W1's gate: the prototype's MEASURED throughput, and how close a run must come
    gate = cases["W1"].args["gate_mb_s"]
    assert gate == {"dbr19_fuse": 109.0, "dbr19_direct": 143.0, "serverless": 137.0}
    assert cases["W1"].args["gate_tolerance"] == 0.25


def test_the_geotiff_time_series_baseline_is_bounded():
    """One rst_sample call per point and hour cost 1-2.5 s on Databricks, so the baseline reads
    four hourly files and as many points as three runs fit in the budget (else 10)."""
    from databricks.labs.gbx.bench import zarr_bench as zb

    assert zb._R2_COG_HOURS == 4
    assert zb._r2_points(1.0, 3, 600.0, 1000) == 100  # 300 x 3 s > 600 s
    assert zb._r2_points(0.001, 3, 600.0, 1000) == 1000
    assert zb._r2_points(100.0, 3, 600.0, 1000) == 10


def test_r2_measures_the_baseline_it_reports(monkeypatch):
    """``run_r2`` reads the first four hourly files, sizes the points from a cold probe of
    10, and measures, and reports, exactly those: a 60 s probe (6 s a point) leaves 30
    points for three runs in 600 s."""
    import pyspark.sql.functions as F

    import databricks.labs.gbx.pyrx.functions as prx
    import databricks.labs.gbx.zarr as gz
    from databricks.labs.gbx.bench import zarr_bench as zb

    files = [f"/cog/t2m_{h:04d}.tif" for h in range(24)]
    frames, limits = [], []

    class Points:
        def limit(self, n):
            limits.append(n)
            return self

    class Tiles:
        def crossJoin(self, other):
            return self

        def select(self, *cols):
            return self

    def cog_frame(spark, chosen, **options):
        frames.append(list(chosen))
        return Tiles()

    def walls(fn, reps):
        fn()
        return [60.0] if reps == 1 else [9.0] * reps

    monkeypatch.setattr(zb, "_coords", lambda store: (list(range(24)), None, None))
    monkeypatch.setattr(zb, "_points", lambda spark, n, seed: Points())
    monkeypatch.setattr(zb, "_register_light", lambda spark: None)
    monkeypatch.setattr(zb, "_cog_manifest", lambda d: {"files": files})
    monkeypatch.setattr(zb, "_cog_frame", cog_frame)
    monkeypatch.setattr(zb, "_noop", lambda df: None)
    monkeypatch.setattr(zb, "_walls", walls)
    monkeypatch.setattr(gz, "gbx_zarr_sample", lambda *a, **k: None)
    sampled = types.SimpleNamespace(alias=lambda name: name)
    monkeypatch.setattr(prx, "rst_sample", lambda *a: sampled)
    monkeypatch.setattr(F, "col", lambda name: name)
    monkeypatch.setattr(F, "broadcast", lambda df: df)
    cfg = zb.ZarrBenchConfig(
        "/s", "/cog", "/tmp", "c.s", "r", reps=3, r2_budget_s=600.0
    )
    rows = zb.run_r2(None, cfg)
    base = [r for r in rows if r.fn == "raster_gbx+rst_sample[R2]"][0]
    assert base.status == "ok", base.note
    assert frames == [files[:4], files[:4]]  # the probe and the measured runs
    assert limits == [10, 30]
    assert (
        base.rows == 30 * 4 and "4 hourly GeoTIFFs (one task each) x 30 of" in base.note
    )


def test_the_w1_check_compares_the_steps_in_time_order(monkeypatch):
    """A table's rows come back in any order: the check sorts them by time first."""
    import numpy as np

    import databricks.labs.gbx.zarr as gz
    from databricks.labs.gbx.bench import zarr_bench as zb

    ny, nx, steps = 12, 20, [47, 23, 71]
    y, x = ny // 3, nx // 5
    rows = [
        {
            "time": np.datetime64("2023-01-01T00") + np.timedelta64(s, "h"),
            "t2m": float(zb.field(s, s + 1, ny, nx)[0, y, x]),
        }
        for s in steps
    ]
    picked = []

    def read(spark, table, *, variables, isel, output):
        picked.append(isel["time"])
        return types.SimpleNamespace(collect=lambda: list(reversed(rows)))

    monkeypatch.setattr(gz, "gbx_zarr_read", read)
    assert zb._w1_check(None, "t", ny, nx, t=steps).endswith(": OK")
    assert picked == [sorted(steps)]


def test_the_w1_gate_has_two_parts():
    """The files phase against the prototype's floor, and the fixed cost of a create (its
    claim, CREATE TABLE and commit) against 10 s; the position pass is in neither."""
    from databricks.labs.gbx.bench import zarr_bench as zb

    assert zb.w1_verdict(81.8, "dbr19_fuse") == ("PASS", 81.8)
    assert zb.w1_verdict(81.7, "dbr19_fuse")[0] == "MISS"
    phases = {
        "setup: ownership claim": 1.5,
        "claim: found ledger": 1.4,  # a part of the claim: not counted twice
        "setup: create table": 1.75,
        "position pass": 9.0,
        "files": 8.0,
        "commit": 4.0,
        "commit: merge": 2.5,
    }
    assert zb.w1_fixed(phases) == 7.25
    assert zb.w1_fixed_verdict(10.0) == ("PASS", 10.0)
    assert zb.w1_fixed_verdict(10.01)[0] == "MISS"


def test_the_write_cube_makes_one_block_per_task():
    """W1 says ten tasks: each block of the cube (and of the xarray baseline) is made by a
    task of its own, from a range split one id per partition; no hashed repartition."""
    from databricks.labs.gbx.bench import zarr_bench as zb

    calls = []

    class Frame:
        def mapInArrow(self, fn, schema):
            return self

        def unionByName(self, other):
            return self

    class Spark:
        def range(self, *args):
            calls.append(args)
            return Frame()

        def createDataFrame(self, rows, schema):
            return Frame()

    zb._cube_blocks(Spark(), 240, 721, 1440, 24)
    assert calls == [(0, 10, 1, 10)]


def test_run_w1_judges_each_leg_by_its_warm_runs_in_alternating_order(monkeypatch):
    """1 cold + 5 warm runs a leg: the legs alternate (FUSE first in odd repetitions, direct
    first in even ones, counting from 1), the cold run is each leg's first, and the row's
    throughput and both verdicts come from the median of the WARM files phase and fixed
    cost, with the first-three verdict beside it."""
    import databricks.labs.gbx.zarr as gz
    from databricks.labs.gbx.bench import zarr_bench as zb

    # files phase / fixed parts per create, in each leg's own run order (run 1 cold)
    plan = {
        "fuse": [
            (30.0, 9.0),
            (10.0, 3.0),
            (10.5, 3.2),
            (9.5, 3.1),
            (11.0, 3.3),
            (12.0, 3.4),
        ],
        "direct": [
            (25.0, 8.0),
            (8.0, 2.0),
            (8.2, 2.1),
            (8.4, 2.2),
            (8.6, 2.3),
            (8.8, 2.4),
        ],
    }
    seen, current = [], {}

    class Phases:
        def __init__(self):
            self.t = {}
            current["ph"] = self

        def restore(self):
            pass

    def write(data, table, *, access, **kw):
        files, part = plan[access][sum(1 for a in seen if a == access)]
        seen.append(access)
        current["ph"].t.update(
            {
                "files": files,
                "setup: ownership claim": part,
                "setup: create table": part,
                "commit": part,
            }
        )

    monkeypatch.setattr(zb, "_w1_phases", Phases)
    monkeypatch.setattr(zb, "_cube_blocks", lambda *a, **k: None)
    monkeypatch.setattr(zb, "_w1_check", lambda *a, **k: "verify OK")
    monkeypatch.setattr(zb, "_drop", lambda *a, **k: None)
    monkeypatch.setattr(zb, "_plain_w1", lambda *a, **k: 5.0)
    monkeypatch.setattr(gz, "gbx_zarr_write", write)
    cfg = zb.ZarrBenchConfig(
        "/s", "/c", "/tmp", "c.s", "r", reps=6, w1_shape=(240, 721, 1440)
    )
    rows = {r.fn: r for r in zb.run_w1(None, cfg)}
    assert seen == ["fuse", "direct", "direct", "fuse"] * 3
    fuse, direct = rows["gbx_zarr_write[W1,fuse]"], rows["gbx_zarr_write[W1,direct]"]
    mb = 240 * 721 * 1440 * 4 / 1e6
    assert fuse.throughput_rows_s == pytest.approx(
        mb / 10.5
    )  # warm files [10, 10.5, 9.5, 11, 12]
    assert direct.throughput_rows_s == pytest.approx(mb / 8.4)
    assert fuse.measured_iters == 5 and "cold_s=" in fuse.note
    assert "part 1 >= 81.8 MB/s: PASS" in fuse.note  # 94.9 MB/s warm
    assert "fixed cost median 9.60" in fuse.note and "part 2 <= 10 s: PASS" in fuse.note
    assert "create #1 on this compute" in fuse.note and "create #2" in direct.note
    assert "FIRST THREE (cold included): files 94.9 MB/s PASS" in fuse.note


def test_every_bench_row_names_the_wheel_it_ran():
    from databricks.labs.gbx.bench import zarr_bench as zb

    cfg = zb.ZarrBenchConfig("/s", "/c", "/tmp", "c.s", "r")
    row = zb._row(cfg, "f", [1.0], 1, "x")
    assert row.note.startswith(f"wheel_record={zb.wheel_fingerprint()}; ")
    assert zb._error_row(cfg, "f", ValueError("e")).note.startswith("wheel_record=")
    wheel = "databricks/labs/gbx/a.py,sha256=x,1\ngeobrix.dist-info/RECORD,,\n"
    installed = wheel + "databricks/labs/gbx/__pycache__/a.cpython-312.pyc,,\n"
    assert zb.record_fingerprint(wheel) == zb.record_fingerprint(installed)


def test_legs_and_probe_variants_take_turns():
    from databricks.labs.gbx.bench import zarr_bench as zb

    assert [zb._leg_order(("fuse", "direct"), r) for r in range(3)] == [
        ("fuse", "direct"),
        ("direct", "fuse"),
        ("fuse", "direct"),
    ]
    assert [zb._rotated(("a", "b", "c"), i)[0] for i in range(4)] == [
        "a",
        "b",
        "c",
        "a",
    ]


def test_the_plain_put_baseline_puts_one_object_a_task(spark, tmp_path):
    """W1D's baseline: one task per block, each putting ``size`` bytes with plain obstore
    through the direct-access store (a file:// store here), timed per step."""
    from databricks.labs.gbx.bench import zarr_bench as zb
    from databricks.labs.gbx.zarr._access import AccessConfig

    cloud = tmp_path / "cloud"
    cloud.mkdir()
    url = f"file://{cloud}"
    cfg = zb.ZarrBenchConfig("/s", "/c", str(tmp_path / "mount"), "c.s", "r")
    access = AccessConfig(
        "direct", url, {"k": "x"}, "aws", str(tmp_path / "mount"), url
    )
    wall, recs = zb._plain_puts(spark, cfg, access, 3, 6, 8, 100, "t")
    assert wall > 0 and len(recs) == 3
    assert sorted(r["partition"] for r in recs) == [0, 1, 2]
    assert all(r["bytes"] == 100 and r["put_s"] >= 0 and r["host"] for r in recs)
    puts = sorted(p.name for p in cloud.rglob("part-*.bin"))
    assert puts == ["part-0.bin", "part-1.bin", "part-2.bin"]


def test_w1d_alternates_the_traced_create_and_the_plain_puts(monkeypatch):
    import databricks.labs.gbx.zarr as gz
    import databricks.labs.gbx.zarr._access as acc
    from databricks.labs.gbx.bench import zarr_bench as zb
    from databricks.labs.gbx.zarr import _register

    seen, current = [], {}

    class Phases:
        def __init__(self):
            self.t, self.traces = {}, []
            current["ph"] = self

        def restore(self):
            pass

    def write(data, table, *, access, **kw):
        assert access == "direct"
        seen.append("gbx")
        current["ph"].t.update({"files": 8.0, "commit": 4.0})
        current["ph"].traces = [
            {
                "host": "h",
                "start": 0.0,
                "end": 3.0,
                "packs": [{"bytes": 9, "s": 1.5, "how": "put"}],
            }
        ]

    def puts(spark, cfg, access, n, ny, nx, size, tag):
        seen.append("puts")
        return 5.0, [{"put_s": 1.0, "start": 0.0, "end": 2.0} for _ in range(n)]

    monkeypatch.setattr(zb, "_w1_phases", Phases)
    monkeypatch.setattr(zb, "_cube_blocks", lambda *a, **k: None)
    monkeypatch.setattr(zb, "_w1_check", lambda *a, **k: "verify OK")
    monkeypatch.setattr(zb, "_drop", lambda *a, **k: None)
    monkeypatch.setattr(zb, "_plain_puts", puts)
    monkeypatch.setattr(gz, "gbx_zarr_write", write)
    monkeypatch.setattr(acc, "resolve_access", lambda *a, **k: "cfg")
    monkeypatch.setattr(_register, "qualified_table_name", lambda spark, t: t)
    monkeypatch.setattr(_register, "claim_root", lambda *a, **k: False)
    monkeypatch.setattr(zb.os, "makedirs", lambda *a, **k: None)
    cfg = zb.ZarrBenchConfig("/s", "/c", "/tmp", "c.s", "r", reps=4)
    rows = {r.fn: r for r in zb.run_w1d(None, cfg)}
    assert seen == ["gbx", "puts", "puts", "gbx", "gbx", "puts", "puts", "gbx"]
    gbx, base = rows["gbx_zarr_write[W1D,direct]"], rows["obstore.put[W1D,baseline]"]
    assert "warm pack puts median 1.50" in gbx.note and "owner re-claim" in gbx.note
    assert base.measured_iters == 3 and "cold_s=5.0" in base.note
    assert "warm puts median 1.00" in base.note


def _w1_stubs(monkeypatch, write):
    import databricks.labs.gbx.zarr as gz
    from databricks.labs.gbx.bench import zarr_bench as zb

    current = {}

    class Phases:
        def __init__(self):
            self.t, self.traces = {}, []
            current["ph"] = self

        def restore(self):
            pass

    monkeypatch.setattr(zb, "_w1_phases", Phases)
    monkeypatch.setattr(zb, "_cube_blocks", lambda *a, **k: None)
    monkeypatch.setattr(zb, "_w1_check", lambda *a, **k: "verify OK")
    monkeypatch.setattr(zb, "_drop", lambda *a, **k: None)
    monkeypatch.setattr(zb, "_plain_w1", lambda *a, **k: 5.0)
    monkeypatch.setattr(gz, "gbx_zarr_write", lambda *a, **k: write(current["ph"], **k))
    return zb


def test_a_w1_leg_that_fails_is_an_error_row_and_the_other_leg_runs_on(monkeypatch):
    seen = []

    def write(ph, *, access, **kw):
        seen.append(access)
        if access == "direct" and seen.count("direct") == 2:
            raise RuntimeError("direct put refused")
        ph.t.update({"files": 8.0, "commit": 3.0})

    zb = _w1_stubs(monkeypatch, write)
    cfg = zb.ZarrBenchConfig("/s", "/c", "/tmp", "c.s", "r", reps=4)
    rows = {r.fn: r for r in zb.run_w1(None, cfg)}
    assert seen.count("direct") == 2 and seen.count("fuse") == 4
    assert rows["gbx_zarr_write[W1,direct]"].status == "error"
    assert "direct put refused" in rows["gbx_zarr_write[W1,direct]"].note
    assert rows["gbx_zarr_write[W1,fuse]"].status == "ok"


def test_a_w1_leg_with_no_run_is_an_error_row(monkeypatch):
    zb = _w1_stubs(monkeypatch, lambda ph, **kw: None)
    cfg = zb.ZarrBenchConfig("/s", "/c", "/tmp", "c.s", "r", reps=0)
    rows = [r for r in zb.run_w1(None, cfg) if r.fn.startswith("gbx_zarr_write")]
    assert rows and all(r.status == "error" and "no run" in r.note for r in rows)


def test_the_update_window_probe_runs_only_with_a_write_case(monkeypatch):
    from databricks.labs.gbx.bench import zarr_bench as zb

    ran = []
    monkeypatch.setattr(zb, "shard_index_probe", lambda d: ran.append("shard") or [])
    monkeypatch.setattr(
        zb, "sample_density_probe", lambda *a: ran.append("density") or []
    )
    monkeypatch.setattr(
        zb, "update_window_probe", lambda *a: ran.append("window") or []
    )
    monkeypatch.setattr(zb, "run_r1", lambda spark, cfg: [])
    monkeypatch.setattr(zb, "run_w1", lambda spark, cfg: [])
    monkeypatch.setattr(zb.os, "makedirs", lambda *a, **k: None)
    zb.run_zarr_cases(
        None, zb.ZarrBenchConfig("/s", "/c", "/tmp", "c.s", "r", cases=("R1",))
    )
    assert ran == ["shard", "density"]
    ran.clear()
    zb.run_zarr_cases(
        None, zb.ZarrBenchConfig("/s", "/c", "/tmp", "c.s", "r", cases=("W1",))
    )
    assert ran == ["shard", "density", "window"]
