"""The Zarr benchmark cases: R1-R3 reads and W1-W2 writes (light tier only).

They sit beside the function REGISTRY, not in it: the Zarr API has no ``gbx_*`` SQL
binding, so the function-coverage check (``REGISTRY == registered_rst()``) is unchanged.
"""

import types

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
