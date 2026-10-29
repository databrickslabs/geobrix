"""The Zarr benchmark cases: R1-R3 reads and W1-W2 writes (light tier only).

They sit beside the function REGISTRY, not in it: the Zarr API has no ``gbx_*`` SQL
binding, so the function-coverage check (``REGISTRY == registered_rst()``) is unchanged.
"""

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
