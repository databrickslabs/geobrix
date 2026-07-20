"""Smoke tests for the --benchmark-pmtiles / --pmtiles-only wiring in build_bench_notebook."""

from databricks.labs.gbx.bench import cluster as cl


def _cfg(**kw):
    base = dict(
        wheel="/Volumes/c/s/v/geobrix-0.4.2-py3-none-any.whl",
        corpus="/Volumes/c/s/v/bench-corpus",
        out_dir="/Volumes/c/s/v/bench-out/run1",
        table="main.default.bench_results",
        run_id="run1",
        functions="rst_width,rst_slope",
        modes="both",
        row_counts="1000",
        warmup=1,
        measured=3,
        heavyweight=True,
        lightweight=True,
    )
    base.update(kw)
    return base


def _src(nb):
    return "\n".join("".join(c.get("source", [])) for c in nb["cells"])


def _section_cells(nb):
    return [
        "".join(c["source"])
        for c in nb["cells"]
        if "".join(c["source"]).lstrip().startswith("# (")
    ]


def test_benchmark_pmtiles_cell_contains_run_pmtiles_write_and_parity():
    nb = cl.build_bench_notebook(_cfg(benchmark_pmtiles=True))
    src = _src(nb)
    assert "run_pmtiles_write" in src
    assert "PMTILES PARITY" in src
    # Standard fn-bench sections still present (benchmark_pmtiles does NOT suppress them).
    secs = _section_cells(nb)
    assert len(secs) == 4  # both tiers, both modes


def test_pmtiles_only_omits_per_function_sections():
    nb = cl.build_bench_notebook(_cfg(pmtiles_only=True))
    src = _src(nb)
    assert "run_pmtiles_write" in src
    assert "PMTILES PARITY" in src
    # pmtiles_only must skip per-fn section cells entirely.
    secs = _section_cells(nb)
    assert (
        secs == []
    ), f"expected no per-fn section cells, got: {[s[:60] for s in secs]}"


def test_pmtiles_only_lightweight_only():
    nb = cl.build_bench_notebook(_cfg(pmtiles_only=True, heavyweight=False))
    src = _src(nb)
    assert "run_pmtiles_write" in src
    # No parity check when only one tier runs.
    assert "PMTILES PARITY" not in src or "LIGHTWEIGHT and HEAVYWEIGHT" in src


def test_benchmark_pmtiles_false_no_pmtiles_cell():
    nb = cl.build_bench_notebook(_cfg(benchmark_pmtiles=False, pmtiles_only=False))
    src = _src(nb)
    assert "run_pmtiles_write" not in src
    assert "PMTILES PARITY" not in src


def test_pmtiles_only_preamble_flags_set():
    nb = cl.build_bench_notebook(_cfg(pmtiles_only=True))
    src = _src(nb)
    assert "PMTILES_ONLY = True" in src
    assert (
        "BENCHMARK_PMTILES = False" in src
    )  # pmtiles_only=True, benchmark_pmtiles default=False


def test_benchmark_pmtiles_preamble_flags_set():
    nb = cl.build_bench_notebook(_cfg(benchmark_pmtiles=True))
    src = _src(nb)
    assert "BENCHMARK_PMTILES = True" in src
    assert "PMTILES_ONLY = False" in src


def test_pmtiles_cell_uses_correct_write_fmts():
    nb = cl.build_bench_notebook(_cfg(benchmark_pmtiles=True))
    src = _src(nb)
    assert 'write_fmt="pmtiles_gbx"' in src
    assert 'write_fmt="pmtiles"' in src


def test_pmtiles_cell_uses_sink_lw_hw_variables():
    # The cell must reference the canonical preamble-defined names.
    nb = cl.build_bench_notebook(_cfg(benchmark_pmtiles=True))
    src = _src(nb)
    assert "_sink([_r])" in src
    assert "lw.append(_r)" in src
    assert "hw.append(_r)" in src
