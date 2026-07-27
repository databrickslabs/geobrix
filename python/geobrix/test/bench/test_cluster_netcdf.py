"""Smoke tests for the --benchmark-netcdf / --netcdf-only wiring in build_bench_notebook.

The NetCDF reader bench is a same-corpus heavy-vs-light throughput comparison
(light netcdf_gbx vs heavy netcdf_gdal) over a .nc pool staged separately. These
assert the cell is emitted, wired to the shared sink/lw/hw names, uses the
.*\\.nc$ filter for both tiers, and SKIPS CLEANLY when the pool is empty.
"""

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


def test_benchmark_netcdf_cell_present_and_wired():
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf=True))
    src = _src(nb)
    assert "netcdf_gbx" in src
    assert "netcdf_gdal" in src
    assert "run_format_read" in src
    # Same-corpus: both tiers filter the same .nc pool.
    assert r".*\.nc$" in src
    assert "/netcdf" in src
    # Uses canonical preamble-defined sink/accumulator names.
    assert "_sink([_r])" in src
    assert "lw.append(_r)" in src
    assert "hw.append(_r)" in src
    # Standard fn-bench sections still present (benchmark_netcdf does NOT suppress them).
    assert len(_section_cells(nb)) == 4


def test_benchmark_netcdf_cell_skips_cleanly_when_empty():
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf=True))
    src = _src(nb)
    # Guard: empty/missing pool => clear skip message, not a failure.
    assert "NETCDF BENCH SKIPPED" in src
    assert "list_corpus_files" in src


def test_netcdf_only_omits_per_function_sections():
    nb = cl.build_bench_notebook(_cfg(netcdf_only=True))
    src = _src(nb)
    assert "netcdf_gbx" in src
    assert "netcdf_gdal" in src
    assert _section_cells(nb) == []


def test_netcdf_preamble_flags_set():
    nb = cl.build_bench_notebook(_cfg(netcdf_only=True))
    src = _src(nb)
    assert "NETCDF_ONLY = True" in src
    assert (
        "BENCHMARK_NETCDF = False" in src
    )  # netcdf_only=True, benchmark_netcdf default

    nb2 = cl.build_bench_notebook(_cfg(benchmark_netcdf=True))
    src2 = _src(nb2)
    assert "BENCHMARK_NETCDF = True" in src2
    assert "NETCDF_ONLY = False" in src2


def test_benchmark_netcdf_false_no_netcdf_cell():
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf=False, netcdf_only=False))
    src = _src(nb)
    # "netcdf_gdal" appears in the always-emitted preamble help comment, so it is
    # not a reliable sentinel. The cell's runtime output strings are cell-only.
    assert "NETCDF BENCH" not in src
    assert "list_corpus_files" not in src


def test_netcdf_only_lightweight_only():
    nb = cl.build_bench_notebook(_cfg(netcdf_only=True, heavyweight=False))
    src = _src(nb)
    assert "netcdf_gbx" in src
    # Heavy leg is guarded by HEAVYWEIGHT at runtime; the cell is tier-agnostic
    # source, so netcdf_gdal text is still present but gated by `if HEAVYWEIGHT`.
    assert "if HEAVYWEIGHT:" in src
