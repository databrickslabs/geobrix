"""Smoke tests for the --benchmark-vector / --vector-only wiring in build_bench_notebook."""

from databricks.labs.gbx.bench import cluster as cl


def _cfg(**kw):
    base = dict(
        wheel="/Volumes/c/s/v/geobrix-0.4.1-py3-none-any.whl",
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


def test_benchmark_vector_cell_contains_run_format_read_and_parity():
    nb = cl.build_bench_notebook(_cfg(benchmark_vector=True))
    src = _src(nb)
    assert "run_format_read" in src
    assert "VECTOR PARITY" in src
    # Standard fn-bench sections still present (benchmark_vector does NOT suppress them).
    secs = _section_cells(nb)
    assert len(secs) == 4  # both tiers, both modes


def test_vector_only_omits_per_function_sections():
    nb = cl.build_bench_notebook(_cfg(vector_only=True))
    src = _src(nb)
    assert "run_format_read" in src
    assert "VECTOR PARITY" in src
    # vector_only must skip per-fn section cells entirely.
    secs = _section_cells(nb)
    assert (
        secs == []
    ), f"expected no per-fn section cells, got: {[s[:60] for s in secs]}"


def test_vector_only_lightweight_only():
    nb = cl.build_bench_notebook(_cfg(vector_only=True, heavyweight=False))
    src = _src(nb)
    assert "run_format_read" in src
    # No parity check when only one tier runs.
    assert "VECTOR PARITY" not in src or "LIGHTWEIGHT and HEAVYWEIGHT" in src


def test_benchmark_vector_false_no_vector_cell():
    nb = cl.build_bench_notebook(_cfg(benchmark_vector=False, vector_only=False))
    src = _src(nb)
    assert "VECTOR PARITY" not in src
    # run_format_read may appear in the readers cell if benchmark_readers is also set,
    # but the vector formats should not appear.
    assert "geojson_gbx" not in src
    assert "shapefile_gbx" not in src


def test_vector_only_preamble_flags_set():
    nb = cl.build_bench_notebook(_cfg(vector_only=True))
    src = _src(nb)
    assert "VECTOR_ONLY = True" in src
    assert (
        "BENCHMARK_VECTOR = False" in src
    )  # vector_only=True, benchmark_vector default=False


def test_benchmark_vector_preamble_flags_set():
    nb = cl.build_bench_notebook(_cfg(benchmark_vector=True))
    src = _src(nb)
    assert "BENCHMARK_VECTOR = True" in src
    assert "VECTOR_ONLY = False" in src


def test_vector_cell_uses_correct_formats():
    nb = cl.build_bench_notebook(_cfg(benchmark_vector=True))
    src = _src(nb)
    assert "geojson_gbx" in src
    assert "geojson_ogr" in src
    assert "shapefile_gbx" in src
    assert "shapefile_ogr" in src
    assert "gpkg_gbx" in src
    assert "gpkg_ogr" in src
    assert "file_gdb_gbx" in src
    assert "file_gdb_ogr" in src


def test_vector_cell_uses_sink_lw_hw_variables():
    # The cell must reference the canonical preamble-defined names.
    nb = cl.build_bench_notebook(_cfg(benchmark_vector=True))
    src = _src(nb)
    assert "_sink([_r])" in src
    assert "lw.append(_r)" in src
    assert "hw.append(_r)" in src
