"""Smoke tests for the --benchmark-netcdf-writer / --netcdf-writer-only wiring.

The NetCDF WRITER bench times the light ``netcdf_gbx`` writer throughput over two
legs -- a raster leg (regular grids at {CORPUS}/netcdf) and a vector leg (S5P
swaths at {CORPUS}/netcdf-swath) -- reusing the reader-cycle corpora. It is
LIGHT-ONLY: there is no heavy NetCDF writer. These assert the cell is emitted,
wired to the shared sink/lw names, uses ``run_format_write`` with
read_fmt=write_fmt="netcdf_gbx", carries mode=vector on the vector leg (so it
flows to both the reader and the writer), and SKIPS CLEANLY when a corpus is
empty/missing.
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


def test_benchmark_netcdf_writer_cell_present_and_wired():
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf_writer=True))
    src = _src(nb)
    assert "run_format_write" in src
    # Same format both sides of the write (light netcdf_gbx read + write).
    assert 'read_fmt="netcdf_gbx"' in src
    assert 'write_fmt="netcdf_gbx"' in src
    # Both legs' inputs + outputs. RASTER now reads a distinct-variable single-grid
    # corpus (so parts/single/merge share one input and single+merge can fold vars).
    assert 'f"{CORPUS}/netcdf-distinct"' in src
    assert 'f"{CORPUS}/netcdf-out"' in src
    assert 'f"{CORPUS}/netcdf-swath"' in src
    assert 'f"{CORPUS}/netcdf-swath-out"' in src
    # Vector leg carries mode=vector (flows to BOTH read + write via run_format_write).
    assert '"mode": "vector"' in src or '"mode":"vector"' in src
    assert r".*\.nc$" in src
    # Canonical preamble names; light-only (no hw.append in this cell).
    assert "_sink([_wr])" in src
    assert "lw.append(_wr)" in src
    # Standard fn-bench sections still present (benchmark flag does NOT suppress them).
    assert len(_section_cells(nb)) == 4


def test_netcdf_writer_emits_singlefile_legs():
    """Each mode (raster, vector) also runs a parallel singleFile measured leg."""
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf_writer=True))
    src = _src(nb)
    # singleFile write option flows to the writer on the single legs.
    assert '"singleFile": "true"' in src or '"singleFile":"true"' in src
    # Distinct out-dirs so mode=overwrite doesn't clobber the parts-leg output.
    assert 'f"{CORPUS}/netcdf-out-single"' in src
    assert 'f"{CORPUS}/netcdf-swath-out-single"' in src
    # Rows are distinguishable in the store via a label tag on both single legs.
    assert 'label="singleFile"' in src
    assert src.count('label="singleFile"') == 2


def test_netcdf_writer_emits_merge_legs():
    """Each mode (raster, vector) also runs a THIRD post-hoc merge measured leg."""
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf_writer=True))
    src = _src(nb)
    # merge write options flow to the writer on the merge legs; keepParts=true is
    # REQUIRED so the on-disk parts survive across warmup+measured iterations.
    assert '"merge": "true"' in src or '"merge":"true"' in src
    assert '"keepParts": "true"' in src or '"keepParts":"true"' in src
    # Distinct out-dirs so mode=overwrite doesn't clobber the parts/single dirs.
    assert 'f"{CORPUS}/netcdf-out-merge"' in src
    assert 'f"{CORPUS}/netcdf-swath-out-merge"' in src
    # Rows are distinguishable in the store via a "merge" label on both merge legs.
    assert 'label="merge"' in src
    assert src.count('label="merge"') == 2
    # RASTER legs read a distinct-variable single-grid corpus (parts/single/merge
    # need distinct vars sharing one grid; the same-variable NASA-NEX corpus would
    # make single+merge fail by design). Repointed from {CORPUS}/netcdf.
    assert 'f"{CORPUS}/netcdf-distinct"' in src
    assert 'f"{CORPUS}/netcdf"' not in src


def test_netcdf_writer_light_only_no_heavy_leg():
    nb = cl.build_bench_notebook(_cfg(netcdf_writer_only=True))
    src = _src(nb)
    # A comment/log makes clear heavy has no netcdf writer (light-only throughput).
    assert "light-only" in src.lower()
    assert (
        "no heavy netcdf writer" in src.lower() or "heavy has no netcdf" in src.lower()
    )


def test_netcdf_writer_cell_skips_cleanly_when_empty():
    nb = cl.build_bench_notebook(_cfg(benchmark_netcdf_writer=True))
    src = _src(nb)
    # Guard: empty/missing pools => clear skip messages, not a failure.
    assert "NETCDF WRITER RASTER BENCH SKIPPED" in src
    assert "NETCDF WRITER SWATH BENCH SKIPPED" in src
    assert "list_corpus_files" in src


def test_netcdf_writer_only_omits_per_function_sections():
    nb = cl.build_bench_notebook(_cfg(netcdf_writer_only=True))
    src = _src(nb)
    assert "run_format_write" in src
    assert _section_cells(nb) == []


def test_netcdf_writer_preamble_flags_set():
    nb = cl.build_bench_notebook(_cfg(netcdf_writer_only=True))
    src = _src(nb)
    assert "NETCDF_WRITER_ONLY = True" in src
    assert "BENCHMARK_NETCDF_WRITER = False" in src

    nb2 = cl.build_bench_notebook(_cfg(benchmark_netcdf_writer=True))
    src2 = _src(nb2)
    assert "BENCHMARK_NETCDF_WRITER = True" in src2
    assert "NETCDF_WRITER_ONLY = False" in src2


def test_benchmark_netcdf_writer_false_no_cell():
    nb = cl.build_bench_notebook(
        _cfg(benchmark_netcdf_writer=False, netcdf_writer_only=False)
    )
    src = _src(nb)
    assert "NETCDF WRITER" not in src


def test_netcdf_writer_only_corpus_json_read_is_lazy():
    """A --netcdf-writer-only run must NOT require the function-bench corpus.json."""
    nb = cl.build_bench_notebook(_cfg(netcdf_writer_only=True))
    src = _src(nb)
    assert 'corpus = _m.Corpus.read(f"{CORPUS}/corpus.json")\nfnspecs' not in src
    assert "_READER_ONLY" in src or "NETCDF_WRITER_ONLY" in src
