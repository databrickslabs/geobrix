"""TDD tests for Task 10: FILE-matrix / GeoPackage chunkSize / layout sweep / layout scan
cells added to build_bench_notebook (bench/cluster.py).

New flags (cfg keys): file_matrix / file_matrix_only, gpkg_chunksize / gpkg_chunksize_only,
layout_sweep / layout_sweep_only, layout_scan / layout_scan_only.

The tests verify:
  - Each flag triggers its cell (correct leg functions referenced in source).
  - The *_only variant suppresses fn-bench sections (no run_light / run_heavy).
  - All four cells are absent by default (flags default OFF).
  - Isolation invariants are present in the cell source (per-mode / per-layout targets).
  - Existing build_bench_notebook callers without the new flags are unaffected.
"""

from databricks.labs.gbx.bench import cluster as cl


def _base_cfg(**overrides):
    """Minimal valid cfg for build_bench_notebook. All new flags absent (default OFF)."""
    cfg = dict(
        wheel="/Volumes/c/s/v/geobrix-0.5.0-py3-none-any.whl",
        corpus="/Volumes/c/s/v/bench-corpus",
        out_dir="/Volumes/c/s/v/bench-out/run1",
        table="main.default.bench_results",
        run_id="run1",
        functions="rst_width",
        modes="both",
        row_counts="10,100",
        warmup=2,
        measured=5,
        heavyweight=True,
        lightweight=True,
    )
    cfg.update(overrides)
    return cfg


def _src(nb):
    """Concatenate all cell sources from a built notebook dict."""
    return "\n".join(
        (
            c["source"]
            if isinstance(c.get("source"), str)
            else "".join(c.get("source", []))
        )
        for c in nb["cells"]
    )


# ---------------------------------------------------------------------------
# FILE matrix
# ---------------------------------------------------------------------------


def test_notebook_includes_file_matrix_cell_when_flag_set():
    nb = cl.build_bench_notebook(_base_cfg(file_matrix=True))
    src = _src(nb)
    assert "run_gtiff_file_read" in src
    assert "run_gpkg_file_read" in src
    assert "file_mode" in src


def test_notebook_file_matrix_only_suppresses_fn_benchmarks():
    nb = cl.build_bench_notebook(_base_cfg(file_matrix_only=True))
    src = _src(nb)
    assert "run_gtiff_file_read" in src
    assert "run_gpkg_file_read" in src
    # fn-bench section cells suppressed: these marker comments appear only in the section cells
    assert "# (a) Lightweight pure-core" not in src
    assert "# (c) Lightweight spark-path" not in src


def test_notebook_file_matrix_cell_absent_by_default():
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_gtiff_file_read" not in src


def test_notebook_file_matrix_sweeps_all_three_modes():
    """Cell contains "fuse", "external", and "managed" strings."""
    nb = cl.build_bench_notebook(_base_cfg(file_matrix=True))
    src = _src(nb)
    for fm in ("fuse", "external", "managed"):
        assert fm in src, f"Expected file_mode {fm!r} in FILE-matrix cell"


def test_notebook_file_matrix_has_per_mode_isolation():
    """Cell uses mode-specific paths / guards so no mode inherits warm state."""
    nb = cl.build_bench_notebook(_base_cfg(file_matrix=True))
    src = _src(nb)
    # Per-mode isolation: either a loop variable, per-mode source, or per-mode guard.
    # Any of these patterns signal that the cell doesn't reuse state across modes.
    assert (
        "bench_fm_" in src
        or "_fm ==" in src
        or "for _fm in" in src
        or "file_mode=_fm" in src
    ), "FILE-matrix cell should use a per-mode loop or per-mode source variable"


def test_notebook_file_matrix_preamble_variables():
    """_PREAMBLE contains the BENCHMARK_FILE_MATRIX + FILE_MATRIX_ONLY config vars."""
    nb = cl.build_bench_notebook(_base_cfg(file_matrix=True))
    src = _src(nb)
    assert "BENCHMARK_FILE_MATRIX" in src
    assert "FILE_MATRIX_ONLY" in src


# ---------------------------------------------------------------------------
# GeoPackage chunkSize sweep
# ---------------------------------------------------------------------------


def test_notebook_includes_gpkg_chunksize_cell_when_flag_set():
    nb = cl.build_bench_notebook(_base_cfg(gpkg_chunksize=True))
    src = _src(nb)
    assert "run_gpkg_chunksize_sweep" in src
    assert "chunk_size" in src or "chunk_sizes" in src


def test_notebook_gpkg_chunksize_only_suppresses_fn_benchmarks():
    nb = cl.build_bench_notebook(_base_cfg(gpkg_chunksize_only=True))
    src = _src(nb)
    assert "run_gpkg_chunksize_sweep" in src
    assert "# (a) Lightweight pure-core" not in src
    assert "# (c) Lightweight spark-path" not in src


def test_notebook_gpkg_chunksize_cell_absent_by_default():
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_gpkg_chunksize_sweep" not in src


def test_notebook_gpkg_chunksize_checks_fanout_invariance():
    """Cell contains a check that partition count is stable across chunkSizes."""
    nb = cl.build_bench_notebook(_base_cfg(gpkg_chunksize=True))
    src = _src(nb)
    assert "input_partitions" in src or "fanout-invariant" in src


def test_notebook_gpkg_chunksize_preamble_variables():
    nb = cl.build_bench_notebook(_base_cfg(gpkg_chunksize=True))
    src = _src(nb)
    assert "BENCHMARK_GPKG_CHUNKSIZE" in src
    assert "GPKG_CHUNKSIZE_ONLY" in src


# ---------------------------------------------------------------------------
# Layout sweep
# ---------------------------------------------------------------------------


def test_notebook_includes_layout_sweep_cell_when_flag_set():
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    assert "run_file_write_layout_sweep" in src


def test_notebook_layout_sweep_only_suppresses_fn_benchmarks():
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep_only=True))
    src = _src(nb)
    assert "run_file_write_layout_sweep" in src
    assert "# (a) Lightweight pure-core" not in src
    assert "# (c) Lightweight spark-path" not in src


def test_notebook_layout_sweep_cell_absent_by_default():
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_file_write_layout_sweep" not in src


def test_notebook_layout_sweep_uses_per_layout_targets():
    """run_file_write_layout_sweep is called with a target_prefix (per-layout isolation)."""
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    assert "target_prefix" in src or "bench_ls_" in src or "bench_layout_" in src


def test_notebook_layout_sweep_covers_both_formats():
    """Cell includes both fmt='gtiff' and fmt='gpkg' legs."""
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    assert "gtiff" in src
    assert "gpkg" in src


def test_notebook_layout_sweep_preamble_variables():
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    assert "BENCHMARK_LAYOUT_SWEEP" in src
    assert "LAYOUT_SWEEP_ONLY" in src


# ---------------------------------------------------------------------------
# Layout scan
# ---------------------------------------------------------------------------


def test_notebook_includes_layout_scan_cell_when_flag_set():
    nb = cl.build_bench_notebook(_base_cfg(layout_scan=True))
    src = _src(nb)
    assert "run_layout_scan_comparison" in src


def test_notebook_layout_scan_only_suppresses_fn_benchmarks():
    nb = cl.build_bench_notebook(_base_cfg(layout_scan_only=True))
    src = _src(nb)
    assert "run_layout_scan_comparison" in src
    assert "# (a) Lightweight pure-core" not in src
    assert "# (c) Lightweight spark-path" not in src


def test_notebook_layout_scan_cell_absent_by_default():
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_layout_scan_comparison" not in src


def test_notebook_layout_scan_passes_tables_by_layout():
    """Cell constructs a tables_by_layout dict and passes it to run_layout_scan_comparison."""
    nb = cl.build_bench_notebook(_base_cfg(layout_scan=True))
    src = _src(nb)
    assert "tables_by_layout" in src


def test_notebook_layout_scan_covers_both_formats():
    """Scan cell references both gtiff and gpkg layout tables (symmetric with sweep).

    The layout sweep writes bench_layout_gtiff_* AND bench_layout_gpkg_* tables;
    the scan must cover both formats so the comparison is not one-sided.
    """
    nb = cl.build_bench_notebook(_base_cfg(layout_scan=True))
    src = _src(nb)
    assert "bench_layout_gtiff_order" in src, "gtiff layout tables must be scanned"
    assert "bench_layout_gtiff_cluster" in src
    assert "bench_layout_gtiff_plain" in src
    assert "bench_layout_gpkg_order" in src, "gpkg layout tables must be scanned"
    assert "bench_layout_gpkg_cluster" in src
    assert "bench_layout_gpkg_plain" in src


def test_notebook_layout_scan_preamble_variables():
    nb = cl.build_bench_notebook(_base_cfg(layout_scan=True))
    src = _src(nb)
    assert "BENCHMARK_LAYOUT_SCAN" in src
    assert "LAYOUT_SCAN_ONLY" in src


# ---------------------------------------------------------------------------
# Preamble config vars (new cfg keys present in every build)
# ---------------------------------------------------------------------------


def test_preamble_has_file_filespace_variable():
    """FILE_FILESPACE is in the preamble (default empty string)."""
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "FILE_FILESPACE" in src


def test_preamble_has_gpkg_corpus_variable():
    """GPKG_CORPUS is in the preamble (defaulted to CORPUS + '/bench-corpus-gpkg')."""
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "GPKG_CORPUS" in src
    assert "bench-corpus-gpkg" in src


# ---------------------------------------------------------------------------
# Regression: existing build still works unchanged
# ---------------------------------------------------------------------------


def test_existing_build_unaffected_by_new_flags():
    """Existing callers without new flags produce a structurally valid notebook."""
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    # The wheel path is referenced (two-step install: force-reinstall --no-deps the wheel).
    assert "/Volumes/c/s/v/geobrix-0.5.0-py3-none-any.whl" in src
    # The classic install MUST force-reinstall the geobrix CODE ONLY (--force-reinstall
    # --no-deps): the fixed 0.5.0 version means a plain install runs STALE code, but a
    # FULL --force-reinstall reinstalls the whole closure (slow + bumps rio-tiler's unpinned
    # cachetools past pyiceberg's <7 cap). Deps are then a separate NON-forced step.
    assert "--force-reinstall" in src
    assert "--no-deps" in src
    assert "geobrix[light-dbr19]" in src  # the non-forced deps step
    assert "dbutils.notebook.exit" in src
    assert nb["nbformat"] == 4
    # None of the four new cell types should appear
    assert "run_gtiff_file_read" not in src
    assert "run_gpkg_chunksize_sweep" not in src
    assert "run_file_write_layout_sweep" not in src
    assert "run_layout_scan_comparison" not in src


def test_all_four_cells_emitted_together():
    """All four flags can be combined in one notebook build."""
    nb = cl.build_bench_notebook(
        _base_cfg(
            file_matrix=True,
            gpkg_chunksize=True,
            layout_sweep=True,
            layout_scan=True,
        )
    )
    src = _src(nb)
    assert "run_gtiff_file_read" in src
    assert "run_gpkg_chunksize_sweep" in src
    assert "run_file_write_layout_sweep" in src
    assert "run_layout_scan_comparison" in src


def test_layout_sweep_gtiff_write_source_is_cached_not_limited():
    """The GeoTIFF write-sweep source must be .cache()'d and must NOT rely on .limit() to
    bound a raster_gbx read. Spark 4's Python DataSource API has no limit pushdown, so
    `.load(dir).limit(N)` still opens EVERY tile in dir on each action -- the write leg's ~5
    actions would re-scan the whole directory and inflate the write timing. The source dir is
    sized instead (GBX_BENCH_CORPUS -> a ~1k-tile pool) and .cache()'d so the actions share one
    metadata read."""
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    assert "_ls_tile_df" in src
    assert ".cache()" in src
    # the ineffective .limit()-based cap must be gone (it did not bound the read)
    assert "_LS_WRITE_TILES" not in src
