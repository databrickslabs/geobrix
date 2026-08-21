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


def test_notebook_layout_scan_covers_gtiff_all_layouts():
    """Scan cell reads the gtiff external layout tables for all three layouts.

    The layout sweep writes FILE EXTERNAL tables for all three layouts; the scan reads them via
    read_file_table to compare sequential-scan and shuffle-input cost across layouts. Raster
    writes one FILE-ref row PER TILE, so the layout dimension has a real scan/prune effect.
    """
    nb = cl.build_bench_notebook(_base_cfg(layout_scan=True))
    src = _src(nb)
    # External table names contain both the format and layout in their identifier.
    assert (
        "bench_layout_gtiff_external_order" in src
    ), "gtiff external order table must be scanned"
    assert "bench_layout_gtiff_external_cluster" in src
    assert "bench_layout_gtiff_external_plain" in src
    # scan must use external file_mode (not managed)
    assert 'file_mode="external"' in src


def test_notebook_layout_scan_skips_gpkg_by_design():
    """The gpkg layout scan is cleanly SKIPPED BY DESIGN, never wired into the scan.

    A vector FILE write stores the whole .gpkg as ONE FILE reference (one row per external
    table), so a per-layout sequential-scan comparison across order/cluster/plain is
    meaningless. The cell must print a clear skip note and must NOT construct a
    tables_by_layout mapping over the gpkg external tables.
    """
    nb = cl.build_bench_notebook(_base_cfg(layout_scan=True))
    src = _src(nb)
    # A clear skip note referencing gpkg must be present.
    assert "gpkg SKIPPED BY DESIGN" in src
    # The gpkg external tables must NOT be scanned (not passed to run_layout_scan_comparison).
    assert "bench_layout_gpkg_external_order" not in src
    assert "bench_layout_gpkg_external_cluster" not in src
    assert "bench_layout_gpkg_external_plain" not in src
    # A gpkg-specific tables_by_layout mapping must be gone.
    assert "_tables_by_layout_gpkg" not in src
    assert "_scan_rows_gpkg" not in src


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


# ---------------------------------------------------------------------------
# Layout sweep -- write-mode sweep (fuse / external / managed)
# ---------------------------------------------------------------------------


def test_layout_sweep_with_filespace_sweeps_all_three_modes():
    """When file_filespace is set, the generated cell contains external and managed
    write legs with schema.table targets (not just the fuse Volume-path target)."""
    nb = cl.build_bench_notebook(
        _base_cfg(
            layout_sweep=True,
            file_filespace="/Volumes/x/y/z/fs",
            table="main.default.bench_results",
        )
    )
    src = _src(nb)
    # All three modes must appear in the sweep cell source.
    for fm in ("fuse", "external", "managed"):
        assert fm in src, f"Expected write mode {fm!r} in layout-sweep cell"
    # External and managed legs reference schema.table targets via _TABLE_SCHEMA.
    # The source contains the f-string template text (not the runtime value).
    assert "bench_layout_gtiff_external" in src
    assert "bench_layout_gtiff_managed" in src
    # Managed leg must conditionally pass FILE_FILESPACE as filespace.
    assert "FILE_FILESPACE" in src


def test_layout_sweep_without_filespace_uses_only_fuse():
    """When file_filespace is empty/absent, the generated cell only runs fuse mode
    (the mode tuple collapses to ('fuse',) at runtime)."""
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    # The mode conditional and the fuse target must be present.
    assert "_ls_modes" in src
    assert "FILE_FILESPACE" in src
    assert "fuse" in src
    # The cell template always includes the external/managed branches (they are in the
    # f-string template source); they execute only when FILE_FILESPACE is truthy.
    # What we care about is that the _ls_modes guard is present.
    assert '("fuse",)' in src  # the else-branch of the mode conditional


def test_layout_sweep_external_uses_three_layouts():
    """External mode must sweep all three layouts (order, cluster, plain) -- this is the
    meaningful FILE-table layout dimension that the scan reads via read_file_table.
    The fuse DataSource writer ignores the layout arg, so sweeping all three layouts on
    fuse would be redundant; external is the correct host for the 3-layout dimension."""
    nb = cl.build_bench_notebook(
        _base_cfg(
            layout_sweep=True,
            file_filespace="/Volumes/x/y/z/fs",
        )
    )
    src = _src(nb)
    assert '"order", "cluster", "plain"' in src


def test_layout_sweep_fuse_and_managed_use_order_only():
    """Fuse and managed modes sweep only the 'order' layout (mode-comparison baseline)."""
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    # The else-branch for non-external modes must set layouts to ("order",) only.
    assert '("order",)' in src


def test_layout_sweep_per_mode_target_distinct():
    """Each mode writes to its own target prefix (no cross-mode contamination).
    The source text contains the template expressions for each mode's target."""
    nb = cl.build_bench_notebook(
        _base_cfg(
            layout_sweep=True,
            file_filespace="/Volumes/x/y/z/fs",
            table="cat.schema.bench_results",
        )
    )
    src = _src(nb)
    # fuse: Volume path prefix (literal string expression)
    assert 'OUT + "/bench_layout_gtiff"' in src
    # external and managed: schema.table prefixes via _TABLE_SCHEMA (f-string in source)
    assert "bench_layout_gtiff_external" in src
    assert "bench_layout_gtiff_managed" in src
    # The two FILE-mode names must be different (no shared target between external/managed).
    assert "bench_layout_gtiff_external" in src
    assert "bench_layout_gtiff_managed" in src
    ext_idx = src.find("bench_layout_gtiff_external")
    mgd_idx = src.find("bench_layout_gtiff_managed")
    assert ext_idx != mgd_idx, "external and managed must use distinct target names"


def test_layout_sweep_table_schema_derived_from_table():
    """_TABLE_SCHEMA is derived at runtime from TABLE (first two dot-separated parts).
    The cell source contains the derivation expression."""
    nb = cl.build_bench_notebook(
        _base_cfg(
            layout_sweep=True,
            file_filespace="/Volumes/x/y/z/fs",
            table="mycatalog.myschema.bench_results",
        )
    )
    src = _src(nb)
    # The derivation expression must be present in the cell.
    assert "_TABLE_SCHEMA" in src
    assert 'TABLE.split(".")[:2]' in src


def test_layout_sweep_managed_passes_filespace():
    """Both FILE legs pass FILE_FILESPACE as filespace; only fuse passes None.

    Managed needs the filespace for FILE MANAGED storage; vector external needs it
    as the staging directory where the assembled .gpkg lands before try_to_file
    references it (raster external ignores it). The cell therefore sets
    _ls_filespace = FILE_FILESPACE for both external and managed, None for fuse.
    """
    nb = cl.build_bench_notebook(
        _base_cfg(
            layout_sweep=True,
            file_filespace="/Volumes/x/y/z/fs",
        )
    )
    src = _src(nb)
    # The cell sets _ls_filespace = FILE_FILESPACE if _ls_mode in (external, managed) else None.
    assert "FILE_FILESPACE if _ls_mode" in src


def test_layout_sweep_write_sweep_progress_includes_mode():
    """[write-sweep] progress prints include the file_mode so each line is identifiable."""
    nb = cl.build_bench_notebook(_base_cfg(layout_sweep=True))
    src = _src(nb)
    assert "[write-sweep]" in src
    assert "mode=" in src


# ---------------------------------------------------------------------------
# Vector FILE-column-table read (Task 5)
# ---------------------------------------------------------------------------


def test_notebook_includes_vector_table_read_cell_when_flag_set():
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "run_vector_table_read_sweep" in src


def test_notebook_vector_table_read_only_suppresses_fn_benchmarks():
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read_only=True))
    src = _src(nb)
    assert "run_vector_table_read_sweep" in src
    assert "# (a) Lightweight pure-core" not in src
    assert "# (c) Lightweight spark-path" not in src


def test_notebook_vector_table_read_cell_absent_by_default():
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_vector_table_read_sweep" not in src


def test_notebook_vector_table_read_preamble_variables():
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "BENCHMARK_VECTOR_TABLE_READ" in src
    assert "VECTOR_TABLE_READ_ONLY" in src


def test_notebook_vector_table_read_guards_on_file_filespace():
    """Cell skips cleanly when FILE_FILESPACE is not set."""
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "FILE_FILESPACE" in src
    assert "VECTOR TABLE READ SKIPPED" in src


def test_notebook_vector_table_read_covers_managed_and_external():
    """Cell includes both managed and external mode legs."""
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "managed" in src
    assert "external" in src


def test_notebook_vector_table_read_fuse_is_na_by_design():
    """Cell includes fuse mode (which yields na_by_design at runtime)."""
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "fuse" in src
    assert "na_by_design" in src


def test_notebook_vector_table_read_reuses_layout_sweep_tables():
    """Cell reads the bench_layout_gpkg tables produced by the layout sweep."""
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "bench_layout_gpkg_managed_order" in src
    assert "bench_layout_gpkg_external_order" in src


def test_notebook_vector_table_read_fn_in_results():
    """Cell queries fn='gpkg_table_read' from the results table."""
    nb = cl.build_bench_notebook(_base_cfg(vector_table_read=True))
    src = _src(nb)
    assert "gpkg_table_read" in src


def test_notebook_vector_table_read_regression_existing_build_unaffected():
    """Adding vector_table_read=False (default) leaves the existing cell set unchanged."""
    nb_default = cl.build_bench_notebook(_base_cfg())
    nb_explicit = cl.build_bench_notebook(_base_cfg(vector_table_read=False))
    assert _src(nb_default) == _src(nb_explicit)


# ---------------------------------------------------------------------------
# Raster decode-read (Task C)
# ---------------------------------------------------------------------------


def test_notebook_includes_raster_decode_read_cell_when_flag_set():
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    assert "run_raster_decode_read_sweep" in src


def test_notebook_raster_decode_read_only_suppresses_fn_benchmarks():
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read_only=True))
    src = _src(nb)
    assert "run_raster_decode_read_sweep" in src
    assert "# (a) Lightweight pure-core" not in src
    assert "# (c) Lightweight spark-path" not in src


def test_notebook_raster_decode_read_cell_absent_by_default():
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_raster_decode_read_sweep" not in src


def test_notebook_raster_decode_read_preamble_variables():
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    assert "BENCHMARK_RASTER_DECODE_READ" in src
    assert "RASTER_DECODE_READ_ONLY" in src


def test_notebook_raster_decode_read_guards_on_file_filespace():
    """Cell skips cleanly when FILE_FILESPACE is not set."""
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    assert "FILE_FILESPACE" in src
    assert "RASTER DECODE READ SKIPPED" in src


def test_notebook_raster_decode_read_covers_all_three_modes():
    """Cell includes managed, external, and fuse mode legs."""
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    for fm in ("managed", "external", "fuse"):
        assert fm in src, f"Expected file_mode {fm!r} in raster-decode-read cell"


def test_notebook_raster_decode_read_uses_gtiff_layout_tables():
    """Cell reads the bench_layout_gtiff tables produced by the layout sweep."""
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    assert "bench_layout_gtiff_managed_order" in src
    assert "bench_layout_gtiff_external_order" in src


def test_notebook_raster_decode_read_includes_ordering_comparison():
    """Cell uses skip_ordering=None for managed/external so both ordering variants run."""
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    # The ordering-amortization comparison: skip_ordering=None triggers the two-row emit.
    assert "skip_ordering" in src
    assert "skip-order" in src or "skip_ordering=None" in src


def test_notebook_raster_decode_read_fn_in_results():
    """Cell queries fn='gtiff_decode_read' from the results table."""
    nb = cl.build_bench_notebook(_base_cfg(raster_decode_read=True))
    src = _src(nb)
    assert "gtiff_decode_read" in src


def test_notebook_raster_decode_read_regression_existing_build_unaffected():
    """Adding raster_decode_read=False (default) leaves the existing cell set unchanged."""
    nb_default = cl.build_bench_notebook(_base_cfg())
    nb_explicit = cl.build_bench_notebook(_base_cfg(raster_decode_read=False))
    assert _src(nb_default) == _src(nb_explicit)


def test_existing_build_does_not_include_raster_decode_read():
    """Regression: run_raster_decode_read_sweep absent in the default build."""
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "run_raster_decode_read_sweep" not in src


def test_raster_decode_read_and_vector_table_read_can_combine():
    """Both flags can be set together; both cells are emitted."""
    nb = cl.build_bench_notebook(
        _base_cfg(raster_decode_read=True, vector_table_read=True)
    )
    src = _src(nb)
    assert "run_raster_decode_read_sweep" in src
    assert "run_vector_table_read_sweep" in src
