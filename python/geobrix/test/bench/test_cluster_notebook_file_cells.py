"""The FILE-matrix / GeoPackage chunkSize / layout sweep / layout scan / vector-table-read / decode-read cells of
build_bench_notebook (bench/cluster.py).

Each flag emits its leg, its ``<flag>_only`` form drops the function-bench sections, the default build has none of
them, and the layout-sweep cell isolates its targets per mode.
"""

import pytest

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


_FN_BENCH_MARKERS = ("# (a) Lightweight pure-core", "# (c) Lightweight spark-path")

# flag -> (the leg it must call, the preamble variables it sets)
_CELLS = {
    "file_matrix": (
        "run_gtiff_file_read",
        ("BENCHMARK_FILE_MATRIX", "FILE_MATRIX_ONLY"),
    ),
    "gpkg_chunksize": (
        "run_gpkg_chunksize_sweep",
        ("BENCHMARK_GPKG_CHUNKSIZE", "GPKG_CHUNKSIZE_ONLY"),
    ),
    "layout_sweep": (
        "run_file_write_layout_sweep",
        ("BENCHMARK_LAYOUT_SWEEP", "LAYOUT_SWEEP_ONLY"),
    ),
    "layout_scan": (
        "run_layout_scan_comparison",
        ("BENCHMARK_LAYOUT_SCAN", "LAYOUT_SCAN_ONLY"),
    ),
    "vector_table_read": (
        "run_vector_table_read_sweep",
        ("BENCHMARK_VECTOR_TABLE_READ", "VECTOR_TABLE_READ_ONLY"),
    ),
    "raster_decode_read": (
        "run_raster_decode_read_sweep",
        ("BENCHMARK_RASTER_DECODE_READ", "RASTER_DECODE_READ_ONLY"),
    ),
}


def _build(**cfg):
    return _src(cl.build_bench_notebook(_base_cfg(**cfg)))


@pytest.mark.parametrize("flag", _CELLS)
def test_flag_emits_its_leg_and_only_drops_the_function_bench(flag):
    leg, preamble = _CELLS[flag]
    on, only = _build(**{flag: True}), _build(**{f"{flag}_only": True})
    assert leg in on and all(v in on for v in preamble)
    assert leg in only
    assert all(m in on for m in _FN_BENCH_MARKERS)
    assert not any(
        m in only for m in _FN_BENCH_MARKERS
    ), "the _only form suppresses the function benchmarks"


def test_default_build_has_none_of_the_cells_and_a_false_flag_is_the_default():
    default = _build()
    assert not any(leg in default for leg, _ in _CELLS.values())
    assert default == _build(vector_table_read=False, raster_decode_read=False)
    # every flag together still builds, with every leg
    together = _build(**{flag: True for flag in _CELLS})
    assert all(leg in together for leg, _ in _CELLS.values())


def test_default_build_installs_the_wheel_code_only_then_its_deps():
    """A fixed version means a plain install runs stale code, but a full --force-reinstall reinstalls the whole
    closure (and bumps rio-tiler's unpinned cachetools past pyiceberg's cap): code forced, deps a separate step.
    """
    nb = cl.build_bench_notebook(_base_cfg())
    src = _src(nb)
    assert "/Volumes/c/s/v/geobrix-0.5.0-py3-none-any.whl" in src
    assert "--force-reinstall" in src and "--no-deps" in src
    assert "'geobrix @ file://' + WHL" in src
    assert "dbutils.notebook.exit" in src
    assert nb["nbformat"] == 4
    assert "FILE_FILESPACE" in src and "bench-corpus-gpkg" in src


def test_file_matrix_sweeps_each_mode_in_its_own_source():
    src = _build(file_matrix=True)
    assert all(fm in src for fm in ("fuse", "external", "managed"))
    assert "for _fm in" in src or "file_mode=_fm" in src


def test_layout_sweep_modes_layouts_and_targets():
    """With a filespace the sweep adds external (three layouts) and managed legs, each with its own target; without
    one it collapses to fuse. Spark 4's Python data source has no limit pushdown, so the write source is cached, not
    .limit()-ed (a .limit() would re-scan the whole directory on each of the leg's actions).
    """
    without = _build(layout_sweep=True)
    assert '("fuse",)' in without and '("order",)' in without and "_ls_modes" in without
    assert (
        "_ls_tile_df" in without
        and ".cache()" in without
        and "_LS_WRITE_TILES" not in without
    )
    assert "[write-sweep]" in without and "mode=" in without

    src = _build(
        layout_sweep=True,
        file_filespace="/Volumes/x/y/z/fs",
        table="mycatalog.myschema.bench_results",
    )
    assert '"order", "cluster", "plain"' in src
    assert 'OUT + "/bench_layout_gtiff"' in src
    assert "bench_layout_gtiff_external" in src and "bench_layout_gtiff_managed" in src
    assert 'TABLE.split(".")[:2]' in src and "FILE_FILESPACE if _ls_mode" in src


def test_layout_scan_reads_the_gtiff_layouts_and_skips_gpkg_by_design():
    """A vector FILE write stores the whole .gpkg as one FILE reference, so a per-layout scan comparison is
    meaningless: the cell prints a skip note and wires no gpkg tables."""
    src = _build(layout_scan=True)
    assert "tables_by_layout" in src and 'file_mode="external"' in src
    for layout in ("order", "cluster", "plain"):
        assert f"bench_layout_gtiff_external_{layout}" in src
        assert f"bench_layout_gpkg_external_{layout}" not in src
    assert "gpkg SKIPPED BY DESIGN" in src and "_tables_by_layout_gpkg" not in src


def test_read_cells_reuse_the_layout_sweep_tables_and_skip_without_a_filespace():
    vec = _build(vector_table_read=True)
    assert "VECTOR TABLE READ SKIPPED" in vec and "gpkg_table_read" in vec
    assert (
        "bench_layout_gpkg_managed_order" in vec
        and "bench_layout_gpkg_external_order" in vec
    )
    assert "na_by_design" in vec, "fuse is not applicable to a table read"

    ras = _build(raster_decode_read=True)
    assert "RASTER DECODE READ SKIPPED" in ras and "gtiff_decode_read" in ras
    assert (
        "bench_layout_gtiff_managed_order" in ras
        and "bench_layout_gtiff_external_order" in ras
    )
    assert all(fm in ras for fm in ("managed", "external", "fuse"))
    assert "skip_ordering" in ras
