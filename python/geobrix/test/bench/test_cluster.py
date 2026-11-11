import pytest

from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import results as R


def _row(**kw):
    base = dict(
        run_id="r",
        api="lightweight",
        fn="rst_width",
        category="accessor",
        mode="pure-core",
        tile_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.0,
        warmup_iters=1,
        measured_iters=2,
        iter_median_s=1.0,
        iter_min_s=1.0,
        iter_p90_s=1.0,
        throughput_mpix_s=1.0,
        throughput_rows_s=1.0,
        peak_rss_mb=0.0,
        status="ok",
        note="",
        env_arch="x",
        env_cpu_model="x",
        env_cpu_count=1,
        env_os="x",
        env_gbx_version="0.4.0",
        env_gdal_version="3.12.1",
        env_runtime_version="x",
        env_where="venv",
        output_fingerprint="",
    )
    base.update(kw)
    return R.ResultRow(**base)


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    s = SparkSession.builder.master("local[2]").appName("cluster-test").getOrCreate()
    yield s


def test_rows_to_dataframe_schema_and_where(spark):
    df = cl.rows_to_dataframe([_row(), _row(fn="rst_avg")], spark, where="cluster")
    cols = df.columns
    assert len(cols) == 71
    assert {"run_mode", "concurrent", "cluster_id", "part"} <= set(
        cols
    )  # how the run was set up
    assert "output_fingerprint" in cols
    assert "iter_total_wall_clock_s" in cols
    assert "avg_wall_clock_s" in cols
    assert "per_tile_avg_s" in cols
    assert "per_tile_avg_ms" in cols
    # run_event_num is the FIRST column (monotonic per-run event index).
    assert cols[0] == "run_event_num"
    # Column ORDER: the headline timing metrics sit right after `mode` (per_tile_avg_s
    # immediately left of per_tile_avg_ms), the per-iter distribution (iter_*) trails,
    # then the virtual-tile fields, plan_s, the FILE-matrix dimensions, and the levels leg's
    # level/form/compute + attribution split last.
    assert cols == cl.ORDER
    mo = cols.index("mode")
    assert cols[mo + 1] == "avg_wall_clock_s"
    assert cols[mo + 2] == "per_tile_avg_s"
    assert cols[mo + 3] == "per_tile_avg_ms"
    assert cols[-26:-6] == [
        "level",
        "form",
        "compute",
        "ser_sent_bytes",
        "ser_recv_bytes",
        "py_eval_s",
        "py_eval_frac",
        "native_s",
        "shuffle_bytes",
        "spill_bytes",
        "peak_exec_mem_bytes",
        "gc_s",
        "py_worker_rss_mb",
        "failure_mode",
        "setup_s",
        "warmup_s",
        "run_mode",
        "concurrent",
        "cluster_id",
        "part",
    ]
    assert cols[-39:-26] == [
        "iter_min_s",
        "iter_p90_s",
        "iter_total_wall_clock_s",
        "split_strategy",
        "input_tile",
        "output_disposition",
        "plan_s",
        "file_mode",
        "layout",
        "input_partitions",
        "launched_tasks",
        "slots_available",
        "chunk_size",
    ]
    assert cols[-6:] == [
        "env_spark_version",
        "env_pyspark_version",
        "env_dbr_version",
        "env_version",
        "env_caps",
        "path",
    ]
    vals = {r["fn"]: r["env_where"] for r in df.collect()}
    assert vals == {"rst_width": "cluster", "rst_avg": "cluster"}


def test_the_spark_provenance_columns_round_trip_and_older_rows_read_empty(spark):
    row = _row(
        env_spark_version="4.3.0",
        env_pyspark_version="4.3.0.dev0",
        env_dbr_version="19.x-snapshot-scala2.13",
        env_version="6",
        env_caps='{"arrow_grouped_agg_iter":"on:override"}',
        path="merge_agg:arrow_iter",
    )
    got = cl.rows_to_dataframe([row, _row()], spark).collect()
    by_path = {r["path"]: r for r in got}
    assert by_path["merge_agg:arrow_iter"]["env_spark_version"] == "4.3.0"
    assert by_path["merge_agg:arrow_iter"]["env_caps"].startswith('{"arrow_grouped')
    assert by_path[""]["env_spark_version"] == ""  # a row that did not record them


def test_run_heavy_runs_only_the_specs_marked_heavy():
    """The emitted run_heavy hands the JVM only the specs whose ``heavy`` flag is set."""
    assert "if mode in f.modes and f.heavy" in cl._HEAVY_HELPERS


def test_quadbin_grid_cell_has_no_heavyweight_leg():
    """Quadbin has no heavyweight form: the grid cell times and registers the lightweight level only."""
    assert 'api="heavyweight"' not in cl._CELL_GRID_QUADBIN
    assert "HEAVYWEIGHT" not in cl._CELL_GRID_QUADBIN
    assert 'level="L1"' not in cl._CELL_GRID_QUADBIN


def _cfg(**kw):
    base = dict(
        wheel="/Volumes/c/s/v/geobrix-0.4.3-py3-none-any.whl",
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


def _all_source(nb):
    return "\n".join("".join(c.get("source", [])) for c in nb["cells"])


def _section_cells(nb):
    # The per-(tier x mode) section cells are the ones whose first line is the "# (x)" tag.
    return [
        "".join(c["source"])
        for c in nb["cells"]
        if "".join(c["source"]).lstrip().startswith("# (")
    ]


def test_build_bench_notebook_install_setup_and_exit_cells():
    """The classic install is a two-step subprocess (force-reinstall the geobrix CODE only via --no-deps, then a
    non-forced deps install), not the old single %pip URL reference; the setup cell is collapsed and the install
    cell is not."""
    nb = cl.build_bench_notebook(_cfg())
    src = _all_source(nb)
    assert nb["nbformat"] == 4
    assert "/Volumes/c/s/v/geobrix-0.4.3-py3-none-any.whl" in src
    assert "--force-reinstall" in src and "--no-deps" in src
    assert (
        "'geobrix @ file://' + WHL" in src
    )  # the env wheel's own deps, no light extra
    assert "light-dbr19" not in src and "light_dbr19" not in src
    assert "HeavyBenchMain" in src and "_jvm" in src and "bench_results" in src
    assert "dbutils.notebook.exit" in src
    setup = nb["cells"][1]
    assert "import json" in "".join(setup["source"])
    assert setup["metadata"].get("collapsed") is True
    assert setup["metadata"].get("jupyter", {}).get("source_hidden") is True
    assert nb["cells"][0]["metadata"].get("collapsed") is not True


def test_build_bench_notebook_puts_the_launchers_wheel_guard_right_after_the_install():
    """The launcher hands the guard cell's source in; it follows the install cell (and the restart it ends with), so it
    checks the geobrix the run will use. Without one (a direct caller) the notebook is as it was."""
    plain = cl.build_bench_notebook(_cfg())
    guarded = cl.build_bench_notebook(_cfg(guard_cell="WHEEL_GUARD = run_guard({'wheel': '/Volumes/c/s/v/x.whl'}, spark=spark)\n"))
    assert len(guarded["cells"]) == len(plain["cells"]) + 1
    install = next(i for i, c in enumerate(guarded["cells"]) if "restartPython" in "".join(c["source"]))
    assert "WHEEL_GUARD = run_guard(" in "".join(guarded["cells"][install + 1]["source"])
    assert "run_guard(" not in _all_source(plain)


@pytest.mark.parametrize(
    "overrides,sections",
    [
        (
            {},
            [
                ("lightweight", "pure-core"),
                ("heavyweight", "pure-core"),
                ("lightweight", "spark-path"),
                ("heavyweight", "spark-path"),
            ],
        ),
        (
            {"modes": "pure-core"},
            [("lightweight", "pure-core"), ("heavyweight", "pure-core")],
        ),
        (
            {"heavyweight": False},
            [("lightweight", "pure-core"), ("lightweight", "spark-path")],
        ),
        ({"lightweight": False, "modes": "pure-core"}, [("heavyweight", "pure-core")]),
    ],
)
def test_build_bench_notebook_one_cell_per_tier_and_mode_in_order(overrides, sections):
    """Each (tier, mode) is one section cell, in pure-core then spark-path order with light before heavy, calling
    show_section so its table renders when the cell finishes; a tier that is off leaves no trace of it.
    """
    nb = cl.build_bench_notebook(_cfg(**overrides))
    secs = _section_cells(nb)
    assert len(secs) == len(sections)
    for sec, (tier, mode) in zip(secs, sections):
        runner = "run_light" if tier == "lightweight" else "run_heavy"
        assert f'show_section("{tier}", "{mode}", {runner}("{mode}"))' in sec
    src = _all_source(nb)
    assert ("HeavyBenchMain" in src) == overrides.get("heavyweight", True)
    assert ("run_pure_core" in src or "run_spark_path" in src) == overrides.get(
        "lightweight", True
    )
    if not overrides:
        assert len(nb["cells"]) == 8  # install + setup + 4 sections + epilogue + exit


@pytest.mark.parametrize(
    "overrides,present",
    [
        ({}, ["RESUME = False", "PARTITION_SIZE = 0"]),
        (
            {"resume": True},
            [
                "RESUME = True",
                "def _done_fns(",
                "def _purge_errors(",
                "TRUNCATE_ALL = False",
                "TRUNCATE = False",
                "FIX_ERRORS = True",
            ],
        ),
        ({"resume": True, "fix_errors": False}, ["FIX_ERRORS = False"]),
        (
            {"partition_size": 8},
            ["PARTITION_SIZE = 8", "partition_size=PARTITION_SIZE"],
        ),
    ],
)
def test_build_bench_notebook_threads_the_run_options(overrides, present):
    src = _all_source(cl.build_bench_notebook(_cfg(**overrides)))
    assert all(p in src for p in present), [p for p in present if p not in src]
    if overrides.get("resume"):
        assert (
            src.count("_done_fns(") >= 3
        )  # the def and run_light / run_heavy consulting it


def test_remap_heavy_iter_to_seconds():
    # Heavy Scala jsonl emits MILLISECOND keys; remap renames to *_s and /1000, and derives
    # per_tile_avg_{s,ms} from the median + rows (Scala emits neither).
    d = cl._remap_heavy_iter_to_seconds(
        {
            "fn": "rst_width",
            "rows": 1000,
            "median_ms": 70785.0,
            "min_ms": 70000.0,
            "p90_ms": 71000.0,
            "total_wall_clock_ms": 75000.0,
            "avg_wall_clock_ms": 72000.0,
        }
    )
    assert d["iter_median_s"] == pytest.approx(70.785)
    assert d["iter_min_s"] == pytest.approx(70.0)
    assert d["iter_p90_s"] == pytest.approx(71.0)
    assert d["iter_total_wall_clock_s"] == pytest.approx(75.0)
    assert d["avg_wall_clock_s"] == pytest.approx(72.0)
    # per-tile derived from the original ms median / rows
    assert d["per_tile_avg_ms"] == pytest.approx(70.785)
    assert d["per_tile_avg_s"] == pytest.approx(0.070785)
    # old ms keys removed
    assert "median_ms" not in d and "avg_wall_clock_ms" not in d


def test_remap_heavy_iter_to_seconds_zero_rows():
    # No rows -> per-tile is 0 (no divide-by-zero), timings still convert.
    d = cl._remap_heavy_iter_to_seconds({"rows": 0, "median_ms": 5000.0})
    assert d["iter_median_s"] == pytest.approx(5.0)
    assert d["per_tile_avg_ms"] == 0.0 and d["per_tile_avg_s"] == 0.0


def test_build_bench_notebook_redo_functions_threading():
    # Default: empty redo list; _purge_functions present but a no-op.
    src_default = "\n".join(
        "".join(c.get("source", [])) for c in cl.build_bench_notebook(_cfg())["cells"]
    )
    assert "REDO_FUNCTIONS = ''" in src_default
    assert "def _purge_functions(" in src_default
    # --redo-functions <csv> -> the list threads through, INDEPENDENT of --set/--functions,
    # and _purge_functions is wired into both run_light and run_heavy.
    src_redo = "\n".join(
        "".join(c.get("source", []))
        for c in cl.build_bench_notebook(
            _cfg(redo_functions="rst_combineavg_agg,rst_merge_agg")
        )["cells"]
    )
    assert "REDO_FUNCTIONS = 'rst_combineavg_agg,rst_merge_agg'" in src_redo
    assert src_redo.count("_purge_functions(") >= 3  # def + light + heavy call sites


def test_build_bench_notebook_explain_only_threading():
    # Default: EXPLAIN_ONLY = False (no plan-dump branch taken).
    src_default = "\n".join(
        "".join(c.get("source", [])) for c in cl.build_bench_notebook(_cfg())["cells"]
    )
    assert "EXPLAIN_ONLY = False" in src_default
    # explain_only -> EXPLAIN_ONLY True + run_spark_path invoked with explain_only=True
    # so run_light prints/persists plans instead of timing.
    src_ex = "\n".join(
        "".join(c.get("source", []))
        for c in cl.build_bench_notebook(
            _cfg(explain_only=True, modes="spark-path", heavyweight=False)
        )["cells"]
    )
    assert "EXPLAIN_ONLY = True" in src_ex
    assert "explain_only=True" in src_ex
    assert "explain_dir=EXPLAIN_DIR" in src_ex


def _build_min_notebook(cl, input_tile="materialized"):
    """Render the bench notebook source string for the given input_tile value."""
    cfg = _cfg(input_tile=input_tile)
    return "\n".join(
        "".join(c.get("source", [])) for c in cl.build_bench_notebook(cfg)["cells"]
    )


def test_notebook_threads_input_tile():
    from databricks.labs.gbx.bench import cluster as cl  # noqa: F811

    src = _build_min_notebook(cl, input_tile="virtual")
    assert "INPUT_TILE" in src
    assert "input_tile=INPUT_TILE" in src
    assert "'virtual'" in src or '"virtual"' in src


def _bng_cell_legs(functions, api="heavyweight"):
    """Run the BNG grid cell's leg table with stub legs for one tier; return the leg names it calls."""
    src = cl._CELL_GRID_BNG.split("\nif LIGHTWEIGHT:")[0]
    from databricks.labs.gbx.bench import spec

    ns = {
        "spark": None,
        "RUN_ID": "r",
        "SPARK_WARMUP": 0,
        "SPARK_MEASURED": 1,
        "FUNCTIONS": functions,
        "_s": spec,
    }
    exec(src, ns)  # noqa: S102 -- the cell's own definitions, as the notebook runs them

    class _StubReaders:
        def __getattr__(self, name):
            def _leg(*_a, api, **_kw):
                return (name, api)

            return _leg

    ns["_rd"] = _StubReaders()
    return [name for name, _api in ns["_bng_legs"](api)]


def test_bng_grid_cell_runs_only_the_bng_legs_named_in_functions():
    """--functions with bng_* names selects those BNG legs; other names are ignored."""
    legs = _bng_cell_legs("rst_slope,bng_kringexplode,bng_tessellateexplode")
    assert legs == ["run_bng_kringexplode", "run_bng_tessellateexplode"]


def test_bng_grid_cell_runs_every_leg_without_bng_functions():
    """Light runs all 23 legs; heavy runs 22: eastnorthasbng has no JVM form (it measured slower than light)."""
    light = _bng_cell_legs("rst_slope,rst_ndvi", api="lightweight")
    assert len(light) == 23 and len(set(light)) == 23
    assert (
        light[0] == "run_bng_pointascell" and light[-1] == "run_bng_tessellateexplode"
    )
    heavy = _bng_cell_legs("rst_slope,rst_ndvi")
    assert heavy == [leg for leg in light if leg != "run_bng_eastnorthasbng"]
    assert len(heavy) == 22


def test_bng_grid_cell_rejects_an_unknown_bng_name():
    """A misspelt bng_* name stops the cell instead of silently running every leg."""
    with pytest.raises(ValueError, match="bng_kringexplod"):
        _bng_cell_legs("bng_kringexplod")


def test_bng_grid_cell_ignores_bng_functions_of_the_function_benchmark():
    """bng_cellfill is a registered bench function, not a BNG leg: all legs of the tier run."""
    assert len(_bng_cell_legs("bng_cellfill", api="lightweight")) == 23
    assert len(_bng_cell_legs("bng_cellfill")) == 22


@pytest.mark.parametrize(
    "serverless, extras, want",
    [
        (True, "", "'geobrix[zarr] @ file://'"),
        (False, "", "'geobrix[zarr] @ file://'"),
        (False, "zarr,vizx", "'geobrix[zarr,vizx] @ file://'"),
    ],
)
def test_the_zarr_cell_runs_alone_with_the_env_wheel_zarr_extra(
    serverless, extras, want
):
    """--zarr-only: one Zarr cell carrying the launcher's settings, no function sections,
    and the install cell adds the env wheel's zarr extra (or --extras)."""
    zarr = dict(
        zarr_store="/Volumes/c/s/v/era5.zarr",
        zarr_cog_dir="/Volumes/c/s/v/cog",
        zarr_scratch="/Volumes/c/s/v/scratch",
        zarr_schema="c.scratch",
        zarr_cases="R1,W1",
        zarr_reps=3,
    )
    nb = cl.build_bench_notebook(
        _cfg(
            heavyweight=False,
            modes="spark-path",
            zarr_only=True,
            serverless=serverless,
            extras=extras,
            **zarr,
        )
    )
    srcs = ["".join(c["source"]) for c in nb["cells"]]
    assert want in srcs[0] and "light_" not in srcs[0] and "light-" not in srcs[0]
    assert not _section_cells(nb)
    (cell,) = [s for s in srcs if "_zb.run_zarr_cases" in s]
    assert "'cases': ('R1', 'W1')" in cell and f"'serverless': {serverless}" in cell
    assert "'store': '/Volumes/c/s/v/era5.zarr'" in cell
    # W1 is selected: the schema of its manifest tables is made if the catalog lacks it
    assert "CREATE SCHEMA IF NOT EXISTS {_zcfg.schema}" in cell
    compile(cell, "zarr_cell", "exec")
    # on Serverless the job environment can install everything: the cell only reports it
    if serverless:
        env = cl.build_bench_notebook(
            _cfg(
                heavyweight=False,
                zarr_only=True,
                serverless=True,
                env_deps=True,
                **zarr,
            )
        )
        assert "installed by the job environment" in "".join(env["cells"][0]["source"])
    # without a Zarr leg the install cell is unchanged: the env wheel, no extra
    assert "'geobrix @ file://' + WHL" in "".join(
        cl.build_bench_notebook(_cfg())["cells"][0]["source"]
    )


def test_a_runner_notebook_starts_with_the_header_cell_when_the_launcher_gives_one():
    nb = cl.build_bench_notebook(_cfg(header_md="## r\n- **Testing:** x\n"))
    first = nb["cells"][0]
    assert (
        first["cell_type"] == "markdown"
        and "outputs" not in first
        and "execution_count" not in first
    )
    assert "".join(first["source"]).startswith("## r")
    assert "WHL" in "".join(nb["cells"][1]["source"])  # the install cell follows it
    assert (
        cl.build_bench_notebook(_cfg())["cells"][0]["cell_type"] == "code"
    )  # no header: as before


def test_the_smoke_header_says_it_is_the_smoke_and_what_gates_on_it():
    cfg = _cfg(
        levels_mvp="IO", out_dir="/Volumes/o/r1-smoke", table="c.s.t", run_id="r1-smoke"
    )
    head = cl.bench_header(
        cfg,
        dict(
            purpose="why",
            smoke=True,
            compute="dedicated",
            profile="p",
            sha="abc",
            ceiling_min=60,
        ),
    )
    assert (
        head.startswith("## r1-smoke (smoke)")
        and "1 scale (smoke rows)" in head
        and "the smoke gates the real run" in head
    )
    assert "~2-3 min" in head
