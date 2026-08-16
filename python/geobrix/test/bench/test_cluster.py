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
    assert len(cols) == 39
    assert "output_fingerprint" in cols
    assert "iter_total_wall_clock_s" in cols
    assert "avg_wall_clock_s" in cols
    assert "per_tile_avg_s" in cols
    assert "per_tile_avg_ms" in cols
    # run_event_num is the FIRST column (monotonic per-run event index).
    assert cols[0] == "run_event_num"
    # Column ORDER: the headline timing metrics sit right after `mode` (per_tile_avg_s
    # immediately left of per_tile_avg_ms), the per-iter distribution (iter_*) trails,
    # then the virtual-tile fields, and `plan_s` is the final column.
    assert cols == cl.ORDER
    mo = cols.index("mode")
    assert cols[mo + 1] == "avg_wall_clock_s"
    assert cols[mo + 2] == "per_tile_avg_s"
    assert cols[mo + 3] == "per_tile_avg_ms"
    assert cols[-7:] == [
        "iter_min_s",
        "iter_p90_s",
        "iter_total_wall_clock_s",
        "split_strategy",
        "input_tile",
        "output_disposition",
        "plan_s",
    ]
    vals = {r["fn"]: r["env_where"] for r in df.collect()}
    assert vals == {"rst_width": "cluster", "rst_avg": "cluster"}


def test_build_bench_notebook_cells():
    cfg = dict(
        wheel="/Volumes/c/s/v/geobrix-0.4.3-py3-none-any.whl",
        corpus="/Volumes/c/s/v/bench-corpus",
        out_dir="/Volumes/c/s/v/bench-out/run1",
        table="main.default.bench_results",
        run_id="run1",
        functions="rst_width,rst_slope",
        modes="both",
        row_counts="10,100",
        warmup=2,
        measured=5,
        heavyweight=True,
        lightweight=True,
    )
    nb = cl.build_bench_notebook(cfg)
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert (
        'geobrix[light-dbr19] @ file:///Volumes/c/s/v/geobrix-0.4.3-py3-none-any.whl'
        in src
    )
    assert "HeavyBenchMain" in src and "_jvm" in src
    assert "run_spark_path" in src or "run_pure_core" in src
    assert "bench_results" in src
    assert "dbutils.notebook.exit" in src
    assert nb["nbformat"] == 4


def test_build_bench_notebook_lightweight_only_omits_heavyweight():
    cfg = dict(
        wheel="w.whl",
        corpus="c",
        out_dir="o",
        table="t",
        run_id="r",
        functions="",
        modes="both",
        row_counts="10",
        warmup=1,
        measured=1,
        heavyweight=False,
        lightweight=True,
    )
    nb = cl.build_bench_notebook(cfg)
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "HeavyBenchMain" not in src  # heavyweight leg genuinely absent
    assert (
        "run_pure_core" in src or "run_spark_path" in src
    )  # lightweight still present


def test_build_bench_notebook_heavyweight_only_omits_lightweight():
    cfg = dict(
        wheel="w.whl",
        corpus="c",
        out_dir="o",
        table="t",
        run_id="r",
        functions="",
        modes="pure-core",
        row_counts="10",
        warmup=1,
        measured=1,
        heavyweight=True,
        lightweight=False,
    )
    nb = cl.build_bench_notebook(cfg)
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "HeavyBenchMain" in src
    assert "run_pure_core" not in src and "run_spark_path" not in src


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


def _section_cells(nb):
    # The per-(tier x mode) section cells are the ones whose first line is the "# (x)" tag.
    return [
        "".join(c["source"])
        for c in nb["cells"]
        if "".join(c["source"]).lstrip().startswith("# (")
    ]


def test_build_bench_notebook_one_cell_per_section_in_order():
    # both tiers + both modes -> 4 section cells in pure-core(light,heavy), spark(light,heavy)
    # order, each calling show_section so its table+summary render when the cell finishes.
    nb = cl.build_bench_notebook(_cfg())
    secs = _section_cells(nb)
    assert len(secs) == 4
    assert 'show_section("lightweight", "pure-core", run_light("pure-core"))' in secs[0]
    assert 'show_section("heavyweight", "pure-core", run_heavy("pure-core"))' in secs[1]
    assert (
        'show_section("lightweight", "spark-path", run_light("spark-path"))' in secs[2]
    )
    assert (
        'show_section("heavyweight", "spark-path", run_heavy("spark-path"))' in secs[3]
    )
    # 1 install cell ([light-dbr19] @ file:// -- no uninstall/restartPython dance) +
    # setup + 4 sections + epilogue + exit (exit is its own cell so the compare
    # summary render isn't truncated -- see build_bench_notebook)
    assert len(nb["cells"]) == 8
    src = "\n".join("".join(c["source"]) for c in nb["cells"])
    assert "def show_section(" in src
    assert "dbutils.notebook.exit" in src


def test_build_bench_notebook_pure_core_only_has_no_spark_section():
    nb = cl.build_bench_notebook(_cfg(modes="pure-core"))
    secs = _section_cells(nb)
    assert len(secs) == 2  # light pure-core, heavy pure-core
    assert all("spark-path" not in s for s in secs)


def test_build_bench_notebook_lightweight_both_modes_two_sections():
    nb = cl.build_bench_notebook(_cfg(heavyweight=False))
    secs = _section_cells(nb)
    assert len(secs) == 2  # light pure-core, light spark-path
    assert all("run_heavy" not in s for s in secs)
    assert 'show_section("lightweight", "pure-core"' in secs[0]
    assert 'show_section("lightweight", "spark-path"' in secs[1]


def test_build_bench_notebook_resume_emits_function_granular_logic():
    nb = cl.build_bench_notebook(_cfg(resume=True))
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "RESUME = True" in src
    # function-granular resume: load done fns, run only the missing ones
    assert "def _done_fns(" in src
    assert "def _purge_errors(" in src
    # run_light / run_heavy consult _done_fns (2 call sites + the def)
    assert src.count("_done_fns(") >= 3
    # resume keeps existing rows -> no truncate
    assert "TRUNCATE_ALL = False" in src
    assert "TRUNCATE = False" in src


def test_build_bench_notebook_default_no_resume():
    nb = cl.build_bench_notebook(_cfg())
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "RESUME = False" in src


def test_build_bench_notebook_partition_size_threading():
    # default -> PARTITION_SIZE = 0 (auto: n/(slots*4)); --override-partition-size flows through.
    src_default = "\n".join(
        "".join(c.get("source", [])) for c in cl.build_bench_notebook(_cfg())["cells"]
    )
    assert "PARTITION_SIZE = 0" in src_default
    src_ov = "\n".join(
        "".join(c.get("source", []))
        for c in cl.build_bench_notebook(_cfg(partition_size=8))["cells"]
    )
    assert "PARTITION_SIZE = 8" in src_ov
    assert "partition_size=PARTITION_SIZE" in src_ov


def test_build_bench_notebook_fix_errors_default_and_override():
    # Default: fix errors on resume (purge + re-run errored fns).
    src_default = "\n".join(
        "".join(c.get("source", []))
        for c in cl.build_bench_notebook(_cfg(resume=True))["cells"]
    )
    assert "FIX_ERRORS = True" in src_default
    # --no-fix-errors -> keep errored fns as-is.
    src_no = "\n".join(
        "".join(c.get("source", []))
        for c in cl.build_bench_notebook(_cfg(resume=True, fix_errors=False))["cells"]
    )
    assert "FIX_ERRORS = False" in src_no


def test_build_bench_notebook_setup_cell_collapsed():
    # cells[1] (the big setup cell) is collapsed by default; the single install cell
    # ([light-dbr19] @ file://) and the section cells are not.
    nb = cl.build_bench_notebook(_cfg())
    setup = nb["cells"][1]
    assert "import json" in "".join(setup["source"])  # it's the assembled setup, sanity
    assert setup["metadata"].get("collapsed") is True
    assert setup["metadata"].get("jupyter", {}).get("source_hidden") is True
    # install cell (cells[0]) stays expanded
    assert nb["cells"][0]["metadata"].get("collapsed") is not True


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
