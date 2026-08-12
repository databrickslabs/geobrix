import time

import pytest

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import runner as rn
from databricks.labs.gbx.bench import spec as s


def test_time_iters_returns_distribution():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        time.sleep(0.001)

    stats = rn.time_iters(fn, warmup=2, measured=5)
    assert calls["n"] == 7  # warmup + measured
    assert stats["measured_iters"] == 5
    assert stats["iter_median_ms"] >= 0.5
    assert (
        stats["iter_min_ms"] <= stats["iter_median_ms"] <= stats["iter_p90_ms"] + 1e-6
    )


def test_time_iters_warmup_fn_used_for_warmup_only():
    # warm-up iters run warmup_fn (the cheap, slot-spread stand-in); measured iters run fn.
    n = {"warm": 0, "meas": 0}

    def fn():
        n["meas"] += 1

    def warm():
        n["warm"] += 1

    rn.time_iters(fn, warmup=2, measured=3, warmup_fn=warm)
    assert n["warm"] == 2  # all warm-up iters used warmup_fn
    assert n["meas"] == 3  # measured iters used fn only (not warmup_fn)


def test_time_iters_defaults_warmup_to_fn():
    # Without warmup_fn, warm-up falls back to fn (warmup + measured calls total).
    n = {"c": 0}

    def fn():
        n["c"] += 1

    rn.time_iters(fn, warmup=2, measured=3)
    assert n["c"] == 5


def test_capture_env_has_required_fields():
    env = rn.capture_env(where="venv")
    for k in (
        "env_arch",
        "env_os",
        "env_cpu_count",
        "env_gdal_version",
        "env_gbx_version",
        "env_where",
    ):
        assert k in env
    assert env["env_where"] == "venv"


def test_run_pure_core_produces_ok_rows(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=9,
        tile_px=[32, 64],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width", "rst_avg"])
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        warmup=1,
        measured=2,
        where="venv",
    )
    assert rows, "expected result rows"
    assert all(r.status == "ok" for r in rows)
    assert {r.fn for r in rows} == {"rst_width", "rst_avg"}
    assert all(r.mode == "pure-core" and r.rows == 1 for r in rows)
    # consistency fingerprint captured for every pure-core row
    assert all(r.output_fingerprint for r in rows)
    # one row per (fn x size_sweep tile). Non-BNG fns skip the GB-only tile
    # (role "bng_gb"), so the expected tile count excludes it.
    _sweep = [t for t in corpus.size_sweep if t.role != "bng_gb"]
    assert len(rows) == 2 * len(_sweep)


def test_run_pure_core_geometry_in_fns_produce_raster_fingerprints(tmp_path):
    # bucket D: the 3 geometry-in constructors burn/interpolate the tile's
    # GeometrySet (boxes/points/zpoints) into a NEW raster at the tile's own
    # extent/size/srid. They run via input_kind == "geometry" (core_fn(ds, args,
    # geom)) and emit a comparable raster fingerprint.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=9,
        tile_px=[32, 64],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=1,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(
        functions=["rst_rasterize", "rst_gridfrompoints", "rst_dtmfromgeoms"]
    )
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected result rows"
    assert all(r.status == "ok" for r in rows), [
        (r.fn, r.status, r.note) for r in rows if r.status != "ok"
    ]
    assert {r.fn for r in rows} == {
        "rst_rasterize",
        "rst_gridfrompoints",
        "rst_dtmfromgeoms",
    }
    # raster output -> a non-empty fingerprint classified as "raster"
    import json

    for r in rows:
        assert r.output_fingerprint, r.fn
        fp = json.loads(r.output_fingerprint)
        assert fp["kind"] == "raster", (r.fn, fp.get("kind"))
    _sweep = [t for t in corpus.size_sweep if t.role != "bng_gb"]
    assert len(rows) == 3 * len(_sweep)


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    from pyspark.sql import SparkSession

    # Pin worker + driver Python to this interpreter so local executors match the
    # driver minor version (otherwise PYTHON_VERSION_MISMATCH on Python UDFs).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    sess = (
        SparkSession.builder.master("local[2]")
        .appName("bench-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield sess


def test_run_spark_path_produces_ok_rows(tmp_path, spark):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=4,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=6,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=2,
        where="venv",
    )
    assert rows and all(r.status == "ok" for r in rows)
    assert all(r.mode == "spark-path" and r.fn == "rst_width" for r in rows)
    # per_tile_avg_s is the amortized per-tile wall-clock (iter_median_s / rows);
    # per_tile_avg_ms is the same value in milliseconds.
    for r in rows:
        assert r.per_tile_avg_s == pytest.approx(r.iter_median_s / r.rows, rel=1e-9)
        assert r.per_tile_avg_ms == pytest.approx(r.per_tile_avg_s * 1000.0, rel=1e-9)
    assert sorted({r.rows for r in rows}) == [2, 4]


def test_run_pure_core_emits_per_fn_progress(tmp_path, capsys):
    # Per-function progress lines must be printed to stdout as each fn finishes:
    # leg header "[bench] lightweight pure-core: N fn(s) to run", then per-fn
    # "[i/N] fn_name  lightweight pure-core  <ms>  <status>" for each fn.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=11,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width", "rst_avg"])
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="prog-test",
        warmup=1,
        measured=1,
        where="venv",
    )
    out = capsys.readouterr().out
    lines = out.splitlines()
    # Leg header is the first line.
    assert any("[bench] lightweight pure-core:" in ln for ln in lines)
    # One progress line per fn (2 fns).
    fn_lines = [
        ln for ln in lines if "lightweight pure-core" in ln and "[bench]" not in ln
    ]
    assert len(fn_lines) == 2
    # Each progress line contains "i/N", the fn name, tier+mode, and a status.
    for i, fn_name in enumerate(["rst_width", "rst_avg"], 1):
        matching = [ln for ln in fn_lines if fn_name in ln]
        assert matching, f"no progress line for {fn_name}"
        pl = matching[0]
        # i/N prefix
        assert f"[{i}/2]" in pl or f"/{2}]" in pl
        assert "lightweight pure-core" in pl
        assert "ok" in pl or "error" in pl or "na_by_design" in pl
    # Result rows are unaffected.
    assert rows and all(r.fn in {"rst_width", "rst_avg"} for r in rows)


def test_run_pure_core_progress_on_error_fn(tmp_path, capsys, monkeypatch):
    # When a fn raises, its progress line must still be printed with status=error,
    # and the remaining fns must still run (the loop continues).
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=13,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width", "rst_avg"])
    import databricks.labs.gbx.bench.runner as _rn_mod

    _orig = _rn_mod._capture_and_call

    call_count = {"n": 0}

    def _patched_cap(fs, *a, **kw):
        call_count["n"] += 1
        if fs.name == "rst_width":
            raise RuntimeError("simulated error")
        return _orig(fs, *a, **kw)

    monkeypatch.setattr(_rn_mod, "_capture_and_call", _patched_cap)
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="err-test",
        warmup=1,
        measured=1,
        where="venv",
    )
    out = capsys.readouterr().out
    lines = out.splitlines()
    fn_lines = [
        ln for ln in lines if "lightweight pure-core" in ln and "[bench]" not in ln
    ]
    # Both fns produce a progress line.
    assert len(fn_lines) == 2
    err_lines = [ln for ln in fn_lines if "rst_width" in ln]
    assert err_lines and "error" in err_lines[0]
    ok_lines = [ln for ln in fn_lines if "rst_avg" in ln]
    assert ok_lines and "ok" in ok_lines[0]
    # Error rows present for rst_width; ok rows present for rst_avg.
    assert any(r.fn == "rst_width" and r.status == "error" for r in rows)
    assert any(r.fn == "rst_avg" and r.status == "ok" for r in rows)


def test_run_spark_path_emits_per_fn_progress(tmp_path, spark, capsys):
    # Per-function progress lines for spark-path:
    # leg header "[bench] lightweight spark-path: N fn(s) to run", then per-fn line.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=17,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="sp-prog",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert any("[bench] lightweight spark-path:" in ln for ln in lines)
    fn_lines = [
        ln for ln in lines if "lightweight spark-path" in ln and "[bench]" not in ln
    ]
    assert len(fn_lines) == 1
    line = fn_lines[0]
    assert "[1/1]" in line
    assert "rst_width" in line
    assert "lightweight spark-path" in line
    assert "ok" in line or "error" in line
    # Result rows unaffected.
    assert rows and all(r.fn == "rst_width" for r in rows)


def test_emit_explain_prints_and_writes_file(tmp_path, spark, capsys):
    # _emit_explain echoes a labeled formatted plan to stdout AND tees it to
    # {explain_dir}/{label}.explain.txt so a run's plans can be harvested off a Volume.
    df = spark.range(4).selectExpr("id", "id * 2 AS dbl")
    rn._emit_explain("demo/label", df, str(tmp_path))
    captured = capsys.readouterr().out
    assert "EXPLAIN: demo/label" in captured
    # label slug sanitizes the "/" so the filename is filesystem-safe
    f = tmp_path / "demo_label.explain.txt"
    assert f.exists()
    body = f.read_text()
    assert "EXPLAIN: demo/label" in body and "Physical Plan" in body


def test_run_spark_path_explain_only_writes_plans_no_rows(tmp_path, spark):
    # --explain-only builds each spark-path fn's plan, prints/persists it, and returns NO
    # rows (no timing, no sink). Exercises the explain_only + partition_size params and the
    # explain_dir tee.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=7,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    explain_dir = tmp_path / "explain"
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=s.select(functions=["rst_width"]),
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
        partition_size=1,
        explain_only=True,
        explain_dir=str(explain_dir),
    )
    assert rows == []  # explain-only produces no result rows
    written = list(explain_dir.glob("rst_width*.explain.txt"))
    assert written, "expected an explain file for rst_width"
    assert "Physical Plan" in written[0].read_text()


def test_runner_main_writes_shard(tmp_path):
    dg.generate_corpus(
        out_dir=tmp_path,
        seed=2,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    out = tmp_path / "lw.jsonl"
    rn.main(
        [
            "--corpus",
            str(tmp_path),
            "--out",
            str(out),
            "--functions",
            "rst_width",
            "--mode",
            "pure-core",
            "--row-counts",
            "2,4",
            "--warmup",
            "1",
            "--measured",
            "2",
            "--run-id",
            "cli",
        ]
    )
    from databricks.labs.gbx.bench import results as r

    rows = r.read_jsonl(out)
    assert rows and all(x.fn == "rst_width" for x in rows)


def test_spark_path_runs_a_warmup_before_timing(tmp_path, spark, monkeypatch):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=5,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    calls = {"noop_saves": 0}
    import pyspark.sql.readwriter

    orig_save = pyspark.sql.readwriter.DataFrameWriter.save

    def counting_save(self, *a, **k):
        calls["noop_saves"] += 1
        return orig_save(self, *a, **k)

    monkeypatch.setattr(pyspark.sql.readwriter.DataFrameWriter, "save", counting_save)
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=s.select(functions=["rst_width"]),
        run_id="t",
        row_counts=[2],
        warmup=1,
        measured=1,
        where="venv",
    )
    # 1 warm-up + (warmup 1 + measured 1) timed saves for the single (fn,rows) cell = 3
    assert calls["noop_saves"] >= 3
    assert rows and all(r.status == "ok" for r in rows)


def test_run_spark_path_tile_aggregators_produce_raster_fingerprints(tmp_path, spark):
    # bucket A (tile aggregators): combineavg/merge/frombands/derivedband each reduce
    # a fixed deterministic group (the synthesized tiles for the recipe) to ONE output
    # tile via df.groupBy(key).agg(col_fn(...)). The harness collects the single out
    # tile and fingerprints its raster bytes (consistency), and times the scaled
    # groupBy (perf). Both signals are exercised here on local[2].
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=8,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    fns = s.select(
        functions=[
            "rst_combineavg_agg",
            "rst_merge_agg",
            "rst_frombands_agg",
            "rst_derivedband_agg",
        ]
    )
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected aggregate rows"
    assert all(r.status == "ok" for r in rows), [
        (r.fn, r.status, r.note) for r in rows if r.status != "ok"
    ]
    assert {r.fn for r in rows} == {
        "rst_combineavg_agg",
        "rst_merge_agg",
        "rst_frombands_agg",
        "rst_derivedband_agg",
    }
    assert all(r.mode == "spark-path" for r in rows)
    # consistency: the smallest-N row carries a raster fingerprint (the single
    # aggregated output tile); larger-N rows are timing-only (empty fingerprint).
    import json

    for name in ("rst_combineavg_agg", "rst_merge_agg", "rst_frombands_agg"):
        fp = {r.rows: r.output_fingerprint for r in rows if r.fn == name}
        assert fp.get(2), f"{name} has no consistency fingerprint at N=2"
        parsed = json.loads(fp[2])
        assert parsed["kind"] == "raster", (name, parsed.get("kind"))


def test_run_aggregate_passes_minimal_warmup_fn(tmp_path, spark, monkeypatch):
    # The agg perf loop must hand time_iters a warmup_fn (the minimal one-key-per-partition
    # warm scaled), so the warm-up no longer re-aggregates the full n-key group. Spy on
    # time_iters to capture that a warmup_fn is supplied for the timed agg jobs, while still
    # producing correct ok rows + a consistency fingerprint.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=8,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_combineavg_agg"])

    saw_warmup_fn = {"count": 0}
    real_time_iters = rn.time_iters

    def spy(fn, warmup, measured, warmup_fn=None):
        if warmup_fn is not None:
            saw_warmup_fn["count"] += 1
        return real_time_iters(fn, warmup, measured, warmup_fn=warmup_fn)

    monkeypatch.setattr(rn, "time_iters", spy)

    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected aggregate rows"
    assert all(r.status == "ok" for r in rows), [
        (r.fn, r.status, r.note) for r in rows if r.status != "ok"
    ]
    # One timed agg job per row count (N=2, N=4), each given a minimal warmup_fn.
    assert saw_warmup_fn["count"] == 2, saw_warmup_fn["count"]
    import json

    fp = {r.rows: r.output_fingerprint for r in rows if r.fn == "rst_combineavg_agg"}
    assert fp.get(2), "no consistency fingerprint at N=2 with warm-up path"
    assert json.loads(fp[2])["kind"] == "raster"


def test_run_spark_path_geometry_aggregators_produce_raster_fingerprints(
    tmp_path, spark
):
    # bucket A (geometry aggregators): rasterize/gridfrompoints/dtmfromgeoms each
    # reduce a fixed group of (geom_wkb, value[, ...]) rows from the tile's
    # GeometrySet to ONE tile via df.groupBy(key).agg(col_fn(...)). Consistency =
    # the single out tile's raster fingerprint; perf = the scaled groupBy timing.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=8,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(
        functions=[
            "rst_rasterize_agg",
            "rst_gridfrompoints_agg",
            "rst_dtmfromgeoms_agg",
        ]
    )
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected aggregate rows"
    assert all(r.status == "ok" for r in rows), [
        (r.fn, r.status, r.note) for r in rows if r.status != "ok"
    ]
    assert {r.fn for r in rows} == {
        "rst_rasterize_agg",
        "rst_gridfrompoints_agg",
        "rst_dtmfromgeoms_agg",
    }
    import json

    for name in (
        "rst_rasterize_agg",
        "rst_gridfrompoints_agg",
        "rst_dtmfromgeoms_agg",
    ):
        fp = {r.rows: r.output_fingerprint for r in rows if r.fn == name}
        assert fp.get(2), f"{name} has no consistency fingerprint at N=2"
        parsed = json.loads(fp[2])
        assert parsed["kind"] == "raster", (name, parsed.get("kind"))


def test_h3_rasterize_cells_is_deterministic_331():
    # The fixed cell-set recipe (res-9 NYC cell grid-disk'd k=10) must yield a
    # stable 331-cell set -- the cross-tier sanity count the Scala BenchDispatch
    # also asserts (h3RaggNCells). Order-stable + unique.
    cells = s.h3_rasterize_cells()
    assert len(cells) == s._H3RAGG_N_CELLS == 331
    assert len(set(cells)) == 331
    # Re-running gives the identical ordered list (deterministic).
    assert s.h3_rasterize_cells() == cells
    # Signed Spark-Long range (h3 ids past 2^63 wrap negative).
    assert all(-(1 << 63) <= c < (1 << 63) for c in cells)


def test_h3_aggregate_df_builds_cellid_value_rows(spark):
    # The h3_aggregate group builder yields (cellid LONG, value DOUBLE) rows for
    # the fixed cell set, with value NULL on every row (presence-mask burn).
    fs = s.select(functions=["rst_h3_rasterize_agg"])[0]
    assert fs.input_kind == "h3_aggregate"
    df = rn._h3_aggregate_df(spark, fs)
    assert [f.name for f in df.schema.fields] == ["cellid", "value"]
    rows = df.collect()
    assert len(rows) == 331
    assert all(r["value"] is None for r in rows)
    assert {r["cellid"] for r in rows} == set(s.h3_rasterize_cells())


def test_run_spark_path_h3_rasterize_agg_produces_raster_fingerprint(tmp_path, spark):
    # bucket A (H3 rasterize aggregator): rst_h3_rasterize_agg reduces a group of
    # (cellid, value) rows -> ONE tile via df.groupBy(key).agg(col_fn(...)),
    # burning each H3 cell's centroid pixel onto the EXPLICIT hardcoded grid.
    # Consistency = the single out tile's raster fingerprint; perf = scaled groupBy.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=8,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_h3_rasterize_agg"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected aggregate rows"
    assert all(r.status == "ok" for r in rows), [
        (r.fn, r.status, r.note) for r in rows if r.status != "ok"
    ]
    assert {r.fn for r in rows} == {"rst_h3_rasterize_agg"}
    assert all(r.mode == "spark-path" for r in rows)
    import json

    fp = {r.rows: r.output_fingerprint for r in rows if r.fn == "rst_h3_rasterize_agg"}
    assert fp.get(2), "no consistency fingerprint at N=2"
    parsed = json.loads(fp[2])
    assert parsed["kind"] == "raster", parsed.get("kind")


def test_udtf_grid_fn_spark_path_runs_via_lateral_not_error(tmp_path, spark):
    # The rastertogrid* / tessellate fns are registered Python UDTFs, so their
    # lightweight spark-path must run via SQL LATERAL (a table-function join over the
    # tile DataFrame), NOT the scalar .select(col_fn(...)) path -- the scalar
    # prx.<fn> wrapper raises NotImplementedError. This proves the runner takes the
    # udtf branch and produces OK rows (not error rows) with a real timing.
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=11,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_h3_rastertogridavg"])
    assert fns and fns[0].udtf, "rst_h3_rastertogridavg must be flagged udtf=True"
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows, "expected UDTF spark-path rows"
    assert all(r.status == "ok" for r in rows), [
        (r.fn, r.status, r.note) for r in rows if r.status != "ok"
    ]
    assert all(
        r.mode == "spark-path" and r.fn == "rst_h3_rastertogridavg" for r in rows
    )
    # the udtf branch tags its rows so the invocation shape is auditable in the store
    assert all(r.note == "lateral-udtf" for r in rows)
    assert sorted({r.rows for r in rows}) == [2, 4]


def test_udtf_lateral_sql_builds_expected_shape():
    # The LATERAL SQL feeds the tile struct as the first UDTF arg and the scalar
    # bench args (resolution etc.) as SQL literals after it, projecting t.* so the
    # whole per-tile row fan-out is realized.
    fs = s.select(functions=["rst_h3_rastertogridavg"])[0]
    sql = rn._udtf_lateral_sql("_v", fs)
    assert "LATERAL gbx_rst_h3_rastertogridavg(d.tile, 7)" in sql
    assert sql.startswith("SELECT t.* FROM _v AS d")


def test_pure_core_emits_na_by_design_for_low_band_count(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=3,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=1,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_ndvi"])  # band-math needs 2 bands
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows and all(r.status == "na_by_design" for r in rows)
    assert all("band" in r.note.lower() for r in rows)


def test_build_input_tile_virtual_is_bytes_free(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=9,
        tile_px=[64],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=1,
        row_tile_px=64,
        row_bands=1,
        row_dtype="float32",
    )
    te = next(t for t in corpus.size_sweep if t.role != "bng_gb")
    p = tmp_path / te.path
    content = p.read_bytes()
    mat = rn._build_input_tile(str(p), content, 7, "materialized")
    assert mat["raster"] is not None and mat["cellid"] == 7
    virt = rn._build_input_tile(str(p), content, 7, "virtual")
    assert virt["raster"] is None
    assert virt["path"] and virt["window"] is not None
    assert virt["cellid"] == 7  # corpus cellid preserved (not _fromfile_impl's 0)


def test_build_input_tile_virtual_no_silent_fallback(monkeypatch):
    import databricks.labs.gbx.pyrx.functions as pf

    monkeypatch.setattr(pf, "_fromfile_impl", lambda *a, **kw: None)
    with pytest.raises(ValueError, match="returned null"):
        rn._build_input_tile("/any.tif", b"", 0, "virtual")


def test_run_spark_path_accepts_input_tile_kwarg():
    import inspect

    assert "input_tile" in inspect.signature(rn.run_spark_path).parameters


def test_disposition_accessor_uses_classifier():
    assert rn._disposition_of(s.REGISTRY["rst_avg"], None) == "materialized"
    assert rn._disposition_of(s.REGISTRY["rst_width"], None) == "deferred"


def test_disposition_tile_returning_from_output_tile():
    assert rn._disposition_of(s.REGISTRY["rst_slope"], {"raster": b"xx"}) == "materialized"
    assert rn._disposition_of(s.REGISTRY["rst_slope"], {"raster": None}) == "deferred"
    # no sample available -> "na" (not a crash)
    assert rn._disposition_of(s.REGISTRY["rst_slope"], None) == "na"
