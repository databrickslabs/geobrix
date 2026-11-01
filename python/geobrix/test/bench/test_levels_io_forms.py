"""The IO forms of the levels leg (read paths and writers): their shape, the scratch guard, and a local probe."""

import os

import numpy as np
import pytest

from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight


def _corpus(tmp_path, n=4):
    import rasterio
    from rasterio.transform import from_origin

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    for i in range(n):
        with rasterio.open(
            rows / f"t{i}.tif",
            "w",
            driver="GTiff",
            width=16,
            height=16,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(i * 16, 16, 1, 1),
        ) as dst:
            dst.write(np.full((1, 16, 16), i, dtype="float32"))
    return str(tmp_path / "corpus")


def test_the_io_set_holds_the_reads_the_udf_the_floor_and_the_three_writers():
    forms = {f.variant: f for f in lf.FORMS["IO"]}
    assert set(forms) == {
        "a",
        "b",
        "c",
        "udf",
        "d-py",
        "d-rs",
        "floor",
        "W0",
        "W1",
        "W2",
    }
    # the floor sits beside the d forms: `summarize` keys floor_median_s on (fn, level, rows)
    assert (forms["floor"].fn, forms["floor"].level) == (
        forms["d-py"].fn,
        forms["d-py"].level,
    )
    assert {f.input for f in lf.FORMS["IO"]} == {"io_rows"}
    assert [lf.BASE_ROWS["io_rows"] * k for k in lf.SCALES["IO"]] == [1000, 4000, 10000]
    for f in lf.FORMS["IO"]:  # every form resolves to a function of this module
        assert callable(getattr(lf, f.run.rpartition(":")[2])), f.variant
        assert not f.setup or callable(
            getattr(lf, f.setup.rpartition(":")[2])
        ), f.variant


def test_a_writer_form_deletes_only_a_strict_child_of_the_runs_scratch(tmp_path):
    scratch = lf._io_scratch(
        str(tmp_path / "vol" / "data" / "corpus")
    )  # a corpus outside geobrix/0.6.0
    out = os.path.join(scratch, "io_1")
    os.makedirs(out)
    open(os.path.join(out, "f.tif"), "wb").write(b"x")
    lf.remove_scratch(out, scratch)
    assert not os.path.exists(out)
    sibling = tmp_path / "corpus"
    sibling.mkdir()
    for bad in (
        scratch,
        str(sibling),
        os.path.join(scratch, "..", "corpus"),
        os.path.join(scratch, ".."),
    ):
        with pytest.raises(ValueError, match="refusing to delete"):
            lf.remove_scratch(bad, scratch)
    assert sibling.exists()


def test_the_scratch_directory_is_beside_the_corpus_on_a_volume_and_in_temp_elsewhere():
    assert (
        lf._io_scratch("/Volumes/c/s/v/data/corpus/") == "/Volumes/c/s/v/data/scratch"
    )  # wherever the corpus is
    assert lf._io_scratch("/tmp/x/corpus").endswith(lf.SCRATCH_PREFIX.rstrip("/"))


def test_each_io_form_installs_the_level_it_measures(monkeypatch):
    """c (L1), udf (L3), W2 (L1) in the order of the set: W2 must not time the L3 UDF the `udf` form left installed."""
    import databricks.labs.gbx as gbx
    from databricks.labs.gbx.bench import levels

    installed = []
    monkeypatch.setattr(
        gbx,
        "register",
        lambda spark, only=None, _levels=None: installed.append(_levels[lf.IO_FN][0]),
    )
    ctx = levels._Ctx(None, 0, 1, "", None, None, {})
    by_variant = {f.variant: f for f in lf.FORMS["IO"]}
    seen = []
    for v in ("c", "udf", "W2", "W2"):
        levels._ensure_setup(ctx, by_variant[v])
        seen.append(ctx.registered[lf.IO_FN][0])
    assert seen == ["L1", "L3", "L1", "L1"] and installed == [
        "L1",
        "L3",
        "L1",
    ]  # the repeat installs nothing


def test_an_explicit_default_looking_ladder_is_kept_for_a_set_with_its_own():
    from databricks.labs.gbx.bench import levels

    assert levels.ladder("IO") == lf.IO_SCALES and levels.ladder("H") == (1, 4, 16)
    assert levels.ladder("IO", (1, 4, 16)) == (1, 4, 16)  # asked for, not the default


def test_the_light_io_forms_run_on_a_local_corpus(spark_session_local, tmp_path):
    """b, d-py, the row-wise UDF and the light writer W1 on four tiles; the JVM forms are skipped without the JAR."""
    corpus = _corpus(tmp_path)
    failures, skipped = preflight.probe_failures(
        spark_session_local, "IO", corpus, rows=4, forms=("b", "d-py", "udf", "W1")
    )
    assert failures == [], failures
    assert not os.path.exists(lf._io_scratch(corpus)) or not os.listdir(
        lf._io_scratch(corpus)
    )


def test_the_io_floor_is_the_d_py_shell_with_the_decode_skipped(
    spark_session_local, tmp_path
):
    """The floor reads the same bytes-free virtual tiles through the same `mapInArrow` shell as d-py, and keeps them
    virtual: no raster is decoded, so its median is the cost of the shell alone."""
    corpus = _corpus(tmp_path)
    rows = (
        lf._io_read(spark_session_local, corpus + "/rows", 4, "floor")
        .select("tile")
        .collect()
    )
    assert len(rows) == 4
    assert all(r["tile"]["raster"] is None and r["tile"]["path"] for r in rows)
    decoded = (
        lf._io_read(spark_session_local, corpus + "/rows", 4, "d-py")
        .select("tile")
        .collect()
    )
    assert all(
        r["tile"]["raster"] is not None for r in decoded
    )  # d-py does decode the same tiles


def test_the_io_floor_is_recorded_as_the_floor_and_reported_beside_d_py(
    spark_session_local, tmp_path
):
    """The floor row is `ok` (its rasters are NULL by design, not an error) and d-py's line carries its median."""
    from databricks.labs.gbx.bench import levels

    corpus = _corpus(tmp_path)
    got = levels.run_mvp(
        spark_session_local, "IO", "io-floor", 0, 1, corpus=corpus, schema=None, scales=(1,), base_rows=4,
        where="test", only_levels=("DF",),
    )  # fmt: skip
    by_form = {r.form: r for r in got}
    assert by_form["floor"].status == "ok", (
        by_form["floor"].failure_mode,
        by_form["floor"].note,
    )
    assert "NULL" not in (by_form["floor"].note or "")
    assert by_form["d-py"].status == "ok"
    lines = levels.summarize(got).splitlines()
    cells = {
        ln.split("|")[3].strip(): [c.strip() for c in ln.split("|")]
        for ln in lines
        if "| read | DF |" in ln
    }
    assert cells["d-py"][7] != "-"  # floor_median_s
    assert cells["floor"][7] == "-"


# ---- every form takes exactly n tiles and decodes / writes them with the same parallelism


def _plan(df) -> list:
    """The physical plan of `df` as its node lines, root first (so a node's ancestors come before it)."""
    return df._jdf.queryExecution().executedPlan().treeString().splitlines()


def _at(plan, *needles):
    """The index of the first plan line holding any of `needles` (-1 when none does)."""
    return next((i for i, ln in enumerate(plan) if any(k in ln for k in needles)), -1)


def test_the_io_partition_count_follows_the_compute_and_keeps_a_few_rows_per_partition(
    monkeypatch,
):
    """P is a function of n and the slots only (every form gets the same one), never over n, and never so few that
    a partition holds a long run of ~13 MB tiles (a Python batch of those OOM-killed a local JVM at 8 partitions).
    """
    from databricks.labs.gbx.bench import runner

    for slots, n, want in [
        (8, 6, 6),
        (8, 96, 12),
        (8, 400, 50),
        (200, 1000, 200),
        (8, 1000, 125),
    ]:
        monkeypatch.setattr(
            runner, "_bench_parallelism", lambda spark, default=8, s=slots: s
        )
        got = lf.io_partitions(None, n)
        assert got == want, (slots, n, got)
        assert (
            1 <= got <= n and -(-n // got) <= lf.IO_ROWS_PER_PARTITION
        )  # a few rows per partition


@pytest.mark.parametrize("kind", ["udf", "d-py", "d-rs", "floor", "virtual"])
def test_the_limit_sits_below_the_exchange_that_feeds_the_decode(
    spark_session_local, tmp_path, kind
):
    """`limit(n)` then `repartition(P, ...)` then the decode: the decode node is above the exchange, the exchange above
    the global limit. (Before, Spark ran a row UDF BELOW the limit, on every file of the corpus.)
    """
    lf.register_io_udf(spark_session_local)
    corpus = _corpus(tmp_path, 8)
    plan = _plan(lf._io_read(spark_session_local, corpus + "/rows", 3, kind))
    decode = {
        "udf": ("BatchEvalPython", "ArrowEvalPython"),
        "d-py": ("MapInArrow",), "d-rs": ("MapInArrow",), "floor": ("MapInArrow",),
        "virtual": ("Exchange",),
    }[kind]  # fmt: skip
    top, exch, limit = (
        _at(plan, *decode),
        _at(plan, "Exchange rangepartitioning"),
        _at(plan, "GlobalLimit", "CollectLimit"),
    )
    assert min(top, exch, limit) >= 0, "\n".join(plan)
    assert top <= exch < limit, "\n".join(plan)


def test_the_row_udf_decodes_exactly_n_tiles_not_the_corpus(
    spark_session_local, tmp_path
):
    """A counting stand-in for the row UDF, installed under its name: it is called once per tile it is given."""
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    count_file = tmp_path / "calls.txt"

    def spy(tile):
        with open(str(count_file), "a") as fh:
            fh.write("x\n")
        return tile

    spark_session_local.udf.register(lf.IO_FN, spy, V2_TILE_SCHEMA)
    corpus = _corpus(tmp_path, 8)
    got = lf._io_read(spark_session_local, corpus + "/rows", 3, "udf").collect()
    assert len(got) == 3
    assert count_file.read_text().count("x") == 3  # was 8: the decode ran on every file


def test_the_scan_decoding_light_reader_reads_exactly_n_files(
    spark_session_local, tmp_path
):
    """`b` decodes in the scan, so n is the file list it is given: n scan tasks, n tiles."""
    corpus = _corpus(tmp_path, 8)
    df = lf._io_read(spark_session_local, corpus + "/rows", 3, "b")
    assert (
        df.rdd.getNumPartitions() == 3 and df.count() == 3
    )  # was 8 tasks (one per corpus file)


def test_the_only_files_regex_keeps_exactly_the_named_files_for_python_and_java():
    """One filter for both readers: python `re.match` (light) and java `matches` of `.*<regex>.*` (heavy)."""
    import re

    names = ["r0.tif", "r1.tif", "r10.tif", "r100.tif", "r2.tif", "a.b.tif"]
    rx = re.compile(lf._only_files_regex(["/v/rows/" + k for k in names]))
    every = names + ["r11.tif", "r1.tiff", "xr1.tif", "r1xtif", "r.tif", "ra.b.tif"]
    assert [k for k in every if rx.match("/v/rows/" + k)] == names


def test_the_only_files_regex_is_anchored_to_the_rows_folder_not_just_the_file_name():
    """The heavy reader is recursive: a same-named file in a subfolder or another folder must not match."""
    import re

    rx = re.compile(lf._only_files_regex(["/v/rows/r1.tif", "/v/rows/r2.tif"]))
    assert rx.match("/v/rows/r1.tif") and rx.match("/v/rows/r2.tif")
    assert not rx.match("/v/rows/sub/r1.tif")
    assert not rx.match("/v/other/r1.tif")
    assert not rx.match("/v/rows2/r1.tif")
    assert re.compile(lf._only_files_regex([])).match("/v/rows/r1.tif") is None


def test_the_light_writers_take_the_same_limited_input(spark_session_local, tmp_path):
    """W1 writes the virtual tiles: its input has the limit under the exchange, as the read forms do."""
    corpus = _corpus(tmp_path, 8)
    plan = _plan(lf._io_named(spark_session_local, corpus + "/rows", 3, "virtual"))
    exch, limit = _at(plan, "Exchange rangepartitioning"), _at(
        plan, "GlobalLimit", "CollectLimit"
    )
    assert 0 <= exch < limit, "\n".join(plan)


# ---- a scale asks for no more tiles than the corpus holds


def test_a_scale_asking_for_more_tiles_than_the_corpus_holds_is_an_error_not_a_cap(
    spark_session_local, tmp_path, monkeypatch, capsys
):
    """On the cluster the staged rows dir held 1000 tiles, so the 1x / 4x / 10x labels all read the same 1000."""
    from databricks.labs.gbx.bench import levels

    corpus = _corpus(tmp_path, 4)
    monkeypatch.setitem(
        lf.FORMS, "IO", [f for f in lf.FORMS["IO"] if f.variant in ("b", "floor")]
    )
    short = levels.run_mvp(
        spark_session_local, "IO", "io-short", 0, 1, corpus=corpus, schema=None, scales=(1,), base_rows=10,
        where="test",
    )  # fmt: skip
    assert [r.status for r in short] == ["error", "error"]
    why = short[0].failure_mode
    assert "asks for 10 tiles" in why and "holds only 4" in why and corpus in why, why
    assert f"[LEVELS IO] corpus {corpus}/rows holds 4 tiles" in capsys.readouterr().out
    exact = levels.run_mvp(
        spark_session_local, "IO", "io-exact", 0, 1, corpus=corpus, schema=None, scales=(1,), base_rows=4,
        where="test",
    )  # fmt: skip
    assert [(r.status, r.rows) for r in exact] == [("ok", 4), ("ok", 4)]


# ---- the timed form reads, decodes and writes: the listing and the file filter are built with the input


def _count_listings(monkeypatch):
    from databricks.labs.gbx.ds import _listing

    calls = []
    real = _listing.list_files

    def counting(*a, **kw):
        calls.append(a[0])
        return real(*a, **kw)

    monkeypatch.setattr(_listing, "list_files", counting)
    return calls


@pytest.mark.parametrize("form", ["io_a", "io_b"])
def test_a_and_b_do_not_list_the_corpus_or_build_the_file_filter_inside_the_timed_form(
    spark_session_local, tmp_path, monkeypatch, form
):
    """The input builder lists the corpus once and carries the filter; the form only reads."""
    corpus = _corpus(tmp_path, 4)
    src = lf._io_spec(spark_session_local, 3, corpus, None)
    calls = _count_listings(monkeypatch)
    monkeypatch.setattr(
        lf,
        "_only_files_regex",
        lambda *a: pytest.fail("the filter was built in the form"),
    )
    if form == "io_b":  # a needs the JAR; b is the same path with the light reader
        n, fp, n_out, *_ = lf.io_b(spark_session_local, src)
        assert n == 3
    else:
        with pytest.raises(
            Exception
        ) as err:  # no JAR here: it must fail in the read, not in the listing
            lf.io_a(spark_session_local, src)
        assert "the filter was built" not in str(err.value)
    assert calls == []


def test_the_input_builder_lists_the_corpus_once_per_scale_not_once_per_form(
    spark_session_local, tmp_path, monkeypatch
):
    corpus = _corpus(tmp_path, 4)
    calls = _count_listings(monkeypatch)
    for _ in range(6):  # one build per form of the set
        lf._io_spec(spark_session_local, 3, corpus, None)
    assert len(calls) == 1, calls
    lf._io_spec(spark_session_local, 4, corpus, None)  # the next scale
    assert len(calls) == 2, calls


def test_the_first_files_skip_hidden_files_as_the_reader_does(
    spark_session_local, tmp_path
):
    """A `._x.tif` / `_x.tif` sorts first and is skipped by the reader: b would get fewer than n rows."""
    corpus = _corpus(tmp_path, 4)
    rows = os.path.join(corpus, "rows")
    for hidden in ("._t0.tif", "_t0.tif"):
        open(os.path.join(rows, hidden), "wb").write(b"junk")
    got = lf._first_files(rows, 3)
    assert [os.path.basename(f) for f in got] == ["t0.tif", "t1.tif", "t2.tif"]
    df = lf._io_read(spark_session_local, rows, 3, "b")
    assert df.count() == 3


@pytest.mark.parametrize("n", [40, 49, 96])
def test_no_partition_holds_more_than_the_cap_of_tiles(
    spark_session_local, tmp_path, monkeypatch, n
):
    """`IO_ROWS_PER_PARTITION` is a guarantee, not a hash's average (a skewed partition OOM-killed a local JVM)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import runner

    monkeypatch.setattr(runner, "_bench_parallelism", lambda spark, default=8: 1)
    corpus = _corpus(tmp_path, n)
    df = lf._io_read(spark_session_local, corpus + "/rows", n, "virtual")
    sizes = [
        r[0]
        for r in df.groupBy(F.spark_partition_id()).count().select("count").collect()
    ]
    assert sum(sizes) == n and max(sizes) <= lf.IO_ROWS_PER_PARTITION, sizes
    assert df.columns == ["tile"]


# ---- the virtual input plans and reads the first n files, not the folder


def _light_readers(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def _max_tasks_in_job(spark, group, action):
    """The task count of the widest stage `action()` ran (a job group of its own): counts tasks, never time."""
    sc = spark.sparkContext
    sc.setJobGroup(group, group)
    action()
    tracker = sc.statusTracker()
    stages = [
        tracker.getStageInfo(sid)
        for jid in tracker.getJobIdsForGroup(group)
        for sid in tracker.getJobInfo(jid).stageIds
    ]
    return max(s.numTasks for s in stages if s is not None)


@pytest.mark.parametrize("kind", ["virtual", "udf", "d-py", "floor"])
def test_the_virtual_input_plans_and_reads_only_n_files_of_a_bigger_folder(
    spark_session_local, tmp_path, kind
):
    """40 files, n = 3: no stage has more than 3 tasks. (Before, the scan planned one task per file of the folder, so a
    10,000-file corpus paid 10,000 tasks for the first n tiles whatever n was.)
    """
    _light_readers(spark_session_local)
    lf.register_io_udf(spark_session_local)
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    if kind == "udf":
        spark_session_local.udf.register(lf.IO_FN, lambda t: t, V2_TILE_SCHEMA)
    corpus = _corpus(tmp_path, 40)
    n = 3
    df = lf._io_read(spark_session_local, corpus + "/rows", n, kind)
    widest = _max_tasks_in_job(
        spark_session_local, f"virt-{kind}", lambda: df.collect()
    )
    assert widest <= n, widest


def test_the_virtual_input_scan_has_n_partitions_not_one_per_file(
    spark_session_local, tmp_path
):
    """The same through the reader's own partition count: the scan under `_virtual_tiles` holds the n files."""
    _light_readers(spark_session_local)
    corpus = _corpus(tmp_path, 40)
    rows = corpus + "/rows"
    only = lf._only_files_regex(lf._first_files(rows, 3))
    scan = (
        spark_session_local.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .option("filterRegex", only)
        .load(rows)
    )
    assert scan.rdd.getNumPartitions() == 3
    assert (
        _max_tasks_in_job(
            spark_session_local,
            "virt-scan",
            lambda: lf._virtual_tiles(spark_session_local, rows, 3).collect(),
        )
        <= 3
    )


def _read_names(df):
    return sorted(os.path.basename(r["path"]) for r in df.select("tile.path").collect())


def test_every_io_form_reads_the_same_n_file_names(spark_session_local, tmp_path):
    """b (decodes in the scan, given `only`) and the virtual-input forms (given the same `only`) take the same first n
    sorted files; the writers' input is the virtual one.
    """
    _light_readers(spark_session_local)
    lf.register_io_udf(spark_session_local)
    corpus = _corpus(tmp_path, 12)
    rows = corpus + "/rows"
    n = 3
    only = lf._only_files_regex(lf._first_files(rows, n))
    want = sorted(os.path.basename(f) for f in lf._first_files(rows, n))
    assert len(want) == n
    got = {
        kind: _read_names(lf._io_read(spark_session_local, rows, n, kind, only=only))
        for kind in ("b", "virtual", "floor")
    }
    # built without the builder's filter: the same first n files
    got["virtual-no-only"] = _read_names(
        lf._io_read(spark_session_local, rows, n, "virtual")
    )
    got["writer-input"] = sorted(
        os.path.basename(r["tile"]["path"])
        for r in lf._io_named(
            spark_session_local, rows, n, "virtual", only=only
        ).collect()
    )
    assert all(v == want for v in got.values()), (want, got)


def test_every_io_form_takes_its_files_from_the_one_filter_it_is_given(
    spark_session_local, tmp_path
):
    """One way to pick files: the `filterRegex` of the input builder. Given a filter for t5..t7, b, the virtual input and
    the writers' input all read t5..t7 (so the builder's filter, not a second listing, decides what every form reads).
    """
    _light_readers(spark_session_local)
    corpus = _corpus(tmp_path, 12)
    rows = corpus + "/rows"
    files = lf._first_files(rows, None)
    picked = files[5:8]
    only = lf._only_files_regex(picked)
    want = sorted(os.path.basename(f) for f in picked)
    for kind in (
        "b",
        "virtual",
        "floor",
    ):  # the d / c / udf forms decode above the same virtual input
        df = lf._io_read(spark_session_local, rows, 3, kind, only=only)
        assert _read_names(df) == want, kind
    named = lf._io_named(spark_session_local, rows, 3, "virtual", only=only)
    assert sorted(os.path.basename(r["tile"]["path"]) for r in named.collect()) == want
