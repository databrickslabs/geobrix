"""Run hardening: liveness, the smoke stage, the preflight and the client-input cap of the bench launcher."""

import ast
import json
import pathlib
import time

import pytest

from databricks.labs.gbx.bench import clientcap
from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import liveness, preflight

B = "gbx_bng_eastnorthasbng"
SRC = pathlib.Path(lf.__file__).parent
CFG = dict(
    corpus="/c", out_dir="/o", table="a.b.c", run_id="r", functions="", set="core", modes="spark-path",
    row_counts="1", warmup=0, measured=1, lightweight=True, heavyweight=False, levels_mvp="S",
    levels_only=True, wheel="/w.whl",
)  # fmt: skip


def source(nb):
    return "\n".join("".join(c["source"]) for c in nb["cells"])


# ---- liveness: the heartbeat file


def test_the_heartbeat_names_the_form_its_elapsed_time_and_the_stack_of_the_working_thread(
    tmp_path,
):
    hb = liveness.Heartbeat(
        str(tmp_path / "out" / "heartbeat.json"),
        interval=0.05,
        phase="smoke",
        token="launch-1",
    ).start()

    def working_here():
        hb.begin("gbx_bng:L4:uc-scalar", 3, 10)
        time.sleep(0.3)  # the beats come from the daemon thread while this one is busy
        return json.loads((tmp_path / "out" / "heartbeat.json").read_text())

    beat = working_here()
    hb.end()
    hb.stop()
    final = json.loads((tmp_path / "out" / "heartbeat.json").read_text())
    assert beat["form"] == "gbx_bng:L4:uc-scalar" and beat["form_elapsed_s"] > 0.1
    assert (beat["form_index"], beat["form_total"], beat["phase"]) == (3, 10, "smoke")
    assert (
        beat["token"] == "launch-1"
    )  # the launcher ignores a file another launch left
    assert any("working_here" in ln for ln in beat["stack"]), beat["stack"]
    assert (
        final["state"] == "done" and final["form"] is None and final["forms_done"] == 1
    )
    assert final["seq"] > beat["seq"]


# ---- liveness: each form's rows sink when that form finishes


def tiny_forms(monkeypatch, n=2):
    forms = [
        f
        for f in lf.FORMS["S"]
        if f.fn == B and f.level == "L3" and f.variant in ("c0", "c1")
    ][:n]
    monkeypatch.setitem(lf.FORMS, "S", forms)
    return forms


def test_a_form_sinks_its_rows_before_the_next_form_starts_and_reports_its_tasks(
    spark_session_local, tmp_path, monkeypatch
):
    tiny_forms(monkeypatch)
    log = []

    class HB:
        def begin(self, label, index, total, **kw):
            log.append(("begin", label, index, total))

        def end(self, **kw):
            log.append(("end",))

    rows = levels.run_mvp(
        spark_session_local, "S", "t", 0, 1, corpus=str(tmp_path), schema=None, scales=(1,), where="venv",
        base_rows=200, only_levels=("L3",), heartbeat=HB(), report_tasks=True,
        on_form=lambda rs: log.append(("sink", [r.form for r in rs])),
    )  # fmt: skip
    assert [e[0] for e in log] == [
        "begin",
        "end",
        "sink",
        "begin",
        "end",
        "sink",
    ]  # sunk before the next begins
    assert log[0][1:] == (f"{B}:L3:c0 @1x", 1, 2)
    assert [r.status for r in rows] == ["ok", "ok"]
    assert all(r.input_partitions >= 1 and "tasks=" in r.note for r in rows)


def test_the_injected_stall_form_sleeps_on_the_driver_after_the_first_form(
    spark_session_local, tmp_path, monkeypatch
):
    tiny_forms(monkeypatch)
    seen = []
    rows = levels.run_mvp(
        spark_session_local, "S", "t", 0, 1, corpus=str(tmp_path), schema=None, scales=(1,), where="venv",
        base_rows=50, only_levels=("L3",), inject_stall=0.4, on_form=lambda rs: seen.append(rs[0].fn),
    )  # fmt: skip
    assert seen == [B, "inject_stall", B]
    stall = rows[1]
    assert stall.status == "ok" and stall.iter_median_s >= 0.4


def test_a_form_is_timed_and_capped_per_scale_not_once_for_the_whole_ladder(
    spark_session_local, tmp_path, monkeypatch
):
    """A healthy form whose 1x/4x/16x ladder totals more than the 1x cap was cancelled as stalled: the beat now
    names the scale and carries that scale's cap (the 1x cap times the scale)."""
    tiny_forms(monkeypatch, 1)
    log = []

    class HB:
        def begin(self, label, index, total, *, scale=1, cap_s=None):
            log.append(("begin", label, scale, cap_s))

        def end(self, *, form_done=True):
            log.append(("end", form_done))

    levels.run_mvp(
        spark_session_local, "S", "t", 0, 1, corpus=str(tmp_path), schema=None, scales=(1, 4), where="venv",
        base_rows=50, only_levels=("L3",), heartbeat=HB(), form_cap_s=60,
        on_form=lambda rs: log.append(("sink", len(rs))),
    )  # fmt: skip
    assert log == [
        ("begin", f"{B}:L3:c0 @1x", 1, 60),
        ("end", False),
        ("begin", f"{B}:L3:c0 @4x", 4, 240),
        ("end", True),
        ("sink", 2),  # rows still sink per form, after its last scale
    ]


def test_a_beat_carries_its_scale_and_cap_and_forms_done_counts_forms_not_scales(
    tmp_path,
):
    path = tmp_path / "hb.json"
    hb = liveness.Heartbeat(str(path), interval=60, token="t").start()
    hb.begin("f @4x", 1, 2, scale=4, cap_s=3600)
    beat = json.loads(path.read_text())
    assert (beat["form"], beat["scale"], beat["cap_s"]) == ("f @4x", 4, 3600)
    hb.end(form_done=False)
    assert json.loads(path.read_text())["forms_done"] == 0
    hb.end()
    hb.stop()
    assert json.loads(path.read_text())["forms_done"] == 1


def test_beats_from_two_threads_always_leave_a_parseable_file(tmp_path):
    """The work thread and the daemon thread both write the file; a reader must never see a half-written one."""
    import threading

    path = tmp_path / "hb.json"
    hb = liveness.Heartbeat(str(path), interval=3600).start()
    stop, bad = threading.Event(), []

    def writer():
        while not stop.is_set():
            hb.beat()

    def reader():
        while not stop.is_set():
            try:
                json.loads(path.read_text())
            except FileNotFoundError:
                pass
            except ValueError as exc:
                bad.append(str(exc))

    threads = [threading.Thread(target=f) for f in (writer, writer, reader)]
    [t.start() for t in threads]
    time.sleep(1.0)
    stop.set()
    [t.join() for t in threads]
    hb.stop()
    assert bad == [] and json.loads(path.read_text())["state"] == "done"
    assert [p.name for p in tmp_path.iterdir()] == [
        "hb.json"
    ]  # no temp file left behind


def test_the_levels_cell_passes_the_form_cap_so_each_scale_carries_its_own():
    src = source(cl.build_bench_notebook(CFG))
    assert "heartbeat=HEARTBEAT, form_cap_s=STALL_CAP_S" in src


# ---- the notebook: faulthandler + heartbeat in the setup, the smoke cells


def test_the_setup_arms_the_stack_dump_and_the_heartbeat_and_the_levels_cell_sinks_per_form():
    src = source(cl.build_bench_notebook(CFG))
    assert "_fh.dump_traceback_later(STALL_CAP_S, repeat=True, file=_sys.stderr)" in src
    assert (
        'HEARTBEAT = _live.Heartbeat(OUT + "/heartbeat.json"' in src
        and "token=LAUNCH_TOKEN" in src
    )
    assert "on_form=_sink, heartbeat=HEARTBEAT" in src
    for cell in cl.build_bench_notebook(CFG)["cells"]:
        ast.parse("".join(cell["source"]).replace("%pip", "#"))


def test_the_smoke_notebook_checks_the_import_on_a_worker_then_runs_each_form_once_and_exits_with_its_result():
    nb = cl.build_bench_notebook(
        {**CFG, "smoke": True, "smoke_rows": 8, "inject_stall": 2}
    )
    src = source(nb)
    assert "_F.udf(_worker_probe" in src and "import databricks.labs.gbx as _g" in src
    assert "scales=(1,), base_rows=SMOKE_ROWS" in src and "report_tasks=True" in src
    assert src.rstrip().endswith("dbutils.notebook.exit(_json.dumps(_smoke))")
    assert (
        "SMOKE = True" in src
        and "SMOKE_ROWS = 8" in src
        and "INJECT_STALL = 2.0" in src
    )
    assert (
        "dbutils.notebook.exit(json.dumps(result))" not in src
    )  # the bench's own exit is not in the smoke


# ---- the preflight: SQL that cannot run, found locally


def form(level, sql, variant="c0"):
    return lf.Form("S", B, level, variant, "bng_points", sql=sql)


GOOD = lf._BNG
OVERFLOW = (
    "SELECT count(*) AS n, sum(xxhash64(c)) AS fp, count(c) AS n_out "
    "FROM (SELECT {fn}(e, n, 3) AS c FROM {src})"
)
IN_AGG = "SELECT count(*) AS n, sum(xxhash64({fn}(e, n, 3))) AS fp, count(*) AS n_out FROM {src}"


def test_the_preflight_finds_a_hash_sum_that_overflows_and_a_java_udf_inside_an_aggregate(
    spark_session_local, tmp_path, monkeypatch
):
    sql = lambda tmpl: tmpl.replace("{fn}", B)  # noqa: E731
    forms = [
        form("L3", sql(GOOD), "c0"),
        form("L3", sql(OVERFLOW), "c1"),
        form("L2", sql(GOOD), "-"),
        form("L2", sql(IN_AGG), "-"),
    ]
    monkeypatch.setitem(lf.FORMS, "S", forms)
    failures, _ = preflight.probe_failures(
        spark_session_local, "S", str(tmp_path), rows=64
    )
    text = "\n".join(failures)
    assert "ARITHMETIC_OVERFLOW" in text and "L3/c1" in text
    assert "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION" in text
    assert len(failures) == 2, failures  # the good L3 form and the good L2 form pass


BROKEN = "SELECT count(*) AS n, sum(nope) AS fp, count(*) AS n_out FROM {src}"


def test_the_preflight_skips_a_memory_probe_and_says_so(
    spark_session_local, tmp_path, monkeypatch
):
    """A memory or scale probe is not a statement check: it is not run (its broken SQL proves it), and it is listed."""
    probe = lf.Form(
        "S", B, "L3", "count_part", "bng_points", sql=BROKEN, memory_probe=True
    )
    monkeypatch.setitem(lf.FORMS, "S", [form("L3", lf._BNG.replace("{fn}", B)), probe])
    failures, skipped = preflight.probe_failures(
        spark_session_local, "S", str(tmp_path), rows=8
    )
    assert failures == []
    assert any(
        "L3/count_part" in x and "skipped by design" in x for x in skipped
    ), skipped


def test_the_preflight_still_fails_a_form_that_is_not_a_memory_probe(
    spark_session_local, tmp_path, monkeypatch
):
    monkeypatch.setitem(lf.FORMS, "S", [form("L3", BROKEN, "c0")])
    failures, _ = preflight.probe_failures(
        spark_session_local, "S", str(tmp_path), rows=8
    )
    assert len(failures) == 1 and "L3/c0" in failures[0]


def test_the_preflight_does_not_run_the_gdal_engine_forms_when_this_interpreter_has_no_gdal(
    spark_session_local, tmp_path, monkeypatch
):
    from databricks.labs.gbx.engine import run as engine_run

    monkeypatch.setattr(engine_run, "unavailable_reason", lambda: "no osgeo")
    monkeypatch.setitem(
        lf.FORMS,
        "S",
        [form("L3", BROKEN, "engine-c0"), form("L3", BROKEN, "floor-c1@256")],
    )
    failures, skipped = preflight.probe_failures(
        spark_session_local, "S", str(tmp_path), rows=8
    )
    assert failures == []
    assert len([x for x in skipped if "not checkable here" in x]) == 2, skipped


def test_a_jvm_form_gets_a_light_stand_in_only_when_the_stand_in_can_run_here():
    """No stand-in for a form whose own setup registers JVM names, nor for one whose JVM level returns another shape."""
    ok = form("L2", lf._BNG.replace("{fn}", B), "-")
    with_setup = lf.Form(
        "S", B, "L2", "-", "bng_points", sql=ok.sql + " ", setup="m:register_jvm"
    )
    other_shape = lf.Form(
        "G", "gbx_rst_retile", "L1", "tvf", "tiles_mat", sql="SELECT 1, 2, 3 FROM {src}"
    )
    got = preflight._jvm_sql_standins([ok, with_setup, other_shape])
    assert [(f.fn, f.sql) for f in got] == [(B, ok.sql)]
    assert got[0].level == "L3" and got[0].variant == "c0"


def test_a_set_with_no_form_table_has_nothing_to_probe():
    """IO has no forms yet: the probe is skipped (it raised a KeyError that refused every --levels IO launch)."""
    assert "IO" not in lf.FORMS
    assert preflight.probe_failures(None, "IO", "/nonexistent") == ([], [])


def test_an_unknown_set_is_an_error_not_an_empty_pass():
    """Only a set KNOWN to have no form table is empty; a misspelled one must not pass the preflight silently."""
    with pytest.raises(ValueError, match="XYZ"):
        preflight.probe_failures(None, "XYZ", "/nonexistent")
    with pytest.raises(ValueError, match="XYZ"):
        preflight.client_cap_problems("XYZ", (1,))


def test_the_probe_runs_only_the_forms_levels_forms_selects(
    spark_session_local, tmp_path, monkeypatch
):
    good = form("L3", lf._BNG.replace("{fn}", B), "c0")
    monkeypatch.setitem(lf.FORMS, "S", [good, form("L3", BROKEN, "c1")])
    every, _ = preflight.probe_failures(spark_session_local, "S", str(tmp_path), rows=8)
    assert len(every) == 1 and "L3/c1" in every[0]
    kept, _ = preflight.probe_failures(
        spark_session_local, "S", str(tmp_path), rows=8, forms=("c0",)
    )
    assert kept == []  # the broken form was excluded by the user


def test_a_levels_forms_name_that_matches_no_variant_is_reported():
    assert preflight.unknown_form_names("SE", ("today@256", "today@1024")) == []
    assert preflight.unknown_form_names("SE", ("today@256", "tday@256")) == ["tday@256"]
    assert preflight.unknown_form_names("SE", ()) == []
    assert preflight.unknown_form_names("IO", ("anything",)) == ["anything"]  # IO has no forms


def test_the_memory_probes_of_the_g_set_are_marked():
    """The collect-a-partition shape and the big-tile forms are memory probes by design; nothing else in G is."""
    marked = {(f.fn, f.level, f.variant) for f in lf.FORMS["G"] if f.memory_probe}
    for f in lf.FORMS["G"]:
        by_shape = f.variant.startswith("count_part") or f.input == "tiles_big"
        assert f.memory_probe == by_shape, (f.fn, f.level, f.variant)
    assert ("gbx_rst_retile", "L3", "count_part-big") in marked
    assert not any(
        f.memory_probe for mvp in ("S", "A", "H", "SE") for f in lf.FORMS[mvp]
    )


def test_the_preflight_refuses_a_ladder_that_builds_gigabytes_on_the_client():
    assert preflight.client_cap_problems(
        "SE", (1, 4, 16)
    )  # se_dem_1024 x 256 tiles is 1 GB
    assert clientcap.MAX_CLIENT_BYTES == 64 * 2**20
    with pytest.raises(clientcap.ClientInputTooLarge, match="executors"):
        clientcap.check_estimate("probe", 1000, 4_197_794)


def test_no_levels_input_is_built_with_a_raw_create_dataframe():
    """Every client-built DataFrame of the levels leg and its probes goes through the cap."""
    for name in ("levels_forms.py", "probes.py"):
        tree = ast.parse((SRC / name).read_text())
        raw = [
            n.lineno
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and getattr(n.func, "attr", "") == "createDataFrame"
        ]
        assert raw == [], (name, raw)


def test_a_few_rows_of_tiles_come_from_the_first_files_not_a_limit_over_the_whole_directory(
    spark_session_local, tmp_path, monkeypatch
):
    """`limit(16)` over the corpus reads every file before it keeps 16: minutes for the smoke's input on Serverless."""
    (tmp_path / "rows").mkdir()
    files = [str(tmp_path / "rows" / f"t{i}.tif") for i in range(5)]
    monkeypatch.setattr(lf, "_corpus_rows", lambda corpus: files)
    seen = []

    def column(spark, pairs, virtual, keyed):
        seen.append((pairs, virtual, keyed))
        return spark.createDataFrame([(1,)], "tile int")

    monkeypatch.setattr(lf, "_tile_column", column)
    src = lf.INPUTS["tiles_virt"](spark_session_local, 3, str(tmp_path), None)
    assert src and seen == [
        ([("0", files[0]), ("1", files[1]), ("2", files[2])], True, False)
    ]
