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
        str(tmp_path / "out" / "heartbeat.json"), interval=0.05, phase="smoke"
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
        def begin(self, label, index, total):
            log.append(("begin", label, index, total))

        def end(self):
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
    assert log[0][1:] == (f"{B}:L3:c0", 1, 2)
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


# ---- the notebook: faulthandler + heartbeat in the setup, the smoke cells


def test_the_setup_arms_the_stack_dump_and_the_heartbeat_and_the_levels_cell_sinks_per_form():
    src = source(cl.build_bench_notebook(CFG))
    assert "_fh.dump_traceback_later(STALL_CAP_S, repeat=True, file=_sys.stderr)" in src
    assert 'HEARTBEAT = _live.Heartbeat(OUT + "/heartbeat.json"' in src
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
