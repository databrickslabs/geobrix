"""The MVP levels leg: forms timed per level, with the attribution split beside the timing."""

import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
import types

import pytest

from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import results

B = "gbx_bng_eastnorthasbng"
RST = "gbx_rst_transformcrs"
LINUX = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="reads /proc/self/clear_refs"
)


def run_s(spark, tmp_path, scales, base_rows=2000, mvp="S", **kw):
    kw = {"only_levels": ("L3",), **kw}
    return levels.run_mvp(
        spark, mvp, "t1", 1, 1, corpus=str(tmp_path), schema=None, scales=scales,
        where="venv", base_rows=base_rows, **kw,
    )  # fmt: skip


def test_levels_leg_runs_mvp_s_light_forms(spark_session_local, tmp_path):
    spark = spark_session_local
    # 20k rows and up: Spark's sent-bytes counter stops growing past one 10k-row Arrow batch
    rows = run_s(spark, tmp_path, scales=(1, 4), base_rows=20000)
    assert rows and {r.level for r in rows} == {"L3"}
    assert all(r.status in ("ok", "na_by_design") for r in rows)
    ok = [r for r in rows if r.status == "ok"]
    assert all(r.iter_median_s > 0 and r.rows > 0 and r.output_fingerprint for r in ok)
    c0 = [r for r in ok if r.fn == B and r.form == "c0"]
    # at each scale every variant computes the same cells; `floor` is not one (a constant)
    cells = {
        (r.rows, r.output_fingerprint) for r in ok if r.fn == B and r.form != "floor"
    }
    assert len(cells) == len({rows for rows, _ in cells})
    # No tile corpus here: the raster forms are skipped with a reason, not dropped.
    assert {r.fn for r in rows if r.status == "na_by_design"} == {RST}
    for r in ok:
        assert "attr=" in r.note and r.compute == "dedicated" and r.native_s is None
        if "attr=rest" in r.note:  # classic reads the Spark REST API
            assert r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
    # Spark counts no more bytes sent for 4x the DOUBLEs: NULL, and says so. WKT scales.
    for r in c0:
        assert r.ser_sent_bytes is None and "Spark under-counts numeric" in r.note
    assert all(
        r.ser_sent_bytes > 0
        for r in ok
        if r.fn == "gbx_st_makevalid" and "attr=rest" in r.note
    )
    # The floor form is the same Arrow shell with a constant body: its fingerprint is the
    # constant's, and its median sits beside each variant's in the table.
    h = spark.sql("SELECT pmod(xxhash64('TQ3080'), 2147483647)").first()[0]
    floor = {r.rows: r for r in ok if r.form == "floor"}
    assert len(floor) == 2 and all(
        r.output_fingerprint == str(n * h) for n, r in floor.items()
    )
    table = levels.summarize(rows)
    assert "input unavailable" in table  # a skipped form says why
    for r in c0:
        line = next(
            x for x in table.splitlines() if f"| c0 | dedicated | {r.rows} |" in x
        )
        assert f"| {floor[r.rows].iter_median_s:.3f} |" in line
    # a cached input is named by its recipe version
    assert spark.catalog.tableExists(f"mvp_in_bng_points_v{lf.RECIPE_VERSION}_20000")


class _Forbidden:
    """A Connect-like session. Whatever attribute the code reads that is not set here (a Connect
    session has no sparkContext, _jvm, _jsc, rdd, or settable conf) is recorded: the code under
    test may swallow the error, so the test reads the record."""

    __module__ = "pyspark.sql.connect.session"
    touched: list = []

    def __getattr__(self, name):
        _Forbidden.touched.append(name)
        raise AttributeError(name)


class _Node:
    def __init__(self, name, metrics, children=()):
        self.name, self.children = name, list(children)
        self.metrics = [
            type("M", (), {"name": k, "value": v}) for k, v in metrics.items()
        ]


def _connect_df(root, graph):
    df = type(
        "DataFrame", (_Forbidden,), {"__module__": "pyspark.sql.connect.dataframe"}
    )()
    df.sparkSession = _Forbidden()
    walk = type("C", (), {"extract_graph": lambda s: (root, graph)})
    df.executionInfo = type("I", (), {"metrics": walk()})() if graph else None
    return df


def test_connect_session_records_unavailable_metrics_instead_of_failing():
    _Forbidden.touched.clear()
    none = attribution.from_plan(_connect_df(0, None), 0.0)  # before the plan ran
    assert none["source"] == "none" and all(
        none[k] is None for k in attribution.METRICS
    )
    peak = attribution.WorkerPeak(
        _Forbidden()
    )  # C1: nothing reaches every worker from Connect
    peak.start()
    assert peak.read() is None and "Connect" in peak.reason
    # Two stages (an exchange between them); the Python stage's pipeline holds a nested one.
    graph = {
        0: _Node("AdaptiveSparkPlan", {}, [1]),
        1: _Node("WholeStageCodegen (3)", {"pipelineTime": 10}, [2]),
        2: _Node("Exchange", {"shuffleBytesWritten": 64}, [3]),
        3: _Node("WholeStageCodegen (2)", {"pipelineTime": 2000}, [4]),
        4: _Node("HashAggregate", {"peakMemory": 7, "spillSize": 5}, [5]),
        5: _Node("ArrowEvalPython", {"pythonDataSent": 2048, "pythonTotalTime": 1500}, [6]),
        6: _Node("WholeStageCodegen (1)", {"pipelineTime": 1900}),
    }  # fmt: skip
    got = attribution.from_plan(_connect_df(0, graph), 0.0)
    assert got["source"] == "executionInfo"
    assert (got["ser_sent_bytes"], got["py_eval_s"], got["peak_exec_mem_bytes"]) == (
        2048,
        1.5,
        7,
    )
    assert (got["spill_bytes"], got["shuffle_bytes"]) == (5, 64)
    assert (
        got["ser_recv_bytes"] is None and got["gc_s"] is None
    )  # nobody reported: NULL, not 0
    assert got["py_eval_frac"] == pytest.approx(1.5 / 2.01)  # 2.0 + 0.01, not + 1.9
    assert _Forbidden.touched == []  # nothing a Connect session refuses was even tried


@LINUX
def test_worker_peak_is_the_shapes_not_the_workers_lifetime(spark_session_local):
    from pyspark.sql import functions as F

    spark = spark_session_local

    @F.udf("long")
    def heavy(x):
        b = bytearray(400 * 1024 * 1024)
        for i in range(0, len(b), 4096):
            b[i] = 1  # touch every page
        return len(b)

    @F.udf("long")
    def light(x):
        return x

    def peak_of(df):
        peak = attribution.WorkerPeak(spark)
        peak.start()
        df.collect()
        return peak.read()

    assert peak_of(spark.range(4, numPartitions=4).select(heavy("id"))) >= 400
    # a trivial shape after it does not inherit the 400 MB
    assert peak_of(spark.range(1000, numPartitions=4).select(F.sum(light("id")))) < 150


_N1 = """
import json, sys
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[4]").config("spark.default.parallelism", "2")
         .getOrCreate())
from databricks.labs.gbx.bench import attribution
sys.platform = "linux"  # the slot check runs before any worker is touched
p = attribution.WorkerPeak(spark)
p.start()
print(json.dumps({"peak": p.read(), "reason": p.reason}))
"""


def test_a_stale_default_parallelism_reads_null_not_a_partial_reset():
    # 4 real slots but a default parallelism of 2: a 2-task sweep would reset only some workers
    # and still read a number (a trivial shape after a heavy one read 531 MB, with no note).
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
    }  # same code
    out = subprocess.run(
        [sys.executable, "-c", _N1],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    got = json.loads(out.stdout.strip().splitlines()[-1])
    assert (
        got["peak"] is None
        and "default parallelism (2) != executor slots (4)" in got["reason"]
    )


@pytest.mark.parametrize(
    "master, parallelism, executors, slots",
    [
        # a cluster's driver is no executor, whatever cores it reports; local mode's IS
        ("spark://h:7077", 16, {"driver": 8, "0": 8, "1": 8}, 16),
        ("local[4]", 4, {"driver": 4}, 4),
    ],
)
def test_the_slot_count_leaves_out_a_cluster_drivers_cores(
    monkeypatch, master, parallelism, executors, slots
):
    sc = types.SimpleNamespace(
        master=master, uiWebUrl="http://x", applicationId="a", getConf=dict,
        defaultParallelism=parallelism,
    )  # fmt: skip
    rest = [{"id": i, "totalCores": c, "isActive": True} for i, c in executors.items()]
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(attribution, "_get", lambda url: rest)
    peak = attribution.WorkerPeak(types.SimpleNamespace(sparkContext=sc))
    assert (peak._slots, peak.reason) == (slots, None)


def test_the_floor_udf_is_registered_once_outside_the_timed_region(
    spark_session_local, tmp_path, monkeypatch
):
    from pyspark.sql.udf import UDFRegistration

    calls, real = [], UDFRegistration.register
    monkeypatch.setattr(
        UDFRegistration, "register",
        lambda self, name, *a, **k: calls.append(name) or real(self, name, *a, **k),
    )  # fmt: skip
    only_floor = [f for f in lf.FORMS["S"] if f.variant == "floor"]
    monkeypatch.setitem(lf.FORMS, "S", only_floor)
    rows = run_s(spark_session_local, tmp_path, scales=(1, 2))
    assert [r.status for r in rows] == ["ok", "ok"]  # four timed iterations in all
    assert calls.count("gbx_bng_floor") == 1


def test_a_busy_cluster_cannot_stall_the_worker_reset(spark_session_local, monkeypatch):
    from pyspark.sql import functions as F

    spark = spark_session_local
    slots = spark.sparkContext.defaultParallelism
    monkeypatch.setattr(sys, "platform", "linux")  # only the wait is under test
    monkeypatch.setattr(attribution.WorkerPeak, "TIMEOUT_S", 2.0)
    monkeypatch.setattr(attribution.WorkerPeak, "_failed", {})
    nap = F.udf(lambda x: time.sleep(7) or x, "long")
    busy = threading.Thread(
        target=lambda: spark.range(slots, numPartitions=slots)
        .select(nap("id"))
        .collect()
    )
    busy.start()
    time.sleep(1.5)  # every slot is taken
    peak, t0 = attribution.WorkerPeak(spark), time.time()
    peak.start()
    assert time.time() - t0 < 6 and peak.read() is None and "timed out" in peak.reason
    busy.join()


@pytest.mark.parametrize(
    "text, expect",
    [
        ("total (min, med, max (stageId: taskId))\n398.5 KiB (199.2 KiB, 1 B)", 398.5 * 1024),
        ("total (min, med, max (stageId: taskId))\n1.8 s (893 ms, 894 ms)", 1.8),
        ("\n893 ms (3 ms, 4 ms)", 0.893),
        ("20,000", 20000.0),
        ("128.0 B", 128.0),
        ("2.5 m", 150.0),
        (7, 7.0),
        ("n/a", 0.0),
    ],
)  # fmt: skip
def test_rest_metric_text_is_read_past_its_header(text, expect):
    assert attribution._num(text) == pytest.approx(expect)


@pytest.mark.parametrize(
    "t0, expect",
    [
        (1000.0, 7),
        (1000.2, 8),
        (1000.4, 9),
        (1001.0, None),
    ],  # first at or after t0, not newest
)
def test_the_rest_execution_is_the_one_the_action_started(t0, expect):
    stamp = (
        lambda s: f"1970-01-01T00:{int(s) // 60 % 60:02d}:{s % 60:06.3f}GMT"
    )  # noqa: E731
    times = ((6, 995.0), (7, 1000.1), (8, 1000.3), (9, 1000.5))
    execs = [{"id": i, "submissionTime": stamp(t)} for i, t in times]
    got = attribution._pick_execution(execs, t0)
    assert (got["id"] if got else None) == expect


def test_old_rows_load_and_unavailable_is_null_in_the_delta_columns(
    tmp_path, spark_session_local
):
    fields = dataclasses.fields(results.ResultRow)
    old = {
        f.name: "x" if f.type in (str, "str") else 0
        for f in fields
        if f.default is dataclasses.MISSING
    }
    f = tmp_path / "old.jsonl"
    f.write_text(json.dumps(old) + "\n")
    (row,) = results.read_jsonl(f)
    assert (row.level, row.form, row.compute, row.failure_mode) == (
        "na",
        "na",
        "na",
        "",
    )
    new = (
        "ser_sent_bytes",
        "py_eval_s",
        "py_eval_frac",
        "native_s",
        "py_worker_rss_mb",
    )
    assert all(getattr(row, k) is None for k in new)  # NULL, not a made-up 0
    assert {"level", "form", "compute", "failure_mode", "gc_s", "py_eval_frac"} <= set(
        cl.ORDER
    )
    # A batch where a metric is None in every row still has the column's real type, so it
    # appends to a table that holds numbers (an all-None pandas column would be `void`).
    df = cl.rows_to_dataframe([row], spark_session_local)
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert types["ser_sent_bytes"] == "bigint" and types["gc_s"] == "double"
    assert "void" not in types.values() and df.collect()[0]["ser_sent_bytes"] is None


def test_a_form_with_null_outputs_is_an_error_unless_it_allows_them(
    spark_session_local, monkeypatch, tmp_path
):
    sql = "SELECT count(*) AS n, sum(x) AS fp, count(x) AS n_out FROM (SELECT nullif(id, 3) AS x FROM {src})"

    def view(spark, n, corpus, schema):
        spark.range(n).createOrReplaceTempView("t_null_in")
        return "t_null_in"

    monkeypatch.setitem(lf.INPUTS, "t_in", view)
    out = {}
    for allows in (False, True):
        form = levels.Form(
            "T", "f", "L1", "-", "t_in", sql, register=False, allows_null=allows
        )
        monkeypatch.setitem(lf.FORMS, "T", [form])
        (out[allows],) = run_s(
            spark_session_local, tmp_path, (1,), 10, mvp="T", only_levels=None
        )
    assert (
        out[False].status == "error"
        and "1 of 10 outputs NULL" in out[False].failure_mode
    )
    assert out[True].status == "ok" and "1 of 10 outputs NULL" in out[True].note


def test_levels_leg_on_a_connect_session_reports_what_executioninfo_carries(
    spark_connect_local, tmp_path
):
    # Runs alone (the fixture skips when a classic session is already up in the process).
    ok = [
        r
        for r in run_s(spark_connect_local, tmp_path, scales=(1, 4))
        if r.status == "ok"
    ]
    assert ok and all(r.compute == "standard" for r in ok)
    for r in ok:
        assert "attr=executionInfo" in r.note and "unavailable:" in r.note
        assert r.gc_s is None and r.py_worker_rss_mb is None  # not measurable here
        assert r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
        assert "Connect" in r.note  # the reason the worker peak is NULL


def test_notebook_runs_the_levels_leg_alone_when_asked():
    cfg = dict(
        wheel="/Volumes/c/s/v/w.whl", corpus="/Volumes/c/s/v/corpus", out_dir="/Volumes/c/s/v/out",
        table="c.s.bench_results", run_id="r1", functions="", modes="spark-path", row_counts="10",
        warmup=0, measured=1, lightweight=True, levels_mvp="S", levels_uc="c.s",
    )  # fmt: skip

    def sources(**kw):
        nb = cl.build_bench_notebook({**cfg, **kw})
        return "\n".join("".join(c["source"]) for c in nb["cells"])

    both, only = sources(), sources(levels_only=True)
    assert "LEVELS_MVP = 'S'" in both and "LEVELS_UC = 'c.s'" in both
    assert "_lv.run_mvp(" in both and "_lv.run_mvp(" in only
    assert "LEVELS_ONLY = False" in both and "LEVELS_ONLY = True" in only
    # the function-benchmark sections run beside the leg, and not at all with --levels-only
    assert "# (c) Lightweight spark-path" in both
    assert "# (c) Lightweight spark-path" not in only
    assert "_lv.run_mvp(" not in sources(levels_mvp="")  # off unless asked
