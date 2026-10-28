"""The MVP levels leg: forms timed per level, with the attribution split beside the timing."""

import dataclasses
import importlib.util
import json
import os
import subprocess
import sys

import pytest

from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import levels, results


def _run_s(spark, tmp_path, scales, base_rows=2000, **kw):
    return levels.run_mvp(
        spark,
        "S",
        "t1",
        1,
        1,
        corpus=str(tmp_path),
        schema=None,
        scales=scales,
        where="venv",
        only_levels=("L3",),
        base_rows=base_rows,
        **kw,
    )


def test_levels_leg_runs_mvp_s_light_forms(spark_session_local, tmp_path):
    # 20k rows and up: Spark's sent-bytes counter stops growing past one 10k-row Arrow batch
    rows = _run_s(spark_session_local, tmp_path, scales=(1, 4), base_rows=20000)
    assert (
        rows
        and {r.level for r in rows} == {"L3"}
        and all(r.status in ("ok", "na_by_design") for r in rows)
    )
    ok = [r for r in rows if r.status == "ok"]
    assert all(r.iter_median_s > 0 and r.rows > 0 and r.output_fingerprint for r in ok)
    bng = {r.form: r.output_fingerprint for r in ok if r.fn == "gbx_bng_eastnorthasbng"}
    assert len(set(bng.values())) == 1  # every L3 variant computes the same cells
    # No tile corpus here: the raster forms are skipped with a reason, not dropped.
    assert {r.fn for r in rows if r.status == "na_by_design"} == {
        "gbx_rst_transformcrs"
    }
    table = levels.summarize(rows)
    assert "| gbx_bng_eastnorthasbng | L3 | c0 | dedicated |" in table
    assert "input unavailable" in table  # a skipped form says why
    for r in ok:
        assert "attr=" in r.note and r.compute == "dedicated"
        assert r.native_s is None  # NULL until a level reports it
    # The split rides in the same row; a classic session reads the Spark REST API.
    for r in (r for r in ok if "attr=rest" in r.note):
        assert (
            r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
        )  # I1: plan metric, no profiler
        assert "profiler" not in r.note
    by_fn = {fn: [r for r in ok if r.fn == fn] for fn in {r.fn for r in ok}}
    # I2: Spark reports no more bytes sent for 4x the DOUBLEs, so that figure is NULL, said so;
    # string input (WKT) does scale and keeps its figure.
    for r in by_fn["gbx_bng_eastnorthasbng"]:
        assert r.ser_sent_bytes is None and "Spark under-counts numeric" in r.note
        assert r.ser_recv_bytes > 0
    for r in (r for r in by_fn["gbx_st_makevalid"] if "attr=rest" in r.note):
        assert r.ser_sent_bytes > 0


def _refused(name):
    def get(self):
        _Forbidden.touched.append(name)
        raise RuntimeError(f"{name} is not available on a Connect session")

    return property(get)


class _Forbidden:
    """A Connect-like session. What Connect refuses is recorded when touched (and raises, but
    the code under test may swallow that, so the test reads the record)."""

    __module__ = "pyspark.sql.connect.session"
    touched: list = []
    sparkContext = _refused("sparkContext")
    _jvm = _refused("_jvm")
    _jsc = _refused("_jsc")
    rdd = _refused("rdd")
    conf = _refused("conf")

    def range(self, *a, **k):
        raise RuntimeError("no cluster behind this stub")


class _Node:
    def __init__(self, name, metrics, children=()):
        self.name, self.metrics, self.children = name, metrics, list(children)


class _M:
    def __init__(self, name, value):
        self.name, self.value = name, value


class _Info:
    def __init__(self, root, graph):
        self.metrics = type("C", (), {"extract_graph": lambda s: (root, graph)})()


def _connect_df(info):
    df = type(
        "DataFrame", (_Forbidden,), {"__module__": "pyspark.sql.connect.dataframe"}
    )()
    df.sparkSession, df.executionInfo = _Forbidden(), info
    return df


def test_connect_session_records_unavailable_metrics_instead_of_failing():
    spark = _Forbidden()
    _Forbidden.touched.clear()
    # Before the plan ran there are no metrics at all: every one is None, none raises.
    none = attribution.from_plan(_connect_df(None), 0.0)
    assert none["source"] == "none"
    assert all(none[k] is None for k in attribution.METRICS)
    # C1: nothing can reset every Python worker from a Connect session: NULL, with the reason.
    peak = attribution.WorkerPeak(spark)
    peak.start()
    assert peak.read() is None and "Connect" in peak.reason
    # After it ran, executionInfo answers what it carries and the rest stay None. The plan is
    # two stages (an exchange between them); the Python stage's pipeline holds a nested one.
    graph = {
        0: _Node("AdaptiveSparkPlan", [], [1]),
        1: _Node("WholeStageCodegen (3)", [_M("pipelineTime", 10)], [2]),
        2: _Node("Exchange", [_M("shuffleBytesWritten", 64)], [3]),
        3: _Node(
            "WholeStageCodegen (2)",
            [_M("pipelineTime", 2000)],
            [4],
        ),
        4: _Node(
            "HashAggregate",
            [_M("peakMemory", 7), _M("spillSize", 5)],
            [5],
        ),
        5: _Node(
            "ArrowEvalPython",
            [_M("pythonDataSent", 2048), _M("pythonTotalTime", 1500)],
            [6],
        ),
        6: _Node("WholeStageCodegen (1)", [_M("pipelineTime", 1900)]),
    }
    got = attribution.from_plan(_connect_df(_Info(0, graph)), 0.0)
    assert got["source"] == "executionInfo"
    assert (got["ser_sent_bytes"], got["py_eval_s"], got["peak_exec_mem_bytes"]) == (
        2048,
        1.5,
        7,
    )
    assert (got["spill_bytes"], got["shuffle_bytes"]) == (5, 64)
    assert got["ser_recv_bytes"] is None  # no node reported it: unknown, not 0
    assert got["py_eval_frac"] == pytest.approx(1.5 / 2.01)  # 2.0 + 0.01, not + 1.9
    assert got["gc_s"] is None  # GC needs the driver's stage list: not on Connect
    assert _Forbidden.touched == []  # nothing a Connect session refuses was even tried


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="reads /proc/self/clear_refs"
)
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

    def peak_of(df_fn):
        peak = attribution.WorkerPeak(spark)
        peak.start()
        df_fn()
        return peak.read()

    big = peak_of(lambda: spark.range(4, numPartitions=4).select(heavy("id")).collect())
    small = peak_of(
        lambda: spark.range(1000, numPartitions=4).select(F.sum(light("id"))).collect()
    )
    assert big >= 400  # the 400 MB the shape held
    assert small < 150  # a trivial shape after it does not inherit it


@pytest.mark.parametrize(
    "text, expect",
    [
        (
            "total (min, med, max (stageId: taskId))\n398.5 KiB (199.2 KiB, 1 B)",
            398.5 * 1024,
        ),
        ("total (min, med, max (stageId: taskId))\n1.8 s (893 ms, 894 ms)", 1.8),
        ("\n893 ms (3 ms, 4 ms)", 0.893),
        ("20,000", 20000.0),
        ("128.0 B", 128.0),
        ("2.5 m", 150.0),
        (7, 7.0),
        ("n/a", 0.0),
    ],
)
def test_rest_metric_text_is_read_past_its_header(text, expect):
    assert attribution._num(text) == pytest.approx(expect)


@pytest.mark.parametrize(
    "t0, expect",
    [
        (1000.0, 7),  # the first execution at or after t0, not the newest (9)
        (1000.2, 8),
        (1000.4, 9),
        (1001.0, None),  # nothing has started since: the driver has not caught up
    ],
)
def test_the_rest_execution_is_the_one_the_action_started(t0, expect):
    stamp = (
        lambda s: f"1970-01-01T00:{int(s) // 60 % 60:02d}:{s % 60:06.3f}GMT"
    )  # noqa: E731
    execs = [
        {"id": 6, "submissionTime": stamp(1000.0 - 5)},  # the previous form's query
        {"id": 7, "submissionTime": stamp(1000.1)},
        {"id": 8, "submissionTime": stamp(1000.3)},
        {"id": 9, "submissionTime": stamp(1000.5)},
    ]
    got = attribution._pick_execution(execs, t0)
    assert (got["id"] if got else None) == expect


def test_old_rows_load_and_unavailable_is_null_in_the_delta_columns(
    tmp_path, spark_session_local
):
    old = {
        f.name: ("x" if f.type in (str, "str") else 0)
        for f in dataclasses.fields(results.ResultRow)
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
    # The Delta column order carries every new field (cluster.ORDER is checked against them).
    assert {"level", "form", "compute", "failure_mode", "gc_s", "py_eval_frac"} <= set(
        cl.ORDER
    )
    # A batch where a metric is None in every row still has the column's real type, so it
    # appends to a table that holds numbers (an all-None pandas column would be `void`).
    df = cl.rows_to_dataframe([row], spark_session_local)
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert types["ser_sent_bytes"] == "bigint" and types["gc_s"] == "double"
    assert "void" not in types.values()
    assert df.collect()[0]["ser_sent_bytes"] is None


def test_a_form_with_null_outputs_is_an_error_unless_it_allows_them(
    spark_session_local, monkeypatch, tmp_path
):
    from databricks.labs.gbx.bench import levels_forms as lf

    sql = (
        "SELECT count(*) AS n, sum(x) AS fp, count(x) AS n_out "
        "FROM (SELECT nullif(id, 3) AS x FROM {src})"
    )

    def view(spark, n, corpus, schema):
        spark.range(n).createOrReplaceTempView("t_null_in")
        return "t_null_in"

    def form(**kw):
        return levels.Form("T", "f", "L1", "-", "t_in", sql, register=False, **kw)

    monkeypatch.setitem(lf.INPUTS, "t_in", view)
    out = {}
    for allows in (False, True):
        monkeypatch.setitem(lf.FORMS, "T", [form(allows_null=allows)])
        (out[allows],) = levels.run_mvp(
            spark_session_local,
            "T",
            "t1",
            0,
            1,
            corpus=str(tmp_path),
            schema=None,
            scales=(1,),
            where="venv",
            base_rows=10,
        )
    assert (
        out[False].status == "error"
        and "1 of 10 outputs NULL" in out[False].failure_mode
    )
    assert out[True].status == "ok" and "1 of 10 outputs NULL" in out[True].note


def test_input_tables_are_named_by_recipe_version(spark_session_local, monkeypatch):
    from databricks.labs.gbx.bench import levels_forms as lf

    names = set()
    for v in (1, 2):
        monkeypatch.setattr(lf, "RECIPE_VERSION", v)
        names.add(lf.INPUTS["bng_points"](spark_session_local, 10, "", None))
    assert len(names) == 2  # a changed recipe never reuses an older recipe's table


def test_levels_leg_on_a_connect_session_reports_what_executioninfo_carries(
    spark_connect_local, tmp_path
):
    # Runs alone (the fixture skips when a classic session is already up in the process).
    ok = [
        r
        for r in _run_s(spark_connect_local, tmp_path, scales=(1, 4))
        if r.status == "ok"
    ]
    assert ok and all(r.compute == "standard" for r in ok)
    for r in ok:
        assert "attr=executionInfo" in r.note and "unavailable:" in r.note
        assert r.gc_s is None and r.py_worker_rss_mb is None  # not measurable here
        assert r.py_eval_s > 0 and 0 < r.py_eval_frac <= 1.0
        assert "Connect" in r.note  # the reason the worker peak is NULL


_FRAC_SCRIPT = """
import json, sys, tempfile, time
import pandas as pd
from pyspark.sql import SparkSession, functions as F
b = SparkSession.builder
spark = (b.remote("local[4]") if sys.argv[1] == "connect" else b.master("local[4]")).getOrCreate()
from databricks.labs.gbx.bench import attribution, levels

# A Python-bound shape: the light BNG function.
rows = levels.run_mvp(spark, "S", "frac", 1, 1, corpus=tempfile.mkdtemp(), schema=None,
                      scales=(1,), where="venv", only_levels=("L3",), base_rows=200000)
out = {r.fn: r.py_eval_frac for r in rows if r.status == "ok"}

# A mixed one: a trivial UDF beside JVM-heavy string work in the same stage.
@F.pandas_udf("long")
def keep(s: pd.Series) -> pd.Series:
    return s
spark.udf.register("keep_udf", keep)
q = ("SELECT sum(keep_udf(id)) + sum(length(regexp_replace(repeat(cast(id AS string), 60), '1', 'x')))"
     " FROM range(1500000)")
for _ in range(2):
    t0 = time.time()
    df = spark.sql(q)
    df.collect()
out["mixed"] = attribution.from_plan(df, t0)["py_eval_frac"]
print(json.dumps(out))
"""

# Absolute. Both read timers that are rounded (the REST text) or summed over tasks. Idle, the two
# kinds differ by under 0.06 and two runs of one kind by about 0.07; a loaded machine moves the
# mixed shape more, and a wrong denominator (a nested pipeline counted twice) moves it by 0.3+.
TOLERANCE = 0.2


@pytest.mark.skipif(
    importlib.util.find_spec("grpc") is None,
    reason="Spark Connect client deps missing: run gbx:venv:sync --connect",
)
def test_py_eval_frac_agrees_between_classic_and_connect():
    # One process cannot hold both kinds of session, so each runs the same shapes on local[4]
    # in its own interpreter. The ratio is Python task-seconds over executor task-seconds,
    # read from the REST stages on classic and from the plan's pipeline times on Connect.
    env = {
        **os.environ,
        "PYSPARK_PYTHON": sys.executable,
        "PYSPARK_DRIVER_PYTHON": sys.executable,
    }
    # One after the other: with the two on the same cores the mixed shape's share moves by 0.1+.
    got = {}
    for kind in ("classic", "connect"):
        out = subprocess.run(
            [sys.executable, "-c", _FRAC_SCRIPT, kind],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            env=env,
            timeout=600,
        ).stdout
        got[kind] = json.loads(out.strip().splitlines()[-1])
    assert (
        got["classic"].keys()
        == got["connect"].keys()
        == {
            "gbx_bng_eastnorthasbng",
            "gbx_st_makevalid",
            "mixed",
        }
    )
    for shape, frac in got["classic"].items():
        assert 0 < frac <= 1.0 and 0 < got["connect"][shape] <= 1.0
        assert abs(frac - got["connect"][shape]) <= TOLERANCE, (shape, got)
    # the share of the stage that is not Python shows up on both
    assert got["classic"]["mixed"] < 0.95 and got["connect"]["mixed"] < 0.95


def test_notebook_runs_the_levels_leg_alone_when_asked():
    cfg = dict(
        wheel="/Volumes/c/s/v/w.whl",
        corpus="/Volumes/c/s/v/corpus",
        out_dir="/Volumes/c/s/v/out",
        table="c.s.bench_results",
        run_id="r1",
        functions="",
        modes="spark-path",
        row_counts="10",
        warmup=0,
        measured=1,
        lightweight=True,
        levels_mvp="S",
        levels_uc="c.s",
    )

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
