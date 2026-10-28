"""The MVP levels leg: forms timed per level, with the attribution split beside the timing."""

import dataclasses
import json

import pytest

from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import levels, results


def test_levels_leg_runs_mvp_s_light_forms(spark_session_local, tmp_path):
    spark = spark_session_local
    rows = levels.run_mvp(
        spark,
        "S",
        "t1",
        0,
        1,
        corpus=str(tmp_path),
        schema=None,
        scales=(1,),
        where="venv",
        only_levels=("L3",),
        base_rows=2000,
    )
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
    # The split rides in the same row: a classic session reads the Spark REST API.
    for r in ok:
        assert "attr=" in r.note and r.compute == "dedicated"
        assert ("attr=rest" in r.note and r.ser_sent_bytes > 0) or "attr=none" in r.note


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
    def __init__(self, metrics):
        self.metrics = metrics


class _M:
    def __init__(self, name, value):
        self.name, self.value = name, value


class _Info:
    def __init__(self, graph):
        self.metrics = type("C", (), {"extract_graph": lambda s: (0, graph)})()


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
    assert attribution.python_eval_s(spark, lambda: None) is None
    assert attribution.worker_rss_mb(spark, 2) is None
    # After it ran, executionInfo answers what it carries and the rest stay None.
    graph = {
        0: _Node([_M("pythonDataSent", 2048), _M("pythonTotalTime", 1500)]),
        1: _Node([_M("peakMemory", 7), _M("peakMemory", 9), _M("spillSize", 5)]),
    }
    got = attribution.from_plan(_connect_df(_Info(graph)), 0.0)
    assert got["source"] == "executionInfo"
    assert (got["ser_sent_bytes"], got["py_eval_s"], got["peak_exec_mem_bytes"]) == (
        2048,
        1.5,
        9,
    )
    assert (got["spill_bytes"], got["shuffle_bytes"]) == (5, 0)
    assert got["gc_s"] is None  # GC needs the driver's stage list: not on Connect
    assert _Forbidden.touched == []  # nothing a Connect session refuses was even tried


def test_levels_leg_on_a_connect_session_reports_what_executioninfo_carries(
    spark_connect_local, tmp_path
):
    # Runs alone (the fixture skips when a classic session is already up in the process).
    rows = levels.run_mvp(
        spark_connect_local,
        "S",
        "t1",
        0,
        1,
        corpus=str(tmp_path),
        schema=None,
        scales=(1,),
        where="venv",
        only_levels=("L3",),
        base_rows=2000,
    )
    ok = [r for r in rows if r.status == "ok"]
    assert ok and all(r.compute == "standard" for r in ok)
    for r in ok:
        assert "attr=executionInfo" in r.note and "unavailable: gc_s" in r.note
        assert r.ser_sent_bytes > 0 and r.py_eval_s > 0 and r.gc_s == 0.0


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


def test_new_result_fields_default_so_old_rows_still_load(tmp_path):
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
    assert (row.ser_sent_bytes, row.py_eval_s, row.py_worker_rss_mb) == (0, 0.0, 0.0)
    # The Delta column order carries every new field (cluster.ORDER is checked against them).
    assert {"level", "form", "compute", "failure_mode", "gc_s"} <= set(cl.ORDER)


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
