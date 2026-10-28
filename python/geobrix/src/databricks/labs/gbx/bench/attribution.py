"""Attribution split for the levels leg: serialization, Python, shuffle, spill, memory, GC.

Where each number comes from depends on the session:

* Classic sessions read the driver's Spark REST API: the plan's SQL metrics of the execution
  the action started, and the GC and run time of the stages of that execution's jobs.
* Connect sessions (standard access mode, Serverless) read ``df.executionInfo`` (PySpark 4.0+),
  which carries the plan's SQL metrics only.

A Connect session has no ``sparkContext``, ``.rdd``, ``_jvm`` or ``_jsc``, and refuses most
``spark.conf.set`` calls, so nothing here touches them there. What cannot be measured is
``None`` ("unavailable"), never 0 and never an exception; ``source`` says which API answered.
A plan metric is None unless some node of the plan reported it: the REST API leaves out a metric
that was never updated, so on a classic session "no spill" reads as None, not 0.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional

from databricks.labs.gbx.manager.capabilities import is_connect

# The metrics `from_plan` reports, as ResultRow field names.
METRICS = (
    "ser_sent_bytes",
    "ser_recv_bytes",
    "py_eval_s",
    "py_eval_frac",
    "shuffle_bytes",
    "spill_bytes",
    "peak_exec_mem_bytes",
    "gc_s",
)
# Plan metric -> (the engine's key, the Spark UI's label). Connect answers with the key,
# the REST API with the label. Both compare lower-cased.
_BUCKETS = {
    "ser_sent_bytes": ("pythondatasent", "data sent to python workers"),
    "ser_recv_bytes": ("pythondatareceived", "data returned from python workers"),
    "py_eval_s": ("pythontotaltime", "time to run python workers"),
    "shuffle_bytes": ("shufflebyteswritten", "shuffle bytes written"),
    "spill_bytes": ("spillsize", "spill size"),
    "peak_exec_mem_bytes": ("peakmemory", "peak memory"),
}
_CONNECT_SCALE = {
    "py_eval_s": 1e-3
}  # Connect hands over timing metrics in milliseconds
_PIPELINE_TIME = (
    "pipelinetime",
    "duration",
)  # a whole-stage pipeline's time, key / label
_UNITS = {
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
    "kb": 1e3,
    "mb": 1e6,
    "gb": 1e9,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}
_REST_WAIT_S = 3.0  # the SQL execution's end event reaches the REST store a moment late


def _bucket(name: str) -> Optional[str]:
    n = name.strip().lower()
    return next((k for k, names in _BUCKETS.items() if n in names), None)


def _num(text) -> float:
    """A REST metric value in base units. Timing and size metrics put the number under a
    ``total (min, med, max ...)`` header; counters are one line."""
    if isinstance(text, (int, float)):
        return float(text)
    for line in str(text).split("\n"):
        m = re.match(r"\s*([0-9][0-9,]*\.?[0-9]*)\s*([a-zA-Z]*)", line)
        if m:
            return float(m.group(1).replace(",", "")) * _UNITS.get(
                m.group(2).lower(), 1.0
            )
    return 0.0


def _add(out: Dict, key: str, v: float) -> None:
    if out[key] is None:
        out[key] = v
    else:
        out[key] = max(out[key], v) if key == "peak_exec_mem_bytes" else out[key] + v


def _epoch(ts) -> float:
    if not ts:
        return 0.0
    return datetime.strptime(
        ts.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z"
    ).timestamp()


def _get(url: str):
    if not url.startswith("http"):
        raise ValueError(f"not an http url: {url}")
    with urllib.request.urlopen(
        url, timeout=10
    ) as r:  # noqa: S310 - the driver's own UI
        return json.load(r)


def _fraction(py_s: Optional[float], task_s: Optional[float]) -> Optional[float]:
    """Python task-seconds over executor task-seconds. The REST text rounds to two or three
    digits, so a run that is all Python can come out a hair over 1: capped there."""
    if py_s is None or not task_s:
        return None
    return min(1.0, py_s / task_s)


def _stage_root_seconds(root, graph) -> Optional[float]:
    """Executor task-seconds from a Connect plan: the pipeline time of every whole-stage node
    that is the outermost one of its stage. A nested one (the child of a Python node, inside the
    parent's pipeline) is already counted in its parent; an exchange starts a new stage.
    """
    total: List[float] = []
    seen = set()

    def walk(i, inside: bool) -> None:
        if i in seen or i not in graph:
            return
        seen.add(i)
        node = graph[i]
        if "Exchange" in node.name or "QueryStage" in node.name:
            inside = False
        elif node.name.startswith("WholeStageCodegen"):
            if not inside:
                total.extend(
                    float(m.value) * 1e-3
                    for m in node.metrics
                    if m.name.lower() in _PIPELINE_TIME
                )
            inside = True
        for child in node.children:
            walk(child, inside)

    walk(root, False)
    return sum(total) if total else None


def _from_execution_info(df, out: Dict) -> bool:
    info = getattr(df, "executionInfo", None)
    metrics = getattr(info, "metrics", None)
    if metrics is None:
        return False
    root, graph = metrics.extract_graph()
    for node in graph.values():
        for m in node.metrics:
            k = _bucket(m.name)
            if k:
                _add(out, k, float(m.value) * _CONNECT_SCALE.get(k, 1.0))
    out["py_eval_frac"] = _fraction(out["py_eval_s"], _stage_root_seconds(root, graph))
    return True


def _pick_execution(execs: List[Dict], t0: float) -> Optional[Dict]:
    """The first SQL execution that started at or after `t0` (epoch seconds), not the newest
    one: a driver whose listener bus lags still lists the previous query as the newest.
    """
    mine = [e for e in execs if _epoch(e.get("submissionTime")) >= t0 - 0.05]
    return min(mine, key=lambda e: e["id"]) if mine else None


def _from_rest(df, t0: float, out: Dict) -> bool:
    try:
        sc = df.sparkSession.sparkContext
        if not sc.uiWebUrl:
            return False
        base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
        deadline = time.time() + _REST_WAIT_S
        while True:
            mine = _pick_execution(
                _get(f"{base}/sql?details=true&planDescription=false&length=100000"), t0
            )
            if (mine and mine.get("status") != "RUNNING") or time.time() > deadline:
                break
            time.sleep(0.1)
        if mine is None:
            return False
        stages: Dict[int, List[Dict]] = {}
        for jid in mine.get("successJobIds", []):
            for sid in _get(f"{base}/jobs/{jid}")["stageIds"]:
                if sid not in stages:
                    stages[sid] = [
                        s
                        for s in _get(f"{base}/stages/{sid}")
                        if s["status"] == "COMPLETE"
                    ]
    except (
        Exception
    ):  # noqa: BLE001 - no UI URL, or not reachable from here: stays unavailable
        return False
    for node in mine.get("nodes", []):
        for m in node.get("metrics", []):
            k = _bucket(m["name"])
            if k:
                _add(out, k, _num(m["value"]))
    attempts = [a for done in stages.values() for a in done]
    if attempts:
        out["gc_s"] = sum(a.get("jvmGcTime", 0) for a in attempts) / 1000.0
        task_s = sum(a.get("executorRunTime", 0) for a in attempts) / 1000.0
        out["py_eval_frac"] = _fraction(out["py_eval_s"], task_s)
    return True


def from_plan(df, t0: float) -> Dict[str, Optional[float]]:
    """The plan's metrics for the query `df` just ran, which started at or after `t0` (epoch
    seconds). Every key of METRICS, plus ``source``: ``rest`` | ``executionInfo`` | ``none``.

    ``py_eval_s`` is Python-worker task-seconds (summed over tasks) and ``py_eval_frac`` that
    over the execution's executor task-seconds. ``gc_s`` is classic only.
    """
    out: Dict = {k: None for k in METRICS}
    out["source"] = "none"
    if is_connect(df) or is_connect(getattr(df, "sparkSession", None)):
        if _from_execution_info(df, out):
            out["source"] = "executionInfo"
    elif _from_rest(df, t0, out):
        out["source"] = "rest"
    return out


def unavailable(measures: Dict, skip=()) -> List[str]:
    """The names no source answered (None), for a row's note; `skip` are not applicable."""
    return [
        k for k, v in measures.items() if k != "source" and v is None and k not in skip
    ]


class WorkerPeak:
    """Peak RSS (MB) of the Python workers while one shape ran.

    The kernel keeps a high-water mark per process (``VmHWM``) that only grows, and Spark
    reuses Python workers across queries, so reading it after a shape returns the biggest shape
    any earlier query ran on that worker. ``start()`` resets it on every worker (writing 5 to
    ``/proc/self/clear_refs``), ``read()`` collects it afterwards. Both go through a barrier
    stage with one task per slot, which puts one task on every worker process; a plain job
    cannot promise that. ``read()`` is None, with the reason in ``reason``, where that is not
    possible: off Linux, and on a Connect session (it cannot count the slots or reach a
    worker).

    Why not sampling: a sampler would have to run inside the workers during the shape and
    compete with it for slots. The reset needs no thread, and it costs two short jobs.
    """

    # A barrier stage that cannot get a slot per task is retried by the scheduler for minutes. If
    # a sweep fails, the session is not asked again: one stall per run, not one per form.
    _failed: Dict[int, str] = {}

    def __init__(self, spark):
        self.spark = spark
        self.reason: Optional[str] = None
        self._slots = 0
        if is_connect(spark):
            self.reason = "a Connect session cannot reach every Python worker"
        elif not sys.platform.startswith("linux"):
            self.reason = "per-shape worker memory needs Linux (/proc/self/clear_refs)"
        elif id(spark) in WorkerPeak._failed:
            self.reason = WorkerPeak._failed[id(spark)]
        else:
            try:
                sc = spark.sparkContext
                cpus = max(1, int(sc.getConf().get("spark.task.cpus", "1")))
                self._slots = max(1, int(sc.defaultParallelism) // cpus)
            except Exception as exc:  # noqa: BLE001
                self.reason = f"slot count unavailable ({type(exc).__name__})"

    def _sweep(self, reset: bool) -> Optional[float]:
        def task(
            it,
        ):  # nested: pickled by value, so a worker needs no geobrix to run it
            import pandas as pd  # imported first: part of the baseline, not of the shape

            for _ in it:
                pass
            hwm = 0.0
            with open("/proc/self/status") as fh:
                for line in fh:
                    if line.startswith("VmHWM:"):
                        hwm = float(line.split()[1]) / 1024.0
            if reset:
                with open("/proc/self/clear_refs", "w") as fh:
                    fh.write("5")
            yield pd.DataFrame({"hwm": [hwm]})

        n = self._slots
        row = (
            self.spark.range(n, numPartitions=n)
            .mapInPandas(task, "hwm double", barrier=True)
            .agg({"hwm": "max"})
            .first()
        )
        return float(row[0]) if row and row[0] is not None else None

    def start(self) -> None:
        if self.reason is None:
            try:
                self._sweep(reset=True)
            except Exception as exc:  # noqa: BLE001
                self.reason = (
                    f"worker reset failed ({type(exc).__name__}: {str(exc)[:120]})"
                )
                WorkerPeak._failed[id(self.spark)] = self.reason

    def read(self) -> Optional[float]:
        if self.reason is not None:
            return None
        try:
            return self._sweep(reset=False)
        except Exception as exc:  # noqa: BLE001
            self.reason = f"worker read failed ({type(exc).__name__}: {str(exc)[:120]})"
            WorkerPeak._failed[id(self.spark)] = self.reason
            return None
