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
import threading
import time
import urllib.request
import uuid
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


# The sweeps run in Python workers of their own: a worker environment variable gives them a pool
# separate from the shape's (Spark keeps one pool per Python executable and worker environment), so
# a sweep never lends its CPU time or its memory to a worker that ran the shape.
_SWEEP_ENV = "GBX_PEAK_SWEEP"


def _worker_finder():
    """A function that lists the Python workers of the shape: every ``python -m pyspark.daemon``
    / ``pyspark.worker`` process below the executor's JVM that is not one of the sweep's own.

    A factory, so that what it returns is pickled by value and a worker needs no geobrix to run
    it; it reads only /proc.
    """

    def is_worker(
        pid,
    ):  # the daemon and the workers it forks both run `python -m pyspark.daemon`
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                args = fh.read().split(b"\0")
        except OSError:
            return False
        at = args.index(b"-m") + 1 if b"-m" in args else len(args)
        return args[at : at + 1] in ([b"pyspark.daemon"], [b"pyspark.worker"])

    def is_sweep(pid):  # started with the sweep's worker environment
        try:
            with open(f"/proc/{pid}/environ", "rb") as fh:
                return f"{_SWEEP_ENV}=1".encode() in fh.read().split(b"\0")
        except OSError:
            return False  # unreadable: counted, which can only over-state

    def parent(pid):
        try:
            with open(f"/proc/{pid}/stat") as fh:  # "pid (comm) S ppid ..."
                return int(fh.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            return 0

    def find(me):
        import os

        jvm = parent(me)  # the executor: the first ancestor that is no Python worker
        while jvm > 1 and is_worker(jvm):
            jvm = parent(jvm)
        children = {}
        for entry in os.listdir("/proc"):
            if entry.isdigit():
                children.setdefault(parent(int(entry)), []).append(int(entry))
        workers, todo = set(), [jvm]
        while todo:
            for pid in children.get(todo.pop(), []):
                if is_worker(pid) and not is_sweep(pid):
                    workers.add(pid)
                    todo.append(pid)  # the daemon's workers
        return sorted(workers)

    return find


def _sweep_task(reset, find_workers, baseline):
    """The barrier task of one sweep, over the shape's Python workers of its executor.

    With `reset` it clears each worker's high-water mark and returns (as ``ticks``) the CPU time
    each had used by then; how many live workers it could not reset is ``missed``. Without,
    it returns the highest ``VmHWM`` (MB) of the workers that ran since: new ones, and those
    whose CPU time (``utime + stime``) is past its `baseline` entry. An idle worker's RSS,
    whatever it holds from earlier shapes, is not the shape's. ``ran`` counts those workers.
    """

    def inspect(
        pid,
    ):  # (high-water MB, CPU ticks, start time), or None for a process that has gone
        try:
            with open(f"/proc/{pid}/status") as fh:
                lines = fh.read().splitlines()
            with open(f"/proc/{pid}/stat") as fh:  # "pid (comm) S ppid ..."
                stat = fh.read().rsplit(")", 1)[1].split()
        except (OSError, IndexError):
            return None
        if any(ln.startswith("State:") and "Z" in ln.split()[1:2] for ln in lines):
            return None  # a zombie has no memory to read
        mb = max(
            (float(ln.split()[1]) / 1024.0 for ln in lines if ln.startswith("VmHWM:")),
            default=0.0,
        )
        return mb, int(stat[11]) + int(stat[12]), stat[19]

    def task(it):  # nested: pickled by value, so a worker needs no geobrix
        import os
        import socket

        import pandas as pd  # imported first: part of the baseline, not of the shape

        for _ in it:
            pass
        host = socket.gethostname()
        hwm, missed, ran, ticks = 0.0, 0, 0, []
        for pid in find_workers(os.getpid()):
            seen = inspect(pid)
            if seen is None:
                continue  # exited while we looked: nothing left to read or reset
            mb, cpu, started = seen
            key = f"{host}:{pid}:{started}"  # the start time tells a reused pid
            if not reset:
                if cpu > baseline.get(key, -1):
                    hwm, ran = max(hwm, mb), ran + 1
                continue
            ticks.append(f"{key}={cpu}")
            try:
                with open(f"/proc/{pid}/clear_refs", "w") as fh:
                    fh.write("5")
            except OSError:
                missed += inspect(pid) is not None  # it exited meanwhile: not missed
        yield pd.DataFrame(
            {
                "hwm": [hwm],
                "missed": [missed],
                "ran": [ran],
                "own_pool": [int(os.environ.get(_SWEEP_ENV) == "1")],
                "ticks": [";".join(ticks)],
            }
        )

    return task


class WorkerPeak:
    """Peak RSS (MB) of the Python workers that ran the shape.

    The kernel keeps a high-water mark per process (``VmHWM``) that only grows, and Spark
    reuses Python workers across queries, so reading it after a shape returns the biggest shape
    any earlier query ran on that worker. ``start()`` resets it on every worker (writing 5 to
    ``/proc/<pid>/clear_refs``) and notes how much CPU time each has used; ``read()`` collects
    the mark of the workers whose CPU time has moved since, and of those that did not exist
    before. An idle pooled worker, with whatever RSS earlier shapes left it, ran nothing and is
    not counted.

    Both go through a barrier stage with one task per slot, which puts a task on every
    executor. A task does not read only its own process: Spark keeps idle Python workers in a
    pool and hands them out first-in-first-out, and a query with several Python evaluators per
    task (a pandas UDF over a Python UDF) leaves more idle workers than slots, so one task per
    slot would land on other workers than the shape's tasks did (a shape that held 400 MB read
    as the pool's 125 MB). Each task therefore looks at every Python worker of its executor: the
    ``python -m pyspark.daemon`` / ``pyspark.worker`` processes below the executor's JVM. The
    tasks run in Python workers of their own (a worker environment variable keys a separate
    pool), so the sweep's own CPU time never makes a bystander look busy. A worker that exits
    during the shape takes its mark with it.

    ``read()`` is None, with the reason in ``reason``, wherever the reset cannot be shown to
    reach every worker: off Linux, on a Connect session, when the Spark REST API does not give
    the executors' slots, when they differ from ``defaultParallelism`` (a stale
    ``spark.default.parallelism`` would leave an executor without a task and still read a
    number), when a live worker's counter cannot be reset, when the sweep did not get a pool of
    its own, and when no slot frees up within ``TIMEOUT_S`` (a busy cluster). It is also None,
    saying so, when no Python worker ran during the shape (a plan with no Python in it).

    Why not sampling: a sampler would have to run inside the workers during the shape and
    compete with it for slots. The reset needs no thread, and it costs two short jobs. It
    assumes a fixed-size cluster that nothing else is using.
    """

    TIMEOUT_S = 60.0
    # A sweep that fails is not tried again on that session: one failure per run, not one per form.
    _failed: Dict[int, str] = {}

    def __init__(self, spark):
        self.spark = spark
        self.reason: Optional[str] = None
        self._slots = 0
        self._baseline: Dict[str, int] = {}  # worker -> CPU ticks at the reset
        if is_connect(spark):
            self.reason = "a Connect session cannot reach every Python worker"
        elif not sys.platform.startswith("linux"):
            self.reason = "per-shape worker memory needs Linux (/proc/self/clear_refs)"
        elif id(spark) in WorkerPeak._failed:
            self.reason = WorkerPeak._failed[id(spark)]
        else:
            self._slots, self.reason = self._count_slots()

    def _count_slots(self):
        """(slots, reason): the executors' task slots from the REST API, which must agree with
        the default parallelism the barrier stage is sized by."""
        try:
            sc = self.spark.sparkContext
            cpus = max(1, int(sc.getConf().get("spark.task.cpus", "1")))
            base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
            # A cluster's driver runs no tasks, whatever cores it reports; in local mode it IS
            # the executor.
            local = str(sc.master).startswith("local")
            cores = sum(
                e.get("totalCores", 0)
                for e in _get(f"{base}/executors")
                if e.get("isActive", True) and (e.get("id") != "driver" or local)
            )
            declared = int(sc.defaultParallelism) // cpus
        except Exception as exc:  # noqa: BLE001
            return 0, f"executor slots unavailable ({type(exc).__name__})"
        slots = cores // cpus
        if slots < 1 or slots != declared:
            return 0, (
                f"default parallelism ({declared}) != executor slots ({slots}): "
                "a reset would miss workers"
            )
        return slots, None

    def _fail(self, what: str, exc: Exception) -> None:
        if isinstance(exc, TimeoutError):
            self.reason = f"worker {what} timed out: {exc}"
        else:
            self.reason = (
                f"worker {what} failed ({type(exc).__name__}: {str(exc)[:120]})"
            )
        WorkerPeak._failed[id(self.spark)] = self.reason

    def _bounded(self, run):
        """run() as a job group cancelled after TIMEOUT_S: a barrier stage waits for a free slot
        on every worker, and a busy cluster would hold the whole run."""
        sc = self.spark.sparkContext
        group = f"gbx-worker-peak-{uuid.uuid4().hex[:8]}"
        fired = threading.Event()

        def cancel():
            fired.set()
            sc.cancelJobGroup(group)

        timer = threading.Timer(self.TIMEOUT_S, cancel)
        timer.daemon = True
        sc.setJobGroup(group, "gbx worker peak", interruptOnCancel=True)
        timer.start()
        try:
            return run()
        except Exception as exc:
            if fired.is_set():
                raise TimeoutError(
                    f"no free slot for every worker within {self.TIMEOUT_S:g}s"
                ) from exc
            raise
        finally:
            timer.cancel()
            sc.setLocalProperty(
                "spark.jobGroup.id", None
            )  # PySpark has no clearJobGroup

    def _sweep(self, reset: bool) -> List:
        """One row per task: hwm, missed, ran, own_pool and ticks (see `_sweep_task`)."""
        n, sc = self._slots, self.spark.sparkContext

        def run():
            sc.environment[_SWEEP_ENV] = (
                "1"  # captured by the tasks as they are built, nothing else
            )
            try:
                df = self.spark.range(n, numPartitions=n).mapInPandas(
                    _sweep_task(reset, _worker_finder(), self._baseline),
                    "hwm double, missed long, ran long, own_pool long, ticks string",
                    barrier=True,
                )
            finally:
                sc.environment.pop(_SWEEP_ENV, None)
            return df.collect()

        return self._bounded(run)

    def start(self) -> None:
        if self.reason is None:
            try:
                rows = self._sweep(reset=True)
                if not all(r.own_pool for r in rows):
                    raise RuntimeError(
                        "the sweep shares the shape's Python worker pool"
                    )
                missed = sum(r.missed for r in rows)
                if missed:
                    raise RuntimeError(f"{missed} live Python worker(s) not resettable")
                self._baseline = {
                    k: int(v)
                    for r in rows
                    for k, _, v in (e.partition("=") for e in r.ticks.split(";") if e)
                }
            except Exception as exc:  # noqa: BLE001
                self._fail("reset", exc)

    def read(self) -> Optional[float]:
        if self.reason is not None:
            return None
        try:
            rows = self._sweep(reset=False)
        except Exception as exc:  # noqa: BLE001
            self._fail("read", exc)
            return None
        if not sum(r.ran for r in rows):
            self.reason = "no Python worker ran during the shape"
            return None
        return max(r.hwm for r in rows)
