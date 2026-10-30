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
_PEAK_JOB_GROUP = "gbx-worker-peak-"
# Spark stops a Python worker that has been idle for 60 s; a shorter wait rules that out.
_IDLE_STOP_S = 50.0
_LOST_REASON = (
    "a Python worker that ran the shape exited before the read (Spark stops idle workers "
    "after 60 s), so its peak is lost"
)
_MAYBE_LOST_REASON = (
    "a task of the shape ended more than 50 s before the read (Spark stops a worker that "
    "idles for 60 s), so a worker that ran it may have exited and taken its peak"
)
_RETRIED_REASON = (
    "a task of the shape failed, was killed or was retried (the Spark REST API shows it), so a "
    "worker that ran it may have exited and taken its peak"
)
_NO_TASKS_REASON = (
    "the Spark REST API shows no task of the shape, so the workers that ran it cannot be "
    "accounted for"
)
_NO_REST_REASON = (
    "the Spark REST API did not answer for the shape's jobs, so whether a worker that ran it "
    "exited cannot be ruled out"
)
# A read that stops early (CollectLimit: take, head, first, limit; GlobalLimit and LocalLimit;
# TakeOrderedAndProject) can end a worker before the read without failing a task.
_EARLY_STOP = re.compile(
    r"\b(CollectLimit|GlobalLimit|LocalLimit|TakeOrderedAndProject)\b"
)
# The nodes that run Python workers. A plan prints a node before its children, so a limit printed after the first of
# them is below it: the workers consume all of their (limited) input, and nothing ends them early.
_PYTHON_EVAL = re.compile(
    r"\b(?:\w*EvalPython\w*|\w*InPandas|\w*MapInArrow|ArrowAggregatePython)\b"
)
_EARLY_STOP_REASON = (
    "an early-stop plan can end a worker before the read ({node} in the physical plan), so "
    "its peak may be lost"
)
_NO_PLAN_REASON = (
    "the shape's physical plan could not be read, so an early stop, which can end a worker "
    "before the read, cannot be ruled out"
)
# Two pools that ran are two worker environments. Once a worker of one has gone, the other pool's
# releases can hide its idle stop from the REST guard, and the judge has no baseline for a cold pool.
_MULTI_ENV_REASON = "the shape ran Python workers in more than one environment; a worker of one can exit unseen"


def _physical_text(df) -> str:
    """The physical plan of a classic DataFrame that has run, as text: the JVM's executed plan. It is
    not `explain`: `explain` starts a Python worker in the default pool when none is running, and that
    worker, new since the reset, would be counted as a second environment. On Connect (where
    `readers._plan_text` is the bench's plan text, and works) `explain` is used."""
    jdf = getattr(df, "_jdf", None)
    if jdf is not None:
        return jdf.queryExecution().executedPlan().toString()
    from databricks.labs.gbx.bench.readers import _plan_text

    return _plan_text(df)


def _early_stop_node(df) -> Optional[str]:
    """The first early-stop node in the physical plan of `df`: "" when there is none, None when the
    plan cannot be read (no DataFrame, or no plan text)."""
    if df is None:
        return None
    try:
        text = _physical_text(df)
    except Exception:  # noqa: BLE001 - no plan text: an early stop cannot be ruled out
        return None
    if not text.strip():
        return None
    ev = _PYTHON_EVAL.search(text)
    for m in _EARLY_STOP.finditer(text):
        if ev is None or m.start() < ev.start():
            return m.group(1)
    return ""


def _worker_finder():
    """A function that lists the Python workers of the shape: every ``python -m pyspark.daemon``
    / ``pyspark.worker`` process below the executor's JVM that is not one of the sweep's own.
    It returns the JVM's pid, ``{worker pid: parent pid}`` and the daemons' pids (the workers
    that the JVM itself started with ``python -m pyspark.daemon``).

    A factory, so that what it returns is pickled by value and a worker needs no geobrix to run
    it; it reads only /proc.
    """

    def module(
        pid,
    ):  # the daemon and the workers it forks both run `python -m pyspark.daemon`
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                args = fh.read().split(b"\0")
        except OSError:
            return None
        at = args.index(b"-m") + 1 if b"-m" in args else len(args)
        return (args[at : at + 1] or [None])[0]

    def is_worker(pid):
        return module(pid) in (b"pyspark.daemon", b"pyspark.worker")

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
        workers, daemons, todo = {}, set(), [jvm]
        while todo:
            owner = todo.pop()
            for pid in children.get(owner, []):
                if is_worker(pid) and not is_sweep(pid):
                    workers[pid] = owner
                    if owner == jvm and module(pid) == b"pyspark.daemon":
                        daemons.add(pid)
                    todo.append(pid)  # the daemon's workers
        return jvm, workers, daemons

    return find


def _surveyor(find_workers):
    """``survey(me)`` and ``alive(pid)``: what an executor's shape workers have used, as
    ``(scope, members)``.

    ``scope`` names the executor (host and JVM). ``members`` maps ``scope:pid:start`` (the start
    time tells a reused pid) to the process's CPU ticks (``utime + stime``), its high-water mark
    in MB, whether it is a zombie, whether it is a daemon (the process that forks workers), and its
    parent ``ppid`` (a worker's pool is its daemon).
    """

    def stat(pid):
        try:
            with open(f"/proc/{pid}/stat") as fh:  # "pid (comm) S ppid ..."
                f = fh.read().rsplit(")", 1)[1].split()
            with open(f"/proc/{pid}/status") as fh:
                lines = fh.read().splitlines()
        except (OSError, IndexError):
            return None
        mb = [float(ln.split()[1]) / 1024.0 for ln in lines if ln.startswith("VmHWM:")]
        return {
            "zombie": f[0] == "Z",
            "start": f[19],
            "cpu": int(f[11]) + int(f[12]),
            "mb": max(mb, default=0.0),
        }

    def alive(pid):  # a process that has gone, or is a zombie, is not
        info = stat(pid)
        return info is not None and not info["zombie"]

    def survey(me):
        import socket

        jvm, parents, daemons = find_workers(me)
        scope = f"{socket.gethostname()}:{jvm}"
        members = {}
        for pid in parents:
            info = stat(pid)
            if info is not None:
                info.update(pid=pid, daemon=pid in daemons, ppid=parents[pid])
                members[f"{scope}:{pid}:{info['start']}"] = info
        return scope, members

    return survey, alive


def _read_judge():
    """``judge(scope, members, baseline)`` -> ``(hwm, ran, lost, envs)`` for one executor.

    `baseline` is what the reset noted: ``cpu`` (worker -> CPU ticks) and ``daemons``. A daemon is
    not a worker: only the processes it forks are counted. ``hwm`` and ``ran`` are the highest
    mark, and the count, of the live workers that are new or whose CPU ticks passed their entry:
    those ran the shape. ``lost`` counts the workers that have exited since the reset (gone, or
    zombies), and with them their marks. Any worker that was there at the reset voids the reading,
    whichever pool it is in: nothing the kernel keeps says whether a worker that has gone had run
    (the daemon ignores SIGCHLD, so its ``cutime + cstime`` never gets a reaped child's CPU time),
    and Spark stops an idle worker after 60 s, of any pool. So another pool's idle worker voids it
    too, because the judge cannot tell that it did not run.

    ``envs`` counts the pools (a pool is one daemon, and the workers it forked) that ran: a daemon
    that is new since the reset, or a pool with a worker that is new or whose ticks moved. Two
    pools that ran are two worker environments, and a worker of one can exit unseen when the other
    keeps releasing (the cold case has no baseline to show it), so the reading is NULL then.
    """

    def judge(scope, members, baseline):
        cpu0, daemons0 = baseline["cpu"], baseline["daemons"]
        hwm, ran, lost, pools = 0.0, 0, 0, set()
        for key, m in members.items():
            if m["daemon"]:
                if key not in daemons0:  # a pool started since the reset ran
                    pools.add(m["pid"])
                continue  # a daemon is not a worker: the processes it forks are
            if m["zombie"]:
                lost += 1
                pools.add(m["ppid"])
            elif m["cpu"] > cpu0.get(key, -1):  # a new worker has no entry
                hwm, ran = max(hwm, m["mb"]), ran + 1
                pools.add(m["ppid"])
        for key in cpu0:
            gone = key.startswith(f"{scope}:") and key not in members
            if gone and key not in daemons0:
                lost += 1
        return hwm, ran, lost, len(pools)

    return judge


def _sweep_task(reset, survey, alive, judge, baseline):
    """The barrier task of one sweep, over the shape's Python workers of its executor.

    With `reset` it clears each worker's high-water mark and returns (as ``ticks``) what to
    compare with later: each process's CPU ticks (``m,<key>,<ticks>,<1 for a daemon>``); how many
    live workers it could not reset is ``missed``. Without, it returns what `_read_judge` makes
    of them against `baseline`: the highest ``VmHWM`` (MB) of the workers that ran since
    (``hwm``, ``ran`` of them), and ``lost`` workers that have exited. An idle worker's RSS,
    whatever it holds from earlier shapes, is not the shape's.
    """

    def note(members):  # -> (live workers not reset, what to compare with later)
        missed, ticks = 0, []
        for key, m in members.items():
            if m["zombie"]:
                continue  # gone, with nothing to reset
            ticks.append(f"m,{key},{m['cpu']},{int(m['daemon'])}")
            try:
                with open(f"/proc/{m['pid']}/clear_refs", "w") as fh:
                    fh.write("5")
            except OSError:
                missed += alive(m["pid"])  # it exited meanwhile: not missed
        return missed, ticks

    def task(it):  # nested: pickled by value, so a worker needs no geobrix
        import os

        import pandas as pd  # imported first: part of the baseline, not of the shape

        for _ in it:
            pass
        scope, members = survey(os.getpid())
        hwm, ran, lost, envs, missed, ticks = 0.0, 0, 0, 0, 0, []
        if reset:
            missed, ticks = note(members)
        else:
            hwm, ran, lost, envs = judge(scope, members, baseline)
        yield pd.DataFrame(
            {
                "hwm": [hwm],
                "missed": [missed],
                "ran": [ran],
                "lost": [lost],
                "envs": [envs],
                "scope": [scope],
                "own_pool": [int(os.environ.get(_SWEEP_ENV) == "1")],
                "ticks": [";".join(ticks)],
            }
        )

    return task


def _longest_release_gap(ends: Dict[str, List[float]], t1: float) -> float:
    """The longest time, on any executor, from the end of a task to the end of the next one
    there (or to `t1`). Spark stops a Python worker only when the pool has seen no worker
    released for 60 s, so a shorter gap everywhere rules out that a worker went idle and was
    stopped."""
    worst = 0.0
    for times in ends.values():
        times = sorted(times)
        for here, after in zip(times, times[1:] + [t1]):
            worst = max(worst, after - here)
    return worst


def _rest_trouble(base: str, t0: float, t1: float, executors: int, ends: bool) -> str:
    """Why the Spark REST API at `base` says that a worker that ran the shape (the jobs that
    ran in [t0, t1], leaving out the worker sweeps' own) may have exited unseen, or "" when it
    says nothing of the kind. Closed by default: no job, a stage that the API will not give,
    or no answer is trouble too.

    A task attempt that failed or was killed, a retried task and a stage attempt after the
    first all mean a worker may have died with its mark; a worker created by the shape leaves
    no other trace. With `ends` (a shape that ran longer than Spark's idle stop), the end times
    of the tasks must also show that no executor waited more than 50 s between two (or for the
    read after the last): Spark stops a worker only when its pool has seen no worker released
    for 60 s, and a task ends at or just before its worker is released. That needs the end
    of a task to be within 10 s of its worker's release (measured at 12 and 17 ms). `executors`
    is how many executors have a worker that ran: the end times must come from as many.
    """
    jobs = [
        j
        for j in _get(f"{base}/jobs")
        if not (j.get("jobGroup") or "").startswith(_PEAK_JOB_GROUP)
        and not (
            _epoch(j.get("completionTime")) and _epoch(j.get("completionTime")) < t0
        )
    ]
    if not jobs:
        return _NO_TASKS_REASON
    task_ends: Dict[str, List[float]] = {}
    for job in jobs:
        if (
            job.get("numFailedTasks")
            or job.get("numKilledTasks")
            or job.get("numFailedStages")
        ):
            return _RETRIED_REASON
        for sid in set(job.get("stageIds", [])) - set(job.get("skippedStages", [])):
            attempts = _get(f"{base}/stages/{sid}")
            if not attempts:
                return _NO_TASKS_REASON
            for attempt in attempts:
                if (
                    attempt.get("attemptId")
                    or attempt.get("numFailedTasks")
                    or attempt.get("numKilledTasks")
                ):
                    return _RETRIED_REASON
                if ends:
                    url = f"{base}/stages/{sid}/{attempt['attemptId']}/taskList?length=100000"
                    for task in _get(url):
                        if task.get("duration") is None:
                            continue
                        end = _epoch(task["launchTime"]) + task["duration"] / 1000.0
                        if t0 <= end <= t1 + 1.0:
                            task_ends.setdefault(str(task["executorId"]), []).append(
                                end
                            )
    if ends and (len(task_ends) < executors or not task_ends):
        return _NO_TASKS_REASON
    if ends and _longest_release_gap(task_ends, t1) > _IDLE_STOP_S:
        return _MAYBE_LOST_REASON
    return ""


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
    pool), so the sweep's own CPU time never makes a bystander look busy.

    A worker that exits takes its mark with it, and nothing the kernel keeps says whether it had
    run (the daemon ignores SIGCHLD, so a reaped worker's CPU time is not added to the daemon's).
    So the reading is never a bare maximum over the workers that are left:

    - a worker that was there at the reset, in any pool, and has gone voids it. Spark stops an
      idle pool's workers after 60 s, so an idle worker of another pool counts too: the judge
      cannot tell that it did not run. A daemon is not a worker.
    - for every shape, the Spark REST API's jobs and stages of the shape must show no failed or
      killed task and no retry (a worker that was killed, and its task run again on a new one,
      leaves nothing else), and must answer: an empty or missing reply voids it.
    - a worker created by the shape leaves no trace if Spark stops it, after the pool has seen
      no worker released for 60 s. So when the shape ran longer than 50 s, the REST end times of
      its tasks must show every executor releasing a worker at least every 50 s (and the first
      read after the last), or the reading is NULL. That leaves 10 s of margin, for a task that
      ends that long after its worker is released.
    - the physical plan of the DataFrame the shape ran must hold no early stop (CollectLimit,
      GlobalLimit, LocalLimit, TakeOrderedAndProject): a read that stops early ends its workers
      without failing a task (``take``, ``head``, ``first``, ``limit``), so the plan decides it.
    - the shape must have run Python workers in one environment only. A pool is one daemon and the
      workers it forks; a pool counts as having run when its daemon is new since the reset, or when
      one of its workers is new or its CPU ticks moved. Two pools that ran on one executor read NULL:
      a cold pool has no baseline, so a worker of one can exit unseen while the other keeps
      releasing (the REST guard takes its gaps across both pools).

    Known limits, documented and not coded (each is a NULL-or-number edge, not a checked path):

    - a consumer other than a limit node that stops reading early without failing a task (for
      example a sort-merge join that exhausts one side over sorted input) can end a worker unseen;
    - a JVM tail of more than 10 s after a task's Python output ends defeats the margin of the
      50 s guard (the 60 s is OSS Spark 4.0.0's idle constant, not checked on DBR);
    - ``read(df)`` handed a different query than the one that ran: the plan check sees only ``df``,
      so an early stop in another query, or a ``run`` callable that reads rows on the side, is not
      seen. None of the bench's forms does one.

    ``read(df)`` is None, with the reason in ``reason``, wherever the reset cannot be shown to
    reach every worker: off Linux, on a Connect session, when the Spark REST API does not give
    the executors' slots, when they differ from ``defaultParallelism`` (a stale
    ``spark.default.parallelism`` would leave an executor without a task and still read a
    number), when a live worker's counter cannot be reset, when the sweep did not get a pool of
    its own, and when no slot frees up within ``TIMEOUT_S`` (a busy cluster). It is also None,
    saying so, in each case above, when an executor that held workers at the reset is gone, when
    workers exit with their task (``spark.python.worker.reuse=false``), when no Python worker ran
    during the shape (a plan with no Python in it), when the physical plan has an early stop, when
    the plan cannot be read, and when the shape ran Python workers in more than one environment.
    It is not a number that leaves out a worker that ran.

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
        # what the reset noted: worker -> CPU ticks, and the daemons
        self._baseline: Dict = {"cpu": {}, "daemons": set()}
        self._scopes: set = set()  # the executors that held workers at the reset
        self._t0: Optional[float] = None  # when the reset ended
        if is_connect(spark):
            self.reason = "a Connect session cannot reach every Python worker"
        elif not sys.platform.startswith("linux"):
            self.reason = "per-shape worker memory needs Linux (/proc/self/clear_refs)"
        elif id(spark) in WorkerPeak._failed:
            self.reason = WorkerPeak._failed[id(spark)]
        elif self._workers_exit_with_their_task():
            self.reason = (
                "workers exit with their task (spark.python.worker.reuse=false)"
            )
        else:
            self._slots, self.reason = self._count_slots()

    def _workers_exit_with_their_task(self) -> bool:
        try:
            conf = self.spark.sparkContext.getConf()
            return conf.get("spark.python.worker.reuse", "true").lower() == "false"
        except (
            Exception
        ):  # noqa: BLE001 - _count_slots reports a context it cannot reach
            return False

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
        group = f"{_PEAK_JOB_GROUP}{uuid.uuid4().hex[:8]}"
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
        """One row per task: hwm, missed, ran, lost, envs, scope, own_pool, ticks (see `_sweep_task`)."""
        n, sc = self._slots, self.spark.sparkContext
        survey, alive = _surveyor(_worker_finder())
        task = _sweep_task(reset, survey, alive, _read_judge(), self._baseline)

        def run():
            sc.environment[_SWEEP_ENV] = (
                "1"  # captured by the tasks as they are built, nothing else
            )
            try:
                df = self.spark.range(n, numPartitions=n).mapInPandas(
                    task,
                    "hwm double, missed long, ran long, lost long, envs long, scope string, "
                    "own_pool long, ticks string",
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
                self._baseline = {"cpu": {}, "daemons": set()}
                for r in rows:
                    for entry in filter(None, r.ticks.split(";")):
                        _, key, ticks, daemon = entry.split(",")  # "m,key,cpu,daemon"
                        self._baseline["cpu"][key] = int(ticks)
                        if daemon == "1":
                            self._baseline["daemons"].add(key)
                self._scopes = {r.scope for r in rows}
                self._t0 = time.time()
            except Exception as exc:  # noqa: BLE001
                self._fail("reset", exc)

    def _rest_trouble(self, t1: float, executors: int) -> str:
        """`_rest_trouble` for this shape (since the reset), "" if it finds none."""
        if self._t0 is None:
            return ""
        sc = self.spark.sparkContext
        try:
            base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
            return _rest_trouble(
                base, self._t0, t1, executors, t1 - self._t0 > _IDLE_STOP_S
            )
        except Exception:  # noqa: BLE001 - no answer: no way to rule it out
            return _NO_REST_REASON

    def read(self, df) -> Optional[float]:
        """The shape's peak (MB), or None with the reason in ``reason``. `df` is the DataFrame the
        shape ran: its physical plan is checked for an early stop before any worker is read.
        """
        if self.reason is not None:
            return None
        node = _early_stop_node(df) if df is not None else None
        if node is None:
            self.reason = _NO_PLAN_REASON
            return None
        if node:
            self.reason = _EARLY_STOP_REASON.format(node=node)
            return None
        try:
            rows = self._sweep(reset=False)
        except Exception as exc:  # noqa: BLE001
            self._fail("read", exc)
            return None
        # taken after the sweep, so that a sweep that waited for slots counts as shape time
        t1 = time.time()
        if sum(r.lost for r in rows) or self._scopes - {r.scope for r in rows}:
            self.reason = _LOST_REASON
        elif any(r.envs > 1 for r in rows):
            self.reason = _MULTI_ENV_REASON
        elif not sum(r.ran for r in rows):
            self.reason = "no Python worker ran during the shape"
        else:
            ran_on = len({r.scope for r in rows if r.ran})
            self.reason = self._rest_trouble(t1, ran_on) or None
        return None if self.reason else max(r.hwm for r in rows)
