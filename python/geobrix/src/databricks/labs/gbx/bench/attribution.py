"""Attribution split for the levels leg: serialization, Python, shuffle, spill, memory, GC.

Where each number comes from depends on the session:

* Classic sessions read the driver's Spark REST API (the plan's SQL metrics, and the stages'
  GC time).
* Connect sessions (standard access mode, Serverless) read ``df.executionInfo`` (PySpark 4.0+),
  which carries the plan's SQL metrics only.

A Connect session has no ``sparkContext``, ``.rdd``, ``_jvm`` or ``_jsc``, and refuses most
``spark.conf.set`` calls, so nothing here touches them there. What cannot be measured is
``None`` ("unavailable"), never 0 and never an exception; ``source`` says which API answered.
A metric the source answers but the plan does not carry (no Python node in an L1 plan) is 0.0.
"""

from __future__ import annotations

import glob
import json
import os
import pstats
import re
import tempfile
import time
import urllib.request
from datetime import datetime
from typing import Callable, Dict, List, Optional

from databricks.labs.gbx.manager.capabilities import is_connect

# The metrics `from_plan` reports, as ResultRow field names.
METRICS = (
    "ser_sent_bytes",
    "ser_recv_bytes",
    "py_eval_s",
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


def _from_execution_info(df, out: Dict) -> bool:
    info = getattr(df, "executionInfo", None)
    metrics = getattr(info, "metrics", None)
    if metrics is None:
        return False
    for k in _BUCKETS:
        out[k] = 0.0
    _, graph = metrics.extract_graph()
    for node in graph.values():
        for m in node.metrics:
            k = _bucket(m.name)
            if k:
                _add(out, k, float(m.value) * _CONNECT_SCALE.get(k, 1.0))
    return True


def _from_rest(df, t0: float, out: Dict) -> bool:
    try:
        sc = df.sparkSession.sparkContext
        if not sc.uiWebUrl:
            return False
        base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
        deadline = time.time() + _REST_WAIT_S
        while True:
            execs = _get(f"{base}/sql?details=true&planDescription=false&length=100000")
            last = max(execs, key=lambda e: e["id"])
            if last.get("status") != "RUNNING" or time.time() > deadline:
                break
            time.sleep(0.1)
        stages = _get(f"{base}/stages?status=complete")
    except (
        Exception
    ):  # noqa: BLE001 - no UI URL, or not reachable from here: stays unavailable
        return False
    for k in (*_BUCKETS, "gc_s"):
        out[k] = 0.0
    for node in last.get("nodes", []):
        for m in node.get("metrics", []):
            k = _bucket(m["name"])
            if k:
                _add(out, k, _num(m["value"]))
    out["gc_s"] = (
        sum(s.get("jvmGcTime", 0) for s in stages if _started_since(s, t0)) / 1000.0
    )
    return True


def _started_since(stage: Dict, t0: float) -> bool:
    try:
        return _epoch(stage.get("submissionTime")) >= t0
    except ValueError:
        return False


def from_plan(df, t0: float) -> Dict[str, Optional[float]]:
    """The plan's metrics for the query `df` just ran, and the GC time of the stages that
    started at or after `t0` (epoch seconds). Every key of METRICS, plus ``source``:
    ``rest`` | ``executionInfo`` | ``none``."""
    out: Dict = {k: None for k in METRICS}
    out["source"] = "none"
    if is_connect(df) or is_connect(getattr(df, "sparkSession", None)):
        if _from_execution_info(df, out):
            out["source"] = "executionInfo"
    elif _from_rest(df, t0, out):
        out["source"] = "rest"
    return out


def unavailable(measures: Dict) -> List[str]:
    """The names no source answered (None), for a row's note."""
    return [k for k, v in measures.items() if k != "source" and v is None]


def python_eval_s(spark, run: Callable[[], object]) -> Optional[float]:
    """One extra `run()` under the Spark 4 UDF profiler (perf): seconds inside the UDF bodies.

    None where profiling cannot be switched on (a Connect/Serverless session refuses the conf)
    or fails.
    """
    if is_connect(spark):
        return None
    try:
        spark.conf.set("spark.sql.pyspark.udf.profiler", "perf")
    except Exception:  # noqa: BLE001
        return None
    try:
        spark.profile.clear()
        run()
        with tempfile.TemporaryDirectory(prefix="gbx-prof-") as d:
            spark.profile.dump(d, type="perf")
            return sum(
                pstats.Stats(p).total_tt for p in glob.glob(os.path.join(d, "*"))
            )
    except Exception:  # noqa: BLE001
        return None
    finally:
        try:
            spark.conf.unset("spark.sql.pyspark.udf.profiler")
        except Exception:  # noqa: BLE001
            pass


def worker_rss_mb(spark, n: int) -> Optional[float]:
    """Peak RSS (MB) of the Python workers, read in `n` tasks. Approximate: workers are reused,
    and which ones the tasks land on is up to the scheduler. None where it cannot run.
    """

    def _rss(it):  # nested: pickled by value, so a worker needs no geobrix to run it
        import platform
        import resource

        import pandas as pd

        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        for _ in it:
            pass
        yield pd.DataFrame(
            {
                "rss": [
                    kb / (1024 * 1024) if platform.system() == "Darwin" else kb / 1024
                ]
            }
        )

    try:
        row = (
            spark.range(n, numPartitions=n)
            .mapInPandas(_rss, "rss double")
            .agg({"rss": "max"})
            .first()
        )
        return float(row[0]) if row and row[0] is not None else None
    except Exception:  # noqa: BLE001
        return None
