"""The memory signals of a standard access mode cluster (USER_ISOLATION: Spark Connect over classic compute), as they
were read on one: a new 1 x m5d.2xlarge DBR 19.15 job cluster, 2026-11-10 (run 531218115335637). Tests that fake a
standard-access driver or worker apply these, so they judge the signals the budget really sees there.

- The driver's Python (the notebook REPL) has a Spark Connect session and the classic memory environment:
  ``SPARK_WORKER_MEMORY=26985m``, ``DATABRICKS_RUNTIME_VERSION=19.15`` (not Serverless's ``client.*``), no
  ``SPARK_EXECUTOR_MEMORY`` and no ``IS_SERVERLESS``; cgroup v1 limit 28295823360 bytes, no cgroup v2 file.
- A worker's Python UDF process (the sandbox) has none of these: no ``SPARK_WORKER_MEMORY`` /
  ``SPARK_EXECUTOR_MEMORY``, no ``IS_SERVERLESS``, no ``DATABRICKS_RUNTIME_VERSION``, and its cgroup v1 limit is
  the unlimited sentinel 9223372036854775807. Its own budget is therefore Serverless's 64 MiB.
"""

from databricks.labs.gbx.pyrx.core import budget

# what each side reads (the keys the budget probes; the others it reads none of)
DRIVER_ENV = {
    "SPARK_WORKER_MEMORY": "26985m",
    "DATABRICKS_RUNTIME_VERSION": "19.15",
    "SPARK_CONNECT_MODE_ENABLED": "1",
}
WORKER_ENV = {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER", "SPARK_REUSE_WORKER": "1"}
DRIVER_CGROUP_V1 = "28295823360"
WORKER_CGROUP_V1 = "9223372036854775807"

# the memory keys the budget and the manager probe, cleared before a side's own are set
_PROBED = (*budget.CLASSIC_MEMORY_ENV, "IS_SERVERLESS", "DATABRICKS_RUNTIME_VERSION")


def _cgroup(v1: str):
    def read(path):
        if path == budget._CGROUP_V1_MEM:
            return v1
        raise FileNotFoundError(path)  # no cgroup v2 file on either side

    return read


def apply(monkeypatch, side: str) -> None:
    """Make this process look like the ``driver`` or a ``worker`` of the standard-access cluster above."""
    env, v1 = {
        "driver": (DRIVER_ENV, DRIVER_CGROUP_V1),
        "worker": (WORKER_ENV, WORKER_CGROUP_V1),
    }[side]
    for k in _PROBED:
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(budget, "_read_cgroup_file", _cgroup(v1))
    monkeypatch.setattr(  # read once per process: here, read for the side faked now
        budget, "process_read_cap", lambda: budget.decoded_budget_bytes("auto")
    )
