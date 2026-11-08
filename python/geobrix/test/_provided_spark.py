"""Use the Spark session the platform provides, instead of starting a local one (tests run on Databricks).

On a Databricks cluster the notebook that runs pytest already holds a SparkSession, with Delta and the runtime's own
configuration. The suites' ``spark`` fixtures build a local one (``builder.master("local[2]")...getOrCreate()``) and
stop it at the end. On a cluster that is wrong twice: a local master and local-only settings (``spark.jars``, the Delta
log store, the warehouse dir) do not belong on the provided session, and ``stop()`` would end the session every other
shard of the node shares.

``install()`` changes the four calls that do it, for the process, and nothing else:

* ``Builder.master``, ``Builder.appName``, ``Builder.config`` and ``Builder.enableHiveSupport`` return the builder
  unchanged (the settings are recorded in ``IGNORED``), so ``getOrCreate()`` returns the provided session as it is;
* ``SparkSession.stop`` does nothing.

It applies when ``DATABRICKS_RUNTIME_VERSION`` is set, or when ``GBX_TEST_PROVIDED_SPARK`` is a true value (a way to
try it off Databricks); ``GBX_TEST_PROVIDED_SPARK=0`` turns it off on Databricks. Without either, a run is exactly
what it was: the local fixtures start local sessions.

A test that changes session state (a conf, a temp view) puts it back, as it does on a local session that the whole
run shares.
"""

from __future__ import annotations

import os
from typing import List, Tuple

# (builder call, its arguments): what a fixture asked for and the platform's session ignored
IGNORED: List[Tuple[str, tuple]] = []
_installed = False


def wanted(env=None) -> bool:
    """Whether this process should use the provided session."""
    env = os.environ if env is None else env
    flag = env.get("GBX_TEST_PROVIDED_SPARK")
    if flag is not None and flag != "":
        return flag.lower() not in ("0", "false", "no")
    return bool(env.get("DATABRICKS_RUNTIME_VERSION"))


def _patch_builder(builder_cls) -> None:
    def keep(name):
        def call(self, *args, **kwargs):
            IGNORED.append((name, args))
            return self

        call.__name__ = name
        call._gbx_provided = True  # type: ignore[attr-defined]
        return call

    for name in ("master", "appName", "config", "enableHiveSupport"):
        if hasattr(builder_cls, name) and not getattr(
            getattr(builder_cls, name), "_gbx_provided", False
        ):
            setattr(builder_cls, name, keep(name))


def _patch_stop(session_cls) -> None:
    if hasattr(session_cls, "stop") and not getattr(
        session_cls.stop, "_gbx_provided", False
    ):

        def stop(self):  # the provided session belongs to the node, not to a fixture
            return None

        stop._gbx_provided = True  # type: ignore[attr-defined]
        session_cls.stop = stop


def install(env=None) -> bool:
    """Apply the change when it is wanted; True when it is in effect. Safe to call twice."""
    global _installed
    if _installed:
        return True
    if not wanted(env):
        return False
    try:
        from pyspark.sql import session as classic
    except ImportError:
        return False
    _patch_builder(classic.SparkSession.Builder)
    _patch_stop(classic.SparkSession)
    try:  # Spark Connect (a Serverless or shared cluster): the same two changes on the client classes
        from pyspark.sql.connect import session as connect

        _patch_builder(connect.SparkSession.Builder)
        _patch_stop(connect.SparkSession)
    except Exception:  # noqa: BLE001 - the client deps are optional
        pass
    _installed = True
    return True
