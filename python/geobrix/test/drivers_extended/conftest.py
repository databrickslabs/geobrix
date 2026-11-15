"""Fixtures for the extended driver tests: GDAL's own bindings (the env wheel's ``osgeo``) and a local Spark session.

The suite needs the env wheel's ``osgeo`` on rasterio's libgdal, as the engine tests do. It skips where that is not
the case, and runs in the container and in the light CI shards, which carry the payload.
"""

import logging
import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(autouse=True)
def _isolate_gdal_env():
    """Snapshot and restore GDAL/PROJ env vars around every test."""
    keys = ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")
    saved = {k: os.environ.get(k) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


@pytest.fixture(scope="session")
def gb():
    """GDAL's Python bindings (``gdal``, ``ogr``, ``osr``) after the driver policy is on; skips without the payload."""
    from databricks.labs.gbx import _gdal_policy, _osgeo
    from databricks.labs.gbx.engine import run

    why = run.unavailable_reason()
    if why:
        RUN["payload_missing"] = (
            why  # test_zz_ran skips for this, by this flag and not by what it infers from skips
        )
        pytest.skip(why)
    b = _osgeo.bindings("test_drivers_extended")
    _gdal_policy.activate()
    return b


@pytest.fixture(scope="module")
def spark(gb):
    """A plain local session with every GeoBrix source and function registered (the light tier: no JAR)."""
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    from databricks.labs import gbx

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-drivers-extended")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    gbx.register(session)
    yield session


# node id of each test that ran -> "passed", "skipped", "xfailed" or "failed" (read by test_zz_ran.py)
OUTCOMES: dict = {}

# What test_zz_ran.py needs to tell a full run of this directory from a deliberate selection. The hooks below fill it
# and the ``driver_run`` fixture hands it over, so the test reads the state of the conftest pytest loaded whatever the
# import mode, and does not depend on the order the tests ran in.
RUN: dict = {}
_HERE = os.path.dirname(os.path.abspath(__file__))
_ZZ = "test_zz_ran.py"


def _mine(nodeid: str) -> bool:
    return "drivers_extended/" in nodeid.replace("\\", "/")


def pytest_configure(config):
    OUTCOMES.clear()
    RUN.clear()
    RUN.update(
        deselected=0,
        collected_modules=set(),
        payload_missing=None,
        # a node id or keyword on the command line: a file or directory argument names whole modules, which the module
        # check below judges; "::" names tests
        node_args=any("::" in str(a) for a in config.args),
    )


def pytest_deselected(items):
    RUN["deselected"] = RUN.get("deselected", 0) + sum(
        1 for i in items if _mine(i.nodeid)
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    """Record which test modules of this directory were collected, and run test_zz_ran.py last whatever order a plugin
    gave the others."""
    RUN["collected_modules"] = {
        i.nodeid.split("::", 1)[0].rsplit("/", 1)[-1] for i in items if _mine(i.nodeid)
    }
    items.sort(key=lambda i: i.nodeid.split("::", 1)[0].endswith(_ZZ))  # stable


def all_modules() -> set:
    """Every test module of this directory but test_zz_ran.py itself."""
    return {
        f
        for f in os.listdir(_HERE)
        if f.startswith("test_") and f.endswith(".py") and f != _ZZ
    }


def is_deliberate_selection() -> bool:
    """Whether the session ran a part of this directory on purpose: a test id on the command line, a deselected test of
    the directory (``-k``, ``-m``, ``--deselect``, ``--lf``), or a test module that was not collected.
    """
    return bool(
        RUN.get("node_args")
        or RUN.get("deselected")
        or not all_modules() <= RUN.get("collected_modules", set())
    )


@pytest.fixture(scope="session")
def driver_run():
    return RUN


def pytest_runtest_logreport(report):
    if report.when == "call" or (report.when == "setup" and report.outcome != "passed"):
        if hasattr(report, "wasxfail") and report.outcome == "skipped":
            outcome = "xfailed"
        else:
            outcome = report.outcome
        if report.nodeid not in OUTCOMES or report.when == "call":
            OUTCOMES[report.nodeid] = outcome
