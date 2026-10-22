"""Test-root conftest: dependency-aware collection guard for the light-tier suites.

WHY: The light-tier test dirs import packages whose import chain pulls in
light-tier deps -- ``rasterio``, ``shapely``, ``h3``, ``quadbin``, ``pmtiles``,
``pandas``, ``numpy``, ``scipy`` -- that are NOT installed in the remote
heavyweight CI Python environment (``requirements-ci.txt`` ships none of them;
the lightweight CI uses ``requirements-light-env6-ci.txt``). pytest imports a test
module at COLLECTION time to read it, so a bare ``-m "not bench"`` marker filter
does not help: the import (and its ``ModuleNotFoundError``) fires before the
marker is ever seen, turning into a collection ERROR that fails the build.

A directory-level ``collect_ignore`` prevents pytest from even importing those
dirs when the deps are absent. It is gated on ``rasterio`` being importable (the
canonical signal for the light-tier dependency set; ``pmtiles``/``pandas``/
``shapely`` et al. ship in the same locks):

  * Remote heavyweight CI (no rasterio) -> every light dir ignored -> skipped.
  * Local / Docker / light CI (rasterio present) -> not ignored -> collected and run.
  * Explicit ``gbx:test:python --path test/<dir>/...`` in Docker -> deps present,
    so nothing is ignored and the targeted tests still run.

This is robust to the real cause (missing deps) rather than relying on each
caller remembering to pass ``--ignore``.

CONDITION TO MAINTAIN (every light-tier addition must do ALL THREE):
  1. Add the new light test dir to ``_LIGHT_TEST_DIRS`` below, so the heavyweight
     CI phase skips it (otherwise its module-level light imports -- e.g.
     ``from pmtiles.reader import ...`` / ``import pandas`` -- raise
     ``ModuleNotFoundError`` at collection and fail the heavy build).
  2. Add the new light test dir to the explicit pytest dir list in the LIGHT CI
     phase (``.github/actions/pyrx_build/action.yml``), so it is actually RUN.
     The light tier is exercised ONLY in the light phase; the heavy phase skips it.
  3. Add the new light test dir to the explicit ``--ignore`` list in the HEAVY CI
     phase (``.github/actions/python_build/action.yml``, ``LIGHT_IGNORES``). That
     list MUST stay identical to ``_LIGHT_TEST_DIRS``. The collect_ignore fallback
     below already skips light dirs when rasterio is absent, but the explicit
     --ignore is belt-and-suspenders: it guarantees a light dir never runs in heavy
     even if the heavy env drifts (gains rasterio) or a caller targets a light dir
     directly.
Light test dirs so far: pyrx, pyvx, pygx, pmtiles_light, stac, earthdata, vizx,
sample, models, zarr, plus bench + ds + core (the tier-neutral gbx.core.crs resolver
imports rasterio, so test/core is light-tier too).

``bench`` is a light dir (its modules import rasterio/shapely/h3/quadbin via the
bench harness) and meets all three: it is in ``_LIGHT_TEST_DIRS`` below, in the light
run list, and in the heavy ``LIGHT_IGNORES``. Its unit tests guard the light bench
FnSpec registry and the cross-tier coverage/count invariants.

The light phase runs every light dir in ONE pytest process, so process-global state
a test leaves behind (env vars, registries, GDAL/PROJ search paths) reaches the dirs
after it. A test that changes such state must restore it (see the ``_isolate`` /
``_isolate_gdal_env`` fixtures in test/pyvx, test/pyrx and test/ds).
"""

import importlib.util
import os
from pathlib import Path

# Every test dir whose modules import light-tier-only deps. Ignored in the
# heavyweight CI env (no rasterio); collected + run in the light env.
_LIGHT_TEST_DIRS = [
    "bench",
    "core",
    "ds",
    "pyrx",
    "pyvx",
    "pygx",
    "pmtiles_light",
    "stac",
    "earthdata",
    "vizx",
    "sample",
    "models",
    "zarr",
]

# Skip the light-tier suites when their dependencies are not installed.
if importlib.util.find_spec("rasterio") is None:
    collect_ignore = list(_LIGHT_TEST_DIRS)


# ---------------------------------------------------------------------------
# Local Delta Lake for test/zarr (the transactional manifest is a Delta table).
#
# PySpark applies spark.jars only when it LAUNCHES the JVM, so the OSS delta-spark jars
# must be on PYSPARK_SUBMIT_ARGS before the first SparkSession in the process. The light
# phase runs every light dir in ONE process and several dirs start a plain session, so
# the spark_delta fixture cannot add the jars later. They are added here instead, at the
# end of collection and only if test/zarr tests were collected: the heavy phase ignores
# test/zarr, so it never sees them.
#
#   * only when both jars exist locally -- nothing is downloaded here. gbx:test:python
#     and the light CI action fetch them with scripts/ci/fetch-delta-test-jars.sh;
#   * never overrides a PYSPARK_SUBMIT_ARGS the caller already set.
#
# Without the jars the spark_delta fixture fails loudly (see test/zarr/conftest.py).
# ---------------------------------------------------------------------------
# Keep in step with DELTA_VERSION in scripts/ci/fetch-delta-test-jars.sh.
_DELTA_VERSION = "4.0.1"  # Delta 4.0.x runs on Spark 4.0.x


def _delta_jars():
    """delta-spark + delta-storage jars from the first Maven repo holding both, else []."""
    v = _DELTA_VERSION
    repos = [
        os.environ.get("GBX_M2_REPO"),
        # the repo checkout's persistent Maven cache (mounted at /root/geobrix in Docker)
        Path(__file__).resolve().parents[3] / "scripts" / "docker" / "m2",
        Path.home() / ".m2" / "repository",  # CI runners
    ]
    for repo in filter(None, repos):
        jars = [
            Path(repo) / "io/delta/delta-spark_2.13" / v / f"delta-spark_2.13-{v}.jar",
            Path(repo) / "io/delta/delta-storage" / v / f"delta-storage-{v}.jar",
        ]
        if all(j.is_file() for j in jars):
            return jars
    return []


def pytest_collection_finish(session):
    """Put the Delta jars on PYSPARK_SUBMIT_ARGS when test/zarr tests will run."""
    if "PYSPARK_SUBMIT_ARGS" in os.environ:
        return
    zarr_dir = Path(__file__).resolve().parent / "zarr"
    if not any(zarr_dir in Path(str(item.path)).parents for item in session.items):
        return
    jars = _delta_jars()
    if jars:
        # --driver-class-path, not --jars: --jars sets spark.jars, which the pyrx
        # "no GeoBrix JAR in the session" guardrail (and the pyvx parity JAR check)
        # read. Local mode runs the executors in the driver JVM, so the driver
        # classpath is enough.
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--driver-class-path " + os.pathsep.join(map(str, jars)) + " pyspark-shell"
        )
