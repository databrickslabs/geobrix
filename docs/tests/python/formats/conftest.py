"""Fixtures for the Formats doc tests: a local Delta session and the sample ERA5-like cube.

The Zarr examples keep a store's manifest in a Delta table, so they run on a Spark session with
Delta Lake, not on the shared GeoBrix-JAR session the other doc tests use.

- **The Delta jars** (delta-spark and delta-storage 4.0.x) must be on the JVM classpath before
  the first SparkSession of the run starts, so they are put on ``PYSPARK_SUBMIT_ARGS`` when
  this file is imported at collection. They are looked up in the Maven repository the
  ``geobrix-dev`` container keeps (``scripts/docker/m2``) or ``~/.m2``; nothing is downloaded.
- **The session** is the ``spark`` fixture of this directory. It replaces the shared one here
  and stops any session that is already running (the Delta extension is a static setting), so
  these tests are moved to the end of a run: nothing after them needs the shared session.
- **The cube** is the sample cube from the GeoBrix sample data (``global/era5-weather``). It
  is generated, not tracked, so a run without it writes it into a temporary directory with
  ``write_era5_cube``.
"""

import os
import shlex
import shutil
import uuid
from pathlib import Path

import pytest
from path_config import SAMPLE_DATA_BASE

_DELTA_VERSION = "4.0.1"  # Delta 4.0.x runs on Spark 4.0.x
_DELTA_EXT = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[3]
CUBE_SUBDIR = "global/era5-weather"
CUBE_V3 = "era5_like_v3.zarr"


def _delta_jars():
    """The delta-spark and delta-storage jars from the first Maven repository with both."""
    v = _DELTA_VERSION
    repos = [
        os.environ.get("GBX_M2_REPO"),
        _REPO / "scripts" / "docker" / "m2",
        Path.home() / ".m2" / "repository",
    ]
    for repo in filter(None, repos):
        jars = [
            Path(repo) / "io/delta/delta-spark_2.13" / v / f"delta-spark_2.13-{v}.jar",
            Path(repo) / "io/delta/delta-storage" / v / f"delta-storage-{v}.jar",
        ]
        if all(j.is_file() for j in jars):
            return jars
    return []


def _put_delta_on_the_classpath():
    if "PYSPARK_SUBMIT_ARGS" in os.environ:
        return
    jars = _delta_jars()
    if jars:
        classpath = shlex.quote(os.pathsep.join(map(str, jars)))
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-class-path {classpath} pyspark-shell"
        )


_put_delta_on_the_classpath()


def pytest_collection_modifyitems(config, items):
    """Run these tests last: their session replaces the shared one."""
    ours = [i for i in items if _HERE in Path(str(i.path)).resolve().parents]
    rest = [i for i in items if i not in ours]
    items[:] = rest + ours


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    """A local Spark session with Delta Lake (OSS delta-spark 4.0.x), one for this module."""
    import logging

    from pyspark.sql import SparkSession

    if not _delta_jars() and "PYSPARK_SUBMIT_ARGS" not in os.environ:
        pytest.fail(
            "The Delta jars are not in the Maven repository. Fix: run the doc tests in the "
            "geobrix-dev container (gbx:test:python-docs), or fetch them with "
            "scripts/ci/fetch-delta-test-jars.sh."
        )
    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None and _DELTA_EXT not in (
        active.conf.get("spark.sql.extensions", "") or ""
    ):
        active.stop()
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-formats-doc-tests")
        .config("spark.sql.extensions", _DELTA_EXT)
        .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("delta_wh")))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cube(tmp_path_factory):
    """The sample cube as a Zarr v3 store (sharded): the bundle's copy, else a generated one."""
    bundled = Path(SAMPLE_DATA_BASE) / CUBE_SUBDIR / CUBE_V3
    if bundled.is_dir():
        return str(bundled)
    from databricks.labs.gbx.sample.zarr_era5 import V3_NAME, write_era5_cube

    return str(write_era5_cube(tmp_path_factory.mktemp("era5_cube"))[V3_NAME])


@pytest.fixture
def copy_of_cube(cube, tmp_path):
    """A private copy of the cube, for examples that change a store's files."""
    target = tmp_path / "base.zarr"
    shutil.copytree(cube, target)
    return str(target)


@pytest.fixture
def new_table(spark):
    """Names for manifest tables that are dropped when the test ends."""
    made = []

    def make(label):
        name = f"fmt_{label}_{uuid.uuid4().hex[:8]}"
        made.append(name)
        return name

    yield make
    for name in made:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
