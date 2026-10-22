"""Fixtures for the Zarr suites: a local Delta session and synthetic ERA5-like stores.

``spark_delta`` needs the Delta Spark jars on the JVM classpath *before* the first
SparkSession starts. The root conftest puts them on ``PYSPARK_SUBMIT_ARGS`` when this
directory is collected (see ``pytest_collection_finish`` in ``test/conftest.py``);
``gbx:test:python`` fetches the jars into the Maven repo when they are missing.
"""

import pytest

_DELTA_EXT = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


@pytest.fixture(scope="module")
def spark_delta(tmp_path_factory):
    """A local-mode Spark session with Delta Lake enabled (OSS delta-spark 4.0.x).

    The Delta extension is a static conf, so a live session without it is stopped
    first; a session that already has it is reused. Without the Delta jars on the
    classpath this fails loudly instead of skipping.
    """
    import logging

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None and _DELTA_EXT not in (
        active.conf.get("spark.sql.extensions", "") or ""
    ):
        active.stop()
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("gbx-zarr-tests")
            .config("spark.sql.extensions", _DELTA_EXT)
            .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("delta_wh")))
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as e:  # the JVM could not load the Delta classes
        raise RuntimeError(
            "Could not start a Delta-enabled Spark session; the delta-spark jars are "
            "not on the JVM classpath. Fix: run the suite through "
            "scripts/commands/gbx-test-python.sh (it fetches the jars), or set "
            "PYSPARK_SUBMIT_ARGS to include them before pytest starts."
        ) from e
    yield session
    session.stop()


@pytest.fixture(scope="session")
def era5_v3_sharded(tmp_path_factory):
    """Zarr v3 store with sharding: chunks (12, 37, 72) inside shards (24, 74, 144)."""
    from databricks.labs.gbx.zarr._testing import make_era5_like

    return make_era5_like(
        tmp_path_factory.mktemp("era5") / "era5_v3_sharded.zarr",
        zarr_format=3,
        sharded=True,
    )


@pytest.fixture(scope="session")
def era5_v3_unsharded(tmp_path_factory):
    """Zarr v3 store without sharding: one object per (12, 37, 72) chunk."""
    from databricks.labs.gbx.zarr._testing import make_era5_like

    return make_era5_like(
        tmp_path_factory.mktemp("era5") / "era5_v3_unsharded.zarr",
        zarr_format=3,
        sharded=False,
    )


@pytest.fixture(scope="session")
def era5_v2(tmp_path_factory):
    """Zarr v2 store: blosc-lz4 chunks (12, 37, 72), keys like ``t2m/0.0.0``."""
    from databricks.labs.gbx.zarr._testing import make_era5_like

    return make_era5_like(
        tmp_path_factory.mktemp("era5") / "era5_v2.zarr", zarr_format=2, sharded=False
    )
