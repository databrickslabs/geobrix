"""Fixtures for the Zarr suites: a local Delta session and synthetic ERA5-like stores.

``spark_delta`` needs the Delta Spark jars on the JVM classpath *before* the first
SparkSession starts. The root conftest puts them on ``PYSPARK_SUBMIT_ARGS`` when this
directory is collected (see ``pytest_collection_finish`` in ``test/conftest.py``);
``gbx:test:python`` fetches the jars into the Maven repo when they are missing.

Fast inner loop. Every test that runs on the local Spark/Delta session (it asks for
``spark_delta``, directly or through another fixture) is marked ``delta`` when collected,
so ``-m "not delta"`` runs only the Spark-free tests (721 of 861 today, about 20 s)::

    python -m pytest python/geobrix/test/zarr -m "not delta and not integration"

(run it from the repo root in ``.venv-pyrx``; the ``gbx:test:*`` commands add their own
``-m`` and do not pass one through). The default run, with no ``-m delta`` filter, is
complete: it is the one CI and ``gbx:test:light`` use. The Spark-free tests carry each
correctness matrix (selections, dtypes, CF decoding, layouts, conflict classification);
the ``delta`` tests keep one end-to-end test per behaviour, plus the commit, conflict and
rebase tests, which are real Delta semantics.
"""

from pathlib import Path

import pytest

_DELTA_EXT = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
_HERE = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config, items):
    """Mark every zarr test that needs a Spark session as ``delta``.

    A test needs one when any fixture in its closure has ``spark`` in its name, not only
    ``spark_delta``, so a fixture added later under another name cannot leave a test that
    starts Spark in the fast loop.
    """
    for item in items:
        if _HERE in Path(item.path).resolve().parents and any(
            "spark" in name.lower() for name in item.fixturenames
        ):
            item.add_marker(pytest.mark.delta)


def delta_session(warehouse_dir):
    """A local-mode Spark session with Delta Lake enabled (OSS delta-spark 4.0.x).

    Shared by this directory's ``spark_delta`` fixture and the Delta test in test/ds, so the
    recipe lives once; the caller owns the session and stops it. The engine runs without
    whole-stage codegen and adaptive execution: with a few rows per query, compiling and
    re-planning cost more than they save, and every commit and read in these suites is
    dozens of such queries. Delta uses its local log store (single-JVM tests only) and
    Hadoop its checksum-free local file system, which spawn far fewer child processes;
    none of this changes what a commit, a conflict or a read returns.

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
            .config("spark.sql.warehouse.dir", str(warehouse_dir))
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
            # Hadoop's local file system runs `readlink` and `chmod` as child processes
            # (there is no native library to call): dozens of forks for every Delta commit,
            # a fifth of the driver's time on macOS, and more where a fork is slower. The local
            # log store and the checksum-free local file system need a third of them.
            .config("spark.delta.logStore.class", "io.delta.storage.LocalLogStore")
            .config(
                "spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem"
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.file.impl",
                "org.apache.hadoop.fs.local.RawLocalFs",
            )
            .getOrCreate()
        )
    except Exception as e:  # the JVM could not load the Delta classes
        raise RuntimeError(
            "Could not start a Delta-enabled Spark session; the delta-spark jars are "
            "not on the JVM classpath. Fix: run the suite through "
            "scripts/commands/gbx-test-python.sh (it fetches the jars), or set "
            "PYSPARK_SUBMIT_ARGS to include them before pytest starts."
        ) from e
    return session


@pytest.fixture(scope="package")
def spark_delta(tmp_path_factory):
    """The local Delta session (:func:`delta_session`), one for the whole directory.

    Package scope: a JVM start costs seconds, and every module used to pay it. The tests
    clean up after themselves (tables dropped, the session time zone and current schema
    restored), so they share it safely.
    """
    session = delta_session(tmp_path_factory.mktemp("delta_wh"))
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


@pytest.fixture(scope="session")
def dtype_matrix(tmp_path_factory):
    """A Zarr v3 store with one variable per table column kind, on a (3, 4) grid.

    Dimensions ``lev`` (an int32 coordinate) and ``x`` (no coordinate). Returns
    ``(path, expected)`` where ``expected`` maps each column to its 12 values in C order,
    ``None`` for a masked cell and a float NaN for NaN data.
    """
    import numpy as np
    import zarr

    path = tmp_path_factory.mktemp("matrix") / "m.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    lev = g.create_array(
        "lev", shape=(3,), chunks=(3,), dtype="int32", dimension_names=("lev",)
    )
    lev[:] = [10, 20, 30]
    shape, n = (3, 4), 12
    nan = float("nan")
    expected = {"lev": [10] * 4 + [20] * 4 + [30] * 4, "x": [0, 1, 2, 3] * 3}

    def put(name, values, dtype, attrs=None, shown=None):
        arr = g.create_array(
            name,
            shape=shape,
            chunks=shape,
            dtype=dtype,
            dimension_names=("lev", "x"),
            attributes=attrs or {},
        )
        arr[:] = np.asarray(values, dtype=dtype).reshape(shape)
        expected[name] = list(values) if shown is None else shown

    put("i1", [-5 + i for i in range(n)], "int8")
    put("i2", [-300 + i for i in range(n)], "int16")
    put("i4", [2**31 - 1 - i for i in range(n)], "int32")
    put(
        "i8", [2**53 + 1 + i for i in range(n)], "int64"
    )  # beyond float64's exact range
    put("u1", [250 + i % 6 for i in range(n)], "uint8")
    put("u2", [65535 - i for i in range(n)], "uint16")
    put("u4", [2**32 - 1 - i for i in range(n)], "uint32")
    put("u8", [2**64 - 1 - i for i in range(n)], "uint64")
    u8m = [2**64 - 1 if i in (0, 5) else 2**63 + i for i in range(n)]
    put(
        "u8m",
        u8m,
        "uint64",
        {"_FillValue": 2**64 - 1},  # a fill beyond what int64 or float64 can hold
        [None if v == 2**64 - 1 else v for v in u8m],
    )
    put("b", [bool(i % 3) for i in range(n)], "bool")
    put("f2", [0.5 * i for i in range(n)], "float16")
    put("f4", [0.25 * i for i in range(n)], "float32")
    put("f8", [0.1 * i for i in range(n)], "float64")
    masked = [5, -1, 7, -1, 9, 10, 11, 12, -1, 14, 15, 16]
    put(
        "m2",
        masked,
        "int16",
        {"_FillValue": -1},
        [None if v == -1 else v for v in masked],
    )
    fl = [1.5, nan, -9999.0, 2.5, -9999.0, 3.5, nan, 4.5, 5.5, 6.5, 7.5, 8.5]
    put(
        "mf",
        fl,
        "float64",
        {"_FillValue": -9999.0},
        [None if v == -9999.0 else v for v in fl],
    )
    packed = [2, -1, 4, 6, 8, -1, 12, 14, 16, 18, 20, 22]
    put(
        "pk",
        packed,
        "int16",
        {"_FillValue": -1, "scale_factor": 0.5, "add_offset": 10.0},
        [None if v == -1 else v * 0.5 + 10.0 for v in packed],
    )
    return path, expected
